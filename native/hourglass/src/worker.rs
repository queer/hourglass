use std::sync::Arc;

use prost::Message;
use rustler::{Binary, ResourceArc};
use temporalio_client::{Connection, ConnectionOptions};
use temporalio_sdk_core::{
    Worker, WorkerConfig, WorkerVersioningStrategy, init_worker,
};
use temporalio_common::worker::WorkerTaskTypes;
use url::Url;

use crate::error::{BridgeError, ElixirBridgeError, OkOrError};
use crate::runtime::CoreRuntimeResource;

pub struct WorkerResource {
    pub worker: Arc<Worker>,
    pub runtime: ResourceArc<CoreRuntimeResource>,
}

// Worker contains internal mutexes and tokio handles that are not UnwindSafe by default.
// We assert unwind safety here because:
//   (a) WorkerResource is only accessed from Rustler dirty-IO threads through the NIF boundary;
//       there are no shared catch_unwind boundaries that could observe partial state.
//   (b) Worker's internal state uses poisoning mutexes and tokio channels which are correct
//       under panic conditions.
// SAFETY: see above.
impl std::panic::RefUnwindSafe for WorkerResource {}
impl std::panic::UnwindSafe for WorkerResource {}

#[rustler::resource_impl]
impl rustler::Resource for WorkerResource {}

// Hourglass-defined WorkerConfig proto (matches proto/hourglass/worker_config.proto)
#[derive(Clone, PartialEq, Message)]
pub struct HourglassWorkerConfig {
    #[prost(string, tag = "1")]
    pub namespace: String,
    #[prost(string, tag = "2")]
    pub task_queue: String,
    #[prost(uint32, tag = "3")]
    pub max_cached_workflows: u32,
    #[prost(string, tag = "4")]
    pub client_target_url: String,
    // 0 = use default (DEFAULT_OUTSTANDING below). See #298.
    #[prost(uint32, tag = "5")]
    pub max_outstanding_workflow_tasks: u32,
    #[prost(uint32, tag = "6")]
    pub max_outstanding_activities: u32,
    #[prost(uint32, tag = "7")]
    pub max_outstanding_local_activities: u32,
}

// Historical hardcoded value used when the Elixir caller leaves the
// new uint32 fields unset (i.e. zero on the wire).
const DEFAULT_OUTSTANDING: usize = 10;

#[rustler::nif(schedule = "DirtyIo")]
pub fn worker_new(
    env: rustler::Env,
    runtime: ResourceArc<CoreRuntimeResource>,
    config_bin: Binary,
) -> Result<ResourceArc<WorkerResource>, ElixirBridgeError> {
    let cfg = HourglassWorkerConfig::decode(config_bin.as_slice())
        .map_err(|e| BridgeError::InvalidProto(e.to_string()).to_elixir(env))?;

    let target = Url::parse(&cfg.client_target_url)
        .map_err(|e| BridgeError::Unknown(e.to_string()).to_elixir(env))?;

    let conn_opts = ConnectionOptions::new(target)
        .identity(format!("hourglass-worker@{}", cfg.task_queue))
        .build();

    let max_wf = if cfg.max_outstanding_workflow_tasks == 0 {
        DEFAULT_OUTSTANDING
    } else {
        cfg.max_outstanding_workflow_tasks as usize
    };

    let max_act = if cfg.max_outstanding_activities == 0 {
        DEFAULT_OUTSTANDING
    } else {
        cfg.max_outstanding_activities as usize
    };

    let max_local_act = if cfg.max_outstanding_local_activities == 0 {
        DEFAULT_OUTSTANDING
    } else {
        cfg.max_outstanding_local_activities as usize
    };

    let worker_cfg = WorkerConfig::builder()
        .namespace(cfg.namespace)
        .task_queue(cfg.task_queue)
        .max_cached_workflows(cfg.max_cached_workflows as usize)
        .max_outstanding_workflow_tasks(max_wf)
        .max_outstanding_activities(max_act)
        .max_outstanding_local_activities(max_local_act)
        .task_types(WorkerTaskTypes::all())
        .versioning_strategy(WorkerVersioningStrategy::default())
        .build()
        .map_err(|e| BridgeError::Unknown(e).to_elixir(env))?;

    // init_worker is sync but internally spawns tokio tasks; it must run inside the runtime
    // context. We wrap both the async connect and the sync init_worker in a single block_on
    // so that tokio::spawn calls inside Worker::new can reach the runtime.
    let worker = runtime
        .tokio_handle
        .block_on(async {
            let connection = Connection::connect(conn_opts)
                .await
                .map_err(|e| BridgeError::TonicError(e.to_string()))?;
            init_worker(&runtime.runtime, worker_cfg, connection)
                .map_err(|e| BridgeError::Unknown(e.to_string()))
        })
        .map_err(|e| e.to_elixir(env))?;

    Ok(ResourceArc::new(WorkerResource {
        worker: Arc::new(worker),
        runtime,
    }))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn worker_poll_workflow_activation(
    env: rustler::Env<'_>,
    worker: ResourceArc<WorkerResource>,
) -> Result<Binary<'_>, ElixirBridgeError> {
    let activation = worker
        .runtime
        .tokio_handle
        .block_on(worker.worker.poll_workflow_activation())
        .map_err(|e| map_poll_err(e).to_elixir(env))?;

    let bytes = activation.encode_to_vec();
    crate::payload::encode_to_binary(env, bytes).map_err(|e| BridgeError::Unknown(e).to_elixir(env))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn worker_complete_workflow_activation(
    env: rustler::Env,
    worker: ResourceArc<WorkerResource>,
    completion_bin: Binary,
) -> OkOrError {
    use temporalio_common::protos::coresdk::workflow_completion::WorkflowActivationCompletion;

    let completion = match WorkflowActivationCompletion::decode(completion_bin.as_slice()) {
        Ok(c) => c,
        Err(e) => return OkOrError::Err(BridgeError::InvalidProto(e.to_string()).to_elixir(env)),
    };

    match worker
        .runtime
        .tokio_handle
        .block_on(worker.worker.complete_workflow_activation(completion))
    {
        Ok(()) => OkOrError::Ok,
        Err(e) => OkOrError::Err(BridgeError::Unknown(e.to_string()).to_elixir(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn worker_poll_activity_task(
    env: rustler::Env<'_>,
    worker: ResourceArc<WorkerResource>,
) -> Result<Binary<'_>, ElixirBridgeError> {
    let task = worker
        .runtime
        .tokio_handle
        .block_on(worker.worker.poll_activity_task())
        .map_err(|e| map_poll_err(e).to_elixir(env))?;

    let bytes = task.encode_to_vec();
    crate::payload::encode_to_binary(env, bytes).map_err(|e| BridgeError::Unknown(e).to_elixir(env))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn worker_complete_activity_task(
    env: rustler::Env,
    worker: ResourceArc<WorkerResource>,
    completion_bin: Binary,
) -> OkOrError {
    use temporalio_common::protos::coresdk::ActivityTaskCompletion;

    let completion = match ActivityTaskCompletion::decode(completion_bin.as_slice()) {
        Ok(c) => c,
        Err(e) => return OkOrError::Err(BridgeError::InvalidProto(e.to_string()).to_elixir(env)),
    };

    match worker
        .runtime
        .tokio_handle
        .block_on(worker.worker.complete_activity_task(completion))
    {
        Ok(()) => OkOrError::Ok,
        Err(e) => OkOrError::Err(BridgeError::Unknown(e.to_string()).to_elixir(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn worker_record_activity_heartbeat(
    env: rustler::Env,
    worker: ResourceArc<WorkerResource>,
    heartbeat_bin: Binary,
) -> OkOrError {
    use temporalio_common::protos::coresdk::ActivityHeartbeat;

    let hb = match ActivityHeartbeat::decode(heartbeat_bin.as_slice()) {
        Ok(h) => h,
        Err(e) => return OkOrError::Err(BridgeError::InvalidProto(e.to_string()).to_elixir(env)),
    };

    // record_activity_heartbeat is sync fire-and-forget; Core's heartbeat manager buffers and
    // throttles delivery to the server. It may touch tokio internals, so run it inside the
    // runtime context like worker_shutdown does.
    worker.runtime.tokio_handle.block_on(async {
        worker.worker.record_activity_heartbeat(hb);
    });
    let _ = env;
    OkOrError::Ok
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn worker_shutdown(
    env: rustler::Env,
    worker: ResourceArc<WorkerResource>,
) -> OkOrError {
    // initiate_shutdown signals shutdown (cancels the CancellationToken) and returns
    // immediately. Subsequent polls will return PollError::ShutDown. The full async
    // `shutdown()` waits for pollers to drain and requires the lang layer to call poll
    // until ShutDown — that is the workflow execution loop, not a lifecycle NIF.
    //
    // Both initiate_shutdown and shutdown call tokio::spawn internally, so they must run
    // within the tokio runtime context. We use block_on with a trivial async block so the
    // handle enters the runtime for the duration of initiate_shutdown.
    worker.runtime.tokio_handle.block_on(async {
        worker.worker.initiate_shutdown();
    });
    let _ = env;
    OkOrError::Ok
}

// Map poll errors: ShutDown → BridgeError::Shutdown; everything else → TonicError
fn map_poll_err(e: temporalio_sdk_core::PollError) -> BridgeError {
    match e {
        temporalio_sdk_core::PollError::ShutDown => BridgeError::Shutdown,
        temporalio_sdk_core::PollError::TonicError(s) => BridgeError::TonicError(s.to_string()),
    }
}

