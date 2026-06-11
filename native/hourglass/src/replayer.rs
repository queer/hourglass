use std::sync::Arc;

use prost::Message;
use rustler::{Binary, ResourceArc};
use temporalio_common::protos::temporal::api::history::v1::History;
use temporalio_sdk_core::{
    Worker, WorkerConfig, WorkerVersioningStrategy,
    init_replay_worker,
    replay::{HistoryFeeder, HistoryForReplay, ReplayWorkerInput},
};
use temporalio_common::worker::WorkerTaskTypes;
use tokio::sync::Mutex;

use crate::error::{BridgeError, ElixirBridgeError, OkOrError};
use crate::runtime::CoreRuntimeResource;

/// A live replayer: wraps the Core replay `Worker` and keeps the `HistoryFeeder`
/// so callers can push additional histories after construction.
pub struct ReplayerResource {
    /// Held for its Drop side-effect: auto-shuts-down the replay worker when
    /// the resource is garbage-collected by the BEAM.
    #[allow(dead_code)]
    pub worker: Arc<Worker>,
    pub feeder: Mutex<Option<HistoryFeeder>>,
    pub runtime: ResourceArc<CoreRuntimeResource>,
}

// Same reasoning as WorkerResource: NIFs are the only path to this resource;
// Mutex / CancellationToken internals are panic-safe via poisoning.
// SAFETY: see above.
impl std::panic::RefUnwindSafe for ReplayerResource {}
impl std::panic::UnwindSafe for ReplayerResource {}

#[rustler::resource_impl]
impl rustler::Resource for ReplayerResource {}

/// Hourglass-defined ReplayerConfig proto (matches proto/hourglass/replayer_config.proto).
#[derive(Clone, PartialEq, Message)]
pub struct HourglassReplayerConfig {
    #[prost(string, tag = "1")]
    pub namespace: String,
    #[prost(string, tag = "2")]
    pub task_queue: String,
}

/// Create a new replay worker.
///
/// Returns a `ReplayerResource` reference. The feeder channel is open; push
/// histories with `replayer_push_history/3`. Dropping the resource closes the
/// channel and signals end-of-stream to Core.
#[rustler::nif(schedule = "DirtyIo")]
pub fn replayer_new(
    env: rustler::Env,
    runtime: ResourceArc<CoreRuntimeResource>,
    config_bin: Binary,
) -> Result<ResourceArc<ReplayerResource>, ElixirBridgeError> {
    let cfg = HourglassReplayerConfig::decode(config_bin.as_slice())
        .map_err(|e| BridgeError::InvalidProto(e.to_string()).to_elixir(env))?;

    let worker_cfg = build_replayer_worker_config(&cfg.namespace, &cfg.task_queue)
        .map_err(|e| BridgeError::Unknown(e).to_elixir(env))?;

    // HistoryFeeder is the SDK-native channel-as-stream; no tokio-stream dep needed.
    let (feeder, feeder_stream) = HistoryFeeder::new(100);

    let rwi = ReplayWorkerInput::new(worker_cfg, feeder_stream);

    let worker = runtime
        .tokio_handle
        .block_on(async { init_replay_worker(rwi) })
        .map_err(|e| BridgeError::Unknown(e.to_string()).to_elixir(env))?;

    Ok(ResourceArc::new(ReplayerResource {
        worker: Arc::new(worker),
        feeder: Mutex::new(Some(feeder)),
        runtime,
    }))
}

/// Push a single history into the replay worker's stream.
///
/// `history_bin` is a proto-encoded `temporal.api.history.v1.History`.
/// Returns `:ok` on success or `{:error, %Bridge.Error{}}` if the worker has
/// already shut down or the proto cannot be decoded.
#[rustler::nif(schedule = "DirtyIo")]
pub fn replayer_push_history(
    env: rustler::Env,
    replayer: ResourceArc<ReplayerResource>,
    workflow_id: String,
    history_bin: Binary,
) -> OkOrError {
    let history = match History::decode(history_bin.as_slice()) {
        Ok(h) => h,
        Err(e) => return OkOrError::Err(BridgeError::InvalidProto(e.to_string()).to_elixir(env)),
    };

    let item = HistoryForReplay::new(history, workflow_id);

    let feeder_guard = replayer.feeder.blocking_lock();
    let Some(feeder) = feeder_guard.as_ref() else {
        return OkOrError::Err(BridgeError::Shutdown.to_elixir(env));
    };

    match replayer.runtime.tokio_handle.block_on(feeder.feed(item)) {
        Ok(()) => OkOrError::Ok,
        Err(_) => OkOrError::Err(BridgeError::Shutdown.to_elixir(env)),
    }
}

/// Close the feeder channel, signalling to the replay worker that no more histories
/// will arrive. This causes the replay worker to shut down after processing the
/// last history, so `replayer_poll_workflow_activation` returns `{:error, :shutdown}`.
///
/// Must be called after all `replayer_push_history` calls for a replay session.
#[rustler::nif(schedule = "DirtyIo")]
pub fn replayer_close_feeder(env: rustler::Env, replayer: ResourceArc<ReplayerResource>) -> OkOrError {
    let mut feeder_guard = replayer.feeder.blocking_lock();
    // Take the feeder out of the Option, dropping it — this closes the mpsc channel,
    // which ends the stream and triggers Core's shutdown path.
    *feeder_guard = None;
    let _ = env;
    OkOrError::Ok
}

fn build_replayer_worker_config(
    namespace: &str,
    task_queue: &str,
) -> Result<WorkerConfig, String> {
    WorkerConfig::builder()
        .namespace(namespace)
        .task_queue(task_queue)
        .max_cached_workflows(100usize)
        .max_outstanding_workflow_tasks(10usize)
        .max_outstanding_activities(10usize)
        .max_outstanding_local_activities(10usize)
        .task_types(WorkerTaskTypes::all())
        .versioning_strategy(WorkerVersioningStrategy::default())
        .build()
        .map_err(|e| e.to_string())
}

/// Poll for the next workflow activation from the replay worker.
///
/// Returns `{:ok, binary()}` with an encoded `WorkflowActivation` proto, or
/// `{:error, %Bridge.Error{kind: :shutdown}}` when the history stream exhausts.
#[rustler::nif(schedule = "DirtyIo")]
pub fn replayer_poll_workflow_activation<'a>(
    env: rustler::Env<'a>,
    replayer: ResourceArc<ReplayerResource>,
) -> Result<Binary<'a>, crate::error::ElixirBridgeError> {
    let activation = replayer
        .runtime
        .tokio_handle
        .block_on(replayer.worker.poll_workflow_activation())
        .map_err(|e| map_poll_err(e).to_elixir(env))?;

    let bytes = activation.encode_to_vec();
    crate::payload::encode_to_binary(env, bytes)
        .map_err(|e| crate::error::BridgeError::Unknown(e).to_elixir(env))
}

/// Complete a workflow activation on the replay worker.
///
/// `completion_bin` is a proto-encoded `WorkflowActivationCompletion`.
/// Returns `:ok` on success, `{:error, %Bridge.Error{kind: :nondeterminism, ...}}`
/// if the replay worker detects a command-stream divergence.
#[rustler::nif(schedule = "DirtyIo")]
pub fn replayer_complete_workflow_activation(
    env: rustler::Env,
    replayer: ResourceArc<ReplayerResource>,
    completion_bin: Binary,
) -> OkOrError {
    use temporalio_common::protos::coresdk::workflow_completion::WorkflowActivationCompletion;

    let completion =
        match WorkflowActivationCompletion::decode(completion_bin.as_slice()) {
            Ok(c) => c,
            Err(e) => {
                return OkOrError::Err(
                    BridgeError::InvalidProto(e.to_string()).to_elixir(env),
                )
            }
        };

    match replayer
        .runtime
        .tokio_handle
        .block_on(replayer.worker.complete_workflow_activation(completion))
    {
        Ok(()) => OkOrError::Ok,
        Err(e) => {
            let msg = e.to_string();
            let err = if msg.to_lowercase().contains("nondeterminism")
                || msg.to_lowercase().contains("non-determinism")
                || msg.to_lowercase().contains("nondeterministic")
            {
                BridgeError::Nondeterminism(msg)
            } else {
                BridgeError::TonicError(msg)
            };
            OkOrError::Err(err.to_elixir(env))
        }
    }
}

fn map_poll_err(e: temporalio_sdk_core::PollError) -> BridgeError {
    match e {
        temporalio_sdk_core::PollError::ShutDown => BridgeError::Shutdown,
        temporalio_sdk_core::PollError::TonicError(s) => BridgeError::TonicError(s.to_string()),
    }
}
