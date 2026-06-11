use std::sync::Arc;

use prost::Message;
use prost_wkt_types::Duration as ProtoDuration;
use rustler::{Binary, ResourceArc};
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::protos::temporal::api::{
    common::v1::WorkflowExecution,
    enums::v1::HistoryEventFilterType,
    workflowservice::v1::{
        DescribeNamespaceRequest, DescribeWorkflowExecutionRequest,
        GetWorkflowExecutionHistoryRequest, RegisterNamespaceRequest,
        RequestCancelWorkflowExecutionRequest, SignalWorkflowExecutionRequest,
        StartWorkflowExecutionRequest,
    },
};
use temporalio_client::tonic::Request;
use url::Url;

use crate::error::{BridgeError, ElixirBridgeError, OkOrError};
use crate::runtime::CoreRuntimeResource;

pub struct ClientResource {
    pub client: Arc<Client>,
    pub runtime: ResourceArc<CoreRuntimeResource>,
}

// Client contains internal Arc+RwLock state that is not UnwindSafe by default.
// We assert unwind safety here because:
//   (a) ClientResource is only accessed from Rustler dirty-IO threads through the NIF boundary;
//       there are no shared catch_unwind boundaries that could observe partial state.
//   (b) Client's internal state uses Arc + parking_lot::RwLock which are correct under panic.
// SAFETY: see above.
impl std::panic::RefUnwindSafe for ClientResource {}
impl std::panic::UnwindSafe for ClientResource {}

#[rustler::resource_impl]
impl rustler::Resource for ClientResource {}

// Hourglass-defined ClientConfig proto (matches proto/hourglass/client_config.proto)
#[derive(Clone, PartialEq, Message)]
pub struct HourglassClientConfig {
    #[prost(string, tag = "1")]
    pub target_url: String,
    #[prost(string, tag = "2")]
    pub namespace: String,
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn client_new(
    env: rustler::Env,
    runtime: ResourceArc<CoreRuntimeResource>,
    config_bin: Binary,
) -> Result<ResourceArc<ClientResource>, ElixirBridgeError> {
    let cfg = HourglassClientConfig::decode(config_bin.as_slice())
        .map_err(|e| BridgeError::InvalidProto(e.to_string()).to_elixir(env))?;

    let target = Url::parse(&cfg.target_url)
        .map_err(|e| BridgeError::Unknown(e.to_string()).to_elixir(env))?;

    let client = runtime
        .tokio_handle
        .block_on(async {
            let conn_opts = ConnectionOptions::new(target).build();
            let connection = Connection::connect(conn_opts)
                .await
                .map_err(|e| BridgeError::TonicError(e.to_string()))?;
            let client_opts = ClientOptions::new(cfg.namespace.clone()).build();
            Client::new(connection, client_opts).map_err(|e| BridgeError::Unknown(e.to_string()))
        })
        .map_err(|e| e.to_elixir(env))?;

    Ok(ResourceArc::new(ClientResource {
        client: Arc::new(client),
        runtime,
    }))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn client_describe_namespace<'a>(
    env: rustler::Env<'a>,
    client: ResourceArc<ClientResource>,
    namespace: String,
) -> Result<Binary<'a>, ElixirBridgeError> {
    let req = DescribeNamespaceRequest { namespace, ..Default::default() };

    let resp = client
        .runtime
        .tokio_handle
        .block_on(async {
            let mut wf_svc = client.client.connection().workflow_service();
            wf_svc
                .describe_namespace(Request::new(req))
                .await
                .map_err(|e| BridgeError::TonicError(e.to_string()))
        })
        .map_err(|e| e.to_elixir(env))?;

    crate::payload::encode_to_binary(env, resp.into_inner().encode_to_vec())
        .map_err(|e| BridgeError::Unknown(e).to_elixir(env))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn client_register_namespace(
    env: rustler::Env,
    client: ResourceArc<ClientResource>,
    namespace: String,
) -> OkOrError {
    // 1-day retention period is required by the Temporal server.
    let retention = ProtoDuration { seconds: 86_400, nanos: 0 };
    let req = RegisterNamespaceRequest {
        namespace,
        workflow_execution_retention_period: Some(retention),
        ..Default::default()
    };

    match client.runtime.tokio_handle.block_on(async {
        let mut wf_svc = client.client.connection().workflow_service();
        wf_svc
            .register_namespace(Request::new(req))
            .await
            .map_err(|e| BridgeError::TonicError(e.to_string()))
    }) {
        Ok(_) => OkOrError::Ok,
        Err(e) => OkOrError::Err(e.to_elixir(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn client_start_workflow<'a>(
    env: rustler::Env<'a>,
    client: ResourceArc<ClientResource>,
    request_bin: Binary,
) -> Result<Binary<'a>, ElixirBridgeError> {
    let req = StartWorkflowExecutionRequest::decode(request_bin.as_slice())
        .map_err(|e| BridgeError::InvalidProto(e.to_string()).to_elixir(env))?;

    let resp = client
        .runtime
        .tokio_handle
        .block_on(async {
            let mut wf_svc = client.client.connection().workflow_service();
            wf_svc
                .start_workflow_execution(Request::new(req))
                .await
                .map_err(|e| BridgeError::TonicError(e.to_string()))
        })
        .map_err(|e| e.to_elixir(env))?;

    crate::payload::encode_to_binary(env, resp.into_inner().encode_to_vec())
        .map_err(|e| BridgeError::Unknown(e).to_elixir(env))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn client_fetch_history<'a>(
    env: rustler::Env<'a>,
    client: ResourceArc<ClientResource>,
    workflow_id: String,
) -> Result<Binary<'a>, ElixirBridgeError> {
    let namespace = client.client.options().namespace.clone();
    let req = GetWorkflowExecutionHistoryRequest {
        namespace,
        execution: Some(WorkflowExecution {
            workflow_id,
            run_id: String::new(),
        }),
        ..Default::default()
    };

    let resp = client
        .runtime
        .tokio_handle
        .block_on(async {
            let mut wf_svc = client.client.connection().workflow_service();
            wf_svc
                .get_workflow_execution_history(Request::new(req))
                .await
                .map_err(|e| BridgeError::TonicError(e.to_string()))
        })
        .map_err(|e| e.to_elixir(env))?;

    crate::payload::encode_to_binary(env, resp.into_inner().encode_to_vec())
        .map_err(|e| BridgeError::Unknown(e).to_elixir(env))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn client_describe_workflow_execution<'a>(
    env: rustler::Env<'a>,
    client: ResourceArc<ClientResource>,
    workflow_id: String,
    run_id: String,
) -> Result<Binary<'a>, ElixirBridgeError> {
    let namespace = client.client.options().namespace.clone();
    let req = DescribeWorkflowExecutionRequest {
        namespace,
        execution: Some(WorkflowExecution {
            workflow_id,
            run_id,
        }),
    };

    let resp = client
        .runtime
        .tokio_handle
        .block_on(async {
            let mut wf_svc = client.client.connection().workflow_service();
            wf_svc
                .describe_workflow_execution(Request::new(req))
                .await
                .map_err(|e| BridgeError::TonicError(e.to_string()))
        })
        .map_err(|e| e.to_elixir(env))?;

    crate::payload::encode_to_binary(env, resp.into_inner().encode_to_vec())
        .map_err(|e| BridgeError::Unknown(e).to_elixir(env))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn client_await_workflow<'a>(
    env: rustler::Env<'a>,
    client: ResourceArc<ClientResource>,
    request_bin: Binary,
    _timeout_ms: u64,
) -> Result<Binary<'a>, ElixirBridgeError> {
    let req = GetWorkflowExecutionHistoryRequest::decode(request_bin.as_slice())
        .map_err(|e| BridgeError::InvalidProto(e.to_string()).to_elixir(env))?;

    // Ensure close-event filter and wait_new_event are set.
    let req = GetWorkflowExecutionHistoryRequest {
        wait_new_event: true,
        history_event_filter_type: HistoryEventFilterType::CloseEvent as i32,
        ..req
    };

    let resp = client
        .runtime
        .tokio_handle
        .block_on(async {
            let mut wf_svc = client.client.connection().workflow_service();
            wf_svc
                .get_workflow_execution_history(Request::new(req))
                .await
                .map_err(|e| BridgeError::TonicError(e.to_string()))
        })
        .map_err(|e| e.to_elixir(env))?;

    crate::payload::encode_to_binary(env, resp.into_inner().encode_to_vec())
        .map_err(|e| BridgeError::Unknown(e).to_elixir(env))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn client_signal_workflow(
    env: rustler::Env,
    client: ResourceArc<ClientResource>,
    request_bin: Binary,
) -> OkOrError {
    let req = match SignalWorkflowExecutionRequest::decode(request_bin.as_slice()) {
        Ok(r) => r,
        Err(e) => return OkOrError::Err(BridgeError::InvalidProto(e.to_string()).to_elixir(env)),
    };

    match client.runtime.tokio_handle.block_on(async {
        let mut wf_svc = client.client.connection().workflow_service();
        wf_svc
            .signal_workflow_execution(Request::new(req))
            .await
            .map_err(|e| BridgeError::TonicError(e.to_string()))
    }) {
        Ok(_) => OkOrError::Ok,
        Err(e) => OkOrError::Err(e.to_elixir(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn client_cancel_workflow(
    env: rustler::Env,
    client: ResourceArc<ClientResource>,
    request_bin: Binary,
) -> OkOrError {
    let req = match RequestCancelWorkflowExecutionRequest::decode(request_bin.as_slice()) {
        Ok(r) => r,
        Err(e) => return OkOrError::Err(BridgeError::InvalidProto(e.to_string()).to_elixir(env)),
    };

    match client.runtime.tokio_handle.block_on(async {
        let mut wf_svc = client.client.connection().workflow_service();
        wf_svc
            .request_cancel_workflow_execution(Request::new(req))
            .await
            .map_err(|e| BridgeError::TonicError(e.to_string()))
    }) {
        Ok(_) => OkOrError::Ok,
        Err(e) => OkOrError::Err(e.to_elixir(env)),
    }
}

