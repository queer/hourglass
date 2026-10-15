use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message;
use prost_wkt_types::Duration as ProtoDuration;
use rustler::{Binary, ResourceArc};
use temporalio_client::tonic::{Code, Request};
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::protos::temporal::api::{
    common::v1::WorkflowExecution,
    enums::v1::HistoryEventFilterType,
    namespace::v1::UpdateNamespaceInfo,
    operatorservice::v1::DeleteNamespaceRequest,
    workflowservice::v1::{
        DescribeNamespaceRequest, DescribeWorkflowExecutionRequest,
        GetWorkflowExecutionHistoryRequest, ListNamespacesRequest, ListWorkflowExecutionsRequest,
        RegisterNamespaceRequest, RequestCancelWorkflowExecutionRequest,
        SignalWorkflowExecutionRequest, StartWorkflowExecutionRequest, UpdateNamespaceRequest,
    },
};
use tokio::sync::OnceCell;
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

// A runtime's clients, one per configuration: target URL and namespace. Each
// is connected once (one connection, one GetSystemInfo) by the first call that
// needs it, which concurrent calls wait for, and shared by every call after.
// A failed connect leaves nothing behind, so the next call connects anew. A
// connection lost later is made again by the client's channel on the next
// call, which alone may fail for it.
#[derive(Default)]
pub struct Clients(Mutex<HashMap<ClientConfig, Arc<OnceCell<Arc<Client>>>>>);

// A client's configuration: its target URL and namespace.
type ClientConfig = (String, String);

pub async fn shared_client(
    runtime: &CoreRuntimeResource,
    target: Url,
    namespace: String,
) -> Result<Arc<Client>, BridgeError> {
    let cell = {
        let mut clients = runtime.clients.0.lock().unwrap_or_else(|e| e.into_inner());
        clients
            .entry((target.to_string(), namespace.clone()))
            .or_default()
            .clone()
    };

    cell.get_or_try_init(|| async {
        let connection = Connection::connect(ConnectionOptions::new(target).build())
            .await
            .map_err(BridgeError::from_connect)?;
        let client = Client::new(connection, ClientOptions::new(namespace).build())
            .map_err(|e| BridgeError::Unknown(e.to_string()))?;
        Ok(Arc::new(client))
    })
    .await
    .cloned()
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
        .block_on(shared_client(&runtime, target, cfg.namespace))
        .map_err(|e| e.to_elixir(env))?;

    Ok(ResourceArc::new(ClientResource { client, runtime }))
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn client_describe_namespace<'a>(
    env: rustler::Env<'a>,
    client: ResourceArc<ClientResource>,
    namespace: String,
) -> Result<Binary<'a>, ElixirBridgeError> {
    let req = DescribeNamespaceRequest {
        namespace,
        ..Default::default()
    };

    let resp = client
        .runtime
        .tokio_handle
        .block_on(async {
            let mut wf_svc = client.client.connection().workflow_service();
            wf_svc
                .describe_namespace(Request::new(req))
                .await
                .map_err(BridgeError::from_namespace_call)
        })
        .map_err(|e| e.to_elixir(env))?;

    crate::payload::encode_to_binary(env, resp.into_inner().encode_to_vec())
        .map_err(|e| BridgeError::Unknown(e).to_elixir(env))
}

/// Delete `namespace`, with no delay before Temporal reclaims it. Temporal
/// renames it at once and removes it, and everything in it, in the background.
/// Returns the name it holds the namespace under meanwhile.
#[rustler::nif(schedule = "DirtyIo")]
pub fn client_delete_namespace(
    env: rustler::Env,
    client: ResourceArc<ClientResource>,
    namespace: String,
) -> Result<String, ElixirBridgeError> {
    let req = DeleteNamespaceRequest {
        namespace,
        namespace_delete_delay: Some(ProtoDuration {
            seconds: 0,
            nanos: 0,
        }),
        ..Default::default()
    };

    match client.runtime.tokio_handle.block_on(async {
        let mut op_svc = client.client.connection().operator_service();
        op_svc
            .delete_namespace(Request::new(req))
            .await
            .map_err(BridgeError::from_namespace_call)
    }) {
        Ok(resp) => Ok(resp.into_inner().deleted_namespace),
        Err(e) => Err(e.to_elixir(env)),
    }
}

// One page of the namespaces Temporal knows, up to `page_size` (0: the
// server's default), from `next_page_token` (empty: the first page). Returns
// the encoded ListNamespacesResponse, whose next_page_token is empty on the
// last page.
#[rustler::nif(schedule = "DirtyIo")]
pub fn client_list_namespaces<'a>(
    env: rustler::Env<'a>,
    client: ResourceArc<ClientResource>,
    page_size: i32,
    next_page_token: Binary,
) -> Result<Binary<'a>, ElixirBridgeError> {
    let req = ListNamespacesRequest {
        page_size,
        next_page_token: next_page_token.as_slice().to_vec(),
        ..Default::default()
    };

    let resp = client
        .runtime
        .tokio_handle
        .block_on(async {
            let mut wf_svc = client.client.connection().workflow_service();
            wf_svc
                .list_namespaces(Request::new(req))
                .await
                .map_err(BridgeError::from_namespace_call)
        })
        .map_err(|e| e.to_elixir(env))?;

    crate::payload::encode_to_binary(env, resp.into_inner().encode_to_vec())
        .map_err(|e| BridgeError::Unknown(e).to_elixir(env))
}

// Merge `data` into `namespace`'s data: the keys given are set, the others
// left as they are.
#[rustler::nif(schedule = "DirtyIo")]
pub fn client_update_namespace_data(
    env: rustler::Env,
    client: ResourceArc<ClientResource>,
    namespace: String,
    data: HashMap<String, String>,
) -> OkOrError {
    let req = UpdateNamespaceRequest {
        namespace,
        update_info: Some(UpdateNamespaceInfo {
            data,
            ..Default::default()
        }),
        ..Default::default()
    };

    match client.runtime.tokio_handle.block_on(async {
        let mut wf_svc = client.client.connection().workflow_service();
        wf_svc
            .update_namespace(Request::new(req))
            .await
            .map_err(BridgeError::from_namespace_call)
    }) {
        Ok(_) => OkOrError::Ok,
        Err(e) => OkOrError::Err(e.to_elixir(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
pub fn client_register_namespace(
    env: rustler::Env,
    client: ResourceArc<ClientResource>,
    namespace: String,
    data: HashMap<String, String>,
) -> OkOrError {
    // 1-day retention period is required by the Temporal server.
    let retention = ProtoDuration {
        seconds: 86_400,
        nanos: 0,
    };
    let req = RegisterNamespaceRequest {
        namespace: namespace.clone(),
        workflow_execution_retention_period: Some(retention),
        data,
        ..Default::default()
    };

    match client.runtime.tokio_handle.block_on(async {
        let mut wf_svc = client.client.connection().workflow_service();
        wf_svc
            .register_namespace(Request::new(req))
            .await
            .map_err(BridgeError::from_namespace_call)?;
        served(&client, namespace).await
    }) {
        Ok(()) => OkOrError::Ok,
        Err(e) => OkOrError::Err(e.to_elixir(env)),
    }
}

// How long Temporal may take to serve a namespace it has registered.
const SERVED_WITHIN: Duration = Duration::from_secs(60);

// Wait until Temporal serves `namespace`, just registered. For some seconds
// after a registration, Temporal's frontend describes the namespace, but
// answers every other call naming it (a workflow start, a worker's poll) as
// if it did not exist; a call listing its workflows tells.
async fn served(client: &ClientResource, namespace: String) -> Result<(), BridgeError> {
    let deadline = tokio::time::Instant::now() + SERVED_WITHIN;

    loop {
        let req = ListWorkflowExecutionsRequest {
            namespace: namespace.clone(),
            page_size: 1,
            ..Default::default()
        };
        let mut wf_svc = client.client.connection().workflow_service();

        match wf_svc.list_workflow_executions(Request::new(req)).await {
            Ok(_) => return Ok(()),
            Err(status) if status.code() == Code::NotFound => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(BridgeError::Unreachable(format!(
                        "namespace {namespace} registered, but not served after {} s",
                        SERVED_WITHIN.as_secs()
                    )));
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(status) => return Err(BridgeError::from_namespace_call(status)),
        }
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
