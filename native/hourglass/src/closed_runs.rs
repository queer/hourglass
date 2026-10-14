// Asking Temporal which workflow runs have closed, for a Worker's
// closed-workflow watch (Hourglass.Worker.ClosedWorkflowWatch). Neither NIF
// here waits on the network: each starts its work on the Core runtime and
// returns at once, and the outcome is sent to the calling process as a
// message. So a Temporal that is slow, or has gone quiet, holds no BEAM
// scheduler thread (dirty or not) however long it takes, and nothing of a
// check is left on the BEAM once the watch that asked has gone.

use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use rustler::{Atom, Binary, Encoder, Env, OwnedEnv, ResourceArc, Term};
use temporalio_client::tonic::Request;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::protos::temporal::api::{
    common::v1::WorkflowExecution, workflowservice::v1::DescribeWorkflowExecutionRequest,
};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use url::Url;

use crate::client::{ClientResource, HourglassClientConfig};
use crate::error::{BridgeError, ElixirBridgeError, OkOrError};
use crate::runtime::CoreRuntimeResource;

mod atoms {
    rustler::atoms! {
        ok,
        hourglass_client_opened,
        hourglass_runs_described,
    }
}

// How many describes one check has in flight at once.
const CONCURRENCY: usize = 16;

// Open a client of the config's namespace at its target. Sends the caller
// `{:hourglass_client_opened, tag, {:ok, client} | {:error, %Bridge.Error{}}}`,
// within `timeout_ms`.
#[rustler::nif]
pub fn closed_runs_open_client(
    env: Env,
    runtime: ResourceArc<CoreRuntimeResource>,
    config_bin: Binary,
    timeout_ms: u64,
    tag: u64,
) -> OkOrError {
    let cfg = match HourglassClientConfig::decode(config_bin.as_slice()) {
        Ok(cfg) => cfg,
        Err(e) => return OkOrError::Err(BridgeError::InvalidProto(e.to_string()).to_elixir(env)),
    };
    let target = match Url::parse(&cfg.target_url) {
        Ok(target) => target,
        Err(e) => return OkOrError::Err(BridgeError::Unknown(e.to_string()).to_elixir(env)),
    };
    let caller = env.pid();
    let owner = runtime.clone();

    runtime.tokio_handle.spawn(async move {
        let connecting = async {
            let connection = Connection::connect(ConnectionOptions::new(target).build())
                .await
                .map_err(|e| BridgeError::TonicError(e.to_string()))?;
            Client::new(connection, ClientOptions::new(cfg.namespace).build())
                .map_err(|e| BridgeError::Unknown(e.to_string()))
        };

        let opened = match tokio::time::timeout(Duration::from_millis(timeout_ms), connecting).await
        {
            Ok(opened) => opened,
            Err(_elapsed) => Err(timed_out("connect", timeout_ms)),
        };

        let _ = OwnedEnv::new().send_and_clear(&caller, |env| {
            let opened = match opened {
                Ok(client) => Ok(ResourceArc::new(ClientResource {
                    client: Arc::new(client),
                    runtime: owner,
                })),
                Err(e) => Err(e.to_elixir(env)),
            };
            (atoms::hourglass_client_opened(), tag, opened)
        });
    });

    OkOrError::Ok
}

// Describe each of `runs` (workflow id, run id), up to CONCURRENCY at once,
// each given up on after `timeout_ms`. Sends the caller
// `{:hourglass_runs_described, tag, [{workflow_id, run_id, answer}]}`, where
// an answer is `{:ok, describe_response_bytes}` or `{:error, %Bridge.Error{}}`.
// A run whose describe panicked is left out.
#[rustler::nif]
pub fn closed_runs_describe(
    env: Env,
    client: ResourceArc<ClientResource>,
    runs: Vec<(String, String)>,
    timeout_ms: u64,
    tag: u64,
) -> Atom {
    let caller = env.pid();
    let tokio = client.runtime.tokio_handle.clone();

    tokio.spawn(async move {
        let namespace = client.client.options().namespace.clone();
        let permits = Arc::new(Semaphore::new(CONCURRENCY));
        let mut describing = JoinSet::new();

        for (workflow_id, run_id) in runs {
            let client = client.clone();
            let permits = permits.clone();
            let namespace = namespace.clone();

            describing.spawn(async move {
                let _permit = permits.acquire_owned().await;
                let answer = describe(&client, namespace, &workflow_id, &run_id, timeout_ms).await;
                (workflow_id, run_id, answer)
            });
        }

        let mut answers = Vec::new();
        while let Some(joined) = describing.join_next().await {
            if let Ok(answer) = joined {
                answers.push(answer);
            }
        }

        let _ = OwnedEnv::new().send_and_clear(&caller, |env| {
            let answers: Vec<Term> = answers
                .into_iter()
                .map(|(workflow_id, run_id, answer)| {
                    (workflow_id, run_id, encode_answer(env, answer)).encode(env)
                })
                .collect();
            (atoms::hourglass_runs_described(), tag, answers)
        });
    });

    atoms::ok()
}

async fn describe(
    client: &ClientResource,
    namespace: String,
    workflow_id: &str,
    run_id: &str,
    timeout_ms: u64,
) -> Result<Vec<u8>, BridgeError> {
    let req = DescribeWorkflowExecutionRequest {
        namespace,
        execution: Some(WorkflowExecution {
            workflow_id: workflow_id.to_string(),
            run_id: run_id.to_string(),
        }),
    };
    let mut wf_svc = client.client.connection().workflow_service();
    let describing = wf_svc.describe_workflow_execution(Request::new(req));

    match tokio::time::timeout(Duration::from_millis(timeout_ms), describing).await {
        Ok(Ok(resp)) => Ok(resp.into_inner().encode_to_vec()),
        Ok(Err(status)) => Err(BridgeError::TonicError(status.to_string())),
        Err(_elapsed) => Err(timed_out("describe", timeout_ms)),
    }
}

fn encode_answer<'a>(
    env: Env<'a>,
    answer: Result<Vec<u8>, BridgeError>,
) -> Result<Binary<'a>, ElixirBridgeError> {
    match answer {
        Ok(bytes) => crate::payload::encode_to_binary(env, bytes)
            .map_err(|e| BridgeError::Unknown(e).to_elixir(env)),
        Err(e) => Err(e.to_elixir(env)),
    }
}

fn timed_out(what: &str, timeout_ms: u64) -> BridgeError {
    BridgeError::Unknown(format!("{what} timed out after {timeout_ms} ms"))
}
