use rustler::{Atom, Env, NifStruct};
use temporalio_client::errors::ClientConnectError;
use temporalio_client::tonic::{Code, Status};
use temporalio_sdk_core::WorkerValidationError;
use thiserror::Error;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum BridgeError {
    #[error("worker shut down")]
    Shutdown,
    #[error("tonic: {0}")]
    TonicError(String),
    // Temporal could not be reached, or did not answer: worth trying again,
    // unlike a call Temporal refused.
    #[error("unreachable: {0}")]
    Unreachable(String),
    // Temporal does not know the namespace a worker was given, or a call names.
    #[error("namespace not found: {0}")]
    NamespaceNotFound(String),
    #[error("invalid proto: {0}")]
    InvalidProto(String),
    #[error("nondeterminism: {0}")]
    Nondeterminism(String),
    #[error("worker already started: {0}")]
    WorkerAlreadyStarted(String),
    #[error("test: {0}")]
    Test(String),
    #[error("unknown: {0}")]
    Unknown(String),
}

pub mod atoms {
    rustler::atoms! {
        ok,
        error,
        shutdown,
        tonic_error,
        unreachable,
        namespace_not_found,
        invalid_proto,
        nondeterminism,
        worker_already_started,
        test,
        unknown,
    }
}

#[derive(NifStruct)]
#[module = "Hourglass.Bridge.Error"]
pub struct ElixirBridgeError {
    pub kind: Atom,
    pub detail: String,
}

impl BridgeError {
    pub fn to_elixir(&self, _env: Env) -> ElixirBridgeError {
        let (kind, detail) = match self {
            BridgeError::Shutdown => (atoms::shutdown(), String::new()),
            BridgeError::TonicError(s) => (atoms::tonic_error(), s.clone()),
            BridgeError::Unreachable(s) => (atoms::unreachable(), s.clone()),
            BridgeError::NamespaceNotFound(s) => (atoms::namespace_not_found(), s.clone()),
            BridgeError::InvalidProto(s) => (atoms::invalid_proto(), s.clone()),
            BridgeError::Nondeterminism(s) => (atoms::nondeterminism(), s.clone()),
            BridgeError::WorkerAlreadyStarted(s) => (atoms::worker_already_started(), s.clone()),
            BridgeError::Test(s) => (atoms::test(), s.clone()),
            BridgeError::Unknown(s) => (atoms::unknown(), s.clone()),
        };
        ElixirBridgeError { kind, detail }
    }
}

#[allow(dead_code)]
pub type BridgeResult<T> = std::result::Result<T, BridgeError>;

// Whether a gRPC status says Temporal was not reached, or did not answer in
// time, rather than that it refused the call.
fn unreachable_code(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Cancelled
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::Unknown
    )
}

impl BridgeError {
    // Connecting failed: unreachable if the network or Temporal let it down
    // (a transport or DNS failure, or GetSystemInfo not answered), else a
    // refusal or a configuration Temporal will never accept.
    pub fn from_connect(e: ClientConnectError) -> Self {
        let unreachable = match &e {
            ClientConnectError::TonicTransportError(_)
            | ClientConnectError::DnsResolutionError { .. } => true,
            ClientConnectError::SystemInfoCallError(status) => unreachable_code(status.code()),
            _ => false,
        };

        if unreachable {
            BridgeError::Unreachable(e.to_string())
        } else {
            BridgeError::TonicError(e.to_string())
        }
    }

    // A call naming a namespace failed: the namespace is unknown to Temporal,
    // Temporal was not reached or did not answer in time, or it refused.
    pub fn from_namespace_call(status: Status) -> Self {
        match status.code() {
            Code::NotFound => BridgeError::NamespaceNotFound(status.to_string()),
            code if unreachable_code(code) => BridgeError::Unreachable(status.to_string()),
            _ => BridgeError::TonicError(status.to_string()),
        }
    }

    // Describing the worker's namespace failed: unreachable as for a call,
    // unknown to Temporal, or refused to the worker.
    pub fn from_validation(e: WorkerValidationError) -> Self {
        let code = match &e {
            WorkerValidationError::NamespaceDescribeError { source, .. } => source.code(),
        };

        match code {
            Code::NotFound => BridgeError::NamespaceNotFound(e.to_string()),
            code if unreachable_code(code) => BridgeError::Unreachable(e.to_string()),
            _ => BridgeError::TonicError(e.to_string()),
        }
    }
}

/// Return type for NIFs that return `:ok | {:error, %Bridge.Error{}}` on the Elixir side.
///
/// `Result<(), ElixirBridgeError>` encodes as `{:ok, {}}` which is not idiomatic; this type
/// encodes `Ok` as the bare atom `:ok` and `Err` as `{:error, %Bridge.Error{}}`.
pub enum OkOrError {
    Ok,
    Err(ElixirBridgeError),
}

impl rustler::Encoder for OkOrError {
    fn encode<'c>(&self, env: Env<'c>) -> rustler::Term<'c> {
        match self {
            OkOrError::Ok => atoms::ok().encode(env),
            OkOrError::Err(e) => (atoms::error(), e).encode(env),
        }
    }
}

impl<E: Into<ElixirBridgeError>> From<Result<(), E>> for OkOrError {
    fn from(result: Result<(), E>) -> Self {
        match result {
            Ok(()) => OkOrError::Ok,
            Err(e) => OkOrError::Err(e.into()),
        }
    }
}
