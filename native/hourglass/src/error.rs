use rustler::{Atom, Env, NifStruct};
use thiserror::Error;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum BridgeError {
    #[error("worker shut down")]
    Shutdown,
    #[error("tonic: {0}")]
    TonicError(String),
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
