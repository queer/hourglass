mod client;
mod error;
mod payload;
mod replayer;
mod runtime;
mod worker;

use error::BridgeError;

#[rustler::nif]
fn ping() -> &'static str {
    "pong"
}

#[rustler::nif]
fn fail(env: rustler::Env, message: String) -> Result<(), error::ElixirBridgeError> {
    Err(BridgeError::Test(message).to_elixir(env))
}

rustler::init!("Elixir.Hourglass.Bridge");
