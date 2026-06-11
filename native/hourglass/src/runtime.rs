use std::sync::Arc;

use rustler::ResourceArc;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, TokioRuntimeBuilder};
use tokio::runtime::Handle;

use crate::error::BridgeError;

#[allow(dead_code)]
pub struct CoreRuntimeResource {
    pub runtime: Arc<CoreRuntime>,
    pub tokio_handle: Handle,
}

// CoreRuntime contains TelemetryInstance which holds interior mutability
// (UnsafeCell-backed mutexes and dyn-trait subscribers). We assert unwind safety
// here because:
//   (a) Rustler resource objects are never shared across catch_unwind boundaries
//       in a way that would allow observing partial state — they're reference-
//       counted and only accessed via the BEAM's scheduler.
//   (b) The underlying types (RwLock, Mutex) use poisoning and are themselves
//       correct under panic conditions.
// SAFETY: see above.
impl std::panic::RefUnwindSafe for CoreRuntimeResource {}
impl std::panic::UnwindSafe for CoreRuntimeResource {}

#[rustler::resource_impl]
impl rustler::Resource for CoreRuntimeResource {}

#[rustler::nif]
pub fn runtime_new(
    env: rustler::Env,
) -> Result<ResourceArc<CoreRuntimeResource>, crate::error::ElixirBridgeError> {
    let opts = RuntimeOptions::builder()
        .build()
        .map_err(|e| BridgeError::Unknown(e).to_elixir(env))?;

    let runtime = CoreRuntime::new(opts, TokioRuntimeBuilder::default())
        .map_err(|e| BridgeError::Unknown(e.to_string()).to_elixir(env))?;

    let tokio_handle = runtime.tokio_handle();

    Ok(ResourceArc::new(CoreRuntimeResource {
        runtime: Arc::new(runtime),
        tokio_handle,
    }))
}
