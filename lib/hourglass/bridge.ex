defmodule Hourglass.Bridge do
  @moduledoc """
  Rustler NIF wrapper for the hourglass crate.

  ## NIF-reload rescue

  When `Phoenix.CodeReloader` (or any other reload mechanism) re-loads
  this module mid-BEAM-session, the Rustler `@on_load :load_nif`
  callback re-runs and installs a fresh resource-type registry. Every
  pre-reload resource ref (`CoreRuntime`, `Worker`, `Client`, `Replayer`)
  becomes opaque, and the next NIF call that decodes one raises
  `ArgumentError` from the Rustler decoder.

  `with_nif_reload_rescue/2` is the canonical wrapper for any Bridge
  call that operates on a resource ref. It catches `ArgumentError`,
  exits `Hourglass.Runtime` with `:nif_reloaded` (which cascades
  the `Hourglass.Subsystem` `:rest_for_one` subtree, re-acquiring
  every ref against the freshly loaded NIF), and returns
  `{:error, :nif_reloaded}` so the caller's `with` chain unwinds
  cleanly. The rescue does NOT loop: a second `ArgumentError` from a
  caller-side retry is treated identically to the first — the cascade
  is idempotent for a single Runtime pid and `Process.exit/2` on a
  dead pid is a no-op.

  See `signal_nif_reload!/1` for the cascade-signal internals.
  """
  use Rustler, otp_app: :hourglass, crate: :hourglass

  require Logger

  @spec ping() :: String.t()
  def ping, do: :erlang.nif_error(:nif_not_loaded)

  @spec fail(String.t()) :: {:error, Hourglass.Bridge.Error.t()}
  def fail(_message), do: :erlang.nif_error(:nif_not_loaded)

  @spec runtime_new() :: {:ok, reference()} | {:error, Hourglass.Bridge.Error.t()}
  def runtime_new, do: :erlang.nif_error(:nif_not_loaded)

  @spec worker_new(reference(), binary()) ::
          {:ok, reference()} | {:error, Hourglass.Bridge.Error.t()}
  def worker_new(_runtime, _config_bin), do: :erlang.nif_error(:nif_not_loaded)

  @spec worker_poll_workflow_activation(reference()) ::
          {:ok, binary()} | {:error, Hourglass.Bridge.Error.t()}
  def worker_poll_workflow_activation(_worker), do: :erlang.nif_error(:nif_not_loaded)

  @spec worker_complete_workflow_activation(reference(), binary()) ::
          :ok | {:error, Hourglass.Bridge.Error.t()}
  def worker_complete_workflow_activation(_worker, _bin), do: :erlang.nif_error(:nif_not_loaded)

  @spec worker_poll_activity_task(reference()) ::
          {:ok, binary()} | {:error, Hourglass.Bridge.Error.t()}
  def worker_poll_activity_task(_worker), do: :erlang.nif_error(:nif_not_loaded)

  @spec worker_complete_activity_task(reference(), binary()) ::
          :ok | {:error, Hourglass.Bridge.Error.t()}
  def worker_complete_activity_task(_worker, _bin), do: :erlang.nif_error(:nif_not_loaded)

  @spec worker_shutdown(reference()) :: :ok | {:error, Hourglass.Bridge.Error.t()}
  def worker_shutdown(_worker), do: :erlang.nif_error(:nif_not_loaded)

  @spec client_new(reference(), binary()) ::
          {:ok, reference()} | {:error, Hourglass.Bridge.Error.t()}
  def client_new(_runtime, _config_bin), do: :erlang.nif_error(:nif_not_loaded)

  @spec client_start_workflow(reference(), binary()) ::
          {:ok, binary()} | {:error, Hourglass.Bridge.Error.t()}
  def client_start_workflow(_client, _bin), do: :erlang.nif_error(:nif_not_loaded)

  @spec client_await_workflow(reference(), binary(), pos_integer()) ::
          {:ok, binary()} | {:error, Hourglass.Bridge.Error.t()}
  def client_await_workflow(_client, _bin, _timeout_ms), do: :erlang.nif_error(:nif_not_loaded)

  @spec client_fetch_history(reference(), String.t()) ::
          {:ok, binary()} | {:error, Hourglass.Bridge.Error.t()}
  def client_fetch_history(_client, _workflow_id), do: :erlang.nif_error(:nif_not_loaded)

  @spec client_describe_workflow_execution(reference(), String.t(), String.t()) ::
          {:ok, binary()} | {:error, Hourglass.Bridge.Error.t()}
  def client_describe_workflow_execution(_client, _workflow_id, _run_id),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec client_describe_namespace(reference(), String.t()) ::
          {:ok, binary()} | {:error, Hourglass.Bridge.Error.t()}
  def client_describe_namespace(_client, _namespace), do: :erlang.nif_error(:nif_not_loaded)

  @spec client_register_namespace(reference(), String.t()) ::
          :ok | {:error, Hourglass.Bridge.Error.t()}
  def client_register_namespace(_client, _namespace), do: :erlang.nif_error(:nif_not_loaded)

  @spec client_signal_workflow(reference(), binary()) ::
          :ok | {:error, Hourglass.Bridge.Error.t()}
  def client_signal_workflow(_client, _request_bin), do: :erlang.nif_error(:nif_not_loaded)

  @spec client_cancel_workflow(reference(), binary()) ::
          :ok | {:error, Hourglass.Bridge.Error.t()}
  def client_cancel_workflow(_client, _request_bin), do: :erlang.nif_error(:nif_not_loaded)

  @spec replayer_new(reference(), binary()) ::
          {:ok, reference()} | {:error, Hourglass.Bridge.Error.t()}
  def replayer_new(_runtime, _config_bin), do: :erlang.nif_error(:nif_not_loaded)

  @spec replayer_push_history(reference(), String.t(), binary()) ::
          :ok | {:error, Hourglass.Bridge.Error.t()}
  def replayer_push_history(_replayer, _workflow_id, _history_bin),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec replayer_poll_workflow_activation(reference()) ::
          {:ok, binary()} | {:error, Hourglass.Bridge.Error.t()}
  def replayer_poll_workflow_activation(_replayer), do: :erlang.nif_error(:nif_not_loaded)

  @spec replayer_complete_workflow_activation(reference(), binary()) ::
          :ok | {:error, Hourglass.Bridge.Error.t()}
  def replayer_complete_workflow_activation(_replayer, _bin),
    do: :erlang.nif_error(:nif_not_loaded)

  @spec replayer_close_feeder(reference()) :: :ok
  def replayer_close_feeder(_replayer), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Wrap a Bridge NIF call so a post-hot-reload `ArgumentError` (from a
  stale resource ref decoded against a freshly loaded NIF resource-type
  registry) cascades a `Hourglass.Subsystem` restart instead of
  bubbling up as an opaque crash.

  `fun` is a zero-arity function that performs the NIF call. On
  success, its return value is propagated unchanged. On `ArgumentError`,
  the helper:

    1. logs the call site at `:error` level,
    2. signals `Hourglass.Runtime` to exit with `:nif_reloaded`
       (which cascades the rest of the `:rest_for_one` subtree —
       `BridgeHolder`, `WorkerRegistry`, `Worker.Supervisor`,
       `WorkerLauncher` — so every pre-reload resource ref is dropped
       and re-acquired against the freshly loaded NIF), and
    3. returns `{:error, :nif_reloaded}` so callers' `with` chains
       unwind cleanly.

  The rescue is intentionally narrow: only `ArgumentError`, not
  `rescue _`. Widening would silently catch real argument bugs at the
  Bridge boundary.

  No automatic retry inside the helper: the cascade is async and the
  caller's process may itself be killed by the cascade (e.g. if the
  caller is `BridgeHolder`). A second NIF call from the same caller
  would re-raise `ArgumentError`, which the helper handles identically;
  `Process.exit/2` on a now-dead Runtime pid is a no-op, so there is
  no loop hazard, but there is also no point in adding a built-in
  retry. Callers that want one (e.g. `Replayer.replay_history/2`,
  which is a stateless one-shot) wrap a fresh `Runtime.handle()` +
  call themselves.

  Use this helper for any new Bridge call site that allocates or
  operates on a resource ref from process-lifecycle code paths.
  """
  @spec with_nif_reload_rescue(String.t(), (-> result)) :: result | {:error, :nif_reloaded}
        when result: term()
  def with_nif_reload_rescue(call_site, fun)
      when is_binary(call_site) and is_function(fun, 0) do
    fun.()
  rescue
    err in ArgumentError ->
      # Emit the telemetry event BEFORE signalling the cascade.
      # The cascade may kill this process (e.g. BridgeHolder
      # callers); emitting first ensures the event fires even when the
      # caller is about to die.
      :telemetry.execute(
        [:hourglass, :connection, :failed],
        %{count: 1},
        %{failure_class: :nif_reload, call_site: call_site, detail: Exception.message(err)}
      )

      signal_nif_reload!(call_site)
      {:error, :nif_reloaded}
  end

  @doc """
  Signal the `Hourglass.Subsystem` `:rest_for_one` cascade by
  exiting `Hourglass.Runtime` with `:nif_reloaded`. Public so
  `BridgeHolder` (which has its own retry/recycle semantics around
  worker handles and cannot simply return `{:error, :nif_reloaded}`
  from every call shape) can share the same signal path as the
  generic `with_nif_reload_rescue/2` wrapper.

  `call_site` is logged at `:error` level for post-mortem attribution.
  """
  @spec signal_nif_reload!(String.t()) :: :ok
  def signal_nif_reload!(call_site) when is_binary(call_site) do
    Logger.error(
      "Bridge caught ArgumentError from NIF at #{call_site} — " <>
        "treating as NIF module reload that invalidated resource refs. " <>
        "Cascading Hourglass.Runtime restart via Subsystem :rest_for_one."
    )

    case Process.whereis(Hourglass.Runtime) do
      nil ->
        :ok

      pid ->
        Process.exit(pid, :nif_reloaded)
        :ok
    end
  end
end
