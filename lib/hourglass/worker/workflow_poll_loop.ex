# credo:disable-for-this-file Credo.Check.Refactor.ModuleDependencies
defmodule Hourglass.Worker.WorkflowPollLoop do
  @moduledoc """
  Per-Worker Task that drives `BridgeHolder.poll_workflow_activation/1`
  in a tight loop. On each `{:ok, bytes}` result it dispatches the
  activation directly to the shared
  `Hourglass.WorkflowEvaluator.DynamicSupervisor`, spawning a
  fresh ephemeral evaluator Task per activation. The evaluator runs
  once and exits `:normal`; Core handles redelivery on crashes.

  ## Bridge access

  This loop does NOT hold a raw bridge handle. Every iteration calls
  `BridgeHolder.poll_workflow_activation(task_queue)` and the
  Application-level holder mediates the NIF call (dispatching the
  long-poll to its own child Task and replying when Core delivers).

  ## Shutdown

  When `BridgeHolder.poll_workflow_activation/1` returns
  `{:error, %Bridge.Error{kind: :shutdown}}` the loop disambiguates
  via `BridgeHolder.registered?/1`:

    * `registered?` → false: the Worker is being torn down (graceful
      `Worker.terminate/2` already called `unregister_worker`). Exit
      `:normal` and let the inner Supervisor reap the loop.
    * `registered?` → true: the holder recycled the bridge handle
      mid-flight (poll-caller DOWN). Sleep briefly + retry
      against the new handle.

  If the call returns `{:error, :worker_not_registered}` the loop
  treats it as a transient retry condition (sleep + retry) —
  registration may be in flight, e.g. the holder restarted moments
  ago and `Worker.Supervisor.start_worker/1`'s register call hasn't
  completed yet.

  Other bridge errors log + sleep + retry. The Supervisor's
  `:transient` restart for this child means `:normal` exit is final
  (no flapping during graceful shutdown).

  ## Sticky-cache-miss recovery (do NOT crash the loop)

  `WorkflowTypeResolver.resolve/2` has two tiers: the activation's
  `initialize_workflow` job (present on full-history / non-sticky
  deliveries) and the worker's `WorkflowStateCache` (populated by prior
  activations, mirrors Core's sticky cache). An **incremental (sticky)**
  activation for a run this worker never cached — or evicted — exhausts
  both tiers. That is a recoverable Core/worker cache desync, not a bug
  in the activation; it shows up under load when workflows sit idle for
  long stretches or the live-workflow count approaches
  `max_cached_workflows`.

  Historically `resolve/2` raised here, the raise propagated through
  `dispatch/2` → `loop/1`, and the whole poll-loop Task crashed. It
  self-healed only because Core eventually timed out the sticky task and
  re-delivered non-sticky with full history — but every occurrence was a
  crash + `:transient` restart + re-register + replay, spamming errors.

  The correct Temporal behavior is to **fail the current (sticky)
  workflow task**: ship a `WorkflowActivationCompletion{status: {:failed,
  ...}}` for the run. SDK-Core responds by evicting the run from its
  sticky cache and reporting the workflow-task failure to the server; the
  server reschedules the task, and because Core no longer has the run
  cached it fetches full history and re-delivers **non-sticky** — that
  activation carries `initialize_workflow`, so tier 1 resolves it. The
  poll loop never crashes: `dispatch/2` ships the failure inline and
  keeps polling. Workflow tasks retry indefinitely on the server (they do
  not fail the workflow), so this is safe and self-correcting.
  """

  alias Coresdk.WorkflowActivation.WorkflowActivation
  alias Coresdk.WorkflowCompletion.Failure
  alias Coresdk.WorkflowCompletion.WorkflowActivationCompletion
  alias Hourglass.Bridge
  alias Hourglass.BridgeHolder
  alias Hourglass.Worker.WorkflowTypeResolver
  alias Hourglass.WorkflowEvaluator.DynamicSupervisor

  require Logger

  @typedoc "Options accepted by `start_link/1`."
  @type opts :: [
          task_queue: String.t(),
          complete_fn: (String.t(), binary() -> :ok | {:error, term()})
        ]

  @doc """
  Spawn a linked Task that runs the poll loop until the bridge
  reports `:shutdown` or the parent supervisor terminates it.
  """
  @spec start_link(opts()) :: {:ok, pid()}
  def start_link(opts) do
    Task.start_link(__MODULE__, :run, [opts])
  end

  @doc false
  @spec child_spec(opts()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient,
      type: :worker
    }
  end

  @doc """
  Loop entry. Iterates `BridgeHolder.poll_workflow_activation/1`
  until shutdown.
  """
  @spec run(opts()) :: :ok
  def run(opts) do
    task_queue = Keyword.fetch!(opts, :task_queue)

    # `:complete_fn` is a test seam only (mirrors
    # `Hourglass.Worker.WorkflowEvaluator`'s `:complete_fn`): production
    # callers never pass it, so the sticky-cache-miss recovery ships its
    # workflow-task failure through `BridgeHolder`. Tests inject a stub to
    # assert the recovery without a live bridge.
    complete_fn =
      Keyword.get(opts, :complete_fn, &BridgeHolder.complete_workflow_activation/2)

    loop(%{task_queue: task_queue, complete_fn: complete_fn})
  end

  defp loop(state) do
    case BridgeHolder.poll_workflow_activation(state.task_queue) do
      {:ok, bytes} ->
        dispatch(bytes, state)
        loop(state)

      {:error, %Bridge.Error{kind: :shutdown}} ->
        # Recycle vs. graceful-shutdown disambiguation: if BridgeHolder
        # still holds a handle for this task queue, the :shutdown came
        # from a recycle — sleep briefly + retry against the
        # new handle. Otherwise the Worker is being torn down — exit.
        if BridgeHolder.registered?(state.task_queue) do
          Process.sleep(50)
          loop(state)
        else
          :ok
        end

      {:error, :worker_not_registered} ->
        # Holder restart in flight: registration completes asynchronously.
        # Brief sleep + retry; we don't exit, the Worker is still alive.
        Process.sleep(50)
        loop(state)

      {:error, err} ->
        Logger.error("workflow poll loop error: #{inspect(err)}")
        Process.sleep(100)
        loop(state)
    end
  end

  # Public (`@doc false`) only as the poll-loop test seam: feeds decoded
  # bytes + state through the same path `loop/1` uses so the
  # sticky-cache-miss recovery is testable without a live bridge. Returns
  # `:ok` on every path — it must never raise, or it would kill `loop/1`.
  @doc false
  @spec dispatch(binary(), map()) :: :ok
  def dispatch(bytes, state) do
    activation = WorkflowActivation.decode(bytes)

    case WorkflowTypeResolver.resolve(activation, state.task_queue) do
      {:ok, module} ->
        start_evaluator(bytes, activation, module, state)

      {:error, :sticky_cache_miss} ->
        fail_sticky_task(activation, state)
    end
  end

  defp start_evaluator(bytes, activation, module, state) do
    args = %{
      run_id: activation.run_id,
      task_queue: state.task_queue,
      activation_bytes: bytes,
      workflow_module_resolver: fn _activation -> module end
    }

    case DynamicSupervisor.start_child(args) do
      {:ok, _pid} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "WorkflowEvaluator.DynamicSupervisor.start_child failed: " <>
            "#{inspect(reason)} (run_id=#{activation.run_id})"
        )
    end
  end

  # Recover from a sticky-cache miss (see moduledoc "Sticky-cache-miss
  # recovery"): fail the current workflow task so Core evicts the run and
  # re-delivers it non-sticky with full history (tier-1 resolvable). We
  # cannot run the workflow — the module is unknown — so we build a
  # minimal `failed` completion from the run_id alone and ship it. Always
  # returns `:ok`: a shipping error is logged, not raised, so the poll
  # loop keeps polling (Core's sticky-task timeout then forces the same
  # non-sticky redelivery as a fallback).
  defp fail_sticky_task(activation, state) do
    Logger.info(
      "sticky-cache miss (task_queue=#{state.task_queue}, run_id=#{activation.run_id}): " <>
        "incremental activation with no initialize_workflow job and no WorkflowStateCache " <>
        "entry. Failing the sticky workflow task to force a non-sticky, full-history redelivery."
    )

    completion = %WorkflowActivationCompletion{
      run_id: activation.run_id,
      status:
        {:failed,
         %Failure{
           failure: %Temporal.Api.Failure.V1.Failure{
             message:
               "hourglass sticky-cache miss: run not present in worker WorkflowStateCache; " <>
                 "failing workflow task to force non-sticky replay with full history"
           }
         }}
    }

    bytes = Protobuf.encode(completion)

    case state.complete_fn.(state.task_queue, bytes) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "sticky-cache-miss workflow-task failure could not be shipped " <>
            "(task_queue=#{state.task_queue}, run_id=#{activation.run_id}): #{inspect(reason)}. " <>
            "Core's sticky-task timeout will redeliver non-sticky."
        )

        :ok
    end
  end
end
