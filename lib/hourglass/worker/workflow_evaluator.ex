defmodule Hourglass.Worker.WorkflowEvaluator do
  @moduledoc """
  Per-activation ephemeral evaluator. Spawned via
  `Hourglass.WorkflowEvaluator.DynamicSupervisor`
  (`restart: :temporary`) once per workflow activation.

  Lifecycle:

    1. Decode the activation bytes into a `Coresdk.WorkflowActivation`.
    2. Read the cached `Hourglass.Workflow.State` for this
       `(task_queue, run_id)` from
       `Hourglass.Worker.WorkflowStateCache` (or `State.new/2`
       on first activation).
    3. Resolve the workflow module via the caller-supplied
       `workflow_module_resolver` callback.
    4. Call `Hourglass.Workflow.Evaluator.evaluate/3`, write the
       post-activation state back to the cache (or delete it on
       `:evict`), ship the resulting completion bytes via
       `Hourglass.BridgeHolder.complete_workflow_activation/2`,
       exit `:normal`.

  Crash semantics: a non-`:normal` exit causes the DynSup to log +
  drop the child (`restart: :temporary`). Temporal Core has not yet
  observed a completion for this activation, so it redelivers the
  same activation as a new poll result; the next poll picks it up;
  a fresh evaluator runs over byte-identical inputs and produces a
  byte-identical completion. Free crash recovery, identical to
  Temporal's own retry primitive.

  ## Bridge access

  Evaluators do NOT hold a raw bridge `reference()`. They call
  `BridgeHolder.complete_workflow_activation(task_queue, bytes)`,
  which mediates the NIF call. If the holder has been unregistered
  for this task queue (e.g. Worker is shutting down between dispatch
  and completion), the call returns `{:error, _reason}`; the evaluator
  logs and exits `:normal`. The completion never reached Core, so
  Core's per-activation watchdog timeout fires and Core redelivers
  the same activation. A fresh evaluator runs and produces a
  byte-identical completion — same recovery path as a hard crash,
  without the noisy double-log (Logger + OTP Task termination).
  Mirrors `Hourglass.ActivityRunner`'s completion-error pattern.

  ## Args

    * `:run_id` — string. The Temporal run_id this activation belongs
      to. Used as the state-cache key and for log context.

    * `:task_queue` — string. The task queue the parent Worker is
      polling. Threaded into the State so `schedule_activity`
      commands inherit it as the default task queue, joined with
      `run_id` to key the state cache, and passed to
      `BridgeHolder.complete_workflow_activation/2`.

    * `:activation_bytes` — `binary()`. Raw proto bytes returned by
      `BridgeHolder.poll_workflow_activation/1`. Decoded inside this
      process so a malformed activation only crashes one evaluator
      (Core then redelivers).

    * `:workflow_module_resolver` — `(activation -> module())`. The
      poll loop knows the registered workflow modules; this callback
      maps an activation to the module to run.

    * `:complete_fn` — *optional* `((task_queue :: String.t(), bytes :: binary()) ->
      :ok | {:error, term()})`. Test affordance: lets tests stub out
      the BridgeHolder completion call. Defaults to
      `&Hourglass.BridgeHolder.complete_workflow_activation/2`.
      Production callers do not pass this key.

  ## Exit reasons

    * `:normal` — completion shipped successfully, OR shipping failed
      with `{:error, _}` (logged; Core's per-activation timeout
      redelivers).
    * crash (uncaught exception, e.g. an unexpected return shape from
      `evaluate/3`) — DynSup logs the failure; Core redelivers the
      activation. Should not occur in practice — `evaluate/3` is
      specified to always return `{:ok, completion, state}`.
  """

  alias Coresdk.WorkflowActivation.WorkflowActivation
  alias Hourglass.BridgeHolder
  alias Hourglass.Worker.WorkflowStateCache
  alias Hourglass.Workflow.Evaluator
  alias Hourglass.Workflow.State

  require Logger

  @typedoc """
  The args map accepted by `start_link/1` and `run/1`. See moduledoc
  for field semantics.
  """
  @type args :: %{
          required(:run_id) => String.t(),
          required(:task_queue) => String.t(),
          required(:activation_bytes) => binary(),
          required(:workflow_module_resolver) => (map() -> module()),
          optional(:complete_fn) => (String.t(), binary() -> :ok | {:error, term()})
        }

  @doc """
  Spawn a linked evaluator Task. `args` is documented in the
  moduledoc.
  """
  @spec start_link(args()) :: {:ok, pid()}
  def start_link(args) do
    Task.start_link(__MODULE__, :run, [args])
  end

  @doc """
  Evaluator entry point. Decodes the activation, runs the workflow
  module via `Workflow.Evaluator.evaluate/3`, persists the resulting
  state back to the cache, ships the completion via
  `BridgeHolder.complete_workflow_activation/2`, returns `:ok`.
  Raises on evaluator-error or BridgeHolder-error so the DynSup logs
  and Core redelivers.
  """
  @spec run(args()) :: :ok
  def run(args) do
    %{
      run_id: run_id,
      task_queue: task_queue,
      activation_bytes: activation_bytes,
      workflow_module_resolver: resolver
    } = args

    complete_fn =
      Map.get(args, :complete_fn, &BridgeHolder.complete_workflow_activation/2)

    activation = decode_activation(activation_bytes)
    module = resolver.(activation)
    state = load_state(task_queue, run_id)

    {:ok, completion, new_state} = Evaluator.evaluate(module, activation, state)

    persist_state(task_queue, run_id, new_state)

    ship_completion(complete_fn, task_queue, run_id, completion)
  end

  defp load_state(task_queue, run_id) do
    case WorkflowStateCache.fetch(task_queue, run_id) do
      nil -> State.new(run_id, task_queue)
      %State{} = cached -> cached
    end
  end

  defp ship_completion(complete_fn, task_queue, run_id, completion) do
    completion_bytes = Protobuf.encode(completion)

    case complete_fn.(task_queue, completion_bytes) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "BridgeHolder.complete_workflow_activation failed: " <>
            "#{inspect(reason)} (run_id=#{run_id}). Core will redeliver."
        )

        :ok
    end
  end

  defp persist_state(task_queue, run_id, %State{result: :evict}) do
    WorkflowStateCache.delete(task_queue, run_id)
  end

  defp persist_state(task_queue, run_id, %State{result: :continue_as_new}) do
    WorkflowStateCache.delete(task_queue, run_id)
  end

  defp persist_state(task_queue, run_id, %State{} = state) do
    WorkflowStateCache.put(task_queue, run_id, state)
  end

  defp decode_activation(bytes) do
    WorkflowActivation.decode(bytes)
  end
end
