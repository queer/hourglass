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
  """

  alias Coresdk.WorkflowActivation.WorkflowActivation
  alias Hourglass.Bridge
  alias Hourglass.BridgeHolder
  alias Hourglass.Worker.WorkflowTypeResolver
  alias Hourglass.WorkflowEvaluator.DynamicSupervisor

  require Logger

  @typedoc "Options accepted by `start_link/1`."
  @type opts :: [
          task_queue: String.t()
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

    loop(%{task_queue: task_queue})
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

  defp dispatch(bytes, state) do
    activation = WorkflowActivation.decode(bytes)
    module = WorkflowTypeResolver.resolve(activation, state.task_queue)

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
end
