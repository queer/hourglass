defmodule Hourglass.Worker.ActivityPollLoop do
  @moduledoc """
  Per-Worker Task that drives `BridgeHolder.poll_activity_task/1`
  in a tight loop. Each `{:ok, bytes}` is decoded into a
  `Coresdk.ActivityTask.ActivityTask` and dispatched directly to
  `Hourglass.ActivityExecutor.DynamicSupervisor.start_child/1`
  (the shared Application-level executor DynSup).

  ## Bridge access

  This loop does NOT hold a raw bridge handle. It calls
  `BridgeHolder.poll_activity_task(task_queue)` and the
  Application-level holder mediates the NIF call.

  ## Shutdown

  Same shape as `WorkflowPollLoop`: `:shutdown` from the bridge is
  disambiguated against `BridgeHolder.registered?/1` — exit `:normal`
  if the holder no longer has a handle (graceful Worker teardown),
  sleep + retry if it still does (handle recycle).
  `:worker_not_registered` → brief sleep + retry; other errors → log
  + sleep + retry.
  """

  alias Coresdk.ActivityTask.ActivityTask
  alias Hourglass.ActivityExecutor.DynamicSupervisor
  alias Hourglass.Bridge
  alias Hourglass.BridgeHolder

  require Logger

  @typedoc "Options accepted by `start_link/1`."
  @type opts :: [
          task_queue: String.t()
        ]

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
  Loop entry. Iterates `BridgeHolder.poll_activity_task/1` until
  shutdown.
  """
  @spec run(opts()) :: :ok
  def run(opts) do
    task_queue = Keyword.fetch!(opts, :task_queue)

    loop(%{task_queue: task_queue})
  end

  defp loop(state) do
    case BridgeHolder.poll_activity_task(state.task_queue) do
      {:ok, bytes} ->
        dispatch(bytes, state)
        loop(state)

      {:error, %Bridge.Error{kind: :shutdown}} ->
        # Recycle vs. graceful-shutdown disambiguation; see
        # WorkflowPollLoop for the rationale.
        if BridgeHolder.registered?(state.task_queue) do
          Process.sleep(50)
          loop(state)
        else
          :ok
        end

      {:error, :worker_not_registered} ->
        Process.sleep(50)
        loop(state)

      {:error, err} ->
        Logger.error("activity poll loop error: #{inspect(err)}")
        Process.sleep(100)
        loop(state)
    end
  end

  defp dispatch(bytes, state) do
    activity_task = ActivityTask.decode(bytes)

    args = %{
      activity_task: activity_task,
      task_queue: state.task_queue
    }

    case DynamicSupervisor.start_child(args) do
      {:ok, _pid} ->
        :ok

      {:error, reason} ->
        Logger.error("ActivityExecutor.DynamicSupervisor.start_child failed: #{inspect(reason)}")
    end
  end
end
