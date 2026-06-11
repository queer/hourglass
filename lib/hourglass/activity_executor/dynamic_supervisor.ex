defmodule Hourglass.ActivityExecutor.DynamicSupervisor do
  @moduledoc """
  Application-level shared `DynamicSupervisor` for ephemeral activity
  executor Tasks. Each child wraps `Hourglass.ActivityRunner.run/3`
  (called via the 2-arg form, which fills in the default
  `complete_fn` arg pointing at `BridgeHolder.complete_activity_task/2`).

  Started once for the whole VM (registered as `__MODULE__`). Promoted
  from a per-Worker DynSup to a shared one for the same reason as
  `WorkflowEvaluator.DynamicSupervisor` — activity executors have no
  per-task-queue state; they call
  `BridgeHolder.complete_activity_task/2` with the task queue at
  completion time.

  In-flight executors survive Worker child crashes (Workers no longer
  parent the DynSup). If the BridgeHolder restarts mid-execution the
  completion call returns `{:error, :worker_not_registered}` and Core
  redelivers via heartbeat-/start-to-close-timeout. Idempotency on
  activity-task redelivery is the caller's responsibility — Hourglass
  provides no built-in deduplication cache.

  `restart: :temporary` — activity completion (success or failure) is
  shipped back to Core inside `run/3`, so OTP-level restart is not
  appropriate. Core owns activity-level retry.
  """

  use DynamicSupervisor

  alias Hourglass.ActivityRunner

  @typedoc "Args passed to `start_child/1`."
  @type child_args :: %{
          required(:activity_task) => map(),
          required(:task_queue) => String.t()
        }

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(_opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl DynamicSupervisor
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Start a fresh activity executor Task under the shared DynSup. The
  Task runs `ActivityRunner.run(activity_task, task_queue)`
  (the 2-arg form of `ActivityRunner.run/3`, with the default
  `complete_fn`) and exits when it returns.
  """
  @spec start_child(child_args()) :: DynamicSupervisor.on_start_child()
  def start_child(%{
        activity_task: activity_task,
        task_queue: task_queue
      }) do
    spec = %{
      id: __MODULE__.Child,
      start: {Task, :start_link, [fn -> ActivityRunner.run(activity_task, task_queue) end]},
      restart: :temporary,
      type: :worker
    }

    DynamicSupervisor.start_child(__MODULE__, spec)
  end
end
