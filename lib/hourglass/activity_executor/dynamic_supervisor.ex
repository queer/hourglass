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

  ## Executors by task queue

  Each executor registers under its task queue in
  `Hourglass.ActivityExecutor.Registry` (a duplicate-key `Registry`
  started next to this supervisor), so a stopping `Hourglass.Worker`
  can find the executors of its own queue, and the task tokens of the
  activities they run, with `running/1`. The executor running an
  activity also registers under that activity's task token, with the
  workflow run the activity belongs to, so `running_activity?/2` can
  tell whether a dispatch is still going on
  (`Hourglass.Activity.heartbeat/1`), and `running_activities/1` which
  runs a Worker's activities belong to (its "Activities of a closed
  workflow"). A Worker
  stops only the executors still running once its drain is over, whose
  outcome either was already reported to Temporal or can never be
  (`Hourglass.Worker`'s "Graceful stop").
  """

  use DynamicSupervisor

  alias Hourglass.ActivityRunner

  @registry Hourglass.ActivityExecutor.Registry

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
      start: {Task, :start_link, [fn -> execute(activity_task, task_queue) end]},
      restart: :temporary,
      type: :worker
    }

    DynamicSupervisor.start_child(__MODULE__, spec)
  end

  @doc """
  The executors running for `task_queue`, each with the task token of the
  activity task it runs.
  """
  @spec running(String.t()) :: [{pid(), binary()}]
  def running(task_queue) when is_binary(task_queue) do
    Registry.lookup(@registry, task_queue)
  end

  @doc """
  Child spec for `Hourglass.ActivityExecutor.Registry`, the duplicate-key
  `Registry` each executor registers in under its task queue. Started by
  `Hourglass.Application` just before this supervisor.
  """
  @spec registry_child_spec() :: Supervisor.child_spec()
  def registry_child_spec, do: Registry.child_spec(keys: :duplicate, name: @registry)

  @doc """
  The activities running for `task_queue`: for each, the executor running
  it, its task token, and the workflow id and run id of the run it belongs
  to. Executors acknowledging a cancel are not among them, nor is an
  activity whose task names no run.
  """
  @spec running_activities(String.t()) :: [{pid(), binary(), String.t(), String.t()}]
  def running_activities(task_queue) when is_binary(task_queue) do
    for {pid, task_token} <- running(task_queue),
        {^pid, {workflow_id, run_id}} <- Registry.lookup(@registry, {:activity, task_token}),
        do: {pid, task_token, workflow_id, run_id}
  end

  @doc """
  Whether `pid` is the executor running the activity whose task token is
  `task_token` — that is, whether that dispatch of the activity is still
  going on. A Cancel task for the token does not count: only the
  executor that runs the activity itself does. Nor does an executor that
  has exited, which the registry may list for a moment after.
  """
  @spec running_activity?(binary(), pid()) :: boolean()
  def running_activity?(task_token, pid) when is_binary(task_token) and is_pid(pid) do
    @registry
    |> Registry.lookup({:activity, task_token})
    |> Enum.any?(fn {owner, _run} -> owner == pid end) and Process.alive?(pid)
  end

  defp execute(activity_task, task_queue) do
    {:ok, _owner} = Registry.register(@registry, task_queue, activity_task.task_token)

    # The executor that runs an activity — not one acknowledging a cancel of
    # it — also registers under its task token, with the run it belongs to,
    # for `running_activity?/2` and `running_activities/1`.
    with %{variant: {:start, start}} <- activity_task do
      {:ok, _owner} =
        Registry.register(@registry, {:activity, activity_task.task_token}, run_of(start))
    end

    ActivityRunner.run(activity_task, task_queue)
  end

  defp run_of(%{workflow_execution: %{workflow_id: workflow_id, run_id: run_id}})
       when workflow_id != "" and run_id != "",
       do: {workflow_id, run_id}

  defp run_of(_start), do: nil
end
