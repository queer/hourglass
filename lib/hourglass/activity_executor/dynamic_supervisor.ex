# credo:disable-for-this-file Credo.Check.Refactor.ModuleDependencies
#
# It runs each activity's dispatch: the registry it keeps, the runner, and the
# closed-workflow watch that admits an activity are each a dependency it has
# to have.
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

  ## An activity of an ended run never starts

  Before an executor runs an activity, it asks Temporal about the
  activity's workflow run, through the closed-workflow watch of the
  activity's Worker (`Hourglass.Worker`, "Activities of a closed
  workflow"). If the run has closed, or (for an activity that opts in,
  `use Hourglass.Activity, ends_on_cancel_request: true`) has a cancel
  requested of it, the activity's `execute/1` is never called: the
  activity is reported to Temporal as ended for that reason
  (`Hourglass.ActivityRunner.report_cancelled/3`), and
  `[:hourglass, :activity, :not_started]` is emitted. Temporal hands out
  no task of a closed run, but Core fetches tasks ahead of the worker
  asking for them, and a busy or paused worker may start one long after
  Temporal handed it out, so only asking tells. A run Temporal has not
  answered for within a second has not ended, and its activity starts.

  ## Executors by task queue

  Each executor registers under its task queue in
  `Hourglass.ActivityExecutor.Registry` (a duplicate-key `Registry`
  started next to this supervisor), so a stopping `Hourglass.Worker`
  can find the executors of its own queue, and the task tokens of the
  activities they run, with `running/1`. The executor running an
  activity also registers under that activity's task token, with the
  workflow run the activity belongs to and whether it ends on a cancel
  request, so `running_activity?/2` can tell whether a dispatch is
  still going on (`Hourglass.Activity.heartbeat/1`), and
  `running_activities/1` which runs a Worker's activities belong to (its
  "Activities of a closed workflow"). A Worker
  stops only the executors still running once its drain is over, whose
  outcome either was already reported to Temporal or can never be
  (`Hourglass.Worker`'s "Graceful stop").
  """

  use DynamicSupervisor

  alias Hourglass.ActivityRunner
  alias Hourglass.Worker.ClosedWorkflowWatch

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
  it, its task token, the run it belongs to (`{workflow_id, run_id}`), and
  whether it ends on a cancel requested of that run. Executors
  acknowledging a cancel are not among them, nor is an activity whose task
  names no run, nor one not yet started.
  """
  @spec running_activities(String.t()) ::
          [{pid(), binary(), {String.t(), String.t()}, boolean()}]
  def running_activities(task_queue) when is_binary(task_queue) do
    for {pid, task_token} <- running(task_queue),
        {^pid, {{_workflow_id, _run_id} = run, opted_in}} <-
          Registry.lookup(@registry, {:activity, task_token}),
        do: {pid, task_token, run, opted_in}
  end

  @doc false
  # The registry executors, and each Worker's closed-workflow watch, register in.
  @spec registry() :: atom()
  def registry, do: @registry

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

    case activity_task do
      %{variant: {:start, start}} -> start(activity_task, start, task_queue)
      _cancel -> ActivityRunner.run(activity_task, task_queue)
    end
  end

  # Run the activity, unless its run has ended ("An activity of an ended run
  # never starts"). The executor that runs it — not one acknowledging a cancel
  # of it — also registers under its task token, with its run and whether it
  # opted in, for `running_activity?/2` and `running_activities/1`.
  defp start(activity_task, start, task_queue) do
    token = activity_task.task_token

    case ClosedWorkflowWatch.admit(task_queue, token, start) do
      {:start, run, opted_in} ->
        {:ok, _owner} = Registry.register(@registry, {:activity, token}, {run, opted_in})
        ActivityRunner.run(activity_task, task_queue)

      :ended ->
        :ok
    end
  end
end
