defmodule Hourglass.WorkflowEvaluator.DynamicSupervisor do
  @moduledoc """
  Application-level shared `DynamicSupervisor` for per-activation
  `Hourglass.Worker.WorkflowEvaluator` Tasks.

  Started once under `Hourglass.Application`; one instance for the whole
  VM (registered as `__MODULE__`). Promoted from a per-Worker DynSup to
  a shared one because evaluator Tasks have no per-task-queue state —
  the only per-queue thing they touch is the
  `WorkflowStateCache`, which is keyed by `(task_queue, run_id)` and
  is itself app-level.

  In-flight evaluator Tasks living under this DynSup survive Worker
  child crashes (Workers no longer parent the DynSup). If the
  BridgeHolder restarts mid-evaluation the in-flight Tasks see
  `{:error, :worker_not_registered}` from `complete_workflow_activation/2`,
  exit, and Core redelivers the activation.

  `restart: :temporary` — exited evaluators are not restarted by the
  supervisor. If an evaluator crashed before shipping a completion,
  Core redelivers the same activation as a new poll result.
  """

  use DynamicSupervisor

  alias Hourglass.Worker.WorkflowEvaluator

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(_opts \\ []) do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl DynamicSupervisor
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Start a fresh `WorkflowEvaluator` Task under the shared DynSup.
  `args` is the evaluator args map documented on `WorkflowEvaluator`.
  """
  @spec start_child(WorkflowEvaluator.args()) :: DynamicSupervisor.on_start_child()
  def start_child(args) do
    spec = %{
      id: WorkflowEvaluator,
      start: {WorkflowEvaluator, :start_link, [args]},
      restart: :temporary,
      type: :worker
    }

    DynamicSupervisor.start_child(__MODULE__, spec)
  end
end
