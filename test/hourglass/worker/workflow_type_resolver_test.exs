defmodule Hourglass.Worker.WorkflowTypeResolverTest do
  use ExUnit.Case, async: true

  alias Coresdk.WorkflowActivation.InitializeWorkflow
  alias Coresdk.WorkflowActivation.ResolveActivity
  alias Coresdk.WorkflowActivation.WorkflowActivation
  alias Coresdk.WorkflowActivation.WorkflowActivationJob

  alias Hourglass.Worker.WorkflowStateCache
  alias Hourglass.Worker.WorkflowTypeResolver
  alias Hourglass.Workflow.State

  # Synthetic workflow modules used as routing targets — shape doesn't
  # matter, the resolver only inspects the module's stringified atom.
  defmodule WorkflowA do
    use Hourglass.Workflow
    @impl Hourglass.Workflow.Behaviour
    def run(_args), do: :ok
  end

  defmodule WorkflowB do
    use Hourglass.Workflow
    @impl Hourglass.Workflow.Behaviour
    def run(_args), do: :ok
  end

  defmodule WorkflowC do
    use Hourglass.Workflow
    @impl Hourglass.Workflow.Behaviour
    def run(_args), do: :ok
  end

  describe "resolve/2 — initialize_workflow path (structural)" do
    # Structural resolution: the workflow_type IS the module atom stringified.
    # No registration list needed — every order yields the correct module.
    test "resolves WorkflowB from its type name regardless of any ordering context" do
      activation = init_activation("run-1", Atom.to_string(WorkflowB))
      queue = unique_queue()

      assert {:ok, WorkflowB} == WorkflowTypeResolver.resolve(activation, queue)
    end

    test "resolves WorkflowA from its type name" do
      activation = init_activation("run-1a", Atom.to_string(WorkflowA))
      assert {:ok, WorkflowA} == WorkflowTypeResolver.resolve(activation, unique_queue())
    end

    test "raises a descriptive error when the workflow_type atom is unknown in this VM" do
      # "Elixir.NotRegistered.Workflow" is not a known atom — structural
      # resolution must raise a clear error rather than silently fail.
      activation = init_activation("run-2", "Elixir.NotRegistered.Workflow")

      assert_raise RuntimeError,
                   ~r/Elixir\.NotRegistered\.Workflow.*not a known atom/s,
                   fn ->
                     WorkflowTypeResolver.resolve(activation, unique_queue())
                   end
    end
  end

  describe "resolve/2 — cached-state fallback" do
    test "uses State.workflow_module from the cache for incremental activations" do
      queue = unique_queue()
      run_id = "run-3-#{System.unique_integer([:positive])}"

      # Seed the cache as if a prior `initialize_workflow` activation had
      # already been processed and the evaluator persisted its state.
      seed_cache(queue, run_id, WorkflowC)

      activation = resolve_activity_only_activation(run_id)

      assert {:ok, WorkflowC} == WorkflowTypeResolver.resolve(activation, queue)
    end

    test "returns {:error, :sticky_cache_miss} when no init_workflow AND the cache is empty" do
      # Incremental (sticky) activation for a run this worker never cached:
      # both tiers exhausted. Recoverable — the resolver must NOT raise (that
      # crashed the poll loop in production); it signals the miss so the loop
      # fails the sticky task and forces a non-sticky, full-history replay.
      activation =
        resolve_activity_only_activation("run-uncached-#{System.unique_integer([:positive])}")

      assert {:error, :sticky_cache_miss} ==
               WorkflowTypeResolver.resolve(activation, unique_queue())
    end
  end

  describe "resolve/2 — initialize_workflow takes precedence over cache" do
    # Defense against a worker-restart edge case: if Core re-delivers a
    # full replay (`initialize_workflow` + history) AND the cache happens
    # to still hold an old entry for the same run_id, the activation's
    # workflow_type wins.
    test "activation's workflow_type wins over a different cached module" do
      queue = unique_queue()
      run_id = "run-precedence-#{System.unique_integer([:positive])}"

      seed_cache(queue, run_id, WorkflowA)

      activation = init_activation(run_id, Atom.to_string(WorkflowB))

      assert {:ok, WorkflowB} == WorkflowTypeResolver.resolve(activation, queue)
    end
  end

  defp init_activation(run_id, workflow_type_name) do
    %WorkflowActivation{
      run_id: run_id,
      jobs: [
        %WorkflowActivationJob{
          variant:
            {:initialize_workflow,
             %InitializeWorkflow{
               workflow_type: workflow_type_name,
               workflow_id: "wfid-" <> run_id,
               arguments: []
             }}
        }
      ]
    }
  end

  defp resolve_activity_only_activation(run_id) do
    %WorkflowActivation{
      run_id: run_id,
      jobs: [
        %WorkflowActivationJob{
          variant: {:resolve_activity, %ResolveActivity{seq: 1, result: nil}}
        }
      ]
    }
  end

  defp seed_cache(task_queue, run_id, module) do
    state = %State{
      run_id: run_id,
      task_queue: task_queue,
      workflow_module: module
    }

    WorkflowStateCache.put(task_queue, run_id, state)
  end

  defp unique_queue, do: "wftr-#{System.unique_integer([:positive])}"
end
