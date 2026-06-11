defmodule Hourglass.Worker.WorkflowTypeResolver do
  @moduledoc """
  Maps a Temporal Core `WorkflowActivation` to the Elixir workflow
  module that should evaluate it.

  Two-tier lookup, in order:

    1. **Structural resolution from the activation's `initialize_workflow`
       job.** Every fresh start, and every replay-from-history delivery to a
       worker that doesn't yet have the workflow cached, includes an
       `InitializeWorkflow` job whose `workflow_type` field is the module's
       stringified atom (e.g. `"Elixir.Hourglass.Workflows.IngestSource"`).
       We recover the module atom via `String.to_existing_atom/1` and verify
       it is a loaded Hourglass workflow by checking for the
       `__workflow_input_type__/0` marker injected by `use Hourglass.Workflow`.
       No registration or module list is required — the type name IS the module.

    2. **From the cached `Workflow.State`.** Subsequent incremental
       activations (e.g. just a `resolve_activity` job) don't repeat the
       workflow type. By the time we receive one, the prior activation
       has already populated `state.workflow_module` via
       `Hourglass.Workflow.Evaluator.evaluate/3`. We look up
       `WorkflowStateCache.fetch(task_queue, run_id)` and return the
       stored module.

  Both tiers fall through if exhausted; the function raises a clear
  error rather than guessing.
  """

  alias Coresdk.WorkflowActivation.WorkflowActivation
  alias Hourglass.Worker.WorkflowStateCache
  alias Hourglass.Workflow.State

  @doc """
  Resolve the workflow module for an activation.

  See moduledoc for lookup order. Raises with a descriptive message
  when neither tier yields a module — that case indicates either an
  unknown/unloaded workflow type or a Core/cache invariant violation
  (e.g. an incremental activation arriving before any
  `initialize_workflow` ever populated the state cache).
  """
  @spec resolve(WorkflowActivation.t(), String.t()) :: module()
  def resolve(%WorkflowActivation{} = activation, task_queue)
      when is_binary(task_queue) do
    cond do
      type_name = workflow_type_from_activation(activation) ->
        resolve_structural!(type_name, task_queue, activation.run_id)

      cached = cached_module(task_queue, activation.run_id) ->
        cached

      true ->
        raise """
        Cannot resolve workflow module for activation
        (task_queue=#{task_queue}, run_id=#{activation.run_id}).
        Activation has no `initialize_workflow` job and the
        WorkflowStateCache has no entry — both lookup tiers exhausted.
        """
    end
  end

  # Resolves a workflow_type name string to a loaded Hourglass workflow module.
  # The wire format is `Atom.to_string(module)`, i.e. "Elixir.My.Workflow".
  # Raises a clear error if the type cannot be parsed, the atom is unknown in
  # this VM, or the resolved module is not a loaded Hourglass workflow.
  defp resolve_structural!(type_name, task_queue, run_id) do
    case parse_workflow_module(type_name) do
      :bad_format ->
        raise """
        Workflow type #{inspect(type_name)} could not be parsed as an Elixir module atom
        (task_queue=#{task_queue}, run_id=#{run_id}).
        The type name must have the form "Elixir.Module.Name" (Atom.to_string(module)).
        """

      :unknown_atom ->
        raise """
        Workflow type #{inspect(type_name)} is not a known atom in this VM
        (task_queue=#{task_queue}, run_id=#{run_id}).
        The module may not be compiled or loaded. Ensure it is reachable from this worker.
        """

      {:ok, mod} ->
        if Code.ensure_loaded?(mod) and function_exported?(mod, :__workflow_input_type__, 0) do
          mod
        else
          raise """
          Workflow type #{inspect(type_name)} did not resolve to a loaded Hourglass workflow
          (task_queue=#{task_queue}, run_id=#{run_id}).
          #{inspect(mod)} is either not loaded or missing __workflow_input_type__/0 —
          ensure the module is compiled and uses `use Hourglass.Workflow`.
          """
        end
    end
  end

  # Parses a workflow_type wire string to an Elixir module atom.
  # Returns `{:ok, atom}`, `:bad_format` (no "Elixir." prefix), or
  # `:unknown_atom` (atom not yet interned in this VM).
  defp parse_workflow_module(type_name) do
    case type_name do
      "Elixir." <> _rest -> {:ok, String.to_existing_atom(type_name)}
      _other -> :bad_format
    end
  rescue
    ArgumentError -> :unknown_atom
  end

  defp workflow_type_from_activation(%WorkflowActivation{jobs: jobs}) when is_list(jobs) do
    Enum.find_value(jobs, fn
      %{variant: {:initialize_workflow, %{workflow_type: name}}} when is_binary(name) -> name
      _job -> nil
    end)
  end

  defp cached_module(task_queue, run_id) do
    case WorkflowStateCache.fetch(task_queue, run_id) do
      %State{workflow_module: module} when module != nil -> module
      _other -> nil
    end
  end
end
