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

  When both tiers are exhausted the function returns
  `{:error, :sticky_cache_miss}` rather than raising. That case is a
  *recoverable* Core/cache desync: an incremental (sticky) activation
  arrived for a run the worker never cached (or evicted). It is NOT a
  bug in the activation — a non-sticky full-history redelivery of the
  same run always carries `initialize_workflow` (tier 1), so the caller
  recovers by failing the sticky workflow task to force that redelivery.
  See `Hourglass.Worker.WorkflowPollLoop` for the recovery.

  A workflow-type name that IS present but cannot be turned into a
  loaded Hourglass workflow (bad format / unknown atom / not a workflow
  module) still raises: replaying won't change the type name, so that is
  a genuine deploy/coding error, not a transient desync.
  """

  alias Coresdk.WorkflowActivation.WorkflowActivation
  alias Hourglass.Worker.WorkflowStateCache
  alias Hourglass.Workflow.State

  @typedoc """
  Result of `resolve/2`: the resolved module, or the recoverable
  sticky-cache-miss signal the poll loop turns into a workflow-task
  failure (forcing a non-sticky, full-history redelivery).
  """
  @type result :: {:ok, module()} | {:error, :sticky_cache_miss}

  @doc """
  Resolve the workflow module for an activation.

  See moduledoc for lookup order. Returns `{:ok, module}` on success and
  `{:error, :sticky_cache_miss}` when neither tier yields a module — the
  recoverable "incremental activation for an uncached run" case. Raises
  only when a workflow-type name is present but unresolvable (unknown or
  unloaded module), which no redelivery can fix.
  """
  @spec resolve(WorkflowActivation.t(), String.t()) :: result()
  def resolve(%WorkflowActivation{} = activation, task_queue)
      when is_binary(task_queue) do
    cond do
      type_name = workflow_type_from_activation(activation) ->
        {:ok, resolve_structural!(type_name, task_queue, activation.run_id)}

      cached = cached_module(task_queue, activation.run_id) ->
        {:ok, cached}

      true ->
        # Both tiers exhausted: a sticky (incremental) activation for a
        # run this worker has no cached State for. Recoverable — signal
        # the caller to fail the sticky task so Core redelivers with full
        # history (which carries `initialize_workflow` → tier 1).
        {:error, :sticky_cache_miss}
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
