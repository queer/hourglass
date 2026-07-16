defmodule Hourglass.Workflow.State do
  @moduledoc """
  Per-run state threaded through `Hourglass.Workflow.Evaluator.evaluate/3`.

  Across activations:

    * `next_global_seq` — monotonic SDK seq counter; persists across
      activations (Temporal requires monotonic seqs per workflow lifetime).
    * `pending_resolvers` — `%{global_seq => command_id}` registered when
      a command was scheduled. The next activation's `resolve_activity{seq}`
      uses this to map back to the command_id.
    * `command_id_to_seq` — inverse of `pending_resolvers`. Lets re-execution
      look up "have I seen this command_id before? what seq did it get?"
    * `resolved_results` — `%{global_seq => decoded_value}` populated from
      prior `resolve_activity` jobs delivered on this or earlier activations.
    * `child_starts` — `%{global_seq => start_resolution}` populated from
      `resolve_child_workflow_execution_start` jobs. A child workflow issues one
      command but resolves **twice** (start, then result); the result reuses the
      `resolved_results` slot for its seq, so the start phase needs its own map.
      `{:started, run_id} | {:start_failed, cause} | {:start_cancelled, failure}`.
    * `input` — the decoded `initialize_workflow` first argument; set once.
    * `workflow_module` — the workflow module bound to this run (e.g.
      `MyApp.Workflows.IngestSource`). Set by
      `Hourglass.Workflow.Evaluator.evaluate/3` on the first
      activation and persisted across cache round-trips so subsequent
      activations (e.g. `resolve_activity` deliveries that don't carry
      `initialize_workflow`) can be routed to the same module by
      the worker's workflow type resolver.

  Per-activation (rebuilt each call):

    * `commands` — newest-first list of `{command_id, command_term}` issued
      during this activation's re-execution. Sorted + assigned seqs at the
      end of `evaluate/3`.
    * `result` — `nil | {:completed, term} | {:failed, term} | :evict`
      determines the completion shape.
    * `child_count` — counter that replaces the `expected_children` MapSet.
      The pure-function evaluator runs `async/1` inline, so it has no child
      pids to track; the counter only exists for diagnostics.
  """

  alias Coresdk.WorkflowCommands.WorkflowCommand

  @typedoc """
  A workflow command_id: a tuple `{child_index_path, local_seq}` deterministic
  per workflow re-run. See `Hourglass.Workflow.CommandAccumulator`.
  """
  @type command_id :: {[non_neg_integer()], non_neg_integer()}

  @type command_term ::
          {:execute_activity, %{module: module(), args: term(), options: keyword()}}
          | {:start_child,
             %{module: module(), args: term(), options: keyword(), workflow_id: String.t()}}
          | {:start_timer, %{duration_ms: non_neg_integer()}}
          | {:uuid, map()}
          | {:random, %{max: pos_integer()}}

  @type command_entry :: {command_id(), command_term()}

  @type result :: nil | {:completed, term()} | {:failed, term()} | :evict | :continue_as_new

  @type t :: %__MODULE__{
          run_id: String.t() | nil,
          task_queue: String.t(),
          input: term(),
          workflow_module: module() | nil,
          next_global_seq: pos_integer(),
          resolved_results: %{optional(pos_integer()) => term()},
          child_starts: %{optional(pos_integer()) => term()},
          pending_resolvers: %{optional(pos_integer()) => command_id()},
          command_id_to_seq: %{optional(command_id()) => pos_integer()},
          commands: [command_entry()],
          result: result(),
          child_count: non_neg_integer(),
          signals: %{optional(String.t()) => [term()]},
          cancel_requested: boolean()
        }

  defstruct run_id: nil,
            task_queue: "",
            input: nil,
            workflow_module: nil,
            next_global_seq: 1,
            resolved_results: %{},
            child_starts: %{},
            pending_resolvers: %{},
            command_id_to_seq: %{},
            commands: [],
            result: nil,
            child_count: 0,
            signals: %{},
            cancel_requested: false

  @doc """
  Build the initial state for a fresh workflow (no prior activations).
  """
  @spec new(String.t(), String.t()) :: t()
  def new(run_id, task_queue) when is_binary(run_id) do
    %__MODULE__{run_id: run_id, task_queue: task_queue || ""}
  end

  @doc """
  After a flush, register the newly-assigned `{seq => command_id}` resolvers
  and bump `next_global_seq`. Also extends `command_id_to_seq` (the inverse
  index used by re-execution to recognise already-scheduled commands).

  `assignments` is `[{seq, command_id, _command_term}]` in seq order.
  """
  @spec register_assignments(t(), [{pos_integer(), command_id(), command_term()}]) :: t()
  def register_assignments(%__MODULE__{} = state, assignments) when is_list(assignments) do
    {pending, lookup, max_seq} =
      Enum.reduce(assignments, {state.pending_resolvers, state.command_id_to_seq, 0}, fn
        {seq, command_id, _cmd}, {pend, look, max_seq} ->
          {Map.put(pend, seq, command_id), Map.put(look, command_id, seq), max(max_seq, seq)}
      end)

    next_seq = max(state.next_global_seq, max_seq + 1)

    %{state | pending_resolvers: pending, command_id_to_seq: lookup, next_global_seq: next_seq}
  end

  @doc """
  Type alias used by callers — the proto module the evaluator emits.
  """
  @spec command_proto_module() :: module()
  def command_proto_module, do: WorkflowCommand
end
