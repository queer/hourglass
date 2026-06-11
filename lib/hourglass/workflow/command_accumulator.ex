defmodule Hourglass.Workflow.CommandAccumulator do
  @moduledoc """
  Process-dict-keyed mutable accumulator that the pure-function evaluator
  uses while re-executing a workflow body.

  The body runs inline on the evaluator process; the dict carries:

    * `:hourglass_temporal_commands_acc` — newest-first list of
      `{command_id, command_term}` issued during this activation.
    * `:hourglass_temporal_child_index_path` — the lex-path identifying the
      currently-active scope (`[]` for the main body; `[0]`, `[1]`, ...
      for direct async children; `[1, 0]` etc. for nested asyncs).
    * `:hourglass_temporal_next_child_index` — local counter for the next async
      child's path component within the current scope.
    * `:hourglass_temporal_local_seq` — local monotonic counter for the current
      scope; bumped by every `next_command_id/0`.
  """

  alias Hourglass.Workflow.State

  @commands_key :hourglass_temporal_commands_acc
  @path_key :hourglass_temporal_child_index_path
  @next_child_key :hourglass_temporal_next_child_index
  @local_seq_key :hourglass_temporal_local_seq
  @evaluator_state_key :hourglass_temporal_evaluator_state
  @signal_index_key :hourglass_temporal_signal_index

  @doc """
  Initialise the dict for the main workflow body. Wipes any prior state on
  the calling process.
  """
  @spec init() :: :ok
  def init do
    Process.put(@commands_key, [])
    Process.put(@path_key, [])
    Process.put(@next_child_key, 0)
    Process.put(@local_seq_key, 0)
    Process.put(@signal_index_key, %{})
    :ok
  end

  @doc """
  Allocate the next deterministic command_id `{path, local_seq}` for the
  current scope. Bumps the scope's `local_seq`.
  """
  @spec next_command_id() :: State.command_id()
  def next_command_id do
    path = Process.get(@path_key, [])
    next_seq = (Process.get(@local_seq_key, 0) || 0) + 1
    Process.put(@local_seq_key, next_seq)
    {path, next_seq}
  end

  @doc """
  Append `{command_id, command_term}` to the per-process command list
  (newest-first).
  """
  @spec append_command(State.command_id(), State.command_term()) :: :ok
  def append_command(command_id, command_term) do
    list = Process.get(@commands_key, [])
    Process.put(@commands_key, [{command_id, command_term} | list])
    :ok
  end

  @doc """
  Take the accumulated commands (newest-first) and clear the list.
  """
  @spec take_commands() :: [State.command_entry()]
  def take_commands do
    list = Process.get(@commands_key, [])
    Process.put(@commands_key, [])
    list
  end

  @doc """
  Push a new async-child scope onto the path stack. Returns a `frame` that
  must be passed to `exit_async_child/1` to restore the parent's scope.

  This is the inline equivalent of spawning a child Task: the child body
  runs on the same BEAM process but with a child-scoped dict.
  """
  @spec enter_async_child() :: %{
          path: [non_neg_integer()],
          next_child: non_neg_integer(),
          local_seq: non_neg_integer()
        }
  def enter_async_child do
    parent_path = Process.get(@path_key, [])
    parent_next_child = Process.get(@next_child_key, 0) || 0
    parent_local_seq = Process.get(@local_seq_key, 0) || 0

    # The path is a short async-nesting prefix (depth 1-3) and the new child
    # index must be appended in order; `++` is correct and cheap at this size.
    # credo:disable-for-next-line Credo.Check.Refactor.AppendSingleItem
    child_path = parent_path ++ [parent_next_child]

    Process.put(@path_key, child_path)
    Process.put(@next_child_key, 0)
    Process.put(@local_seq_key, 0)

    %{path: parent_path, next_child: parent_next_child + 1, local_seq: parent_local_seq}
  end

  @doc """
  Pop a child scope frame (pushed by `enter_async_child/0`) and restore the
  parent scope's path / counters. The parent's `next_child` is bumped so
  the next sibling gets a distinct path component.
  """
  @spec exit_async_child(%{
          path: [non_neg_integer()],
          next_child: non_neg_integer(),
          local_seq: non_neg_integer()
        }) :: :ok
  def exit_async_child(%{path: path, next_child: next_child, local_seq: local_seq}) do
    Process.put(@path_key, path)
    Process.put(@next_child_key, next_child)
    Process.put(@local_seq_key, local_seq)
    :ok
  end

  @doc """
  Wipe all accumulator keys from the calling process's dict. Called on the
  evaluator's exit boundary so a re-used process doesn't leak state.
  """
  @spec clear() :: :ok
  def clear do
    Process.delete(@commands_key)
    Process.delete(@path_key)
    Process.delete(@next_child_key)
    Process.delete(@local_seq_key)
    Process.delete(@signal_index_key)
    :ok
  end

  @doc "Per-execution Nth-call index for await_signal(name); resets each activation via init/0."
  @spec next_signal_index(String.t()) :: non_neg_integer()
  def next_signal_index(name) do
    counts = Process.get(@signal_index_key, %{})
    k = Map.get(counts, name, 0)
    Process.put(@signal_index_key, Map.put(counts, name, k + 1))
    k
  end

  @doc """
  Mark that the evaluator is currently active on this process. The Workflow
  API primitives consult this state to resolve commands.
  """
  @spec mark_evaluator_active(Hourglass.Workflow.State.t()) :: :ok
  def mark_evaluator_active(%State{} = state) do
    Process.put(@evaluator_state_key, state)
    :ok
  end

  @doc """
  Read the evaluator state from the calling process's dict, or `nil` when
  no evaluator is active.
  """
  @spec evaluator_state() :: State.t() | nil
  def evaluator_state do
    Process.get(@evaluator_state_key)
  end

  @doc """
  Clear the evaluator-active marker.
  """
  @spec unmark_evaluator() :: :ok
  def unmark_evaluator do
    Process.delete(@evaluator_state_key)
    :ok
  end
end
