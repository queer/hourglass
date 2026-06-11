defmodule Hourglass.Worker.WorkflowStateCache do
  @moduledoc """
  Per-`(task_queue, run_id)` cache of `Hourglass.Workflow.State` between
  activations.

  Each ephemeral `Hourglass.Worker.WorkflowEvaluator` Task reads the
  cached state on entry and writes the post-activation state back before
  exiting `:normal`. Subsequent activations for the same workflow run
  pick up the seq counters + resolver tables, so re-execution recognises
  previously-scheduled commands and matches them against incoming
  `resolve_activity` jobs.

  Replaces the per-run-id `Hourglass.WorkflowRunner` GenServer that
  used to hold this state in its own struct. Lives in ETS instead so
  evaluator Tasks stay ephemeral (no GenServer lifecycle bound to the run)
  and Core can redeliver an activation to a fresh evaluator without losing
  state.

  ## Why this cache must exist (and the design doc was wrong)

  An earlier design pinned "no ETS holds it for workflow logic — state
  that spans activations is reconstructed by replaying the activation
  jobs Core redelivers from `initialize_workflow` forward." That pin
  was based on a wrong reading of Core's delivery model. In *sticky-cache* mode Core
  delivers **incremental** activations to the worker — only the new
  `resolve_activity` jobs since the previous activation. Only on
  cache-miss / cold worker does Core send the full replay starting at
  `initialize_workflow`.

  An evaluator that starts each activation from `State.new/2` and
  walks only the activation's own jobs sees just the delta, drops
  prior `resolved_results`, and re-suspends at the first activity on
  every retry — wrong. The worker MUST mirror Core's sticky cache by
  caching `State` across activations; this module is that mirror.
  Every other Temporal SDK (Python, Go, Java) has the equivalent. The
  design doc is being treated as a historical artifact on this
  specific point until updated.

  The `Hourglass.Replayer` is the correct counter-example to
  this pattern: it threads `state` through args by-call because it
  feeds Core a captured *History* (full replay every poll, no
  sticky-cache mode). Production workers don't get that.

  ## Eviction

  Terminal states (`{:completed, _}` / `{:failed, _}`) are still cached so
  redelivered terminal activations can re-emit the same completion proto;
  Temporal Server treats the duplicate as a no-op. Entries also clear when
  Core sends `remove_from_cache` (the evaluator returns `:evict` and the
  poll loop deletes the entry).

  ## Non-goal

  This module is data-only. It does not own the ETS table's lifecycle —
  `ensure_table/0` is called from `Hourglass.Application.start/2` at boot
  and may also be called from test setup; it is idempotent and safe to
  invoke more than once. No GenServer mailbox in the hot path; readers +
  writers go directly through ETS.
  """

  alias Hourglass.Workflow.State

  @table :hourglass_workflow_state_cache

  @typedoc "ETS key: `{task_queue, run_id}`."
  @type key :: {String.t(), String.t()}

  @doc """
  Create the shared ETS table if it does not already exist. Idempotent:
  safe to call from both `Hourglass.Application.start/2` and test setup
  without raising. Returns `:ok` regardless of whether the table was
  just created or was already present.
  """
  @spec ensure_table() :: :ok
  def ensure_table do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [
        :named_table,
        :public,
        :set,
        read_concurrency: true,
        write_concurrency: true
      ])
    end

    :ok
  end

  @doc """
  Fetch the cached state for `(task_queue, run_id)`, or `nil` if no state
  exists yet (first activation for this run, or post-eviction).
  """
  @spec fetch(String.t(), String.t()) :: State.t() | nil
  def fetch(task_queue, run_id) when is_binary(task_queue) and is_binary(run_id) do
    case :ets.lookup(@table, {task_queue, run_id}) do
      [{_key, %State{} = state}] -> state
      [] -> nil
    end
  end

  @doc """
  Store the post-activation state for `(task_queue, run_id)`.
  """
  @spec put(String.t(), String.t(), State.t()) :: :ok
  def put(task_queue, run_id, %State{} = state)
      when is_binary(task_queue) and is_binary(run_id) do
    true = :ets.insert(@table, {{task_queue, run_id}, state})
    :ok
  end

  @doc """
  Remove the cache entry for `(task_queue, run_id)`. Called when the
  evaluator returns `:evict` (Core has dropped this run from its sticky
  cache).
  """
  @spec delete(String.t(), String.t()) :: :ok
  def delete(task_queue, run_id) when is_binary(task_queue) and is_binary(run_id) do
    true = :ets.delete(@table, {task_queue, run_id})
    :ok
  end
end
