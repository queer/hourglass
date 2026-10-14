defmodule Hourglass.Worker.WorkflowPollLoopRecoveryTest do
  # async: true — no cluster, no bridge. The sticky-cache-miss recovery is
  # exercised through `WorkflowPollLoop.dispatch/2` with an injected
  # `complete_fn` stub, so nothing here touches Temporal Core. The
  # `WorkflowStateCache` ETS table is started by `Hourglass.Application`.
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Coresdk.WorkflowActivation.RemoveFromCache
  alias Coresdk.WorkflowActivation.ResolveActivity
  alias Coresdk.WorkflowActivation.WorkflowActivation
  alias Coresdk.WorkflowActivation.WorkflowActivationJob
  alias Coresdk.WorkflowCompletion.Success
  alias Coresdk.WorkflowCompletion.WorkflowActivationCompletion
  alias Hourglass.Worker.WorkflowPollLoop
  alias Hourglass.Worker.WorkflowStateCache
  alias Hourglass.Workflow.State

  require Logger

  describe "dispatch/2 — sticky-cache-miss recovery" do
    test "ships a failed workflow-task completion instead of raising" do
      # An incremental (sticky) activation: only a `resolve_activity` job, no
      # `initialize_workflow`, and no `WorkflowStateCache` entry for this
      # run_id (unique, never seeded). Both resolver tiers are exhausted.
      run_id = "recover-#{System.unique_integer([:positive])}"
      queue = "wfp-recover-#{System.unique_integer([:positive])}"

      bytes = encode_incremental_activation(run_id)
      state = %{task_queue: queue, complete_fn: capture_complete_fn(self())}

      # Must NOT raise (the historical bug killed the poll loop here).
      assert :ok = WorkflowPollLoop.dispatch(bytes, state)

      # The recovery shipped a *failed* completion for exactly this run so
      # Core evicts the run and re-delivers non-sticky with full history.
      assert_received {:completion, ^queue, completion_bytes}
      completion = WorkflowActivationCompletion.decode(completion_bytes)

      assert completion.run_id == run_id
      assert {:failed, failure} = completion.status
      assert failure.failure.message =~ "sticky-cache miss"
    end

    test "survives (still returns :ok) when shipping the task failure errors" do
      # Even if the completion can't be shipped (e.g. worker unregistered
      # mid-flight), dispatch must return :ok so `loop/1` keeps polling —
      # Core's sticky-task timeout then forces the same non-sticky redelivery.
      run_id = "recover-err-#{System.unique_integer([:positive])}"
      queue = "wfp-recover-err-#{System.unique_integer([:positive])}"

      bytes = encode_incremental_activation(run_id)
      failing_complete_fn = fn _tq, _bytes -> {:error, :worker_not_registered} end
      state = %{task_queue: queue, complete_fn: failing_complete_fn}

      assert :ok = WorkflowPollLoop.dispatch(bytes, state)
    end
  end

  describe "dispatch/2 — an eviction of a run the worker holds no state for" do
    # Core evicts every run it sees end, a continued-as-new one included, and
    # sends evictions for runs it never had cached. The worker drops a
    # continued-as-new run's state when the run continues, so that eviction
    # finds nothing cached. There is nothing to replay: it is no cache miss.
    test "is acked with an empty success, not failed as a sticky-cache miss" do
      run_id = "evict-uncached-#{System.unique_integer([:positive])}"
      queue = "wfp-evict-#{System.unique_integer([:positive])}"
      bytes = encode_eviction(run_id)
      state = %{task_queue: queue, complete_fn: capture_complete_fn(self())}

      log =
        capture_log([level: :debug], fn ->
          assert :ok = WorkflowPollLoop.dispatch(bytes, state)
          # A miss is logged at info: prove such a line is captured here.
          Logger.info("info probe for #{queue}")
        end)

      assert_received {:completion, ^queue, completion_bytes}
      completion = WorkflowActivationCompletion.decode(completion_bytes)

      assert completion.run_id == run_id
      assert {:successful, %Success{commands: []}} = completion.status
      assert log =~ "info probe for #{queue}"
      refute log =~ run_id
    end

    test "drops any state still cached for the run" do
      run_id = "evict-stale-#{System.unique_integer([:positive])}"
      queue = "wfp-evict-stale-#{System.unique_integer([:positive])}"
      # A state no module was ever bound to resolves nothing.
      :ok = WorkflowStateCache.put(queue, run_id, State.new(run_id, queue))
      bytes = encode_eviction(run_id)
      state = %{task_queue: queue, complete_fn: capture_complete_fn(self())}

      assert :ok = WorkflowPollLoop.dispatch(bytes, state)

      assert_received {:completion, ^queue, _bytes}
      assert WorkflowStateCache.fetch(queue, run_id) == nil
    end

    test "survives (still returns :ok) when shipping the ack errors" do
      run_id = "evict-err-#{System.unique_integer([:positive])}"
      queue = "wfp-evict-err-#{System.unique_integer([:positive])}"
      failing_complete_fn = fn _tq, _bytes -> {:error, :worker_not_registered} end
      bytes = encode_eviction(run_id)
      state = %{task_queue: queue, complete_fn: failing_complete_fn}

      log =
        capture_log(fn ->
          assert :ok = WorkflowPollLoop.dispatch(bytes, state)
        end)

      assert log =~ "eviction ack could not be shipped"
      assert log =~ run_id
    end
  end

  defp encode_eviction(run_id) do
    Protobuf.encode(%WorkflowActivation{
      run_id: run_id,
      jobs: [
        %WorkflowActivationJob{
          variant:
            {:remove_from_cache,
             %RemoveFromCache{message: "Workflow completed", reason: :WORKFLOW_EXECUTION_ENDING}}
        }
      ]
    })
  end

  defp encode_incremental_activation(run_id) do
    Protobuf.encode(%WorkflowActivation{
      run_id: run_id,
      jobs: [
        %WorkflowActivationJob{
          variant: {:resolve_activity, %ResolveActivity{seq: 1, result: nil}}
        }
      ]
    })
  end

  defp capture_complete_fn(test_pid) do
    fn task_queue, completion_bytes ->
      send(test_pid, {:completion, task_queue, completion_bytes})
      :ok
    end
  end
end
