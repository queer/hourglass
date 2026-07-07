defmodule Hourglass.Worker.WorkflowPollLoopRecoveryTest do
  # async: true — no cluster, no bridge. The sticky-cache-miss recovery is
  # exercised through `WorkflowPollLoop.dispatch/2` with an injected
  # `complete_fn` stub, so nothing here touches Temporal Core. The
  # `WorkflowStateCache` ETS table is started by `Hourglass.Application`.
  use ExUnit.Case, async: true

  alias Coresdk.WorkflowActivation.ResolveActivity
  alias Coresdk.WorkflowActivation.WorkflowActivation
  alias Coresdk.WorkflowActivation.WorkflowActivationJob
  alias Coresdk.WorkflowCompletion.WorkflowActivationCompletion
  alias Hourglass.Worker.WorkflowPollLoop

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
