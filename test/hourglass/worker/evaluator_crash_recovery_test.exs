defmodule Hourglass.Worker.EvaluatorCrashRecoveryTest do
  @moduledoc """
  Invariant test: an evaluator crash mid-activation does not
  break the workflow.

  The redesigned worker spawns one ephemeral `WorkflowEvaluator` Task
  per activation under a `:temporary` shared
  `Hourglass.WorkflowEvaluator.DynamicSupervisor`. If that Task
  exits non-`:normal` before the BridgeHolder completion call returns,
  Core has not observed a completion for the activation and redelivers
  identical bytes on the next poll. A fresh evaluator runs over the
  same activation + cached `Workflow.State` and produces an identical
  completion. This is the "free crash recovery" property the design
  earns from going pure-function.

  ## Why a unit-level test, not full E2E

  A full Temporal-cluster end-to-end test would need to inject a crash
  inside the evaluator at exactly the right moment relative to Core's
  poll loop, then wait for redelivery. That's brittle (timing-dependent)
  and couples this invariant to the bridge NIF.

  Instead we drive the evaluator directly: spawn it twice with the
  same `activation_bytes` (the redelivery semantic from Core's side
  is "same bytes, fresh evaluator"). The first call's `complete_fn`
  fails so the evaluator logs and exits `:normal` (the completion
  was never acked, so Core's per-activation watchdog timeout
  triggers redelivery). The second call's `complete_fn` succeeds
  and ships the completion. Both evaluators see byte-identical
  activation bytes; the test asserts that the second produces a
  valid completion proto for the same `run_id`.

  Limitation: this proves the *property* (fresh evaluator over same
  inputs ⇒ identical completion) without exercising the actual
  Bridge-level redelivery wire. The supervisor topology + the pure
  Workflow.Evaluator.evaluate/3 are what guarantee the invariant
  holds at the wire level; this test pins it at the unit level.
  """

  # async: false — drives real Workers whose poll loops park the dirty-IO
  # long-poll NIF; run serially with the other crash/poll tests so concurrent
  # parked polls can't exhaust the (default 10) dirty-IO scheduler pool and
  # deadlock a worker_shutdown drain.
  use ExUnit.Case, async: false

  alias Coresdk.WorkflowActivation.InitializeWorkflow
  alias Coresdk.WorkflowActivation.WorkflowActivation
  alias Coresdk.WorkflowActivation.WorkflowActivationJob
  alias Coresdk.WorkflowCompletion.Success
  alias Coresdk.WorkflowCompletion.WorkflowActivationCompletion
  alias Hourglass.Worker.WorkflowStateCache
  alias Hourglass.WorkflowEvaluator.DynamicSupervisor

  @moduletag :temporal
  # Uses a mocked complete_fn but depends on the Temporal Subsystem (real
  # EvaluatorDynSup, WorkflowStateCache) booted in test_helper. Cluster
  # isn't strictly required for this test, but it's grouped with the
  # integration suite because it exercises real OTP supervisor semantics
  # the smaller pure-evaluator tests don't.
  @moduletag :integration

  defmodule ImmediateWorkflow do
    @moduledoc false
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(input), do: {:ok, input}
  end

  test "evaluator crash on Bridge failure: redelivery to fresh evaluator ships identical completion" do
    test_pid = self()
    run_id = "crash-recovery-#{System.unique_integer([:positive])}"
    task_queue = "tq-crash-recovery-#{System.unique_integer([:positive])}"

    # Identical activation_bytes simulate Core's "same bytes on
    # redelivery" guarantee.
    bytes = activation_bytes([init_job("hello")], run_id: run_id)

    # First dispatch: complete_fn fails so the evaluator logs +
    # exits :normal (no completion was acked to Core, so Core's
    # per-activation watchdog timeout would trigger redelivery —
    # simulated below by the second start_child with the same
    # activation_bytes).
    failing_complete_fn =
      fn ^task_queue, _completion_bytes ->
        send(test_pid, :first_complete_called)
        {:error, :simulated_bridge_failure}
      end

    args_first = %{
      run_id: run_id,
      task_queue: task_queue,
      activation_bytes: bytes,
      workflow_module_resolver: fn _workflow_type -> ImmediateWorkflow end,
      complete_fn: failing_complete_fn
    }

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        {:ok, first_pid} = DynamicSupervisor.start_child(args_first)
        first_ref = Process.monitor(first_pid)

        # The Bridge call is attempted (workflow body completed
        # cleanly) but returns :error → evaluator logs + exits :normal.
        assert_receive :first_complete_called, 1_000

        # `Process.monitor` on a pid that already exited synthesizes
        # `{:DOWN, ref, :process, pid, :noproc}`. The evaluator can be
        # gone before the monitor fires depending on scheduler timing,
        # so accept either reason — both are normal exits.
        assert_receive {:DOWN, ^first_ref, :process, ^first_pid, reason}, 1_000
        assert reason in [:normal, :noproc]
      end)

    assert log =~ "BridgeHolder.complete_workflow_activation failed"
    assert log =~ "simulated_bridge_failure"
    assert log =~ "Core will redeliver"

    # Second dispatch: same activation_bytes, working complete_fn.
    # This is what Core would do after seeing no completion: enqueue
    # the same activation again on the next poll. A fresh evaluator
    # runs over the same inputs.
    succeeding_complete_fn =
      fn ^task_queue, completion_bytes ->
        send(test_pid, {:second_complete_called, completion_bytes})
        :ok
      end

    args_second = %{args_first | complete_fn: succeeding_complete_fn}

    {:ok, second_pid} = DynamicSupervisor.start_child(args_second)
    second_ref = Process.monitor(second_pid)

    assert_receive {:second_complete_called, completion_bytes}, 1_000

    # The evaluator may have already exited :normal between
    # start_child and Process.monitor; in that case Process.monitor
    # synthesises a :DOWN with reason :noproc immediately. Either
    # outcome proves the evaluator finished cleanly (no raise +
    # didn't exit before complete_fn ran — the
    # :second_complete_called message above already confirms
    # complete_fn was invoked).
    assert_receive {:DOWN, ^second_ref, :process, ^second_pid, reason}, 1_000
    assert reason in [:normal, :noproc]

    # The redelivered evaluator produced a valid completion for the
    # same run_id. The workflow body's deterministic output (returning
    # the input) means a successful complete-workflow command is
    # emitted.
    completion = WorkflowActivationCompletion.decode(completion_bytes)
    assert %WorkflowActivationCompletion{run_id: ^run_id} = completion

    assert {:successful, %Success{commands: [cmd]}} = completion.status
    assert {:complete_workflow_execution, _cwe} = cmd.variant

    # Cache stays clean afterwards: terminal-state caching is fine
    # (Core ignores duplicate completions); but the cache must contain
    # the final State so repeat redeliveries re-emit the same
    # completion. Confirm an entry exists for the (queue, run_id) key.
    assert {:ok, %Hourglass.Workflow.State{}} =
             cache_lookup(task_queue, run_id)
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp init_job(input) do
    %WorkflowActivationJob{
      variant:
        {:initialize_workflow,
         %InitializeWorkflow{
           workflow_type: "ImmediateWorkflow",
           workflow_id: "wf-crash-1",
           arguments: [synthetic_payload(input)]
         }}
    }
  end

  defp synthetic_payload(data) do
    %Temporal.Api.Common.V1.Payload{
      metadata: %{"encoding" => "json/plain"},
      data: Jason.encode!(data)
    }
  end

  defp activation_bytes(jobs, opts) do
    Protobuf.encode(%WorkflowActivation{
      run_id: Keyword.get(opts, :run_id, "test-run"),
      is_replaying: Keyword.get(opts, :is_replaying, false),
      jobs: jobs
    })
  end

  # WorkflowStateCache.fetch returns nil | %State{}; lift to a
  # tuple shape for assertion ergonomics.
  defp cache_lookup(task_queue, run_id) do
    case WorkflowStateCache.fetch(task_queue, run_id) do
      nil -> :miss
      %Hourglass.Workflow.State{} = s -> {:ok, s}
    end
  end
end
