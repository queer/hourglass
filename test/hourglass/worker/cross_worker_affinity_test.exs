defmodule Hourglass.Worker.CrossWorkerAffinityTest do
  @moduledoc """
  Invariant test: independent `Worker` instances coexist
  side-by-side with no shared per-task-queue state.

  Bridge handle ownership was pulled out of the per-Worker tree and
  into the Application-level `Hourglass.BridgeHolder`. Evaluator and
  activity-executor Tasks now run under shared Application-level DynSups
  (`Hourglass.WorkflowEvaluator.DynamicSupervisor`,
  `Hourglass.ActivityExecutor.DynamicSupervisor`), not under
  individual Worker subtrees.

  This test exercises two invariants:

    1. **Independent task queues run concurrently to completion** —
       two Workers on distinct task queues each drive their own
       workflow with no interference.

    2. **WorkflowPollLoop crash does not destroy the bridge handle** —
       previously the BridgeHolder lived inside the Worker subtree
       under `:rest_for_one`, so a poll-loop crash cascade-restarted
       BridgeHolder and destroyed the bridge handle. Now BridgeHolder
       is at Application level and the handle survives intra-Worker
       crashes. The replacement poll loop resumes on the same handle.

  NOTE: All tests here are gated `:temporal` + `:integration` — they
  require a live Temporal cluster, the `Hourglass.TestSupport.{EchoActivity,
  EchoWorkflow}` fixtures, `Hourglass.Test.UseRealTemporalBackend`, and the
  `Hourglass` facade. They are excluded from the default `mix test` lane.
  """

  # async: false — kills WorkflowPollLoops parked in the dirty-IO long-poll NIF,
  # orphaning polls that hold dirty-IO scheduler threads until a worker_shutdown
  # drains them. Serial keeps the concurrent orphan count below the (default 10)
  # dirty-IO scheduler pool, which a concurrent run can otherwise exhaust+deadlock.
  use ExUnit.Case, async: false
  use Hourglass.Test.UseRealTemporalBackend

  # NOTE: Hourglass (facade), Hourglass.TestSupport.{EchoActivity, EchoWorkflow},
  # and UUIDv7 are T21/T23 modules. These aliases will resolve once T21+T23 are
  # complete. All tests below are :integration-tagged so they are excluded from
  # the default `mix test` run and will not fail during T20.
  alias Hourglass
  alias Hourglass.BridgeHolder
  alias Hourglass.TestSupport.EchoWorkflow
  alias Hourglass.Worker
  alias Hourglass.WorkflowHandle

  @moduletag :temporal
  # Server-semantic test — proves Temporal's per-task-queue routing under
  # concurrent workers + the poll-loop crash recovery invariant. Both
  # properties live in the real cluster; mocking defeats the point.
  # Default `mix test` excludes :integration.
  @moduletag :integration
  @moduletag timeout: 90_000
  # Kill-scenario tests Process.exit/2 :kill the poll loop on purpose;
  # BridgeHolder logs each recycle, plus the killed loop's OTP
  # termination line. Capture so the runner output stays clean —
  # logs surface on test failure.
  @moduletag capture_log: true

  test "two Workers on distinct task queues run concurrently to completion" do
    # Two Workers, two task queues, two workflows started concurrently.
    # Each Worker has its own bridge handle (registered via the shared
    # BridgeHolder); evaluator + executor Tasks are dispatched to the
    # shared Application-level DynSups but each ephemeral child only
    # ever touches the (run_id, task_queue) it was spawned for.
    queue_a = "cwa-#{System.unique_integer([:positive])}"
    queue_b = "cwb-#{System.unique_integer([:positive])}"

    {:ok, sup_a} =
      start_supervised(
        {Hourglass.Worker, task_queue: queue_a},
        id: :worker_a
      )

    {:ok, sup_b} =
      start_supervised(
        {Hourglass.Worker, task_queue: queue_b},
        id: :worker_b
      )

    assert is_pid(sup_a)
    assert is_pid(sup_b)
    refute sup_a == sup_b

    # Both queues registered with BridgeHolder.
    assert BridgeHolder.registered?(queue_a)
    assert BridgeHolder.registered?(queue_b)

    # Start both workflows before awaiting either; the two Workers
    # must process their respective activations in parallel without
    # one starving the other.
    nonce_a = UUIDv7.generate()
    nonce_b = UUIDv7.generate()

    {:ok, %WorkflowHandle{} = handle_a} =
      Hourglass.start(EchoWorkflow, %{nonce: nonce_a},
        id: "cwa-#{nonce_a}",
        task_queue: queue_a
      )

    {:ok, %WorkflowHandle{} = handle_b} =
      Hourglass.start(EchoWorkflow, %{nonce: nonce_b},
        id: "cwb-#{nonce_b}",
        task_queue: queue_b
      )

    {:ok, result_a} = Hourglass.result(handle_a, timeout: 30_000)
    {:ok, result_b} = Hourglass.result(handle_b, timeout: 30_000)

    assert result_a["echoed"]["nonce"] == nonce_a
    assert result_b["echoed"]["nonce"] == nonce_b
  end

  test "WorkflowPollLoop crash leaves the bridge handle registered" do
    # Bridge handle invariant: poll loops are children of the per-queue Worker;
    # the bridge handle is owned by the Application-level BridgeHolder.
    # When a poll loop crashes:
    #
    #   * The Worker's inner Supervisor restarts the loop under :one_for_one.
    #   * Worker.terminate/2 does NOT fire (only the child crashed; the
    #     Worker itself is still alive).
    #   * The BridgeHolder retains the worker_handle for `queue` — no
    #     re-registration storm, no NIF leak.
    #
    # Previously the BridgeHolder lived inside the Worker subtree under
    # `:rest_for_one`, so a poll-loop crash cascade-restarted the
    # BridgeHolder which destroyed the bridge handle and left in-flight
    # evaluator/executor Tasks holding stale references.
    #
    # NOTE: this test asserts the supervisor-topology invariant only
    # (handle survives, fresh poll loop boots). The end-to-end
    # property — a workflow started after the kill completes — is
    # covered by the `:integration`-tagged test below
    # (`workflow completes after WorkflowPollLoop kill`).
    # That property's bound is Core's redelivery semantic (heartbeat /
    # workflow-task timeout), which can exceed an `async: true`
    # unit-test budget; it lives on the slower E2E lane.
    queue = "cwa-inflight-#{System.unique_integer([:positive])}"

    {:ok, _gs} =
      start_supervised({Hourglass.Worker, task_queue: queue})

    # Drive an initial workflow to completion to confirm the worker is healthy.
    nonce = UUIDv7.generate()

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(EchoWorkflow, %{nonce: nonce},
        id: "cwa-survive-#{nonce}",
        task_queue: queue
      )

    {:ok, result} = Hourglass.result(handle, timeout: 30_000)
    assert result["echoed"]["nonce"] == nonce

    # Kill the WorkflowPollLoop. This is purely intra-Worker —
    # the bridge handle is in BridgeHolder, untouched.
    sup_pid = Worker.inner_supervisor(queue)

    workflow_loop_pid =
      sup_pid
      |> Supervisor.which_children()
      |> Enum.find_value(fn
        {Hourglass.Worker.WorkflowPollLoop, pid, _type, _modules} -> pid
        _child -> nil
      end)

    ref = Process.monitor(workflow_loop_pid)
    Process.exit(workflow_loop_pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^workflow_loop_pid, :killed}, 2_000

    # Bridge handle survives the poll-loop crash.
    # Previously this would have been false (BridgeHolder cascade-died).
    assert BridgeHolder.registered?(queue)

    # The inner Supervisor restarts the loop under :one_for_one. Confirm
    # a fresh WorkflowPollLoop pid replaces the killed one (poll loop is
    # ready to accept activations on the same registered handle).
    assert_eventually(fn ->
      sup_pid
      |> Supervisor.which_children()
      |> Enum.find_value(fn
        {Hourglass.Worker.WorkflowPollLoop, pid, _type, _modules}
        when is_pid(pid) and pid != workflow_loop_pid ->
          pid

        _child ->
          nil
      end)
    end)
  end

  # Integration-tagged: excluded from the default `mix test` lane via
  # `ExUnit.start(exclude: [:integration, ...])` in test_helper.exs.
  # Run with `mix test ... --include integration`.
  #
  # The "WorkflowPollLoop crash leaves the bridge handle registered"
  # test above asserts the supervisor-topology invariant in the unit
  # lane: bridge handle survives, fresh poll loop boots. This test
  # asserts the load-bearing E2E property: a workflow started after
  # the kill *completes*.
  #
  # BridgeHolder monitors every poll caller; on a :DOWN before the Task
  # replies, the holder recycles the bridge handle (`worker_shutdown`
  # drains the orphan poll, `worker_new` allocates a fresh handle).
  # The originally-affected workflow's activation gets redelivered by
  # Core to the new poll on the new handle.
  @tag :integration
  @tag timeout: 90_000
  test "workflow completes after WorkflowPollLoop kill" do
    queue = "cwa-recovery-#{System.unique_integer([:positive])}"

    {:ok, _gs} =
      start_supervised({Hourglass.Worker, task_queue: queue})

    # Drive an initial workflow to completion. Confirms the worker is
    # healthy and the poll loops are settled into a steady-state long
    # poll before we kill anything.
    nonce_1 = UUIDv7.generate()

    {:ok, %WorkflowHandle{} = handle_1} =
      Hourglass.start(EchoWorkflow, %{nonce: nonce_1},
        id: "cwa-recovery-1-#{nonce_1}",
        task_queue: queue
      )

    {:ok, result_1} = Hourglass.result(handle_1, timeout: 30_000)
    assert result_1["echoed"]["nonce"] == nonce_1

    # Snapshot the inner Supervisor + the live WorkflowPollLoop pid.
    sup_pid = Worker.inner_supervisor(queue)

    workflow_loop_pid =
      sup_pid
      |> Supervisor.which_children()
      |> Enum.find_value(fn
        {Hourglass.Worker.WorkflowPollLoop, pid, _type, _modules} -> pid
        _child -> nil
      end)

    assert is_pid(workflow_loop_pid)

    # Start workflow-2 BEFORE killing the poll loop. With Core's
    # default scheduling this maximises the chance that the bridge
    # has an activation parked on the long-poll Task at the moment
    # of the kill — exercising the orphan-Task / lost-reply recovery
    # path that the unit test cannot cover within its budget.
    nonce_2 = UUIDv7.generate()

    {:ok, %WorkflowHandle{} = handle_2} =
      Hourglass.start(EchoWorkflow, %{nonce: nonce_2},
        id: "cwa-recovery-2-#{nonce_2}",
        task_queue: queue
      )

    # Kill the WorkflowPollLoop. The bridge handle in the
    # Application-level BridgeHolder is untouched; the inner
    # Supervisor restarts the loop; the orphaned Task in
    # BridgeHolder.task_sup keeps running until the bridge delivers
    # to it (with the reply going to the dead pid).
    ref = Process.monitor(workflow_loop_pid)
    Process.exit(workflow_loop_pid, :kill)
    assert_receive {:DOWN, ^ref, :process, ^workflow_loop_pid, :killed}, 2_000

    # Bridge handle survives the kill (mirrored by the unit test above).
    # A fresh poll loop boots under the inner Supervisor's :one_for_one
    # strategy.
    assert BridgeHolder.registered?(queue)

    assert_eventually(
      fn ->
        sup_pid
        |> Supervisor.which_children()
        |> Enum.find_value(fn
          {Hourglass.Worker.WorkflowPollLoop, pid, _type, _modules}
          when is_pid(pid) and pid != workflow_loop_pid ->
            pid

          _child ->
            nil
        end)
      end,
      5_000
    )

    # The load-bearing assertion: workflow-2 completes end-to-end.
    # BridgeHolder observes the kill via Process.monitor on the poll
    # caller, recycles the handle (worker_shutdown + worker_new), and
    # the new poll loop polls against the fresh handle. Core redelivers
    # within seconds.
    {:ok, result_2} = Hourglass.result(handle_2, timeout: 60_000)
    assert result_2["echoed"]["nonce"] == nonce_2
  end

  defp assert_eventually(fun, timeout \\ 2_000, interval \\ 25) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(fun, deadline, interval)
  end

  defp do_assert_eventually(fun, deadline, interval) do
    if fun.() do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("condition did not become true within budget")
      else
        Process.sleep(interval)
        do_assert_eventually(fun, deadline, interval)
      end
    end
  end
end
