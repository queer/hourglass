defmodule Hourglass.WorkerGracefulStopIntegrationTest do
  @moduledoc """
  A worker stopped gracefully stops taking tasks, lets the work it holds
  finish and report its outcome to Temporal, and only then lets go of its
  handle on Temporal — against a real Temporal cluster.

  Each test stops a worker while it holds work the test controls: an activity
  (`Hourglass.TestSupport.GatedActivity`) or a workflow task
  (`Hourglass.TestSupport.GatedDecision`) held open at a
  `Hourglass.TestSupport.RaceGate`. The stop runs in its own process, so the
  test can act while the worker is stopping and see exactly when the stop
  returns.

  The activity runs on a queue of its own, served by the worker under test,
  while its workflow runs on a control worker that is never stopped. That lets
  the workflow schedule a second activity on the stopping worker's queue after
  the stop has begun, which the stopping worker must not take.

  Every activity has a start-to-close timeout far longer than any wait here,
  so an outcome Temporal records within a test was reported, not timed out.
  """

  # async: false — each test runs two workers (the control worker and the one
  # it stops), so four dirty-IO long-polls, while `test/test_helper.exs` sizes
  # the suite's concurrency for one worker per test. Run concurrently on the
  # default dirty-IO pool, they would starve it, and a stop that cannot get a
  # scheduler for its NIF calls would hang the suite.
  use ExUnit.Case, async: false
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.Client.Real
  alias Hourglass.TestSupport.GatedDecision
  alias Hourglass.TestSupport.GatedWork
  alias Hourglass.TestSupport.Patience
  alias Hourglass.TestSupport.RaceGate
  alias Hourglass.TestSupport.RetriedWork
  alias Hourglass.TestSupport.WorkerProbes
  alias Hourglass.Worker.WorkflowStateCache
  # credo:disable-for-next-line Credo.Check.Readability.AliasAs
  alias Hourglass.Worker.Supervisor, as: WorkerSupervisor
  alias Hourglass.WorkflowHandle
  alias Hourglass.WorkflowStatus
  alias Temporal.Api.History.V1.History

  @moduletag :temporal
  @moduletag :integration
  @moduletag timeout: Patience.test_timeout(4)
  @moduletag capture_log: true

  setup do
    n = System.unique_integer([:positive])
    control = "stop-control-#{n}"
    queue = "stop-#{n}"
    {:ok, _control} = Patience.start_worker(task_queue: control)

    :telemetry.attach(
      {__MODULE__, queue},
      [:hourglass, :worker, :stopping],
      &__MODULE__.forward_stopping/4,
      %{test: self(), queue: queue}
    )

    on_exit(fn -> :telemetry.detach({__MODULE__, queue}) end)

    %{control: control, queue: queue, id: "stop-#{UUIDv7.generate()}"}
  end

  @doc false
  def forward_stopping(_event, _measurements, %{task_queue: queue}, %{test: test, queue: queue}),
    do: send(test, {:stopping, queue})

  def forward_stopping(_event, _measurements, _metadata, _config), do: :ok

  test "a worker stopped while an activity runs takes no new tasks, and the activity's outcome reaches Temporal before the worker is gone",
       %{control: control, queue: queue, id: id} do
    start_worker!(queue, graceful_shutdown_period: 30_000)
    :ok = RaceGate.arm({id, :activity})
    handle = start!(GatedWork, %{"gate" => id, "queue" => queue}, id, control)
    running = RaceGate.await_held({id, :activity})

    stopper = Task.async(fn -> WorkerSupervisor.stop_worker(queue) end)

    # The stop waits for the activity the worker is running.
    assert Task.yield(stopper, 1_500) == nil
    assert_receive {:stopping, ^queue}, 5_000

    # A task scheduled on the queue once the worker has stopped polling is not
    # taken: the workflow schedules a second activity there now.
    :ok = Hourglass.signal(handle, "more", %{})
    await_pending_activity(handle, "EchoActivity", :PENDING_ACTIVITY_STATE_SCHEDULED)
    Process.sleep(1_500)
    assert pending_state(handle, "EchoActivity") == :PENDING_ACTIVITY_STATE_SCHEDULED
    assert Task.yield(stopper, 0) == nil

    # Let the activity finish: it reports through the worker that is stopping,
    # and only then does the stop return.
    :ok = RaceGate.disarm({id, :activity})
    :ok = RaceGate.release(running)
    assert Task.await(stopper, Patience.ms()) == :ok

    assert [%{result: result}] = activity_completions(id)
    assert result.payloads != []
    assert activity_failures(id) == []
    refute Hourglass.BridgeHolder.registered?(queue)

    # The second activity was left for whichever worker serves the queue next.
    assert pending_state(handle, "EchoActivity") == :PENDING_ACTIVITY_STATE_SCHEDULED
    start_worker!(queue)

    assert {:ok, %{"echoed" => %{"nonce" => ^id}}} =
             Hourglass.result(handle, timeout: Patience.ms())
  end

  test "an activity still running when the grace runs out is reported to Temporal as failed by the worker's shutdown, and its process is stopped",
       %{control: control, queue: queue, id: id} do
    start_worker!(queue, graceful_shutdown_period: 1_000)
    :ok = RaceGate.arm({id, :activity})
    _handle = start!(GatedWork, %{"gate" => id, "queue" => queue}, id, control)
    {activity_pid, _ref} = RaceGate.await_held({id, :activity})
    activity_ref = Process.monitor(activity_pid)

    started = System.monotonic_time(:millisecond)
    assert WorkerSupervisor.stop_worker(queue) == :ok
    elapsed = System.monotonic_time(:millisecond) - started

    # The stop waited out the grace, and not much longer.
    assert elapsed >= 1_000
    assert elapsed < 10_000

    # Temporal heard the outcome before the stop returned: a failure it may
    # retry, not a timeout it would wait ten minutes for.
    assert [failure] = activity_failures(id)
    assert {:application_failure_info, info} = failure.failure_info
    assert info.type == "WorkerShutdown"
    refute info.non_retryable

    # Nothing of the stopped worker's is left running.
    assert_receive {:DOWN, ^activity_ref, :process, ^activity_pid, _reason}, 1_000
    refute Hourglass.BridgeHolder.registered?(queue)

    :ok = RaceGate.disarm({id, :activity})
  end

  test "an activity cut off by the grace is retried at once by the next worker on the queue",
       %{control: control, queue: queue, id: id} do
    start_worker!(queue, graceful_shutdown_period: 500)
    :ok = RaceGate.arm({id, :activity})

    handle =
      start!(GatedWork, %{"gate" => id, "queue" => queue, "attempts" => 2}, id, control)

    _running = RaceGate.await_held({id, :activity})
    assert WorkerSupervisor.stop_worker(queue) == :ok

    # The retry is already scheduled, with the shutdown as its last failure.
    assert {:ok, %WorkflowStatus{pending_activities: [pending]}} = Hourglass.status(handle)
    assert pending.attempt == 2

    assert {:application_failure_info, %{type: "WorkerShutdown"}} =
             pending.last_failure.failure_info

    :ok = RaceGate.disarm({id, :activity})
    start_worker!(queue)
    :ok = Hourglass.signal(handle, "more", %{})

    assert {:ok, %{"gated" => gated, "echoed" => %{"nonce" => ^id}}} =
             Hourglass.result(handle, timeout: Patience.ms())

    # Both activities completed; the first attempt's shutdown failure was
    # retried rather than recorded as the activity's outcome.
    assert gated =~ "worked"
    assert length(activity_completions(id)) == 2
    assert activity_failures(id) == []
  end

  test "a workflow task in flight when the worker stops completes before the worker is gone",
       %{queue: queue, id: id} do
    start_worker!(queue, graceful_shutdown_period: 30_000)
    :ok = RaceGate.arm(id)
    handle = start!(GatedDecision, %{"gate" => id}, id, queue)
    held = RaceGate.await_held(id)

    stopper = Task.async(fn -> WorkerSupervisor.stop_worker(queue) end)
    assert Task.yield(stopper, 1_500) == nil

    :ok = RaceGate.disarm(id)
    :ok = RaceGate.release(held)
    assert Task.await(stopper, Patience.ms()) == :ok

    # The task's completion — which closes the run — reached Temporal before
    # the stop returned.
    assert {:ok, %WorkflowStatus{state: :completed}} = Hourglass.status(handle)
    assert {:ok, %{"decided" => ^id}} = Hourglass.result(handle, timeout: Patience.ms())
  end

  test "a worker with nothing to finish stops at once", %{queue: queue} do
    start_worker!(queue, graceful_shutdown_period: 30_000)

    started = System.monotonic_time(:millisecond)
    assert WorkerSupervisor.stop_worker(queue) == :ok
    assert System.monotonic_time(:millisecond) - started < 5_000

    refute Hourglass.BridgeHolder.registered?(queue)
    assert {:error, :not_found} = WorkerSupervisor.stop_worker(queue)
  end

  # ---------------------------------------------------------------------------
  # A worker that stops because its poll loops' supervisor crashed
  # ---------------------------------------------------------------------------

  test "a worker whose inner supervisor crashes lets its running activity finish and report",
       %{control: control, queue: queue, id: id} do
    start_worker!(queue, graceful_shutdown_period: 30_000)
    :ok = RaceGate.arm({id, :activity})
    _handle = start!(GatedWork, %{"gate" => id, "queue" => queue}, id, control)
    running = RaceGate.await_held({id, :activity})

    crash_inner_supervisor!(queue)
    assert_receive {:stopping, ^queue}, 5_000

    :ok = RaceGate.disarm({id, :activity})
    :ok = RaceGate.release(running)

    poll_until("the activity's completion to be recorded", fn ->
      activity_completions(id) != []
    end)

    assert activity_failures(id) == []
  end

  test "a worker whose inner supervisor crashes reports an activity still running at the grace as failed by the shutdown",
       %{control: control, queue: queue, id: id} do
    start_worker!(queue, graceful_shutdown_period: 1_000)
    :ok = RaceGate.arm({id, :activity})
    _handle = start!(GatedWork, %{"gate" => id, "queue" => queue}, id, control)
    {activity_pid, _ref} = RaceGate.await_held({id, :activity})
    activity_ref = Process.monitor(activity_pid)

    crash_inner_supervisor!(queue)

    poll_until("the activity's failure to be recorded", fn -> activity_failures(id) != [] end)
    assert [failure] = activity_failures(id)

    assert {:application_failure_info, %{type: "WorkerShutdown", non_retryable: false}} =
             failure.failure_info

    assert_receive {:DOWN, ^activity_ref, :process, ^activity_pid, _reason}, 1_000
    :ok = RaceGate.disarm({id, :activity})
  end

  # ---------------------------------------------------------------------------
  # A drain the margin cuts off
  # ---------------------------------------------------------------------------

  test "a worker whose drain the margin cuts off holds no dirty-IO thread once its stop returns",
       %{queue: queue, id: id} do
    # The control worker's polls, parked before this test's worker starts.
    poll_until("the control worker to park its polls", fn -> length(parked_polls()) >= 2 end)
    others = parked_polls()

    start_worker!(queue, graceful_shutdown_period: 0)

    # A workflow task held open for the whole stop: Core never drains, so
    # both poll loops are still parked on polls when the margin runs out.
    :ok = RaceGate.arm(id)
    _handle = start!(GatedDecision, %{"gate" => id}, id, queue)
    _held = RaceGate.await_held(id)

    poll_until("the worker's two polls to park", fn ->
      length(parked_polls() -- others) == 2
    end)

    polls = parked_polls() -- others
    assert WorkerSupervisor.stop_worker(queue) == :ok

    # The stopped worker's polls returned with the stop: neither is still
    # parked in a poll NIF, holding a dirty-IO scheduler thread.
    assert Enum.filter(polls, &(&1 in parked_polls())) == []
    :ok = RaceGate.disarm(id)
  end

  # ---------------------------------------------------------------------------
  # The worker's mirror of Core's workflow cache
  # ---------------------------------------------------------------------------

  test "a stopped worker leaves nothing of its runs in the workflow state cache",
       %{queue: queue, id: id} do
    start_worker!(queue, graceful_shutdown_period: 30_000)

    # A run waiting on a signal: Core keeps it cached, and so does the worker.
    handle = start!(Hourglass.TestSupport.SignalledChild, %{}, id, queue)

    poll_until("the run's state to be cached", fn ->
      WorkflowStateCache.fetch(queue, handle.run_id) != nil
    end)

    assert WorkerSupervisor.stop_worker(queue) == :ok

    # Core does not evict a stopping worker's idle runs, and the cache it
    # mirrored is gone with the handle: so must the mirror be.
    assert WorkflowStateCache.fetch(queue, handle.run_id) == nil

    # The run carries on, from its history, on the next worker.
    start_worker!(queue)
    :ok = Hourglass.signal(handle, "go", %{"n" => 1})
    assert {:ok, %{"go" => %{"n" => 1}}} = Hourglass.result(handle, timeout: Patience.ms())
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  test "two stops at once both return, and the activity reports once",
       %{control: control, queue: queue, id: id} do
    start_worker!(queue, graceful_shutdown_period: 30_000)
    :ok = RaceGate.arm({id, :activity})
    _handle = start!(GatedWork, %{"gate" => id, "queue" => queue}, id, control)
    running = RaceGate.await_held({id, :activity})

    first = Task.async(fn -> WorkerSupervisor.stop_worker(queue) end)
    assert_receive {:stopping, ^queue}, 5_000
    second = Task.async(fn -> WorkerSupervisor.stop_worker(queue) end)
    assert [{^first, nil}, {^second, nil}] = Task.yield_many([first, second], 1_000)

    :ok = RaceGate.disarm({id, :activity})
    :ok = RaceGate.release(running)

    assert Enum.sort([Task.await(first, Patience.ms()), Task.await(second, Patience.ms())]) ==
             Enum.sort([:ok, {:error, :not_found}])

    assert [_completed] = activity_completions(id)
    refute_received {:stopping, ^queue}
  end

  test "a worker stopped while its activity waits out a retry backoff stops at once, and the retry runs on the next worker",
       %{control: control, queue: queue, id: id} do
    start_worker!(queue, graceful_shutdown_period: 30_000)
    handle = start!(RetriedWork, %{"queue" => queue}, id, control)

    poll_until("the first attempt to fail", fn ->
      match?(
        {:ok, %WorkflowStatus{pending_activities: [%{attempt: 2}]}},
        Hourglass.status(handle)
      )
    end)

    started = System.monotonic_time(:millisecond)
    assert WorkerSupervisor.stop_worker(queue) == :ok
    assert System.monotonic_time(:millisecond) - started < 5_000

    start_worker!(queue)
    assert {:ok, %{"attempt" => 2}} = Hourglass.result(handle, timeout: Patience.ms())
  end

  test "a grace of zero reports a running activity as failed by the shutdown at once",
       %{control: control, queue: queue, id: id} do
    start_worker!(queue, graceful_shutdown_period: 0)
    :ok = RaceGate.arm({id, :activity})
    _handle = start!(GatedWork, %{"gate" => id, "queue" => queue}, id, control)
    _running = RaceGate.await_held({id, :activity})

    started = System.monotonic_time(:millisecond)
    assert WorkerSupervisor.stop_worker(queue) == :ok
    assert System.monotonic_time(:millisecond) - started < 5_000

    assert [failure] = activity_failures(id)
    assert {:application_failure_info, %{type: "WorkerShutdown"}} = failure.failure_info
    :ok = RaceGate.disarm({id, :activity})
  end

  test "an activity that traps exits is still stopped once the grace is out",
       %{control: control, queue: queue, id: id} do
    start_worker!(queue, graceful_shutdown_period: 500)
    :ok = RaceGate.arm({id, :activity})

    _handle =
      start!(GatedWork, %{"gate" => id, "queue" => queue, "stubborn" => true}, id, control)

    {activity_pid, _ref} = RaceGate.await_held({id, :activity})
    activity_ref = Process.monitor(activity_pid)

    started = System.monotonic_time(:millisecond)
    assert WorkerSupervisor.stop_worker(queue) == :ok
    assert System.monotonic_time(:millisecond) - started < 10_000

    assert_receive {:DOWN, ^activity_ref, :process, ^activity_pid, :killed}, 1_000

    assert [%{failure_info: {:application_failure_info, %{type: "WorkerShutdown"}}}] =
             activity_failures(id)

    :ok = RaceGate.disarm({id, :activity})
  end

  test "a worker killed outright has its handle dropped, so its queue can be served again",
       %{queue: queue} do
    test = self()

    owner =
      spawn(fn ->
        {:ok, worker} = Hourglass.Worker.start_link(task_queue: queue)
        send(test, {:worker, worker})
        Process.sleep(:infinity)
      end)

    assert_receive {:worker, worker}, 10_000
    assert Hourglass.BridgeHolder.registered?(queue)

    ref = Process.monitor(worker)
    Process.exit(worker, :kill)
    assert_receive {:DOWN, ^ref, :process, ^worker, :killed}
    refute Process.alive?(owner)

    poll_until("the killed worker's handle to be dropped", fn ->
      not Hourglass.BridgeHolder.registered?(queue)
    end)

    start_worker!(queue)
    assert Hourglass.BridgeHolder.registered?(queue)
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp poll_until(what, fun), do: WorkerProbes.poll_until(what, fun)

  # Processes in the VM parked in a poll NIF, each holding a dirty-IO
  # scheduler thread while it waits on Core.
  defp parked_polls do
    for pid <- Process.list(),
        {:current_function, {Hourglass.Bridge, function, 1}} <-
          [Process.info(pid, :current_function)],
        function in [:worker_poll_workflow_activation, :worker_poll_activity_task],
        do: pid
  end

  # Kill the worker's inner supervisor, which takes its poll loops with it and
  # stops the worker (Worker.Supervisor then restarts it).
  defp crash_inner_supervisor!(queue) do
    sup = Hourglass.Worker.inner_supervisor(queue)
    ref = Process.monitor(sup)
    Process.exit(sup, :kill)
    assert_receive {:DOWN, ^ref, :process, ^sup, :killed}
  end

  defp start_worker!(queue, opts \\ []) do
    {:ok, _pid} =
      Patience.retry(fn -> WorkerSupervisor.start_worker([task_queue: queue] ++ opts) end)

    on_exit(fn -> _result = WorkerSupervisor.stop_worker(queue) end)
  end

  defp start!(module, input, id, queue) do
    {:ok, %WorkflowHandle{} = handle} = Hourglass.start(module, input, id: id, task_queue: queue)
    handle
  end

  defp await_pending_activity(handle, type_suffix, state) do
    poll_until("a #{type_suffix} activity to reach #{state}", fn ->
      pending_state(handle, type_suffix) == state
    end)
  end

  defp pending_state(handle, type_suffix) do
    {:ok, description} = Patience.retry(fn -> Real.describe_workflow_execution(handle) end)

    Enum.find_value(description.pending_activities, fn pending ->
      if String.ends_with?(pending.activity_type.name, type_suffix), do: pending.state
    end)
  end

  defp events(workflow_id) do
    {:ok, %History{events: events}} = Hourglass.history(workflow_id)
    events
  end

  defp activity_completions(workflow_id) do
    for %{attributes: {:activity_task_completed_event_attributes, attrs}} <- events(workflow_id),
        do: attrs
  end

  defp activity_failures(workflow_id) do
    for %{attributes: {:activity_task_failed_event_attributes, attrs}} <- events(workflow_id),
        do: attrs.failure
  end
end
