defmodule Hourglass.EndedRunActivityIntegrationTest do
  @moduledoc """
  An activity whose run has closed by the time its task reaches the worker
  never runs; one that opts in (`ends_on_cancel_request: true`) is ended by a
  cancel requested of its workflow too, before it starts or while it runs;
  and one that does not opt in is never ended by a cancel request alone —
  against a real Temporal cluster.

  A task is held between Temporal handing it to the worker and the worker
  starting it by suspending the worker's activity poll loop: Temporal has
  started the activity, and Core has handed the task out, but the worker has
  not yet dispatched it. The test then closes or cancels the workflow, and
  lets the task through.

  Every activity reports to the test when it starts running, on the RaceGate
  key that is its gate, so a test sees whether its `execute/1` was called.
  """

  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.Client.Real
  alias Hourglass.TestSupport.EndsOnCancel
  alias Hourglass.TestSupport.Patience
  alias Hourglass.TestSupport.RaceGate
  alias Hourglass.TestSupport.SchedulerLag
  alias Hourglass.TestSupport.WorkerProbes
  alias Hourglass.Worker.ActivityPollLoop
  alias Hourglass.WorkflowStatus

  @moduletag :temporal
  @moduletag :integration
  @moduletag timeout: Patience.test_timeout(4)
  @moduletag capture_log: true

  @bound 2_000

  setup do
    queue = "ended-run-#{System.unique_integer([:positive])}"

    Patience.start_worker!(
      task_queue: queue,
      closed_workflow_bound: @bound,
      graceful_shutdown_period: 60_000
    )

    attach_events(queue)
    id = "ended-run-#{UUIDv7.generate()}"
    :ok = RaceGate.arm(id)
    %{queue: queue, id: id}
  end

  test "an activity whose run closed before its task started never runs, and is reported cancelled",
       %{queue: queue, id: id} do
    handle = start_held!(%{"gate" => id, "heeding" => true, "ends_on_cancel" => true}, id, queue)

    # The body ends on the cancel: the run completes while the task is held.
    :ok = Hourglass.cancel(handle, "test")
    assert {:ok, %{"cancelled" => true}} = Hourglass.result(handle, timeout: Patience.ms())
    release_activity_tasks(queue)

    assert_receive {[:hourglass, :activity, :not_started],
                    %{workflow_id: ^id, run_id: run_id, reason: :workflow_closed}, _at},
                   Patience.ms()

    assert run_id == handle.run_id
    refute_received {RaceGate, :observed, ^id, {:running, _pid, _token}}

    # It was reported, so Core holds nothing of it: the worker stops at once
    # rather than wait out its grace for it.
    stopping = now()
    :ok = stop_supervised(Hourglass.Worker)
    assert now() - stopping < 5_000
    refute_received {RaceGate, :observed, ^id, {:running, _pid, _token}}
  end

  test "an opted-in activity whose workflow had a cancel requested before its task started never runs",
       %{queue: queue, id: id} do
    # The body does not end on the cancel: the run goes on, waiting on the
    # activity.
    handle = start_held!(%{"gate" => id, "opted_in" => true, "heeding" => true}, id, queue)

    :ok = Hourglass.cancel(handle, "test")
    release_activity_tasks(queue)

    assert_receive {[:hourglass, :activity, :not_started],
                    %{workflow_id: ^id, reason: :workflow_cancel_requested}, _at},
                   Patience.ms()

    # The workflow learns at once that it failed, not retryably.
    assert {:ok, %{"outcome" => outcome}} = Hourglass.result(handle, timeout: Patience.ms())
    assert outcome =~ "WorkflowCancelRequested"
    assert outcome =~ "non_retryable: true"
    refute_received {RaceGate, :observed, ^id, {:running, _pid, _token}}
  end

  test "an opted-in activity running when a cancel is requested of its workflow sees it, and ends by itself within the bound",
       %{queue: queue, id: id} do
    lag = SchedulerLag.start()
    {:ok, handle} = start!(%{"gate" => id, "opted_in" => true, "heeding" => true}, id, queue)
    assert_receive {RaceGate, :observed, ^id, {:running, activity, _token}}, Patience.ms()
    monitor = Process.monitor(activity)

    :ok = Hourglass.cancel(handle, "test")

    assert_receive {RaceGate, :observed, ^id, {:cancel_seen, :workflow_cancel_requested}},
                   Patience.ms()

    assert_receive {:DOWN, ^monitor, :process, ^activity, :normal}, Patience.ms()
    ended = now()

    assert_receive {[:hourglass, :activity, :workflow_cancel_requested], %{workflow_id: ^id},
                    learned},
                   Patience.ms()

    # From the watch learning of the request, it ends within half the bound,
    # give or take a quarter (it heartbeats every 50 ms), when the BEAM ran on
    # time.
    if not SchedulerLag.late?(lag, div(@bound, 4)) do
      assert ended - learned < div(@bound, 2) + div(@bound, 4)
    end

    assert {:ok, %{"outcome" => outcome}} = Hourglass.result(handle, timeout: Patience.ms())
    assert outcome =~ "WorkflowCancelRequested"
  end

  test "an opted-in activity that never heeds the cancel request is stopped within the bound",
       %{queue: queue, id: id} do
    lag = SchedulerLag.start()
    :ok = RaceGate.arm({id, :activity})
    {:ok, handle} = start!(%{"gate" => id, "opted_in" => true}, id, queue)
    {activity, _ref} = RaceGate.await_held({id, :activity})
    monitor = Process.monitor(activity)

    :ok = Hourglass.cancel(handle, "test")

    assert_receive {:DOWN, ^monitor, :process, ^activity, :killed}, Patience.ms()
    stopped = now()

    assert_receive {[:hourglass, :activity, :workflow_cancel_requested], %{workflow_id: ^id},
                    learned},
                   Patience.ms()

    assert_receive {[:hourglass, :activity, :workflow_cancel_requested, :stopped],
                    %{workflow_id: ^id}, _at},
                   Patience.ms()

    if not SchedulerLag.late?(lag, div(@bound, 4)) do
      assert stopped - learned < div(@bound, 2) + div(@bound, 4)
    end

    assert {:ok, %{"outcome" => outcome}} = Hourglass.result(handle, timeout: Patience.ms())
    assert outcome =~ "WorkflowCancelRequested"
    :ok = RaceGate.disarm({id, :activity})
  end

  test "an activity that does not opt in runs, though a cancel was requested of its workflow before it started",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm({id, :activity})
    handle = start_held!(%{"gate" => id}, id, queue)

    :ok = Hourglass.cancel(handle, "test")
    release_activity_tasks(queue)

    # It runs, and is not ended for the request: well past the bound, it is
    # still running, and its workflow still waits on it.
    {activity, _ref} = running = RaceGate.await_held({id, :activity})
    monitor = Process.monitor(activity)
    refute_receive {:DOWN, ^monitor, :process, ^activity, _reason}, 3 * @bound
    assert {:ok, %WorkflowStatus{state: :running}} = Hourglass.status(handle)

    :ok = RaceGate.disarm({id, :activity})
    :ok = RaceGate.release(running)
    assert {:ok, %{"outcome" => outcome}} = Hourglass.result(handle, timeout: Patience.ms())
    assert outcome =~ "worked"
    refute_received {[:hourglass, :activity, _ended], %{workflow_id: ^id}, _at}
  end

  test "an opted-in activity of a running workflow with no cancel requested is not ended",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm({id, :activity})
    {:ok, handle} = start!(%{"gate" => id, "opted_in" => true}, id, queue)
    {activity, _ref} = running = RaceGate.await_held({id, :activity})
    monitor = Process.monitor(activity)

    refute_receive {:DOWN, ^monitor, :process, ^activity, _reason}, 3 * @bound

    :ok = RaceGate.disarm({id, :activity})
    :ok = RaceGate.release(running)
    assert {:ok, %{"outcome" => outcome}} = Hourglass.result(handle, timeout: Patience.ms())
    assert outcome =~ "worked"
    refute_received {[:hourglass, :activity, _ended], %{workflow_id: ^id}, _at}
  end

  # Start `EndsOnCancel` with `input`, with the worker's activity tasks held,
  # and wait until Temporal has handed its activity's task to the worker.
  defp start_held!(input, id, queue) do
    hold_activity_tasks(queue)
    {:ok, handle} = start!(input, id, queue)

    WorkerProbes.poll_until("Temporal to hand the activity's task to the worker", fn ->
      {:ok, described} = Patience.retry(fn -> Real.describe_workflow_execution(handle) end)
      Enum.any?(described.pending_activities, &(&1.state == :PENDING_ACTIVITY_STATE_STARTED))
    end)

    handle
  end

  defp start!(input, id, queue) do
    Patience.start(
      fn -> Hourglass.start(EndsOnCancel, input, id: id, task_queue: queue) end,
      id
    )
  end

  # Suspend the worker's activity poll loop: a task Core hands out meanwhile
  # waits, undispatched, until `release_activity_tasks/1`.
  defp hold_activity_tasks(queue) do
    loop = activity_poll_loop(queue)
    true = :erlang.suspend_process(loop)
    on_exit(fn -> if Process.alive?(loop), do: :erlang.resume_process(loop) end)
    Process.put({:held, queue}, loop)
  end

  defp release_activity_tasks(queue) do
    loop = Process.delete({:held, queue})
    true = :erlang.resume_process(loop)
    :ok
  end

  defp activity_poll_loop(queue) do
    queue
    |> Hourglass.Worker.inner_supervisor()
    |> Supervisor.which_children()
    |> Enum.find_value(fn {id, pid, _type, _modules} -> id == ActivityPollLoop && pid end)
  end

  # Forward the events of an activity ending on `queue` to the test.
  defp attach_events(queue) do
    handler = {__MODULE__, queue}

    :telemetry.attach_many(
      handler,
      [
        [:hourglass, :activity, :not_started],
        [:hourglass, :activity, :workflow_closed],
        [:hourglass, :activity, :workflow_cancel_requested],
        [:hourglass, :activity, :workflow_cancel_requested, :stopped]
      ],
      &__MODULE__.forward_event/4,
      %{test: self(), queue: queue}
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  @doc false
  def forward_event(event, _measurements, %{task_queue: queue} = metadata, %{
        test: test,
        queue: queue
      }),
      do: send(test, {event, metadata, now()})

  def forward_event(_event, _measurements, _metadata, _config), do: :ok

  defp now, do: System.monotonic_time(:millisecond)
end
