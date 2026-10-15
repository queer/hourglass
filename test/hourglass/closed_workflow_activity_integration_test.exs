defmodule Hourglass.ClosedWorkflowActivityIntegrationTest do
  @moduledoc """
  An activity whose workflow has closed — ended on a cancel, terminated, or
  timed out — is ended within the worker's `:closed_workflow_bound`; one whose
  workflow is still running is never ended for it — against a real Temporal
  cluster.

  Each activity is held running by the test: `Hourglass.TestSupport.GatedActivity`
  blocks at a `RaceGate` and never heartbeats, so it cannot see a cancel and has
  to be stopped; `Hourglass.TestSupport.HeedingActivity` heartbeats, sees the
  cancel, cleans up, and ends by itself. Every activity has a start-to-close
  timeout far longer than any wait here, so nothing ends one but its workflow
  closing, and every worker has a grace far longer than any wait here, so a
  worker whose stop returns quickly had nothing of Core's left to wait for.

  The bound is checked in two parts:

    * Once the watch has learned a run closed (its `workflow_closed`
      event), the activity ends within half the bound, give or take a
      quarter. That takes no Temporal, so it is asserted always.
    * From the moment Temporal records the workflow closed (`close_time`),
      the activity ends within the bound. The watch owes that only when
      Temporal answers it in time (`Hourglass.TestSupport.WatchTrace`), so
      it is asserted only then. Temporal's clock may not be this node's (a
      cluster in a VM), so the measure is corrected by how far ahead of this
      node Temporal's clock looks when the test starts its workflow, an
      estimate that errs towards a longer measure, by at most how long the
      start took; it is asserted only when the start took under a quarter of
      the bound.

  Every wait for the activity to end, or for a workflow to get anywhere, is
  as long as the suite waits on Temporal (`Hourglass.TestSupport.Patience`):
  a run that is never ended still fails.
  """

  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.TestSupport.ClosingParent
  alias Hourglass.TestSupport.EndsOnCancel
  alias Hourglass.TestSupport.Generations
  alias Hourglass.TestSupport.Patience
  alias Hourglass.TestSupport.RaceGate
  alias Hourglass.TestSupport.SchedulerLag
  alias Hourglass.TestSupport.WatchTrace
  alias Hourglass.TestSupport.WorkerProbes
  alias Hourglass.WorkflowHandle
  alias Hourglass.WorkflowStatus

  @moduletag :temporal
  @moduletag :integration
  @moduletag timeout: Patience.test_timeout(4)
  @moduletag capture_log: true

  @bound 2_000

  # How soon Temporal must answer the watch for the watch to owe the bound: a
  # quarter of it, as the watch asks every quarter and stops an activity half
  # the bound after learning its run closed. (It waits longer for an answer,
  # at least a second, but an answer that late leaves the bound behind.)
  @answer_time div(@bound, 4)

  setup do
    queue = "closed-wf-#{System.unique_integer([:positive])}"

    worker =
      Patience.start_worker!(
        task_queue: queue,
        closed_workflow_bound: @bound,
        graceful_shutdown_period: 60_000
      )

    watch = :sys.get_state(worker).watch_pid
    :ok = WatchTrace.start(watch)

    # The watch opens its client as it starts; how long that took is not the
    # bound's to judge.
    WorkerProbes.poll_until("the watch to open its client", fn ->
      :sys.get_state(watch).client != nil
    end)

    :ok = WatchTrace.reset(watch)
    attach_closed_workflow_events(queue)

    %{
      queue: queue,
      id: "closed-wf-#{UUIDv7.generate()}",
      watch: %{pid: watch, lag: SchedulerLag.start()}
    }
  end

  test "an activity that never looks for a cancel is stopped within the bound of its workflow ending on a cancel",
       %{queue: queue, id: id, watch: watch} do
    :ok = RaceGate.arm({id, :activity})
    {handle, clock} = start!(EndsOnCancel, %{"gate" => id, "ends_on_cancel" => true}, id, queue)
    {activity, _ref} = RaceGate.await_held({id, :activity})
    monitor = Process.monitor(activity)

    :ok = Hourglass.cancel(handle, "test")

    assert_receive {:DOWN, ^monitor, :process, ^activity, :killed}, Patience.ms()
    assert_ended_within_bound(watch, handle, stopped(id), :completed, clock)
    assert {:ok, %{"cancelled" => true}} = Hourglass.result(handle, timeout: Patience.ms())

    # Both steps were told to telemetry, naming the run.
    run = %{task_queue: queue, workflow_id: id, run_id: handle.run_id}
    assert_received {[:hourglass, :activity, :workflow_closed], ^run, _at}
    assert_received {[:hourglass, :activity, :workflow_closed, :stopped], ^run, _at}

    # The stopped activity was reported, so Core holds nothing of it: the
    # worker stops at once rather than wait out its grace for it.
    stopping = now()
    :ok = stop_supervised(Hourglass.Worker)
    assert now() - stopping < 5_000

    :ok = RaceGate.disarm({id, :activity})
  end

  test "an activity that heeds the cancel sees it, cleans up and ends by itself, within the bound",
       %{queue: queue, id: id, watch: watch} do
    :ok = RaceGate.arm(id)

    {handle, clock} =
      start!(
        EndsOnCancel,
        %{"gate" => id, "ends_on_cancel" => true, "heeding" => true, "cleanup_ms" => 300},
        id,
        queue
      )

    assert_receive {RaceGate, :observed, ^id, {:running, activity, _token}}, Patience.ms()
    monitor = Process.monitor(activity)

    :ok = Hourglass.cancel(handle, "test")

    assert_receive {RaceGate, :observed, ^id, {:cancel_seen, :workflow_closed}}, Patience.ms()
    assert_receive {RaceGate, :observed, ^id, :cleaned_up}, @bound
    # Its work ended with the cleanup; its process ends once it has reported
    # the cancel to Temporal.
    ended = ended()
    assert_receive {:DOWN, ^monitor, :process, ^activity, :normal}, Patience.ms()
    assert_ended_within_bound(watch, handle, ended, :completed, clock)
  end

  test "an activity is stopped within the bound of its workflow being terminated",
       %{queue: queue, id: id, watch: watch} do
    child_id = "#{id}-child"
    :ok = RaceGate.arm({id, :activity})

    {parent, clock} =
      start!(
        ClosingParent,
        %{"how" => "terminate", "child_id" => child_id, "gate" => id},
        id,
        queue
      )

    {activity, _ref} = RaceGate.await_held({id, :activity})
    monitor = Process.monitor(activity)

    :ok = Hourglass.signal(parent, "close", %{})

    assert_receive {:DOWN, ^monitor, :process, ^activity, :killed}, Patience.ms()

    assert_ended_within_bound(
      watch,
      %WorkflowHandle{id: child_id},
      stopped(child_id),
      :terminated,
      clock
    )

    :ok = RaceGate.disarm({id, :activity})
  end

  test "an activity is stopped within the bound of its workflow timing out",
       %{queue: queue, watch: watch} do
    {id, child_id, activity, clock} = start_timing_out!(queue)
    monitor = Process.monitor(activity)

    assert_receive {:DOWN, ^monitor, :process, ^activity, :killed}, Patience.ms()

    assert_ended_within_bound(
      watch,
      %WorkflowHandle{id: child_id},
      stopped(child_id),
      :timed_out,
      clock
    )

    :ok = RaceGate.disarm({id, :activity})
  end

  test "an activity whose workflow is still running is not ended, though a cancel of it was requested",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm({id, :activity})
    {handle, _clock} = start!(EndsOnCancel, %{"gate" => id}, id, queue)
    {activity, _ref} = running = RaceGate.await_held({id, :activity})
    monitor = Process.monitor(activity)

    :ok = Hourglass.cancel(handle, "test")

    # Well past the bound, the activity is still running, and its workflow
    # still waits on it.
    refute_receive {:DOWN, ^monitor, :process, ^activity, _reason}, 3 * @bound
    assert {:ok, %WorkflowStatus{state: :running}} = Hourglass.status(handle)

    # Let it finish: its outcome reaches the workflow as usual.
    :ok = RaceGate.disarm({id, :activity})
    :ok = RaceGate.release(running)

    assert {:ok, %{"outcome" => outcome}} = Hourglass.result(handle, timeout: Patience.ms())
    assert outcome =~ "worked"
  end

  test "an activity still cleaning up when the bound is out is stopped",
       %{queue: queue, id: id, watch: watch} do
    :ok = RaceGate.arm(id)

    # A cleanup longer than the bound leaves it: it is stopped mid-cleanup.
    {handle, clock} =
      start!(
        EndsOnCancel,
        %{"gate" => id, "ends_on_cancel" => true, "heeding" => true, "cleanup_ms" => 60_000},
        id,
        queue
      )

    assert_receive {RaceGate, :observed, ^id, {:running, activity, token}}, Patience.ms()
    monitor = Process.monitor(activity)

    :ok = Hourglass.cancel(handle, "test")

    assert_receive {RaceGate, :observed, ^id, {:cancel_seen, :workflow_closed}}, Patience.ms()
    assert_receive {:DOWN, ^monitor, :process, ^activity, :killed}, @bound
    assert_ended_within_bound(watch, handle, stopped(id), :completed, clock)
    refute_received {RaceGate, :observed, ^id, :cleaned_up}

    WorkerProbes.poll_until("the stopped activity's cancel flag to be cleared", fn ->
      Hourglass.Activity.CancelRegistry.cancelled?(token) == nil
    end)
  end

  test "an activity of a run continued as new is ended, and the next run's activity, of the same workflow id, is not",
       %{queue: queue, id: id, watch: watch} do
    :ok = RaceGate.arm(id)
    {handle, clock} = start!(Generations, %{"gate" => id, "gen" => 1}, id, queue)
    assert_receive {RaceGate, :observed, ^id, {:running, first_run, first}}, Patience.ms()
    first_monitor = Process.monitor(first)

    :ok = Hourglass.signal(handle, "next", %{})
    assert_receive {RaceGate, :observed, ^id, {:running, next_run, next}}, Patience.ms()
    assert next_run != first_run
    next_monitor = Process.monitor(next)

    assert_receive {:DOWN, ^first_monitor, :process, ^first, :killed}, Patience.ms()
    assert_ended_within_bound(watch, handle, stopped(id), :continued_as_new, clock)

    # The next run goes on running, and its activity with it.
    refute_receive {:DOWN, ^next_monitor, :process, ^next, _reason}, 2 * @bound
    send(next, :reply)

    assert {:ok, %{"gen2" => %{"replied" => ^id}}} =
             Hourglass.result(%WorkflowHandle{id: id}, timeout: Patience.ms())
  end

  # Starts a parent whose child times out after a few seconds while the test
  # holds the child's activity. A slow Temporal may time the child out before
  # its activity has started; then it starts another, at most five times.
  # Returns the parent's id, the child's id, the held activity, and the clock.
  defp start_timing_out!(queue, attempts \\ 5) do
    id = "closed-wf-#{UUIDv7.generate()}"
    child_id = "#{id}-child"
    :ok = RaceGate.arm({id, :activity})
    input = %{"how" => "time_out", "child_id" => child_id, "gate" => id, "timeout_ms" => 6_000}
    {_parent, clock} = start!(ClosingParent, input, id, queue)

    case await_held_or_closed({id, :activity}, child_id) do
      {:held, activity} ->
        {id, child_id, activity, clock}

      :closed when attempts > 1 ->
        :ok = RaceGate.disarm({id, :activity})
        start_timing_out!(queue, attempts - 1)

      :closed ->
        flunk("the child timed out before its activity started, every time")
    end
  end

  defp await_held_or_closed(key, workflow_id, deadline \\ nil) do
    deadline = deadline || now() + Patience.ms()

    receive do
      {RaceGate, :reached, ^key, pid, _ref} -> {:held, pid}
    after
      200 ->
        cond do
          match?({:ok, %WorkflowStatus{state: :timed_out}}, Hourglass.status(workflow_id)) ->
            :closed

          now() > deadline ->
            flunk("nothing reached race gate #{inspect(key)}")

          true ->
            await_held_or_closed(key, workflow_id, deadline)
        end
    end
  end

  # Forward the closed-workflow events of `queue` to the test.
  defp attach_closed_workflow_events(queue) do
    test = self()
    handler = {__MODULE__, queue}

    :telemetry.attach_many(
      handler,
      [
        [:hourglass, :activity, :workflow_closed],
        [:hourglass, :activity, :workflow_closed, :stopped]
      ],
      &__MODULE__.forward_event/4,
      %{test: test, queue: queue}
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  @doc false
  def forward_event(event, _measurements, %{task_queue: queue} = metadata, %{
        test: test,
        queue: queue
      }),
      do: send(test, {event, metadata, ended()})

  def forward_event(_event, _measurements, _metadata, _config), do: :ok

  # Starts the workflow. Returns its handle, and Temporal's clock against this
  # node's: how far ahead it looks (its start time, less this node's time just
  # before the start was asked for, so the start's own latency counts as
  # Temporal being ahead), and whether the start was quick enough, under a
  # quarter of the bound, for that to be read by.
  defp start!(module, input, id, queue) do
    asked = DateTime.utc_now()
    {:ok, %WorkflowHandle{} = handle} = Hourglass.start(module, input, id: id, task_queue: queue)
    took = DateTime.diff(DateTime.utc_now(), asked, :millisecond)
    {:ok, %WorkflowStatus{start_time: %DateTime{} = started}} = Hourglass.status(handle)
    {handle, %{ahead: DateTime.diff(started, asked, :millisecond), quick?: took < div(@bound, 4)}}
  end

  # When the watch told telemetry it had learned `workflow_id`'s run closed,
  # and when it had stopped its activity: read from the mailbox without taking
  # the message, as a message may wait there while the test waits on another.
  defp learned_closed(workflow_id),
    do: told(workflow_id, [:hourglass, :activity, :workflow_closed]).at

  defp stopped(workflow_id),
    do: told(workflow_id, [:hourglass, :activity, :workflow_closed, :stopped])

  # The watch tells telemetry it stopped an activity just after the activity
  # is gone, so the event can come just after the test saw the activity go.
  defp told(workflow_id, event, deadline \\ nil) do
    deadline = deadline || now() + Patience.ms()
    {:messages, messages} = Process.info(self(), :messages)

    found =
      Enum.find_value(messages, fn
        {^event, %{workflow_id: ^workflow_id}, at} -> at
        _other -> nil
      end)

    cond do
      found ->
        found

      now() > deadline ->
        flunk("the watch never told telemetry #{inspect(event)} for #{workflow_id}")

      true ->
        Process.sleep(20)
        told(workflow_id, event, deadline)
    end
  end

  # When the activity ended: this node's monotonic and wall-clock time.
  defp ended, do: %{at: now(), wall: DateTime.utc_now()}

  # The workflow `handle` names closed in `state`, and its activity ended
  # (`ended`) within the bound: within half the bound, give or take a quarter,
  # of the watch learning the run had closed; and, when Temporal answered the
  # watch in time and the clocks can be read against each other, within the
  # bound of the close. Both only when the BEAM ran its timers on time
  # (`SchedulerLag`).
  defp assert_ended_within_bound(watch, handle, ended, state, clock) do
    assert {:ok, %WorkflowStatus{state: ^state, close_time: %DateTime{} = closed}} =
             Hourglass.status(handle)

    learned = learned_closed(handle.id)
    on_time? = not SchedulerLag.late?(watch.lag, div(@bound, 4))

    if on_time? do
      assert ended.at - learned < div(@bound, 2) + div(@bound, 4)
    end

    if on_time? and WatchTrace.in_time?(watch.pid, @answer_time) and clock.quick? do
      assert DateTime.diff(ended.wall, closed, :millisecond) + clock.ahead < @bound
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
