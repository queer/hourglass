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

  How long an activity took to end is measured from the moment Temporal
  records its workflow as closed (`close_time`), not from the request that
  led there: the bound starts when the workflow closes. Temporal's clock may
  not be this node's (a cluster in a VM), so the measure is corrected by how
  far ahead of this node Temporal's clock looks when the test starts its
  workflow — an estimate that errs towards a longer measure, never a
  shorter one.
  """

  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.TestSupport.ClosingParent
  alias Hourglass.TestSupport.EndsOnCancel
  alias Hourglass.TestSupport.Generations
  alias Hourglass.TestSupport.RaceGate
  alias Hourglass.TestSupport.WorkerProbes
  alias Hourglass.WorkflowHandle
  alias Hourglass.WorkflowStatus

  @moduletag :temporal
  @moduletag :integration
  @moduletag timeout: 120_000
  @moduletag capture_log: true

  @bound 2_000

  # How long a test waits for what the bound promises, the request that
  # closes the workflow included; whether it came within the bound is
  # asserted against the workflow's close time.
  @wait 3 * @bound

  setup do
    queue = "closed-wf-#{System.unique_integer([:positive])}"

    {:ok, _worker} =
      start_supervised(
        {Hourglass.Worker,
         task_queue: queue, closed_workflow_bound: @bound, graceful_shutdown_period: 60_000}
      )

    %{queue: queue, id: "closed-wf-#{UUIDv7.generate()}"}
  end

  test "an activity that never looks for a cancel is stopped within the bound of its workflow ending on a cancel",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm({id, :activity})
    {handle, ahead} = start!(EndsOnCancel, %{"gate" => id, "ends_on_cancel" => true}, id, queue)
    {activity, _ref} = RaceGate.await_held({id, :activity})
    monitor = Process.monitor(activity)

    attach_closed_workflow_events(queue)
    :ok = Hourglass.cancel(handle, "test")

    assert_receive {:DOWN, ^monitor, :process, ^activity, :killed}, @wait
    assert_within_bound(handle, DateTime.utc_now(), :completed, ahead)
    assert {:ok, %{"cancelled" => true}} = Hourglass.result(handle, timeout: 10_000)

    # Both steps were told to telemetry, naming the run.
    run = %{task_queue: queue, workflow_id: id, run_id: handle.run_id}
    assert_received {[:hourglass, :activity, :workflow_closed], ^run}
    assert_received {[:hourglass, :activity, :workflow_closed, :stopped], ^run}

    # The stopped activity was reported, so Core holds nothing of it: the
    # worker stops at once rather than wait out its grace for it.
    stopping = now()
    :ok = stop_supervised(Hourglass.Worker)
    assert now() - stopping < 5_000

    :ok = RaceGate.disarm({id, :activity})
  end

  test "an activity that heeds the cancel sees it, cleans up and ends by itself, within the bound",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm(id)

    {handle, ahead} =
      start!(
        EndsOnCancel,
        %{"gate" => id, "ends_on_cancel" => true, "heeding" => true, "cleanup_ms" => 300},
        id,
        queue
      )

    assert_receive {RaceGate, :observed, ^id, {:running, activity, _token}}, 20_000
    monitor = Process.monitor(activity)

    :ok = Hourglass.cancel(handle, "test")

    assert_receive {RaceGate, :observed, ^id, {:cancel_seen, :workflow_closed}}, @wait
    assert_receive {RaceGate, :observed, ^id, :cleaned_up}, @bound
    assert_receive {:DOWN, ^monitor, :process, ^activity, :normal}, @bound
    assert_within_bound(handle, DateTime.utc_now(), :completed, ahead)
  end

  test "an activity is stopped within the bound of its workflow being terminated",
       %{queue: queue, id: id} do
    child_id = "#{id}-child"
    :ok = RaceGate.arm({id, :activity})

    {parent, ahead} =
      start!(
        ClosingParent,
        %{"how" => "terminate", "child_id" => child_id, "gate" => id},
        id,
        queue
      )

    {activity, _ref} = RaceGate.await_held({id, :activity})
    monitor = Process.monitor(activity)

    :ok = Hourglass.signal(parent, "close", %{})

    assert_receive {:DOWN, ^monitor, :process, ^activity, :killed}, @wait
    assert_within_bound(%WorkflowHandle{id: child_id}, DateTime.utc_now(), :terminated, ahead)

    :ok = RaceGate.disarm({id, :activity})
  end

  test "an activity is stopped within the bound of its workflow timing out",
       %{queue: queue, id: id} do
    child_id = "#{id}-child"
    :ok = RaceGate.arm({id, :activity})

    input = %{"how" => "time_out", "child_id" => child_id, "gate" => id, "timeout_ms" => 3_000}
    {_parent, ahead} = start!(ClosingParent, input, id, queue)

    {activity, _ref} = RaceGate.await_held({id, :activity})
    monitor = Process.monitor(activity)

    assert_receive {:DOWN, ^monitor, :process, ^activity, :killed}, 3_000 + @wait
    assert_within_bound(%WorkflowHandle{id: child_id}, DateTime.utc_now(), :timed_out, ahead)

    :ok = RaceGate.disarm({id, :activity})
  end

  test "an activity whose workflow is still running is not ended, though a cancel of it was requested",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm({id, :activity})
    {handle, _ahead} = start!(EndsOnCancel, %{"gate" => id}, id, queue)
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

    assert {:ok, %{"outcome" => outcome}} = Hourglass.result(handle, timeout: 10_000)
    assert outcome =~ "worked"
  end

  test "an activity still cleaning up when the bound is out is stopped",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm(id)

    # A cleanup longer than the bound leaves it: it is stopped mid-cleanup.
    {handle, ahead} =
      start!(
        EndsOnCancel,
        %{"gate" => id, "ends_on_cancel" => true, "heeding" => true, "cleanup_ms" => 60_000},
        id,
        queue
      )

    assert_receive {RaceGate, :observed, ^id, {:running, activity, token}}, 20_000
    monitor = Process.monitor(activity)

    :ok = Hourglass.cancel(handle, "test")

    assert_receive {RaceGate, :observed, ^id, {:cancel_seen, :workflow_closed}}, @wait
    assert_receive {:DOWN, ^monitor, :process, ^activity, :killed}, @bound
    assert_within_bound(handle, DateTime.utc_now(), :completed, ahead)
    refute_received {RaceGate, :observed, ^id, :cleaned_up}

    WorkerProbes.poll_until("the stopped activity's cancel flag to be cleared", fn ->
      Hourglass.Activity.CancelRegistry.cancelled?(token) == nil
    end)
  end

  test "an activity of a run continued as new is ended, and the next run's activity, of the same workflow id, is not",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm(id)
    {handle, ahead} = start!(Generations, %{"gate" => id, "gen" => 1}, id, queue)
    assert_receive {RaceGate, :observed, ^id, {:running, first_run, first}}, 20_000
    first_monitor = Process.monitor(first)

    :ok = Hourglass.signal(handle, "next", %{})
    assert_receive {RaceGate, :observed, ^id, {:running, next_run, next}}, @wait
    assert next_run != first_run
    next_monitor = Process.monitor(next)

    assert_receive {:DOWN, ^first_monitor, :process, ^first, :killed}, @wait
    assert_within_bound(handle, DateTime.utc_now(), :continued_as_new, ahead)

    # The next run goes on running, and its activity with it.
    refute_receive {:DOWN, ^next_monitor, :process, ^next, _reason}, 2 * @bound
    send(next, :reply)

    assert {:ok, %{"gen2" => %{"replied" => ^id}}} =
             Hourglass.result(%WorkflowHandle{id: id}, timeout: 10_000)
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
      do: send(test, {event, metadata})

  def forward_event(_event, _measurements, _metadata, _config), do: :ok

  # Starts the workflow. Returns its handle, and how far ahead of this node
  # Temporal's clock looks: its start time, less this node's time just before
  # the start was asked for (so the start's own latency counts as Temporal
  # being ahead).
  defp start!(module, input, id, queue) do
    asked = DateTime.utc_now()
    {:ok, %WorkflowHandle{} = handle} = Hourglass.start(module, input, id: id, task_queue: queue)
    {:ok, %WorkflowStatus{start_time: %DateTime{} = started}} = Hourglass.status(handle)
    {handle, DateTime.diff(started, asked, :millisecond)}
  end

  # The workflow `handle` names closed in `state`, and less than the bound
  # before `ended_at` (this node's time), with Temporal's clock `ahead` ms
  # ahead of this node's.
  defp assert_within_bound(handle, ended_at, state, ahead) do
    assert {:ok, %WorkflowStatus{state: ^state, close_time: %DateTime{} = closed}} =
             Hourglass.status(handle)

    assert DateTime.diff(ended_at, closed, :millisecond) + ahead < @bound
  end

  defp now, do: System.monotonic_time(:millisecond)
end
