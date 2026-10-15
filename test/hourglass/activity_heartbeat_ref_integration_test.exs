defmodule Hourglass.ActivityHeartbeatRefIntegrationTest do
  @moduledoc """
  Another process heartbeats on behalf of an activity with the activity's
  heartbeat ref (`Hourglass.Activity.heartbeat_ref/0` and
  `Hourglass.Activity.heartbeat/1`) — against a real Temporal cluster.

  The activity (`Hourglass.TestSupport.HeartbeatRefActivity`) hands the test
  its ref and never heartbeats itself; the test process heartbeats with the
  ref, as a helper process linked to an activity would.

  `async: false`: one test forces a handle recycle, which kills a poll loop
  parked on a poll.
  """

  use ExUnit.Case, async: false
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.Activity
  alias Hourglass.Activity.HeartbeatRef
  alias Hourglass.Client.Real
  alias Hourglass.TestSupport.GatedWork
  alias Hourglass.TestSupport.Patience
  alias Hourglass.TestSupport.Runs
  alias Hourglass.TestSupport.RaceGate
  alias Hourglass.TestSupport.WorkerProbes
  alias Hourglass.WorkflowHandle
  alias Temporal.Api.History.V1.History

  @moduletag :temporal
  @moduletag :integration
  @moduletag timeout: Patience.test_timeout(4)
  @moduletag capture_log: true

  setup do
    queue = "hb-ref-#{System.unique_integer([:positive])}"
    {:ok, _worker} = Patience.start_worker(task_queue: queue)
    id = "hb-ref-#{UUIDv7.generate()}"
    :ok = Runs.end_on_exit(id)
    %{queue: queue, id: id}
  end

  test "another process heartbeats for the activity, and is told once the activity has ended",
       %{queue: queue, id: id} do
    {handle, ref, running} = start_activity!(id, queue)
    assert %HeartbeatRef{} = ref
    assert last_heartbeat(handle) == nil

    assert Activity.heartbeat(ref) == :ok

    WorkerProbes.poll_until("the heartbeat to reach Temporal", fn ->
      last_heartbeat(handle) != nil
    end)

    :ok = RaceGate.disarm({id, :activity})
    :ok = RaceGate.release(running)
    WorkerProbes.poll_until("the activity's completion", fn -> activity_completions(id) != [] end)

    # The activity is over: the ref sends nothing more, and says so.
    {activity_pid, _ref} = running
    monitor = Process.monitor(activity_pid)
    assert_receive {:DOWN, ^monitor, :process, ^activity_pid, _reason}, 5_000
    assert Activity.heartbeat(ref) == :ended
  end

  test "a ref heartbeats through the handle that handed the activity out, after a recycle",
       %{queue: queue, id: id} do
    {handle, ref, running} = start_activity!(id, queue)

    WorkerProbes.recycle!(queue)

    assert Activity.heartbeat(ref) == :ok

    WorkerProbes.poll_until("the heartbeat to reach Temporal", fn ->
      last_heartbeat(handle) != nil
    end)

    :ok = RaceGate.disarm({id, :activity})
    :ok = RaceGate.release(running)
    WorkerProbes.poll_until("the activity's completion", fn -> activity_completions(id) != [] end)
  end

  test "a ref answers :cancel once Core has cancelled the activity", %{queue: queue, id: id} do
    test = self()
    handler = {__MODULE__, id}

    :telemetry.attach(
      handler,
      [:hourglass, :activity, :cancel_received],
      fn _event, _measurements, _metadata, _config -> send(test, :cancel_received) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    # Nothing heartbeats, so the activity's one-second heartbeat timeout runs
    # out and Core cancels it.
    {_handle, ref, _running} = start_activity!(id, queue, %{"heartbeat_timeout" => 1_000})
    assert_receive :cancel_received, Patience.ms()

    # The cancelled activity is still running (it is held at its gate): the
    # ref tells whoever heartbeats for it that it has been cancelled.
    assert Activity.heartbeat(ref) == :cancel
    :ok = RaceGate.disarm({id, :activity})
  end

  test "outside an activity dispatch there is no ref" do
    assert Activity.heartbeat_ref() == nil
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Start a run of `HeartbeatRefActivity`, and return its workflow handle, the
  # ref the activity handed over, and the gate token holding it.
  defp start_activity!(id, queue, extra \\ %{}) do
    :ok = RaceGate.arm(id)
    :ok = RaceGate.arm({id, :activity})
    input = Map.merge(%{"gate" => id, "queue" => queue, "heartbeat_ref" => true}, extra)

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(GatedWork, input, id: id, task_queue: queue)

    assert_receive {RaceGate, :observed, ^id, {:heartbeat_ref, ref}}, Patience.ms()
    running = RaceGate.await_held({id, :activity})
    {handle, ref, running}
  end

  defp last_heartbeat(handle) do
    {:ok, description} = Patience.retry(fn -> Real.describe_workflow_execution(handle) end)

    case description.pending_activities do
      [pending] -> pending.last_heartbeat_time
      [] -> nil
    end
  end

  defp activity_completions(workflow_id) do
    {:ok, %History{events: events}} = Hourglass.history(workflow_id)
    for %{attributes: {:activity_task_completed_event_attributes, attrs}} <- events, do: attrs
  end
end
