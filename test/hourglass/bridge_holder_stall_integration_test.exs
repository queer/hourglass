defmodule Hourglass.BridgeHolderStallIntegrationTest do
  @moduledoc """
  One activity's completion stuck on its way to Temporal holds up no other
  activity on the node — against a real Temporal cluster.

  Every activity on a node reports through the one `Hourglass.BridgeHolder`.
  A completion or heartbeat that blocked the holder while its bridge call
  waited on the network would stall every other activity's completions and
  heartbeats behind it, on every task queue, for as long as it waited.

  The test stalls one activity's completion at the bridge
  (`Hourglass.TestSupport.StallableBridge`, which stands in for the bridge's
  activity calls in the test build) and shows a second activity on the same
  worker heartbeating and completing meanwhile.

  `async: false`: a stalled bridge call holds a dirty-IO scheduler thread no
  more than a real one would, but the test keeps one stalled on purpose.
  """

  use ExUnit.Case, async: false
  use Hourglass.Test.UseRealTemporalBackend

  alias Coresdk.ActivityHeartbeat
  alias Hourglass.Activity.Info
  alias Hourglass.BridgeHolder
  alias Hourglass.Client.Real
  alias Hourglass.TestSupport.GatedWork
  alias Hourglass.TestSupport.RaceGate
  alias Hourglass.TestSupport.StallableBridge
  alias Hourglass.WorkflowHandle
  alias Temporal.Api.History.V1.History

  @moduletag :temporal
  @moduletag :integration
  @moduletag timeout: 120_000
  @moduletag capture_log: true

  setup do
    queue = "stall-#{System.unique_integer([:positive])}"
    {:ok, _worker} = start_supervised({Hourglass.Worker, task_queue: queue})
    %{queue: queue}
  end

  test "with one activity's completion stuck, another activity on the node still heartbeats and completes",
       %{queue: queue} do
    stuck = "stall-#{UUIDv7.generate()}"
    other = "stall-#{UUIDv7.generate()}"

    :ok = RaceGate.arm({stuck, :activity})
    :ok = RaceGate.arm({other, :activity})
    _stuck_handle = start!(stuck, queue)
    other_handle = start!(other, queue)
    {stuck_pid, _ref} = stuck_running = RaceGate.await_held({stuck, :activity})
    {other_pid, _ref} = other_running = RaceGate.await_held({other, :activity})

    # The first activity finishes, and its completion sticks in the bridge.
    stall = {StallableBridge, :complete, task_token(stuck_pid)}
    :ok = RaceGate.arm(stall)
    :ok = RaceGate.disarm({stuck, :activity})
    :ok = RaceGate.release(stuck_running)
    stalled = RaceGate.await_held(stall)

    # The other activity's heartbeat is recorded at once, and reaches Temporal.
    heartbeat = Task.async(fn -> heartbeat(other_pid, queue) end)
    assert Task.yield(heartbeat, 2_000) == {:ok, :ok}
    poll_until("the heartbeat to reach Temporal", fn -> last_heartbeat(other_handle) != nil end)

    # And its completion reaches Temporal too.
    :ok = RaceGate.disarm({other, :activity})
    :ok = RaceGate.release(other_running)
    poll_until("the other activity's completion", fn -> activity_completions(other) != [] end)

    # The stuck completion, released, still reaches Temporal.
    assert activity_completions(stuck) == []
    :ok = RaceGate.disarm(stall)
    :ok = RaceGate.release(stalled)
    poll_until("the stuck activity's completion", fn -> activity_completions(stuck) != [] end)
  end

  test "with one activity's heartbeat stuck, the same activity's completion and others' still go through",
       %{queue: queue} do
    id = "stall-#{UUIDv7.generate()}"
    :ok = RaceGate.arm({id, :activity})
    _handle = start!(id, queue)
    {pid, _ref} = running = RaceGate.await_held({id, :activity})

    stall = {StallableBridge, :heartbeat, task_token(pid)}
    :ok = RaceGate.arm(stall)
    _stuck_heartbeat = Task.async(fn -> heartbeat(pid, queue) end)
    stalled = RaceGate.await_held(stall)

    :ok = RaceGate.disarm({id, :activity})
    :ok = RaceGate.release(running)
    poll_until("the activity's completion", fn -> activity_completions(id) != [] end)

    :ok = RaceGate.disarm(stall)
    :ok = RaceGate.release(stalled)
  end

  test "a stopping worker's shutdown reports, all stuck on the network, do not push the stop past its budget" do
    queue = "stall-stop-#{System.unique_integer([:positive])}"
    grace = 500

    {:ok, _worker} =
      Hourglass.Worker.Supervisor.start_worker(task_queue: queue, graceful_shutdown_period: grace)

    on_exit(fn -> _result = Hourglass.Worker.Supervisor.stop_worker(queue) end)

    # Three activities still running at the grace, each to be reported as
    # failed by the shutdown — and each report stuck in the bridge.
    ids = for _n <- 1..3, do: "stall-#{UUIDv7.generate()}"
    Enum.each(ids, &(:ok = RaceGate.arm({&1, :activity})))
    Enum.each(ids, &start!(&1, queue))

    stalls =
      for id <- ids do
        {pid, _ref} = RaceGate.await_held({id, :activity})
        stall = {StallableBridge, :complete, task_token(pid)}
        :ok = RaceGate.arm(stall)
        stall
      end

    started = System.monotonic_time(:millisecond)
    assert Hourglass.Worker.Supervisor.stop_worker(queue) == :ok
    elapsed = System.monotonic_time(:millisecond) - started

    # Within the shutdown timeout the worker's child spec sets.
    assert elapsed <
             Hourglass.Worker.child_spec(task_queue: queue, graceful_shutdown_period: grace).shutdown

    # The reports did reach the stalled bridge — all of them, at once.
    for stall <- stalls, do: RaceGate.await_held(stall, 0)
    Enum.each(stalls, &RaceGate.disarm/1)
    Enum.each(ids, &RaceGate.disarm({&1, :activity}))
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp start!(id, queue) do
    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(GatedWork, %{"gate" => id, "queue" => queue}, id: id, task_queue: queue)

    handle
  end

  # The task token of the activity `pid` runs, from the context the runner
  # gives it.
  defp task_token(pid) do
    {:dictionary, dictionary} = Process.info(pid, :dictionary)
    {_key, %Info{task_token: token}} = List.keyfind(dictionary, {Hourglass.Activity, :info}, 0)
    token
  end

  defp heartbeat(pid, queue) do
    bin = Protobuf.encode(%ActivityHeartbeat{task_token: task_token(pid), details: []})
    BridgeHolder.record_heartbeat(queue, bin)
  end

  defp last_heartbeat(handle) do
    {:ok, description} = Real.describe_workflow_execution(handle)
    [pending] = description.pending_activities
    pending.last_heartbeat_time
  end

  defp activity_completions(workflow_id) do
    {:ok, %History{events: events}} = Hourglass.history(workflow_id)
    for %{attributes: {:activity_task_completed_event_attributes, attrs}} <- events, do: attrs
  end

  defp poll_until(what, fun, retries \\ 100)
  defp poll_until(what, _fun, 0), do: flunk("timed out waiting for #{what}")

  defp poll_until(what, fun, retries) do
    if fun.() do
      :ok
    else
      Process.sleep(100)
      poll_until(what, fun, retries - 1)
    end
  end
end
