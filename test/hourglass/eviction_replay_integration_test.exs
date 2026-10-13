defmodule Hourglass.EvictionReplayIntegrationTest do
  @moduledoc """
  A terminal command that an event races is replayed correctly after Core
  evicts the run — against a real Temporal cluster.

  When a workflow task carries a terminal command (complete or fail) and a new
  event lands on the run while that task is in flight, the server rejects the
  task with `UnhandledCommand`. Core then evicts the run and replays its
  history from the start. The replay must rebuild the run from history alone:
  a terminal outcome cached from the rejected task is not part of that
  history, and re-emitting it on the replay's first activation is a
  nondeterminism failure that recurs on every retry and leaves the run
  Running forever.

  Each racing event kind is forced deterministically, not left to timing:
  `Hourglass.TestSupport.RaceGate` holds the workflow task open after the body
  has decided to end, the test lands the racing event, and only then lets the
  task complete. Every test also asserts the history really records the
  `UnhandledCommand` rejection, so a test that failed to force the race fails
  rather than passing vacuously.
  """

  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.Client
  alias Hourglass.Replayer
  alias Hourglass.TestSupport.ActivityThenSignal
  alias Hourglass.TestSupport.CancelObserver
  alias Hourglass.TestSupport.RaceGate
  alias Hourglass.TestSupport.RacedContinue
  alias Hourglass.TestSupport.RacedEnd
  alias Hourglass.Worker.WorkflowStateCache
  alias Hourglass.Workflow.State
  alias Hourglass.WorkflowHandle
  alias Hourglass.WorkflowStatus
  alias Temporal.Api.History.V1.History

  @moduletag :temporal
  @moduletag :integration
  @moduletag timeout: 120_000
  # The rejected task and the replay log their own lines; surface on failure.
  @moduletag capture_log: false

  setup do
    queue = "evict-replay-#{System.unique_integer([:positive])}"
    {:ok, _worker} = start_supervised({Hourglass.Worker, task_queue: queue})
    %{queue: queue, id: "evict-replay-#{UUIDv7.generate()}"}
  end

  test "a cancel request that races a complete command is handled after the replay",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm(id)
    handle = start!(RacedEnd, %{"gate" => id}, id, queue)
    await_first_task_completed(handle)

    :ok = Hourglass.signal(handle, "stop", %{"n" => 1})
    held = RaceGate.await_held(id)

    # The body has decided to COMPLETE (it saw only the signal). Land the cancel
    # inside that workflow task, then let the task finish.
    :ok = Hourglass.cancel(handle, "racing the terminal command")
    :ok = RaceGate.disarm(id)
    :ok = RaceGate.release(held)

    assert %WorkflowStatus{state: :failed} = await_closed(handle)
    assert :WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND in task_failure_causes(id)

    # The replay saw the cancel and ended the run on it — not on the rejected
    # completion.
    assert {"Cancelled", %{"stop" => %{"n" => 1}, "cancelled" => true, "work" => nil}} =
             workflow_failure(id)

    assert :ok = replay(id, RacedEnd)
  end

  test "a signal that races a complete command is handled after the replay",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm(id)
    :ok = RaceGate.arm({id, :activity})
    handle = start!(RacedEnd, %{"gate" => id, "work" => true}, id, queue)

    activity = RaceGate.await_held({id, :activity})
    :ok = RaceGate.disarm({id, :activity})
    :ok = RaceGate.release(activity)

    # The activity's result reached the body, which decided to complete.
    held = RaceGate.await_held(id)
    :ok = Hourglass.signal(handle, "stop", %{"n" => 2})
    :ok = RaceGate.disarm(id)
    :ok = RaceGate.release(held)

    assert %WorkflowStatus{state: :completed} = await_closed(handle)
    assert :WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND in task_failure_causes(id)

    assert {:ok, %{"work" => %{"worked" => ^id}, "stop" => %{"n" => 2}, "cancelled" => false}} =
             Hourglass.result(handle, timeout: 5_000)

    assert :ok = replay(id, RacedEnd)
  end

  test "an activity completion that races a fail command is handled after the replay",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm(id)
    :ok = RaceGate.arm({id, :activity})
    handle = start!(RacedEnd, %{"gate" => id, "work" => true}, id, queue)

    activity = RaceGate.await_held({id, :activity})

    # The cancel reaches the body while the activity is still running: the body
    # decides to FAIL. Let the activity finish inside that workflow task.
    :ok = Hourglass.cancel(handle, "racing the terminal command")
    held = RaceGate.await_held(id)

    :ok = RaceGate.disarm({id, :activity})
    :ok = RaceGate.release(activity)
    await_no_pending_activities(handle)

    :ok = RaceGate.disarm(id)
    :ok = RaceGate.release(held)

    assert %WorkflowStatus{state: :failed} = await_closed(handle)
    assert :WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND in task_failure_causes(id)

    assert {"Cancelled", %{"work" => %{"worked" => ^id}, "cancelled" => true, "stop" => nil}} =
             workflow_failure(id)

    assert :ok = replay(id, RacedEnd)
  end

  test "cancelled?/0 answers a replay of the history exactly as it answered the original run",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm(id)
    handle = start!(CancelObserver, %{"probe" => id}, id, queue)
    await_first_task_completed(handle)

    # One workflow task per event, so each event has its own place in history.
    :ok = Hourglass.signal(handle, "tick", 1)
    original_1 = take_observations(id, 1)
    :ok = Hourglass.signal(handle, "tick", 2)
    original_2 = take_observations(id, 2)
    :ok = Hourglass.cancel(handle, "observed")
    original_3 = take_observations(id, 1)

    # Before the cancel's position in history every answer is false; from the
    # task that delivers it, true.
    assert original_1 == [[1, false]]
    assert original_2 == [[1, false], [2, false]]
    assert original_3 == [[1, true]]

    assert %WorkflowStatus{state: :completed} = await_closed(handle)

    # Replaying the recorded history answers every call identically.
    assert :ok = replay(id, CancelObserver)
    assert drain_observations(id) == original_1 ++ original_2 ++ original_3
  end

  # ---------------------------------------------------------------------------
  # Other ways for the same race, and for a replay, to arise
  # ---------------------------------------------------------------------------

  test "a timer that fires while a complete command is in flight is handled after the replay",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm(id)
    handle = start!(RacedEnd, %{"gate" => id, "timer_ms" => 1_000}, id, queue)
    await_first_task_completed(handle)

    :ok = Hourglass.signal(handle, "stop", %{"n" => 3})
    held = RaceGate.await_held(id)

    # Hold the task open past the timer's deadline so it fires inside it. A
    # timer has no RPC the test can wait on, so this one wait is a duration —
    # the UnhandledCommand assertion below is what proves it landed.
    Process.sleep(2_500)
    :ok = RaceGate.disarm(id)
    :ok = RaceGate.release(held)

    assert %WorkflowStatus{state: :completed} = await_closed(handle)
    assert :WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND in task_failure_causes(id)

    assert {:ok, %{"stop" => %{"n" => 3}, "timer" => "ok"}} =
             Hourglass.result(handle, timeout: 5_000)

    assert :ok = replay(id, RacedEnd)
  end

  test "a child workflow that completes while a complete command is in flight is handled after the replay",
       %{queue: queue, id: id} do
    child_id = "#{id}-child"
    :ok = RaceGate.arm(id)
    handle = start!(RacedEnd, %{"gate" => id, "child" => child_id}, id, queue)
    await_running(child_id)

    :ok = Hourglass.signal(handle, "stop", %{"n" => 4})
    held = RaceGate.await_held(id)

    :ok = Hourglass.signal(child_id, "go", %{"child" => true})

    assert %WorkflowStatus{state: :completed} =
             await_closed(%WorkflowHandle{id: child_id, run_id: ""})

    # The child's close reaches the parent as a separate server-side step.
    Process.sleep(1_000)

    :ok = RaceGate.disarm(id)
    :ok = RaceGate.release(held)

    assert %WorkflowStatus{state: :completed} = await_closed(handle)
    assert :WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND in task_failure_causes(id)

    assert {:ok, %{"stop" => %{"n" => 4}, "child" => %{"go" => %{"child" => true}}}} =
             Hourglass.result(handle, timeout: 5_000)

    assert :ok = replay(id, RacedEnd)
  end

  test "a cancel that races a continue-as-new command is handled after the replay",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm(id)
    handle = start!(RacedContinue, %{"gate" => id}, id, queue)
    await_first_task_completed(handle)

    :ok = Hourglass.signal(handle, "stop", %{"n" => 5})
    held = RaceGate.await_held(id)
    :ok = Hourglass.cancel(handle, "racing continue-as-new")
    :ok = RaceGate.disarm(id)
    :ok = RaceGate.release(held)

    assert %WorkflowStatus{state: :failed} = await_closed(handle)
    assert :WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND in task_failure_causes(id)
    assert {"Cancelled", %{"stop" => %{"n" => 5}}} = workflow_failure(id)
    assert :ok = replay(id, RacedContinue)
  end

  test "a terminal command rejected twice in a row is replayed correctly both times",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm(id)
    handle = start!(RacedEnd, %{"gate" => id}, id, queue)
    await_first_task_completed(handle)

    :ok = Hourglass.signal(handle, "stop", %{"n" => 6})
    first = RaceGate.await_held(id)

    # A signal nothing waits for still makes the server reject the task. Keep
    # the gate armed: the replay's own decision to complete holds again.
    :ok = Hourglass.signal(handle, "unrelated", %{})
    :ok = RaceGate.release(first)

    second = RaceGate.await_held(id)
    :ok = Hourglass.cancel(handle, "racing the second terminal command")
    :ok = RaceGate.disarm(id)
    :ok = RaceGate.release(second)

    assert %WorkflowStatus{state: :failed} = await_closed(handle)
    assert :WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND in task_failure_causes(id)
    assert {"Cancelled", %{"stop" => %{"n" => 6}, "cancelled" => true}} = workflow_failure(id)
    assert :ok = replay(id, RacedEnd)
  end

  test "a worker restart replays the run from history, not from what the old worker cached",
       %{queue: queue, id: id} do
    handle = start!(ActivityThenSignal, %{"nonce" => id}, id, queue)
    assert handle.run_id not in [nil, ""]

    # The activity has run and the body is waiting for the signal: this
    # worker's state for the run now holds the activity's result.
    poll_until("the activity's result to reach the workflow", fn ->
      completed = for %{event_type: :EVENT_TYPE_WORKFLOW_TASK_COMPLETED} <- events(id), do: true
      length(completed) >= 2
    end)

    stale = WorkflowStateCache.fetch(queue, handle.run_id)
    assert %State{resolved_results: resolved} = stale
    assert map_size(resolved) == 1

    # A new worker on the same queue gets a new Core with an empty cache, so it
    # replays the run from the start of its history. The old worker's state
    # for the run is still cached in this VM — a stopping worker unregisters
    # its bridge before Core can drain its cache, so no eviction reaches it.
    # Put it back regardless, so the test does not depend on that.
    :ok = stop_supervised(Hourglass.Worker)
    :ok = WorkflowStateCache.put(queue, handle.run_id, stale)
    {:ok, _worker} = start_supervised({Hourglass.Worker, task_queue: queue})

    :ok = Hourglass.signal(handle, "next", %{"n" => 7})

    assert %WorkflowStatus{state: :completed} = await_closed(handle, 60_000)

    assert {:ok, %{"echoed" => %{"nonce" => ^id}, "next" => %{"n" => 7}}} =
             Hourglass.result(handle, timeout: 5_000)

    assert :ok = replay(id, ActivityThenSignal)
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp start!(module, input, id, queue) do
    {:ok, %WorkflowHandle{} = handle} = Hourglass.start(module, input, id: id, task_queue: queue)
    handle
  end

  # Wait for a closed state. A nondeterminism failure in the history is the
  # failure this suite exists to catch and it never resolves on its own, so it
  # ends the wait at once rather than burning the whole budget.
  defp await_closed(handle, timeout \\ 45_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_await_closed(handle, deadline)
  end

  defp do_await_closed(handle, deadline) do
    {:ok, %WorkflowStatus{state: state} = status} = Hourglass.status(handle)

    cond do
      state != :running ->
        # Closing is not enough: a run that hit a nondeterminism failure and
        # only recovered after Core evicted it again was still replayed wrong.
        assert nondeterminism_failure(handle.id) == nil
        status

      nondeterminism = nondeterminism_failure(handle.id) ->
        flunk("the run is wedged on a nondeterminism failure: #{nondeterminism}")

      System.monotonic_time(:millisecond) > deadline ->
        flunk(
          "the run did not close in time; task failures: #{inspect(task_failure_causes(handle.id))}"
        )

      true ->
        Process.sleep(200)
        do_await_closed(handle, deadline)
    end
  end

  defp await_running(workflow_id) do
    poll_until("#{workflow_id} to start", fn ->
      match?({:ok, %WorkflowStatus{state: :running}}, Hourglass.status(workflow_id))
    end)
  end

  defp await_first_task_completed(handle) do
    poll_until("the first workflow task to complete", fn ->
      handle.id
      |> events()
      |> Enum.any?(&(&1.event_type == :EVENT_TYPE_WORKFLOW_TASK_COMPLETED))
    end)
  end

  defp await_no_pending_activities(handle) do
    poll_until("the activity completion to be recorded", fn ->
      {:ok, %WorkflowStatus{pending_activities: pending}} = Hourglass.status(handle)
      pending == []
    end)
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

  defp events(workflow_id) do
    {:ok, %History{events: events}} = Hourglass.history(workflow_id)
    events
  end

  defp task_failure_causes(workflow_id) do
    for %{attributes: {:workflow_task_failed_event_attributes, attrs}} <- events(workflow_id),
        do: attrs.cause
  end

  defp nondeterminism_failure(workflow_id) do
    workflow_id
    |> events()
    |> Enum.find_value(fn
      %{
        attributes:
          {:workflow_task_failed_event_attributes,
           %{cause: :WORKFLOW_TASK_FAILED_CAUSE_NON_DETERMINISTIC_ERROR, failure: failure}}
      } ->
        failure.message

      _event ->
        nil
    end)
  end

  defp workflow_failure(workflow_id) do
    workflow_id
    |> events()
    |> Enum.find_value(fn
      %{attributes: {:workflow_execution_failed_event_attributes, %{failure: failure}}} ->
        {:application_failure_info, info} = failure.failure_info
        [payload] = info.details.payloads
        {info.type, Jason.decode!(payload.data)}

      _event ->
        nil
    end)
  end

  defp replay(workflow_id, module) do
    {:ok, client} = Client.connect([])
    {:ok, history_bytes} = Client.fetch_history(client, workflow_id)
    Replayer.replay_history(history_bytes, module)
  end

  defp take_observations(probe, count) do
    for _n <- 1..count//1 do
      assert_receive {RaceGate, :observed, ^probe, observation}, 20_000
      observation
    end
  end

  defp drain_observations(probe) do
    receive do
      {RaceGate, :observed, ^probe, observation} -> [observation | drain_observations(probe)]
    after
      0 -> []
    end
  end
end
