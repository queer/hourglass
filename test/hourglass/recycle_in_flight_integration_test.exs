defmodule Hourglass.RecycleInFlightIntegrationTest do
  @moduledoc """
  The work a bridge handle handed out still reports its outcome when the
  holder recycles that handle — against a real Temporal cluster.

  `Hourglass.BridgeHolder` recycles a task queue's handle when a poll caller
  dies parked on a poll: it shuts the old handle down and installs a fresh
  one. An activity the old handle handed out is outstanding in the OLD Core
  worker, and only that worker can report its outcome to Temporal; sent to
  the new one, the completion names a task Core never handed out, and is
  dropped. Temporal would then hear nothing until the activity's
  start-to-close timeout.

  Each test forces the recycle by killing the worker's activity poll loop
  while it is parked on a poll, as `test/hourglass/bridge_holder_test.exs`
  does with a bare poller.

  `async: false`: killing a parked poll loop orphans its poll, which holds a
  dirty-IO scheduler thread until the recycle releases it.
  """

  use ExUnit.Case, async: false
  use Hourglass.Test.UseRealTemporalBackend

  alias Coresdk.ActivityHeartbeat
  alias Hourglass.Activity.Info
  alias Hourglass.BridgeHolder
  alias Hourglass.Client.Real
  alias Hourglass.TestSupport.GatedWork
  alias Hourglass.TestSupport.RaceGate
  alias Hourglass.TestSupport.WorkerProbes
  alias Hourglass.Worker
  alias Hourglass.WorkflowHandle
  alias Temporal.Api.History.V1.History

  @moduletag :temporal
  @moduletag :integration
  @moduletag timeout: 120_000
  @moduletag capture_log: true

  setup do
    queue = "recycle-#{System.unique_integer([:positive])}"
    {:ok, _worker} = start_supervised({Worker, task_queue: queue})
    %{queue: queue, id: "recycle-#{UUIDv7.generate()}"}
  end

  test "an activity running when its handle is recycled reports its outcome to Temporal",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm({id, :activity})
    handle = start!(GatedWork, %{"gate" => id, "queue" => queue}, id, queue)
    running = RaceGate.await_held({id, :activity})

    polls = WorkerProbes.parked_polls()
    WorkerProbes.recycle!(queue)

    # The old handle's polls returned, the orphan the killed loop left included,
    # though the activity the handle handed out is still running: none holds a
    # dirty-IO thread now.
    assert Enum.filter(polls, &(&1 in WorkerProbes.parked_polls())) == []

    :ok = RaceGate.disarm({id, :activity})
    :ok = RaceGate.release(running)

    # The completion reaches Temporal now, through the handle that handed the
    # activity out — not after the activity's two-minute timeout.
    WorkerProbes.poll_until("the activity's completion to be recorded", fn ->
      activity_completions(id) != []
    end)

    assert [%{result: %{payloads: [payload]}}] = activity_completions(id)
    assert Jason.decode!(payload.data) == %{"worked" => id}

    # The fresh handle serves the queue: the rest of the run completes on it.
    :ok = Hourglass.signal(handle, "more", %{})
    assert {:ok, %{"echoed" => %{"nonce" => ^id}}} = Hourglass.result(handle, timeout: 30_000)
  end

  test "an activity running when its handle is recycled can still heartbeat through it",
       %{queue: queue, id: id} do
    :ok = RaceGate.arm({id, :activity})
    handle = start!(GatedWork, %{"gate" => id, "queue" => queue}, id, queue)
    {activity_pid, _ref} = running = RaceGate.await_held({id, :activity})

    WorkerProbes.recycle!(queue)

    # A heartbeat goes to the handle that handed the activity out, which sends
    # it on to Temporal, rather than to the fresh one, which drops it as
    # naming an activity it never handed out.
    assert last_heartbeat(handle) == nil
    assert :ok = heartbeat_from(activity_pid, queue)

    WorkerProbes.poll_until("the heartbeat to reach Temporal", fn ->
      last_heartbeat(handle) != nil
    end)

    :ok = RaceGate.disarm({id, :activity})
    :ok = RaceGate.release(running)

    WorkerProbes.poll_until("the activity's completion to be recorded", fn ->
      activity_completions(id) != []
    end)
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Record a heartbeat as the activity's own process would: the heartbeat
  # names the activity's task token, which `Hourglass.Activity.info/0` holds in
  # that process's dictionary.
  defp heartbeat_from(activity_pid, queue) do
    {:dictionary, dictionary} = Process.info(activity_pid, :dictionary)

    {_key, %Info{task_token: token}} =
      List.keyfind(dictionary, {Hourglass.Activity, :info}, 0)

    bin = Protobuf.encode(%ActivityHeartbeat{task_token: token, details: []})
    BridgeHolder.record_heartbeat(queue, bin)
  end

  defp last_heartbeat(handle) do
    {:ok, description} = Real.describe_workflow_execution(handle)
    [pending] = description.pending_activities
    pending.last_heartbeat_time
  end

  defp start!(module, input, id, queue) do
    {:ok, %WorkflowHandle{} = handle} = Hourglass.start(module, input, id: id, task_queue: queue)
    handle
  end

  defp activity_completions(workflow_id) do
    {:ok, %History{events: events}} = Hourglass.history(workflow_id)
    for %{attributes: {:activity_task_completed_event_attributes, attrs}} <- events, do: attrs
  end
end
