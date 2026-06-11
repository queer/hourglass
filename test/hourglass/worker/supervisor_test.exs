defmodule Hourglass.Worker.SupervisorTest do
  # async: false — kills the workflow poll loop parked in the dirty-IO long-poll
  # NIF, orphaning a poll that holds a dirty-IO scheduler thread until a
  # worker_shutdown drains it. Serial keeps the concurrent orphan count below the
  # (default 10) dirty-IO scheduler pool that a concurrent run can exhaust+deadlock.
  use ExUnit.Case, async: false

  alias Hourglass.BridgeHolder
  alias Hourglass.Worker
  alias Hourglass.Worker.ActivityPollLoop
  alias Hourglass.Worker.WorkflowPollLoop
  alias Hourglass.WorkerRegistry

  @moduletag :temporal
  # Server-semantic test — exercises real Worker lifecycle (register +
  # poll loop + shutdown) against a live cluster. Cannot be mocked.
  # Default `mix test` excludes :integration.
  @moduletag :integration
  # supervisor_test.exs deliberately Process.exit/2 :kill the
  # workflow poll loop to test cascade behavior; BridgeHolder logs the
  # recycle and the killed loop's OTP terminate line lands too. Capture
  # to keep runner output clean (still emitted on failure).
  @moduletag capture_log: true

  defmodule TestWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input), do: {:ok, :done}
  end

  describe "Worker as GenServer + inner Supervisor" do
    # All three tests operate on a started Worker. Start one per test and tear
    # it down via on_exit, so the test bodies don't repeat the boilerplate and
    # a failure mid-test can't leak a registered bridge handle.
    setup do
      queue = "sup-#{System.unique_integer([:positive])}"

      {:ok, gs_pid} =
        Worker.start_link(task_queue: queue)

      on_exit(fn -> if Process.alive?(gs_pid), do: Process.exit(gs_pid, :shutdown) end)
      %{queue: queue, gs_pid: gs_pid}
    end

    test "init registers a bridge handle and starts the two poll loops",
         %{queue: queue, gs_pid: gs_pid} do
      # The via-name resolves to the GenServer pid.
      whereis =
        queue
        |> WorkerRegistry.via()
        |> GenServer.whereis()

      assert gs_pid == whereis

      # BridgeHolder registered the bridge handle.
      assert BridgeHolder.registered?(queue)

      # Inner Supervisor has the two poll loops.
      sup_pid = Worker.inner_supervisor(queue)
      assert is_pid(sup_pid)

      children = Supervisor.which_children(sup_pid)
      assert [_first, _second] = children

      ids =
        children
        |> Enum.map(&elem(&1, 0))
        |> Enum.sort()

      assert Enum.sort([WorkflowPollLoop, ActivityPollLoop]) == ids
    end

    test "graceful shutdown destroys the bridge handle and exits cleanly",
         %{queue: queue, gs_pid: gs_pid} do
      Process.flag(:trap_exit, true)

      assert BridgeHolder.registered?(queue)

      ref = Process.monitor(gs_pid)
      :ok = GenServer.stop(gs_pid, :shutdown, 10_000)
      assert_receive {:DOWN, ^ref, :process, ^gs_pid, _reason}, 10_000

      refute BridgeHolder.registered?(queue)
    end

    test "poll-loop crash restarts the loop independently (does not destroy the bridge)",
         %{queue: queue} do
      sup_pid = Worker.inner_supervisor(queue)

      pids_by_id = fn ->
        sup_pid
        |> Supervisor.which_children()
        |> Map.new(fn {id, pid, _type, _mods} -> {id, pid} end)
      end

      before = pids_by_id.()
      workflow_loop = Map.fetch!(before, WorkflowPollLoop)

      ref = Process.monitor(workflow_loop)
      Process.exit(workflow_loop, :kill)
      assert_receive {:DOWN, ^ref, :process, ^workflow_loop, :killed}, 2_000

      # The loop restarts under :one_for_one; the bridge stays registered.
      assert_eventually(fn ->
        after_pids = pids_by_id.()
        new_loop = Map.get(after_pids, WorkflowPollLoop)
        is_pid(new_loop) and new_loop != workflow_loop
      end)

      assert BridgeHolder.registered?(queue)
    end
  end

  describe "Worker.Supervisor.start_worker/1 + stop_worker/1" do
    # Worker.Supervisor is booted globally in test_helper.exs as a shared
    # singleton; per-test isolation comes from unique task_queue names
    # (each Worker child is keyed by queue, so concurrent async tests
    # don't collide).

    test "start_worker brackets BridgeHolder.register; stop_worker brackets unregister" do
      queue = "ws-bracket-#{System.unique_integer([:positive])}"

      {:ok, _pid} =
        Hourglass.Worker.Supervisor.start_worker(task_queue: queue)

      # Supervised by the global Worker.Supervisor, so it outlives the test
      # process — guarantee teardown even if an assertion below fails.
      on_exit(fn -> _result = Hourglass.Worker.Supervisor.stop_worker(queue) end)

      assert BridgeHolder.registered?(queue)

      :ok = Hourglass.Worker.Supervisor.stop_worker(queue)

      refute BridgeHolder.registered?(queue)
    end

    test "stop_worker on an unregistered queue returns {:error, :not_found}" do
      queue = "ws-missing-#{System.unique_integer([:positive])}"

      assert {:error, :not_found} = Hourglass.Worker.Supervisor.stop_worker(queue)
    end
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
