defmodule Hourglass.BridgeHolderTest do
  # async: false — these tests kill processes parked in the dirty-IO long-poll
  # NIF, orphaning a poll that holds one of the VM's (default 10) dirty-IO
  # scheduler threads until a worker_shutdown (also dirty-IO) drains it. Run
  # concurrently with the rest of the cluster suite, enough orphans pile up to
  # exhaust the dirty-IO schedulers and deadlock the drain. Serial keeps the
  # concurrent orphan count tiny.
  use ExUnit.Case, async: false

  alias Hourglass.Bridge
  alias Hourglass.BridgeHolder
  alias Hourglass.Client

  @moduletag :temporal
  # Bridge-NIF contract test — exercises real BridgeHolder.register_worker /
  # unregister against a live Temporal cluster. Cannot be mocked at this
  # layer; the whole point is proving the NIF round-trips proto bytes
  # correctly. Default `mix test` excludes :integration; run via
  # `mix test --include integration`.
  @moduletag :integration
  # Recycle-on-DOWN tests deliberately Process.exit/2 :kill the poll
  # caller; BridgeHolder responds with a Logger.warning for each
  # recycle. Capture the logs so test runner output stays clean —
  # they're emitted on test failure for diagnostics, not on success.
  @moduletag capture_log: true

  describe "registered?/1" do
    test "returns false for an unregistered task queue" do
      queue = "bh-unregistered-#{System.unique_integer([:positive])}"
      refute BridgeHolder.registered?(queue)
    end
  end

  describe "register_worker/2 + unregister_worker/1 (real bridge)" do
    # These tests exercise register/unregister as the SUBJECT, so they
    # register inside the test body. on_exit guarantees the handle is torn
    # down even if an assertion fails before the inline unregister, so a
    # leaked handle (with a parked long-poll) never destabilises the shared
    # BridgeHolder for concurrent tests.
    test "register publishes a handle; unregister destroys it" do
      queue = "bh-register-#{System.unique_integer([:positive])}"
      on_exit(fn -> _result = BridgeHolder.unregister_worker(queue) end)

      refute BridgeHolder.registered?(queue)
      :ok = BridgeHolder.register_worker(queue, target_url: Client.default_target_url())
      assert BridgeHolder.registered?(queue)

      :ok = BridgeHolder.unregister_worker(queue)
      refute BridgeHolder.registered?(queue)
    end

    test "register twice on the same task queue returns :already_registered" do
      queue = "bh-double-#{System.unique_integer([:positive])}"
      on_exit(fn -> _result = BridgeHolder.unregister_worker(queue) end)

      :ok = BridgeHolder.register_worker(queue, target_url: Client.default_target_url())
      assert {:error, :already_registered} = BridgeHolder.register_worker(queue, [])

      :ok = BridgeHolder.unregister_worker(queue)
    end

    test "unregister of an unknown task queue is idempotent (returns :ok)" do
      :ok =
        BridgeHolder.unregister_worker("never-registered-#{System.unique_integer([:positive])}")
    end
  end

  describe "completion path returns :worker_not_registered when not registered" do
    test "complete_workflow_activation returns {:error, :worker_not_registered}" do
      queue = "bh-complete-wf-#{System.unique_integer([:positive])}"

      assert {:error, :worker_not_registered} =
               BridgeHolder.complete_workflow_activation(queue, <<>>)
    end

    test "complete_activity_task returns {:error, :worker_not_registered}" do
      queue = "bh-complete-act-#{System.unique_integer([:positive])}"

      assert {:error, :worker_not_registered} =
               BridgeHolder.complete_activity_task(queue, <<>>)
    end

    test "poll_workflow_activation returns {:error, :worker_not_registered}" do
      queue = "bh-poll-wf-#{System.unique_integer([:positive])}"

      assert {:error, :worker_not_registered} =
               BridgeHolder.poll_workflow_activation(queue)
    end

    test "poll_activity_task returns {:error, :worker_not_registered}" do
      queue = "bh-poll-act-#{System.unique_integer([:positive])}"

      assert {:error, :worker_not_registered} =
               BridgeHolder.poll_activity_task(queue)
    end
  end

  # The tests below all need a worker handle registered up front and then
  # exercise poll/recycle behaviour against it. A describe-scoped setup
  # registers a fresh queue per test and tears it down via on_exit, so the
  # test bodies never repeat the register/unregister dance and a failure
  # mid-test can't leak a handle.
  describe "long-poll dispatch (real bridge)" do
    setup :registered_queue

    test "poll returns {:error, :shutdown} after unregister destroys the bridge",
         %{queue: queue} do
      # Spawn a poll Task that will block in the dirty NIF until we
      # unregister. We spawn an Elixir Task here so the test process
      # can continue and trigger the unregister.
      test_pid = self()

      spawn_link(fn ->
        result = BridgeHolder.poll_workflow_activation(queue)
        send(test_pid, {:poll_returned, result})
      end)

      # Give the poll a moment to enter the NIF, then drop the bridge.
      Process.sleep(50)
      :ok = BridgeHolder.unregister_worker(queue)

      assert_receive {:poll_returned, {:error, %Bridge.Error{kind: :shutdown}}}, 5_000
    end
  end

  describe "poll-caller DOWN => handle recycle" do
    setup :registered_queue

    test "registered? stays true after a poll caller dies parked on the NIF",
         %{queue: queue} do
      # The DOWN handler must recycle, NOT unregister. After the dust
      # settles the task queue must still have a handle so the parent
      # Worker keeps polling.
      assert BridgeHolder.registered?(queue)

      # Spawn a process that calls the long-poll then immediately gets
      # killed. The dirty-NIF call cannot be cancelled, so an orphan
      # Task stays parked in BridgeHolder.task_sup until the recycle's
      # worker_shutdown drains it.
      poll_pid =
        spawn(fn ->
          _result = BridgeHolder.poll_workflow_activation(queue)
        end)

      # Give the GenServer.call enough time to land + dispatch the
      # poll Task (which monitors poll_pid).
      Process.sleep(50)
      ref = Process.monitor(poll_pid)
      Process.exit(poll_pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^poll_pid, :killed}, 1_000

      # The DOWN handler dispatches an async recycle Task — give it a
      # window for both worker_shutdown + worker_new to land.
      assert_eventually(fn -> BridgeHolder.registered?(queue) end, 10_000)

      # Subsequent polls should succeed against the new handle.
      # Validate by spawning a fresh poller and unregistering — the
      # poll must return {:error, :shutdown}, proving it actually went
      # to the NIF (not :worker_not_registered).
      test_pid = self()

      spawn_link(fn ->
        result = BridgeHolder.poll_workflow_activation(queue)
        send(test_pid, {:fresh_poll, result})
      end)

      Process.sleep(100)
      :ok = BridgeHolder.unregister_worker(queue)
      assert_receive {:fresh_poll, {:error, %Bridge.Error{kind: :shutdown}}}, 5_000
    end

    test "healthy parallel poller sees :shutdown during recycle and can retry",
         %{queue: queue} do
      test_pid = self()

      # Healthy poller (we control its lifecycle).
      healthy =
        spawn(fn ->
          result_a = BridgeHolder.poll_workflow_activation(queue)
          send(test_pid, {:healthy_first, result_a})

          # Mimic the poll-loop's :shutdown retry behavior: if the
          # holder still has a handle, retry. We just observe one
          # retry to prove the new handle is reachable.
          if BridgeHolder.registered?(queue) do
            result_b = BridgeHolder.poll_workflow_activation(queue)
            send(test_pid, {:healthy_second, result_b})
          end
        end)

      # Soon-to-die poller. The DOWN triggers the recycle.
      doomed =
        spawn(fn ->
          _result = BridgeHolder.poll_workflow_activation(queue)
        end)

      Process.sleep(50)
      Process.exit(doomed, :kill)

      # Healthy poller sees :shutdown from the recycle's worker_shutdown.
      assert_receive {:healthy_first, {:error, %Bridge.Error{kind: :shutdown}}}, 10_000

      # Wait for recycle to finish + healthy to re-poll, then drop the
      # new handle to unblock its second poll.
      Process.sleep(200)
      :ok = BridgeHolder.unregister_worker(queue)

      assert_receive {:healthy_second, {:error, %Bridge.Error{kind: :shutdown}}}, 5_000

      # Cleanup the healthy spawn (it has already exited; we just don't
      # want a dangling reference).
      _alive = Process.alive?(healthy)
    end

    test "recycle works for activity poll callers as well", %{queue: queue} do
      assert BridgeHolder.registered?(queue)

      poll_pid =
        spawn(fn ->
          _result = BridgeHolder.poll_activity_task(queue)
        end)

      Process.sleep(50)
      ref = Process.monitor(poll_pid)
      Process.exit(poll_pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^poll_pid, :killed}, 1_000

      assert_eventually(fn -> BridgeHolder.registered?(queue) end, 10_000)

      test_pid = self()

      spawn_link(fn ->
        result = BridgeHolder.poll_activity_task(queue)
        send(test_pid, {:fresh_poll, result})
      end)

      Process.sleep(100)
      :ok = BridgeHolder.unregister_worker(queue)
      assert_receive {:fresh_poll, {:error, %Bridge.Error{kind: :shutdown}}}, 5_000
    end

    test "unregister during recycle wins the race; recycled handle is discarded",
         %{queue: queue} do
      # Kick off a poll, kill the poller (recycle starts), then immediately
      # unregister. The recycle's worker_new will land after the unregister;
      # the holder must shut down the recycled handle on the spot rather than
      # installing it.
      poll_pid =
        spawn(fn ->
          _result = BridgeHolder.poll_workflow_activation(queue)
        end)

      Process.sleep(50)
      Process.exit(poll_pid, :kill)

      # Race in the unregister — it pops state.handles synchronously.
      :ok = BridgeHolder.unregister_worker(queue)

      # The end state must be unregistered. Even after the recycle's
      # worker_new completes (which may take a few hundred ms), the
      # holder must NOT install the new handle.
      Process.sleep(500)
      refute BridgeHolder.registered?(queue)
    end
  end

  # Shared setup for the describes whose tests operate on an already-registered
  # worker handle: register a fresh queue and guarantee its teardown.
  defp registered_queue(_context) do
    queue = "bh-#{System.unique_integer([:positive])}"
    :ok = BridgeHolder.register_worker(queue, target_url: Client.default_target_url())
    on_exit(fn -> _result = BridgeHolder.unregister_worker(queue) end)
    %{queue: queue}
  end

  defp assert_eventually(fun, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_assert_eventually(fun, deadline)
  end

  defp do_assert_eventually(fun, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("condition did not become true within budget")

      true ->
        Process.sleep(25)
        do_assert_eventually(fun, deadline)
    end
  end
end
