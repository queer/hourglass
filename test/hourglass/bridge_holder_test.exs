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
  alias Hourglass.TestSupport.FreezableProxy
  alias Hourglass.TestSupport.Patience
  alias Hourglass.TestSupport.WorkerProbes

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

      :ok =
        Patience.retry(fn ->
          BridgeHolder.register_worker(queue, target_url: Client.default_target_url())
        end)

      assert BridgeHolder.registered?(queue)

      :ok = BridgeHolder.unregister_worker(queue)
      refute BridgeHolder.registered?(queue)
    end

    test "register twice on the same task queue returns :already_registered" do
      queue = "bh-double-#{System.unique_integer([:positive])}"
      on_exit(fn -> _result = BridgeHolder.unregister_worker(queue) end)

      :ok =
        Patience.retry(fn ->
          BridgeHolder.register_worker(queue, target_url: Client.default_target_url())
        end)

      assert {:error, :already_registered} = BridgeHolder.register_worker(queue, [])

      :ok = BridgeHolder.unregister_worker(queue)
    end

    test "unregister of an unknown task queue is idempotent (returns :ok)" do
      :ok =
        BridgeHolder.unregister_worker("never-registered-#{System.unique_integer([:positive])}")
    end

    # Core gives up on Temporal's GetSystemInfo after 30 s. A Temporal that
    # slow must reach the caller as the error worker_new returns, not as the
    # caller's call timing out first.
    @tag timeout: 120_000
    test "a Temporal that never answers is an error from register, not an exit" do
      queue = "bh-silent-#{System.unique_integer([:positive])}"
      on_exit(fn -> _result = BridgeHolder.unregister_worker(queue) end)
      proxy = start_supervised!(FreezableProxy)
      :ok = FreezableProxy.freeze(proxy)

      assert {:error, %Bridge.Error{kind: :tonic_error, detail: detail}} =
               BridgeHolder.register_worker(queue, target_url: FreezableProxy.url(proxy))

      assert detail =~ "Timeout expired"
      refute BridgeHolder.registered?(queue)
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

    test "record_heartbeat on an unregistered task_queue returns :worker_not_registered" do
      assert {:error, :worker_not_registered} =
               BridgeHolder.record_heartbeat(
                 "no-such-queue-#{System.unique_integer([:positive])}",
                 <<>>
               )
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

      parked = WorkerProbes.parked_polls()

      spawn_link(fn ->
        result = BridgeHolder.poll_workflow_activation(queue)
        send(test_pid, {:poll_returned, result})
      end)

      # Once the poll is in the NIF, drop the bridge.
      await_parked(parked)
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
      before = handle(queue)

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
      await_new_handle(queue, before)

      # Subsequent polls should succeed against the new handle.
      # Validate by spawning a fresh poller and unregistering — the
      # poll must return {:error, :shutdown}, proving it actually went
      # to the NIF (not :worker_not_registered).
      test_pid = self()

      parked = WorkerProbes.parked_polls()

      spawn_link(fn ->
        result = BridgeHolder.poll_workflow_activation(queue)
        send(test_pid, {:fresh_poll, result})
      end)

      await_parked(parked)
      :ok = BridgeHolder.unregister_worker(queue)
      assert_receive {:fresh_poll, {:error, %Bridge.Error{kind: :shutdown}}}, 5_000
    end

    test "healthy parallel poller sees :shutdown during recycle and can retry",
         %{queue: queue} do
      test_pid = self()
      parked = WorkerProbes.parked_polls()

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

      # Both polls in the NIF before one of their callers dies.
      WorkerProbes.poll_until("both polls to park", fn ->
        length(WorkerProbes.parked_polls() -- parked) >= 2
      end)

      Process.exit(doomed, :kill)

      # Healthy poller sees :shutdown from the recycle's worker_shutdown.
      assert_receive {:healthy_first, {:error, %Bridge.Error{kind: :shutdown}}}, Patience.ms()

      # Wait for recycle to finish + healthy to re-poll, then drop the
      # new handle to unblock its second poll.
      await_recycled(queue)

      WorkerProbes.poll_until("the healthy poller to poll again", fn ->
        Process.info(healthy, :current_function) in [
          nil,
          {:current_function, {:gen, :do_call, 4}}
        ]
      end)

      :ok = BridgeHolder.unregister_worker(queue)

      assert_receive {:healthy_second, {:error, %Bridge.Error{kind: :shutdown}}}, 5_000

      # Cleanup the healthy spawn (it has already exited; we just don't
      # want a dangling reference).
      _alive = Process.alive?(healthy)
    end

    test "recycle works for activity poll callers as well", %{queue: queue} do
      assert BridgeHolder.registered?(queue)
      before = handle(queue)

      poll_pid =
        spawn(fn ->
          _result = BridgeHolder.poll_activity_task(queue)
        end)

      Process.sleep(50)
      ref = Process.monitor(poll_pid)
      Process.exit(poll_pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^poll_pid, :killed}, 1_000

      await_new_handle(queue, before)

      test_pid = self()

      parked = WorkerProbes.parked_polls()

      spawn_link(fn ->
        result = BridgeHolder.poll_activity_task(queue)
        send(test_pid, {:fresh_poll, result})
      end)

      await_parked(parked)
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
      await_recycled(queue)
      refute BridgeHolder.registered?(queue)
    end
  end

  # Shared setup for the describes whose tests operate on an already-registered
  # worker handle: register a fresh queue and guarantee its teardown.
  defp registered_queue(_context) do
    queue = "bh-#{System.unique_integer([:positive])}"

    :ok =
      Patience.retry(fn ->
        BridgeHolder.register_worker(queue, target_url: Client.default_target_url())
      end)

    on_exit(fn -> _result = BridgeHolder.unregister_worker(queue) end)
    %{queue: queue}
  end

  # Wait for the holder to finish recycling `queue`'s handle: worker_new
  # asks Temporal, so this waits as long as the suite waits on Temporal.
  defp await_recycled(queue) do
    WorkerProbes.poll_until("the holder to finish recycling #{queue}", fn ->
      state = :sys.get_state(BridgeHolder)
      not MapSet.member?(state.recycling, queue)
    end)
  end

  # Wait for the holder to install a handle for `queue` other than `before`.
  defp await_new_handle(queue, before) do
    WorkerProbes.poll_until("the holder to install a new handle for #{queue}", fn ->
      handle(queue) not in [nil, before]
    end)
  end

  defp handle(queue), do: :sys.get_state(BridgeHolder).handles[queue]

  # Wait for a poll to park in the NIF beyond those `parked` before.
  defp await_parked(parked) do
    WorkerProbes.poll_until("a poll to park in the NIF", fn ->
      WorkerProbes.parked_polls() -- parked != []
    end)
  end
end
