defmodule Hourglass.Worker.WorkflowPollLoopTest do
  # async: false — parks the dirty-IO long-poll NIF; run serially with the other
  # cluster poll/crash tests so concurrent parked polls can't exhaust the
  # (default 10) dirty-IO scheduler pool and deadlock a worker_shutdown drain.
  use ExUnit.Case, async: false

  alias Hourglass.BridgeHolder
  alias Hourglass.Worker.WorkflowPollLoop

  @moduletag :temporal
  # Server-semantic test — drives a real workflow poll loop against the
  # Bridge. Cannot be mocked. Default `mix test` excludes :integration.
  @moduletag :integration

  defmodule TestWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input), do: {:ok, :done}
  end

  describe "run/1 transient retry on missing registration" do
    test "loops with brief sleep when no holder is registered (does not exit)" do
      queue = "wfp-no-bridge-#{System.unique_integer([:positive])}"

      Process.flag(:trap_exit, true)

      {:ok, pid} =
        WorkflowPollLoop.start_link(task_queue: queue)

      # The loop should be alive briefly; :worker_not_registered → sleep+retry.
      Process.sleep(100)
      assert Process.alive?(pid)

      Process.exit(pid, :shutdown)

      assert_receive {:EXIT, ^pid, :shutdown}, 1_000
    end
  end

  describe "lifecycle (real bridge)" do
    setup :registered_queue

    test "starts polling once a holder is registered, exits :ok on bridge shutdown",
         %{queue: queue} do
      Process.flag(:trap_exit, true)

      {:ok, loop_pid} =
        WorkflowPollLoop.start_link(task_queue: queue)

      assert Process.alive?(loop_pid)

      ref = Process.monitor(loop_pid)

      # Give the loop a moment to enter the long-poll inside BridgeHolder.
      # Without this sleep there's a race: unregister might land in BH's
      # mailbox BEFORE the loop's first poll call, in which case the
      # loop sees `:worker_not_registered` and retries forever instead of
      # exiting on `:shutdown`.
      Process.sleep(50)

      # Unregister the bridge — its in-flight long-poll returns :shutdown,
      # so the loop exits :normal.
      :ok = BridgeHolder.unregister_worker(queue)

      assert_receive {:DOWN, ^ref, :process, ^loop_pid, :normal}, 5_000
    end
  end

  # Register a fresh worker handle and tear it down via on_exit, so the test
  # body never repeats the register/unregister dance.
  defp registered_queue(_context) do
    queue = "wfp-real-#{System.unique_integer([:positive])}"
    :ok = BridgeHolder.register_worker(queue, target_url: Hourglass.Client.default_target_url())
    on_exit(fn -> _result = BridgeHolder.unregister_worker(queue) end)
    %{queue: queue}
  end
end
