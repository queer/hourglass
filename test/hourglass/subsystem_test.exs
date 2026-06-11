defmodule Hourglass.SubsystemTest do
  # async: false — this test deliberately crashes the globally-shared
  # Hourglass.BridgeHolder singleton and observes the
  # :rest_for_one cascade through WorkerRegistry + Worker.Supervisor.
  # Concurrent async Temporal tests would see their bridge handles and
  # via-name registrations torn out from under them, so the suite must
  # quiesce around this test.
  use ExUnit.Case, async: false

  alias Hourglass.BridgeHolder
  # credo:disable-for-next-line Credo.Check.Readability.AliasAs
  alias Hourglass.Worker.Supervisor, as: WorkerSupervisor
  alias Hourglass.WorkerRegistry

  @moduletag :temporal
  # Server-semantic test — kills the real BridgeHolder GenServer to
  # verify the cascade restarts WorkerRegistry + Worker.Supervisor with
  # fresh empty state. Cannot be mocked: requires real bridge handles.
  # Default `mix test` excludes :integration.
  @moduletag :integration
  # The cascade test deliberately Process.exit/2 :kill the BridgeHolder;
  # Workers' terminate runs after the holder is dead. Capture to keep
  # runner output clean (still emitted on test failure).
  @moduletag capture_log: true

  defmodule TestWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input), do: {:ok, :done}
  end

  describe "BridgeHolder crash cascade" do
    # Previously, BridgeHolder lived as a direct child of Hourglass.Application's
    # :one_for_one supervisor — a crash restarted only BridgeHolder,
    # leaving every Worker GenServer holding a dangling registration.
    # Their poll loops would loop forever on
    # {:error, :worker_not_registered}.
    #
    # The fix wraps BridgeHolder + WorkerRegistry + Worker.Supervisor in
    # Hourglass.Subsystem (:rest_for_one). A BridgeHolder crash
    # cascades through both, dropping every Worker; a fresh BridgeHolder
    # boots with empty state. The application is responsible for
    # re-invoking start_worker/1 for any task queue it wants to keep
    # serving.

    test "killing BridgeHolder cascade-drops Workers; Subsystem restarts the holder fresh" do
      queue = "bh-cascade-#{System.unique_integer([:positive])}"

      # Production-shape Worker booted via Worker.Supervisor (the same
      # path the application uses).
      {:ok, worker_pid} =
        WorkerSupervisor.start_worker(task_queue: queue)

      assert BridgeHolder.registered?(queue)

      whereis =
        queue
        |> WorkerRegistry.via()
        |> GenServer.whereis()

      assert worker_pid == whereis

      # Spawn a sibling process to stand in for an "in-flight
      # evaluator/executor Task" that must survive the cascade. In
      # production those Tasks live under the app-level evaluator +
      # executor DynSups (Hourglass.WorkflowEvaluator.DynamicSupervisor
      # + Hourglass.ActivityExecutor.DynamicSupervisor) — siblings
      # of Subsystem under Application's :one_for_one parent
      # (test_helper mirrors this shape). The shape this test asserts
      # is "the cascade does not reach processes outside the Subsystem
      # subtree" — the bare process here is the minimal stand-in.
      test_pid = self()

      survivor_pid =
        spawn(fn ->
          send(test_pid, {:survivor_started, self()})

          receive do
            :stop -> :ok
          after
            30_000 -> :timed_out
          end
        end)

      assert_receive {:survivor_started, ^survivor_pid}, 1_000
      assert Process.alive?(survivor_pid)

      # Capture the current BridgeHolder pid so we can wait for the
      # Subsystem to bring up a different one.
      old_holder = Process.whereis(Hourglass.BridgeHolder)
      assert is_pid(old_holder)

      worker_ref = Process.monitor(worker_pid)
      holder_ref = Process.monitor(old_holder)

      # The crash. Subsystem (:rest_for_one) cascades:
      # BridgeHolder -> WorkerRegistry -> Worker.Supervisor.
      Process.exit(old_holder, :kill)

      assert_receive {:DOWN, ^holder_ref, :process, ^old_holder, :killed}, 5_000
      assert_receive {:DOWN, ^worker_ref, :process, ^worker_pid, _reason}, 5_000

      # Sibling Task is unaffected by the cascade.
      assert Process.alive?(survivor_pid)

      # Wait for the Subsystem to bring up fresh BridgeHolder +
      # WorkerRegistry + Worker.Supervisor under the same registered
      # names. :rest_for_one starts these in declared order, so once
      # Worker.Supervisor is up the whole subtree is up.
      assert_eventually(fn ->
        with new_holder when is_pid(new_holder) and new_holder != old_holder <-
               Process.whereis(Hourglass.BridgeHolder),
             reg when is_pid(reg) <- Process.whereis(Hourglass.WorkerRegistry),
             ws when is_pid(ws) <- Process.whereis(Hourglass.Worker.Supervisor) do
          true
        else
          _other -> false
        end
      end)

      # Fresh BridgeHolder has no record of the pre-cascade registration.
      refute BridgeHolder.registered?(queue)

      # Worker.Supervisor restarted with no children — caller must
      # re-invoke start_worker/1 to resume serving the task queue.
      # (manual restart IS recovery.)
      assert {:error, :not_found} = WorkerSupervisor.stop_worker(queue)

      # Clean up the survivor.
      send(survivor_pid, :stop)
    end
  end

  # Local helper — avoids cross-file test_helper churn for one usage.
  defp assert_eventually(fun, timeout \\ 5_000, interval \\ 25) do
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
