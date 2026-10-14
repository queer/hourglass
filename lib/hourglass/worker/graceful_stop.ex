defmodule Hourglass.Worker.GracefulStop do
  @moduledoc false
  # A Worker's graceful stop, run from `Hourglass.Worker.terminate/2`: stop
  # taking new tasks, let the work the worker holds finish and report through
  # its still-held handle, bounded by the grace, stop what is left, and only
  # then let go of Temporal. `Hourglass.Worker`'s moduledoc ("Graceful stop")
  # is the contract; the numbered steps below are its steps.

  # credo:disable-for-next-line Credo.Check.Readability.AliasAs
  alias Hourglass.ActivityExecutor.DynamicSupervisor, as: Executors
  alias Hourglass.BridgeHolder

  require Logger

  # After the grace, the time Core is given to deliver its WORKER_SHUTDOWN
  # cancels, the runner to acknowledge them, and Core to report them, before
  # the stop stops waiting for the poll loops.
  @drain_margin 5_000

  # On top of the grace and the drain margin: stopping the inner Supervisor and
  # the executors left running, and dropping the handle.
  @release_margin 10_000

  # How long an executor that traps exits is given to wind down once it has
  # been sent a `:shutdown` exit, before it is killed.
  @executor_stop_timeout 2_000

  @spec margins() :: non_neg_integer()
  def margins, do: @drain_margin + @release_margin

  @spec run(String.t(), pid(), non_neg_integer()) :: :ok
  def run(task_queue, sup_pid, grace) do
    started = now()

    # 1. Stop taking new tasks; keep the handle. 2 and 3: Core hands out what it
    # holds until both sides have drained; the grace is Core's.
    drained? =
      case holder_call(fn -> BridgeHolder.initiate_shutdown(task_queue) end) do
        :ok ->
          :telemetry.execute(
            [:hourglass, :worker, :stopping],
            %{system_time: System.system_time()},
            %{task_queue: task_queue, graceful_shutdown_period: grace}
          )

          await_poll_loops(sup_pid, started + grace + @drain_margin)

        _gone ->
          # No handle to drain: a BridgeHolder cascade killed the holder (its
          # own terminate shut every handle down), or the handle was already
          # dropped. Nothing is left to report through, and the poll loops
          # would only wait for a registration that never comes.
          false
      end

    # 4. Stop what is left.
    stop_inner_supervisor(sup_pid)
    abandoned = stop_executors(task_queue)
    log_abandoned(task_queue, abandoned, drained?)

    # 5. Let go of Temporal.
    _result = holder_call(fn -> BridgeHolder.unregister_worker(task_queue) end)

    :telemetry.execute(
      [:hourglass, :worker, :stopped],
      %{duration: now() - started, abandoned_activities: abandoned},
      %{task_queue: task_queue, drained: drained?}
    )
  end

  # A call into BridgeHolder from a stopping Worker. The holder is gone when a
  # `:rest_for_one` cascade in `Hourglass.Subsystem` killed it first; its own
  # terminate already shut every handle down, so there is nothing left to do,
  # and the exit is not worth a crash report on every Worker of the cascade.
  defp holder_call(fun) do
    fun.()
  catch
    :exit, _reason -> :holder_gone
  end

  # Wait until the inner Supervisor has no poll loop left alive, or the
  # deadline passes. True if the loops all exited — each exits once Core has
  # drained its side. The children are looked up again after every exit, so a
  # loop the Supervisor restarted is waited for too.
  defp await_poll_loops(sup_pid, deadline) do
    case live_children(sup_pid) do
      [] ->
        true

      [pid | _others] ->
        if await_down(pid, Process.monitor(pid), deadline) == :down do
          await_poll_loops(sup_pid, deadline)
        else
          false
        end
    end
  end

  defp live_children(sup_pid) do
    for {_id, pid, _type, _modules} <- Supervisor.which_children(sup_pid), is_pid(pid), do: pid
  catch
    :exit, _reason -> []
  end

  # Kills any poll loop still parked on a poll (the margin ran out). A
  # Supervisor already gone exits `:noproc`, which is fine.
  defp stop_inner_supervisor(sup_pid) do
    Supervisor.stop(sup_pid, :shutdown, 5_000)
  catch
    :exit, _reason -> :ok
  end

  # Stop every activity executor still running for the queue, and return how
  # many there were. Each is sent a `:shutdown` exit, which ends one that does
  # not trap exits at once; one still running `@executor_stop_timeout` later is
  # killed. All of them are gone when this returns.
  defp stop_executors(task_queue) do
    monitored = for pid <- Executors.running(task_queue), do: {pid, Process.monitor(pid)}
    Enum.each(monitored, fn {pid, _ref} -> Process.exit(pid, :shutdown) end)

    deadline = now() + @executor_stop_timeout

    Enum.each(monitored, fn {pid, ref} ->
      if await_down(pid, ref, deadline) == :timeout do
        Process.exit(pid, :kill)
        :down = await_down(pid, Process.monitor(pid), :infinity)
      end
    end)

    length(monitored)
  end

  # Wait for the monitored `pid` to go down, until `deadline`.
  defp await_down(pid, ref, deadline) do
    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :down
    after
      remaining(deadline) ->
        Process.demonitor(ref, [:flush])
        :timeout
    end
  end

  defp remaining(:infinity), do: :infinity
  defp remaining(deadline), do: max(deadline - now(), 0)

  defp now, do: System.monotonic_time(:millisecond)

  defp log_abandoned(_task_queue, 0, _drained?), do: :ok

  defp log_abandoned(task_queue, count, true) do
    Logger.warning(
      "worker for task_queue=#{task_queue} stopped #{count} activity executor(s) still " <>
        "running after the graceful shutdown period; each was reported to Temporal as a " <>
        "retryable WorkerShutdown failure"
    )
  end

  defp log_abandoned(task_queue, count, false) do
    Logger.warning(
      "worker for task_queue=#{task_queue} stopped #{count} activity executor(s) before " <>
        "Core had drained; any whose outcome had not reached Temporal is left to its timeout"
    )
  end
end
