defmodule Hourglass.TestSupport.SchedulerLag do
  @moduledoc """
  How late the BEAM has run a timer since `start/0`, so a test can tell
  whether a bound measured in wall time was the code's to keep.

  A bound of a second or two is kept by timers and processes the BEAM
  schedules; on a machine busy with other suites the BEAM itself runs late,
  and no code keeps a wall-clock bound then. A test asserts such a bound only
  when the BEAM ran on time (`late?/2`), and otherwise only that the thing
  happened at all.
  """

  @tick 10

  @doc "Start measuring, in a process linked to the caller."
  @spec start() :: pid()
  def start do
    spawn_link(fn -> loop(0) end)
  end

  @doc "Whether a timer has run more than `ms` late since `probe` started."
  @spec late?(pid(), non_neg_integer()) :: boolean()
  def late?(probe, ms) do
    send(probe, {:max, self()})

    receive do
      {:max_lag, ^probe, max} -> max > ms
    end
  end

  defp loop(max) do
    asked = System.monotonic_time(:millisecond)

    receive do
      {:max, from} ->
        send(from, {:max_lag, self(), max})
        loop(max)
    after
      @tick -> loop(max(max, System.monotonic_time(:millisecond) - asked - @tick))
    end
  end
end
