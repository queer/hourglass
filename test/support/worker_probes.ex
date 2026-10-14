defmodule Hourglass.TestSupport.WorkerProbes do
  @moduledoc false
  # Test-side probes of a running worker's internals, shared by the
  # integration tests that force a handle recycle or watch dirty-IO threads.

  import ExUnit.Assertions

  alias Hourglass.TestSupport.Patience
  alias Hourglass.Worker
  alias Hourglass.Worker.ActivityPollLoop

  # Kill the queue's activity poll loop while it is parked on a poll, and wait
  # for the holder to install the recycled handle.
  @spec recycle!(String.t()) :: :ok
  def recycle!(queue) do
    before = current_handle(queue)

    loop =
      queue
      |> Worker.inner_supervisor()
      |> Supervisor.which_children()
      |> Enum.find_value(fn {id, pid, _type, _modules} -> id == ActivityPollLoop && pid end)

    poll_until("the activity poll loop to park on a poll", fn ->
      Process.info(loop, :current_function) == {:current_function, {:gen, :do_call, 4}}
    end)

    ref = Process.monitor(loop)
    Process.exit(loop, :kill)
    assert_receive {:DOWN, ^ref, :process, ^loop, :killed}

    poll_until("the holder to recycle the handle", fn ->
      current = current_handle(queue)
      current != nil and current != before
    end)
  end

  # Processes in the VM parked in a poll NIF, each holding a dirty-IO
  # scheduler thread while it waits on Core.
  @spec parked_polls() :: [pid()]
  def parked_polls do
    for pid <- Process.list(),
        {:current_function, {Hourglass.Bridge, function, 1}} <-
          [Process.info(pid, :current_function)],
        function in [:worker_poll_workflow_activation, :worker_poll_activity_task],
        do: pid
  end

  # Wait until `fun` holds, for up to `timeout` ms: by default as long as the
  # suite waits on Temporal (`Hourglass.TestSupport.Patience`).
  @spec poll_until(String.t(), (-> boolean()), pos_integer()) :: :ok
  def poll_until(what, fun, timeout \\ Patience.ms()) do
    await(what, fun, System.monotonic_time(:millisecond) + timeout, timeout)
  end

  defp await(what, fun, deadline, timeout) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("timed out waiting #{timeout} ms for #{what}")

      true ->
        Process.sleep(100)
        await(what, fun, deadline, timeout)
    end
  end

  defp current_handle(queue), do: :sys.get_state(Hourglass.BridgeHolder).handles[queue]
end
