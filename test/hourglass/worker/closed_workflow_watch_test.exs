defmodule Hourglass.Worker.ClosedWorkflowWatchTest do
  @moduledoc """
  The closed-workflow watch against what Temporal does, for runs no real
  activity backs: a run Temporal does not know is ended, and many at once
  within the bound; a namespace it does not know, a Temporal it cannot
  reach, or one that has gone quiet ends nothing and holds nothing; and the
  watch catches up once Temporal answers again.

  Each "activity" is a plain process the test registers the way an executor
  registers the activity it runs (`Hourglass.ActivityExecutor.DynamicSupervisor`),
  so the watch takes it for one.

  The watch owes its bound only when Temporal answers it in time, so how soon
  an activity is ended is asserted only then (`Hourglass.TestSupport.WatchTrace`);
  that it is ended at all is waited for as long as the suite waits on
  Temporal (`Hourglass.TestSupport.Patience`).
  """

  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.Activity.CancelRegistry
  alias Hourglass.Client
  alias Hourglass.TestSupport.FreezableProxy
  alias Hourglass.TestSupport.Patience
  alias Hourglass.TestSupport.SchedulerLag
  alias Hourglass.TestSupport.WatchTrace
  alias Hourglass.TestSupport.WorkerProbes
  alias Hourglass.Worker.ClosedWorkflowWatch

  @moduletag capture_log: true
  @moduletag timeout: Patience.test_timeout(4)

  @bound 400

  # How late the BEAM runs, measured from the start of each test.
  setup do
    Process.put(:scheduler_lag, SchedulerLag.start())
    :ok
  end

  @tag :temporal
  test "an activity of a run Temporal does not know is ended" do
    queue = queue()
    {activity, _token} = fake_activity!(queue, unknown_run())
    monitor = Process.monitor(activity)
    started = System.monotonic_time(:millisecond)
    watch = start_watch!(queue, Client.default_namespace(), Client.default_target_url())

    assert_receive {:DOWN, ^monitor, :process, ^activity, :killed}, Patience.ms()

    if owed?(watch, @bound) do
      assert System.monotonic_time(:millisecond) - started < 2 * @bound
    end
  end

  @tag :temporal
  test "the activities of many closed runs are all ended within the bound" do
    queue = queue()
    bound = 2_000
    activities = for _n <- 1..300, do: elem(fake_activity!(queue, unknown_run()), 0)
    monitors = MapSet.new(activities, &Process.monitor/1)

    started = System.monotonic_time(:millisecond)
    watch = start_watch!(queue, Client.default_namespace(), Client.default_target_url(), bound)

    for _activity <- activities do
      assert_receive {:DOWN, monitor, :process, _pid, :killed}, Patience.ms()
      assert MapSet.member?(monitors, monitor)
    end

    if owed?(watch, bound) do
      assert System.monotonic_time(:millisecond) - started < bound
    end
  end

  @tag :temporal
  test "a namespace Temporal does not know ends nothing" do
    queue = queue()
    {activity, token} = fake_activity!(queue, unknown_run())
    namespace = "no-such-namespace-#{System.unique_integer([:positive])}"
    start_watch!(queue, namespace, Client.default_target_url())

    assert_still_running(activity, token, 3 * @bound)
  end

  test "a Temporal that cannot be reached ends nothing" do
    queue = queue()
    {activity, token} = fake_activity!(queue, unknown_run())
    start_watch!(queue, Client.default_namespace(), "http://127.0.0.1:1")

    assert_still_running(activity, token, 3 * @bound)
  end

  @tag :temporal
  test "a Temporal gone quiet ends nothing and holds nothing, and once it answers again the watch catches up" do
    queue = queue()
    {watch, proxy} = watch_through_proxy!(queue)

    :ok = FreezableProxy.freeze(proxy)
    {activity, token} = fake_activity!(queue, unknown_run())

    # Asked, and no answer: the activity goes on, and the watch waits on
    # nothing — no process of its own, and it answers at once.
    assert_still_running(activity, token, 3_000)
    assert helpers(watch) == []
    {elapsed, _state} = :timer.tc(fn -> :sys.get_state(watch) end)
    assert elapsed < 50_000

    # Temporal answers again: the run it never answered for is found closed.
    monitor = Process.monitor(activity)
    :ok = WatchTrace.reset(watch)
    thawed = System.monotonic_time(:millisecond)
    :ok = FreezableProxy.thaw(proxy)
    assert_receive {:DOWN, ^monitor, :process, ^activity, :killed}, Patience.ms()

    if owed?(watch, @bound) do
      assert System.monotonic_time(:millisecond) - thawed < 3_000
    end
  end

  @tag :temporal
  test "a watch stopped while its check waits on a quiet Temporal stops at once, leaving nothing behind" do
    queue = queue()
    {watch, proxy} = watch_through_proxy!(queue)
    :ok = FreezableProxy.freeze(proxy)

    WorkerProbes.poll_until("a check to wait on the quiet Temporal", fn ->
      :sys.get_state(watch).check != nil
    end)

    assert helpers(watch) == []
    {elapsed, :ok} = :timer.tc(fn -> stop_supervised(queue) end)
    assert elapsed < 100_000
  end

  # A watch whose Temporal is reached through a proxy the test can freeze,
  # once it has opened its client, with a running activity of a running run
  # (so it has a check to make, and ends nothing by it).
  defp watch_through_proxy!(queue) do
    proxy = start_supervised!(FreezableProxy)
    {_running, _token} = fake_activity!(queue, running_run!())
    watch = start_watch!(queue, Client.default_namespace(), FreezableProxy.url(proxy))

    WorkerProbes.poll_until("the watch to open its client", fn ->
      :sys.get_state(watch).client != nil
    end)

    {watch, proxy}
  end

  defp assert_still_running(activity, token, for_ms) do
    monitor = Process.monitor(activity)
    refute_receive {:DOWN, ^monitor, :process, ^activity, _reason}, for_ms
    assert CancelRegistry.cancelled?(token) == nil
    Process.demonitor(monitor, [:flush])
  end

  # The processes the watch has linked to it, other than its supervisor: any
  # it would have started to wait on Temporal for it.
  defp helpers(watch) do
    {:links, links} = Process.info(watch, :links)
    {:dictionary, dictionary} = Process.info(watch, :dictionary)
    links -- Keyword.fetch!(dictionary, :"$ancestors")
  end

  defp queue, do: "watch-#{System.unique_integer([:positive])}"

  defp unknown_run, do: {"watch-#{UUIDv7.generate()}", UUIDv7.generate()}

  # A run Temporal knows and that goes on running: a workflow started on a
  # queue nobody serves.
  defp running_run! do
    {:ok, handle} =
      Hourglass.start(Hourglass.TestSupport.SignalledChild, %{},
        id: "watch-running-#{UUIDv7.generate()}",
        task_queue: "watch-nobody-#{System.unique_integer([:positive])}"
      )

    {handle.id, handle.run_id}
  end

  # A process registered as the executor of an activity of `run`, on `queue`.
  defp fake_activity!(queue, run) do
    token = "token-#{UUIDv7.generate()}"
    test = self()

    activity =
      spawn(fn ->
        {:ok, _owner} = Registry.register(Hourglass.ActivityExecutor.Registry, queue, token)

        {:ok, _owner} =
          Registry.register(Hourglass.ActivityExecutor.Registry, {:activity, token}, run)

        send(test, {:registered, token})
        Process.sleep(:infinity)
      end)

    assert_receive {:registered, ^token}
    on_exit(fn -> Process.exit(activity, :kill) end)
    {activity, token}
  end

  # Whether a watch keeping `bound` owed its bound: Temporal answered it in
  # time (`WatchTrace`), and the BEAM ran its timers on time (`SchedulerLag`).
  defp owed?(watch, bound) do
    lag = Process.get(:scheduler_lag)

    not SchedulerLag.late?(lag, answer_time(bound)) and
      WatchTrace.in_time?(watch, answer_time(bound))
  end

  # A watch, traced (`WatchTrace`) to the test.
  defp start_watch!(queue, namespace, target_url, bound \\ @bound) do
    opts = [namespace: namespace, target_url: target_url]

    watch =
      start_supervised!(%{
        id: queue,
        start: {ClosedWorkflowWatch, :start_link, [queue, bound, opts]}
      })

    :ok = WatchTrace.start(watch)
    watch
  end

  # How soon Temporal must answer a watch keeping `bound` for the watch to owe
  # it: a quarter of the bound, as the watch asks every quarter and stops an
  # activity half the bound after learning its run closed.
  defp answer_time(bound), do: div(bound, 4)
end
