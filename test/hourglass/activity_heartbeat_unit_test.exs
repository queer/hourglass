defmodule Hourglass.ActivityHeartbeatUnitTest do
  use ExUnit.Case, async: false
  alias Hourglass.Activity
  alias Hourglass.Activity.CancelRegistry

  defp put_info(token) do
    Process.put({Hourglass.Activity, :info}, %Hourglass.Activity.Info{
      workflow_id: "w",
      run_id: "r",
      activity_id: "a",
      attempt: 1,
      task_token: token,
      task_queue: "q"
    })
  end

  setup do
    on_exit(fn -> Process.delete({Hourglass.Activity, :info}) end)
    :ok
  end

  test "heartbeat/0 returns :ok outside a dispatch" do
    assert Activity.heartbeat() == :ok
  end

  test "heartbeat/0 returns :ok when the token is not cancelled" do
    t = "hb-#{System.unique_integer([:positive])}"
    put_info(t)
    assert Activity.heartbeat() == :ok
  end

  test "heartbeat/0 returns :cancel when the token is marked" do
    t = "hb-#{System.unique_integer([:positive])}"
    put_info(t)
    CancelRegistry.mark(t, :timed_out)
    assert Activity.heartbeat() == :cancel
  end

  test "heartbeat!/0 is a no-op when not cancelled, raises Cancelled when marked" do
    t = "hb-#{System.unique_integer([:positive])}"
    put_info(t)
    assert Activity.heartbeat!() == :ok
    CancelRegistry.mark(t, :timed_out)
    assert_raise Hourglass.Activity.Cancelled, fn -> Activity.heartbeat!() end
  end

  test "heartbeat_ref/0 is nil outside a dispatch" do
    assert Activity.heartbeat_ref() == nil
  end

  test "heartbeat/1 sends nothing and answers :ended for a ref whose dispatch is not running" do
    # The context is set by hand, as above, but no executor runs the activity:
    # to `heartbeat/1` the dispatch is over.
    t = "hb-#{System.unique_integer([:positive])}"
    put_info(t)
    ref = Activity.heartbeat_ref()
    assert %Hourglass.Activity.HeartbeatRef{} = ref

    handler = {__MODULE__, t}
    test = self()

    :telemetry.attach(
      handler,
      [:hourglass, :activity, :heartbeat],
      fn _event, _measurements, _metadata, _config -> send(test, :heartbeat_sent) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    heartbeat = Task.async(fn -> Activity.heartbeat(ref) end)
    assert Task.await(heartbeat) == :ended
    refute_received :heartbeat_sent
  end
end
