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
end
