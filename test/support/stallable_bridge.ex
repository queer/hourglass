defmodule Hourglass.TestSupport.StallableBridge do
  @moduledoc false
  # Hourglass.Bridge's two activity calls that BridgeHolder makes for an
  # activity in flight — its completion and its heartbeats — standing in for
  # them in the test build (`config :hourglass, :activity_bridge`, read by
  # BridgeHolder at compile time), so a test can make one of them stall, as a
  # call stuck on the network would.
  #
  # A call for a task token blocks while the test holds the RaceGate key
  # `{__MODULE__, :complete | :heartbeat, task_token}` armed, then goes on to
  # the real NIF. With no such key armed — every other test — it goes straight
  # through.

  alias Hourglass.Bridge
  alias Hourglass.TestSupport.RaceGate

  @spec worker_complete_activity_task(reference(), binary()) :: term()
  def worker_complete_activity_task(handle, bytes) do
    :ok = hold(:complete, Coresdk.ActivityTaskCompletion, bytes)
    Bridge.worker_complete_activity_task(handle, bytes)
  end

  @spec worker_record_activity_heartbeat(reference(), binary()) :: term()
  def worker_record_activity_heartbeat(handle, bytes) do
    :ok = hold(:heartbeat, Coresdk.ActivityHeartbeat, bytes)
    Bridge.worker_record_activity_heartbeat(handle, bytes)
  end

  defp hold(kind, message, bytes) do
    RaceGate.hold({__MODULE__, kind, token(message, bytes)})
  end

  defp token(message, bytes) do
    message.decode(bytes).task_token
  rescue
    _malformed -> nil
  end
end
