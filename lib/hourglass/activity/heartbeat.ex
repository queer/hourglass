defmodule Hourglass.Activity.Heartbeat do
  # Implementation behind `Hourglass.Activity.heartbeat/0` and `heartbeat!/0`,
  # which delegate here and remain the public surface — call those, not these.
  #
  # It lives in its own module because heartbeating is the one part of the
  # activity-side API that talks to anything: the bridge holder, the proto
  # encoder, telemetry, the cancel registry and the cancellation exception. Left
  # inline, those five made `Hourglass.Activity` — which is otherwise a `use`
  # macro plus three process-dictionary reads — the most coupled module in the
  # library, at 13 dependencies against a limit of 10.
  @moduledoc false

  alias Hourglass.Activity

  @spec heartbeat() :: :ok | :cancel
  def heartbeat do
    case Activity.try_info() do
      %Activity.Info{
        task_token: token,
        task_queue: q,
        workflow_id: wid,
        run_id: rid,
        activity_id: aid
      }
      when is_binary(token) and token != "" ->
        :telemetry.execute([:hourglass, :activity, :heartbeat], %{count: 1}, %{
          task_queue: q,
          workflow_id: wid,
          run_id: rid,
          activity_id: aid
        })

        hb = %Coresdk.ActivityHeartbeat{task_token: token, details: []}
        _recorded = safe_record(q, Protobuf.encode(hb))
        if Activity.CancelRegistry.cancelled?(token), do: :cancel, else: :ok

      _outside_dispatch ->
        :ok
    end
  end

  @spec heartbeat!() :: :ok
  def heartbeat! do
    case heartbeat() do
      :cancel -> raise(Activity.Cancelled, reason: :cancel_requested)
      :ok -> :ok
    end
  end

  defp safe_record(task_queue, bin) do
    Hourglass.BridgeHolder.record_heartbeat(task_queue, bin)
  catch
    :exit, _reason -> {:error, :holder_unavailable}
  end
end
