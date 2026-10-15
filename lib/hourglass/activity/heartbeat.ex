defmodule Hourglass.Activity.Heartbeat do
  # Implementation behind `Hourglass.Activity.heartbeat/0` and `heartbeat!/0`,
  # which delegate here and remain the public surface — call those, not these.
  #
  # It lives in its own module because heartbeating is the one part of the
  # activity-side API that talks to anything: the bridge holder, the proto
  # encoder, telemetry, the cancel registry and the cancellation exception. Left
  # inline, those five made `Hourglass.Activity` — which is otherwise a `use`
  # macro plus three process-dictionary reads — the most coupled module in the
  # library, at 13 dependencies against a limit of 10. Heartbeating on an
  # activity's behalf from another process (`heartbeat/1`, with a
  # `HeartbeatRef`) lives here for the same reason.
  @moduledoc false

  alias Hourglass.Activity
  alias Hourglass.Activity.HeartbeatRef
  # credo:disable-for-next-line Credo.Check.Readability.AliasAs
  alias Hourglass.ActivityExecutor.DynamicSupervisor, as: Executors

  @spec heartbeat() :: :ok | :cancel
  def heartbeat do
    case Activity.try_info() do
      %Activity.Info{task_token: token} = info when is_binary(token) and token != "" ->
        record(info)

      _outside_dispatch ->
        :ok
    end
  end

  # A ref for the activity this process is running, nil outside a dispatch.
  @spec heartbeat_ref() :: HeartbeatRef.t() | nil
  def heartbeat_ref do
    case Activity.try_info() do
      %Activity.Info{task_token: token} = info when is_binary(token) and token != "" ->
        %HeartbeatRef{info: info, owner: self()}

      _outside_dispatch ->
        nil
    end
  end

  # Heartbeat for the activity `ref` names, from any process — unless that
  # activity's dispatch is over, which `ref` cannot outlive: its attempt is
  # finished, and a heartbeat for it would at best be dropped.
  @spec heartbeat(HeartbeatRef.t()) :: :ok | :cancel | :ended
  def heartbeat(%HeartbeatRef{info: info, owner: owner}) do
    if Executors.running_activity?(info.task_token, owner), do: record(info), else: :ended
  end

  defp record(%Activity.Info{task_token: token, task_queue: q} = info) do
    :telemetry.execute([:hourglass, :activity, :heartbeat], %{count: 1}, %{
      task_queue: q,
      workflow_id: info.workflow_id,
      run_id: info.run_id,
      activity_id: info.activity_id
    })

    hb = %Coresdk.ActivityHeartbeat{task_token: token, details: []}
    _recorded = safe_record(q, Protobuf.encode(hb))
    if Activity.CancelRegistry.cancelled?(token), do: :cancel, else: :ok
  end

  @spec heartbeat!() :: :ok
  def heartbeat! do
    case heartbeat() do
      :cancel -> raise(Activity.Cancelled, reason: cancel_reason())
      :ok -> :ok
    end
  end

  # Why the activity was cancelled, as far as its own process can tell:
  # `:workflow_closed` when its worker found its workflow run closed
  # (`Hourglass.Worker`, "Activities of a closed workflow"),
  # `:workflow_cancel_requested` when it opted in to ending on a cancel
  # request and its worker found one requested, else `:cancel_requested`.
  defp cancel_reason do
    with %Activity.Info{task_token: token} when is_binary(token) <- Activity.try_info(),
         reason when reason in [:workflow_closed, :workflow_cancel_requested] <-
           Activity.CancelRegistry.cancelled?(token) do
      reason
    else
      _other -> :cancel_requested
    end
  end

  defp safe_record(task_queue, bin) do
    Hourglass.BridgeHolder.record_heartbeat(task_queue, bin)
  catch
    :exit, _reason -> {:error, :holder_unavailable}
  end
end
