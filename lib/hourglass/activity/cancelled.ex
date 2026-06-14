defmodule Hourglass.Activity.Cancelled do
  @moduledoc """
  Raised by `Hourglass.Activity.heartbeat!/0` when the running activity has been
  cancelled (heartbeat timeout, workflow cancel, or terminate). `Hourglass.ActivityRunner`
  recognises it and reports a Temporal Cancellation completion rather than a failure.
  """
  defexception [:reason]

  @impl true
  def message(%__MODULE__{reason: reason}),
    do: "activity cancelled (reason=#{inspect(reason)})"
end
