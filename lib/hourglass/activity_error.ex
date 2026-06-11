defmodule Hourglass.ActivityError do
  @moduledoc "Raised by `execute_activity!/2,3` when an activity fails (after retries)."
  defexception [:activity, :reason, :detail]

  @impl Exception
  def message(%__MODULE__{activity: activity, reason: reason}) do
    "activity #{inspect(activity)} failed: #{inspect(reason)}"
  end
end
