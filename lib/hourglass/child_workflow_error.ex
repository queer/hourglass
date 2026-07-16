defmodule Hourglass.ChildWorkflowError do
  @moduledoc "Raised by `execute_child!/2,3` when a child workflow fails, is cancelled, or cannot be started."
  defexception [:workflow, :reason, :detail]

  @impl Exception
  def message(%__MODULE__{workflow: workflow, reason: reason}) do
    "child workflow #{inspect(workflow)} failed: #{inspect(reason)}"
  end
end
