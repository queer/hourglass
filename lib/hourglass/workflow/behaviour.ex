defmodule Hourglass.Workflow.Behaviour do
  @moduledoc """
  Callback required by all workflow modules.

  The return value is whatever the workflow chooses to expose as its
  result — `Hourglass.Workflow.Evaluator` JSON-encodes it into
  the `WorkflowExecutionCompleted` event without inspecting the shape.
  Workflows in this codebase typically return a result map; the
  callback is intentionally `term()` so dialyzer doesn't reject those.
  """

  @callback run(input :: term()) :: term()
end
