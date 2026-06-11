defmodule Hourglass.WorkflowHandle do
  @moduledoc "Identifier for an in-flight workflow execution."

  defstruct [:id, :run_id]

  @type t :: %__MODULE__{id: String.t(), run_id: String.t()}
end
