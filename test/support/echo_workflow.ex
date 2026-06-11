defmodule Hourglass.TestSupport.EchoWorkflow do
  @moduledoc false
  use Hourglass.Workflow, input: :map, output: :map

  @impl Hourglass.Workflow.Behaviour
  def run(args) do
    %{
      "echoed" =>
        execute_activity!(Hourglass.TestSupport.EchoActivity, args,
          start_to_close_timeout: 30_000
        )
    }
  end
end
