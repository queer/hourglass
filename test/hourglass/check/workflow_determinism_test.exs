defmodule Hourglass.Check.WorkflowDeterminismTest do
  use Credo.Test.Case, async: true
  alias Hourglass.Check.WorkflowDeterminism

  test "flags a banned call in a workflow module" do
    """
    defmodule W do
      use Hourglass.Workflow
      def run(_), do: DateTime.utc_now()
    end
    """
    |> to_source_file()
    |> run_check(WorkflowDeterminism)
    |> assert_issue()
  end

  test "ignores a clean workflow module" do
    """
    defmodule W do
      use Hourglass.Workflow
      def run(a), do: Map.put(a, :ok, true)
    end
    """
    |> to_source_file()
    |> run_check(WorkflowDeterminism)
    |> refute_issues()
  end

  test "ignores non-workflow modules even if they call banned functions" do
    """
    defmodule NotAWorkflow do
      def go, do: DateTime.utc_now()
    end
    """
    |> to_source_file()
    |> run_check(WorkflowDeterminism)
    |> refute_issues()
  end
end
