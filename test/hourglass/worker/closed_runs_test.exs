defmodule Hourglass.Worker.ClosedRunsTest do
  @moduledoc """
  Which runs `Hourglass.Worker.ClosedRuns` reads as closed from Temporal's
  answers: every closed status and a run Temporal no longer knows, and
  nothing else — not a running run, not a namespace Temporal does not know,
  not a describe that timed out or failed otherwise. A running run with a
  cancel requested of it reads as such.
  """

  use ExUnit.Case, async: true

  alias Hourglass.Bridge
  alias Hourglass.Worker.ClosedRuns
  alias Temporal.Api.Workflow.V1.WorkflowExecutionExtendedInfo
  alias Temporal.Api.Workflow.V1.WorkflowExecutionInfo
  alias Temporal.Api.Workflowservice.V1.DescribeWorkflowExecutionResponse

  @closed [
    :WORKFLOW_EXECUTION_STATUS_COMPLETED,
    :WORKFLOW_EXECUTION_STATUS_FAILED,
    :WORKFLOW_EXECUTION_STATUS_CANCELED,
    :WORKFLOW_EXECUTION_STATUS_TERMINATED,
    :WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
    :WORKFLOW_EXECUTION_STATUS_TIMED_OUT
  ]

  test "a run in any closed status has closed" do
    for status <- @closed do
      assert closed?(described(status)), "#{status} should read as closed"
    end
  end

  test "a running run, or one whose status is unknown, has not closed" do
    refute closed?(described(:WORKFLOW_EXECUTION_STATUS_RUNNING))
    refute closed?(described(:WORKFLOW_EXECUTION_STATUS_UNSPECIFIED))
    refute closed?({:ok, Protobuf.encode(%DescribeWorkflowExecutionResponse{})})
  end

  test "a running run with a cancel requested has a cancel requested; a closed one has closed" do
    assert state(described(:WORKFLOW_EXECUTION_STATUS_RUNNING, true)) == :cancel_requested
    assert state(described(:WORKFLOW_EXECUTION_STATUS_RUNNING, false)) == :open

    for status <- @closed do
      assert state(described(status, true)) == :closed
    end
  end

  test "a run Temporal no longer knows has closed, whatever its workflow id says" do
    for id <- ["nope", "Namespace x", ~s(Namespace "y" is not found.), "日本語のワークフロー"] do
      detail =
        "code: 'Some requested entity was not found', message: " <>
          inspect("workflow not found for ID: #{id}")

      assert closed?(tonic_error(detail)), "not found for #{inspect(id)} should read as closed"
    end
  end

  test "a namespace Temporal does not know says nothing about the run" do
    detail =
      "code: 'Some requested entity was not found', message: \"Namespace hourglass-test is not found.\""

    refute closed?(tonic_error(detail))
  end

  test "a describe that timed out, could not reach Temporal, or failed otherwise says nothing" do
    refute closed?(
             {:error, %Bridge.Error{kind: :unknown, detail: "describe timed out after 1000 ms"}}
           )

    refute closed?(
             tonic_error(
               "code: 'The operation was cancelled', message: \"Timeout expired\", source: ..."
             )
           )

    refute closed?(tonic_error("code: 'The service is currently unavailable', message: \"x\""))
    refute closed?(tonic_error("code: 'Some invalid argument', message: \"not found\""))
  end

  test "an answer carries its tag; other messages are no answer" do
    assert {:described, 7, []} = ClosedRuns.answer({:hourglass_runs_described, 7, []})
    assert {:opened, 8, :error} = ClosedRuns.answer({:hourglass_client_opened, 8, {:error, :x}})
    assert ClosedRuns.answer({:check}) == :unknown
  end

  defp closed?(answer), do: state(answer) == :closed

  defp state(answer) do
    {:described, 1, [{{"w", "r"}, state}]} =
      ClosedRuns.answer({:hourglass_runs_described, 1, [{"w", "r", answer}]})

    state
  end

  defp described(status, cancel_requested \\ false) do
    response = %DescribeWorkflowExecutionResponse{
      workflow_execution_info: %WorkflowExecutionInfo{status: status},
      workflow_extended_info: %WorkflowExecutionExtendedInfo{cancel_requested: cancel_requested}
    }

    {:ok, Protobuf.encode(response)}
  end

  defp tonic_error(detail), do: {:error, %Bridge.Error{kind: :tonic_error, detail: detail}}
end
