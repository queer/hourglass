defmodule Hourglass.Client.RealRequestTest do
  use ExUnit.Case, async: true

  alias Hourglass.Client.Real
  alias Hourglass.WorkflowHandle
  alias Temporal.Api.Common.V1.Payload
  alias Temporal.Api.Common.V1.Payloads
  alias Temporal.Api.Common.V1.WorkflowExecution
  alias Temporal.Api.Workflowservice.V1.RequestCancelWorkflowExecutionRequest
  alias Temporal.Api.Workflowservice.V1.SignalWorkflowExecutionRequest

  test "build_signal_request builds the proto" do
    req =
      Real.build_signal_request(
        "ns",
        %WorkflowHandle{id: "w1", run_id: "r1"},
        "go",
        %{"v" => 1}
      )

    assert %SignalWorkflowExecutionRequest{
             namespace: "ns",
             workflow_execution: %WorkflowExecution{workflow_id: "w1", run_id: "r1"},
             signal_name: "go",
             input: %Payloads{payloads: [%Payload{data: data}]}
           } = req

    assert Jason.decode!(data) == %{"v" => 1}
    assert is_binary(req.request_id) and req.request_id != ""
  end

  test "build_cancel_request builds the proto" do
    req =
      Real.build_cancel_request(
        "ns",
        %WorkflowHandle{id: "w1", run_id: ""},
        "stop"
      )

    assert %RequestCancelWorkflowExecutionRequest{
             namespace: "ns",
             workflow_execution: %WorkflowExecution{workflow_id: "w1"},
             reason: "stop"
           } = req

    assert is_binary(req.request_id) and req.request_id != ""
  end
end
