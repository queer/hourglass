defmodule Hourglass.TestSupport.UnservedRuns do
  @moduledoc """
  Workflow runs Temporal knows and that go on running, for a test that needs
  one: started on a queue no worker serves. Each times out ten minutes after
  it starts, longer than any test waits, so none is left running on the
  Temporal the suite shares; Temporal drops it a day after.
  """

  alias Hourglass.Bridge
  alias Hourglass.Client
  alias Hourglass.TestSupport.Patience
  alias Temporal.Api.Common.V1.WorkflowType
  alias Temporal.Api.Taskqueue.V1.TaskQueue
  alias Temporal.Api.Workflowservice.V1.StartWorkflowExecutionRequest
  alias Temporal.Api.Workflowservice.V1.StartWorkflowExecutionResponse

  @times_out_after %Google.Protobuf.Duration{seconds: 600}

  @doc "A running run, as `{workflow_id, run_id}`; `prefix` begins its workflow id."
  @spec start!(String.t()) :: {String.t(), String.t()}
  def start!(prefix) do
    {:ok, client} = Patience.retry(fn -> Client.connect() end)
    workflow_id = "#{prefix}-#{UUIDv7.generate()}"

    request = %StartWorkflowExecutionRequest{
      namespace: Client.default_namespace(),
      workflow_id: workflow_id,
      workflow_type: %WorkflowType{name: "Elixir.Hourglass.TestSupport.SignalledChild"},
      task_queue: %TaskQueue{name: "#{prefix}-nobody-#{System.unique_integer([:positive])}"},
      request_id: UUIDv7.generate(),
      workflow_execution_timeout: @times_out_after
    }

    {:ok, bytes} =
      Patience.retry(fn -> Bridge.client_start_workflow(client, Protobuf.encode(request)) end)

    {workflow_id, StartWorkflowExecutionResponse.decode(bytes).run_id}
  end
end
