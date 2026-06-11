defmodule Hourglass.Test.HourglassCase do
  @moduledoc """
  ExUnit case template for mock-backed `Hourglass` behavioral tests.
  Default-suite tests of `Hourglass.{start, status, await}`
  use this — they assert against canned proto responses produced by
  `Hourglass.Client.Mock` (Mox), with no real Temporal cluster.

  Test config (`config/test.exs`) already sets the Mock as the default
  backend, so this template's main jobs are:

    * Import Mox + `Hourglass.Test.HourglassCase` helpers.
    * Set `:set_mox_from_context` so async tests with stubs in setup
      blocks see those stubs in spawned-by-test-process callers.
    * Provide compact helpers for the common scenarios.

  ## Use

      defmodule Hourglass.StatusMockTest do
        use Hourglass.Test.HourglassCase, async: true

        test "status/2 reports :running for a running workflow" do
          handle = handle()
          stub_running(handle)

          assert {:ok, %WorkflowStatus{state: :running}} =
                   Hourglass.status(handle)
        end
      end

  ## Helpers

  All helpers expect the Mock to be the active backend and just install
  Mox `stub` (default-many) or `expect` (count-strict) responses.

  | Helper                             | Mock callback              | Returned status state |
  | ---------------------------------- | -------------------------- | --------------------- |
  | `stub_running/1` (handle)          | `describe_workflow_execution` | `:running`         |
  | `stub_completed/2` (handle, result)| `describe_workflow_execution` | `:completed`       |
  | `stub_failed/2` (handle, opts)     | `describe_workflow_execution` + `fetch_history` | `:failed` |
  | `stub_start_workflow/0`            | `start_workflow`           | n/a (returns handle)  |
  | `handle/0`                         | builds `%WorkflowHandle{}` | n/a                   |
  """

  use ExUnit.CaseTemplate

  alias Temporal.Api.Common.V1.Payload
  alias Temporal.Api.Common.V1.Payloads
  alias Temporal.Api.History.V1.ActivityTaskFailedEventAttributes
  alias Temporal.Api.History.V1.History
  alias Temporal.Api.History.V1.HistoryEvent
  alias Temporal.Api.Workflow.V1.WorkflowExecutionInfo
  alias Temporal.Api.Workflowservice.V1.DescribeWorkflowExecutionResponse

  using do
    quote do
      import Mox
      import Hourglass.Test.HourglassCase

      # credo:disable-for-next-line Credo.Check.Readability.AliasAs
      alias Hourglass.Client.Mock, as: TemporalMock
      alias Hourglass.Error
      alias Hourglass.WorkflowHandle
      alias Hourglass.WorkflowStatus

      setup :set_mox_from_context
      setup :verify_on_exit!
    end
  end

  @doc """
  Builds a `%Hourglass.WorkflowHandle{}` with a fresh
  workflow_id + run_id. Use as the handle argument when stubbing —
  the real workflow_id doesn't matter to the mock; the stubs match
  on the handle struct.
  """
  @spec handle() :: Hourglass.WorkflowHandle.t()
  def handle do
    %Hourglass.WorkflowHandle{
      id: "test-wf-#{UUIDv7.generate()}",
      run_id: "test-run-#{UUIDv7.generate()}"
    }
  end

  @doc """
  Stubs `Hourglass.Client.Mock.describe_workflow_execution/1` to
  return a `DescribeWorkflowExecutionResponse` with the given workflow's
  state set to `:running`. `start_time` is set to "now"; `close_time`
  is left nil. Stub is many-times so multiple calls work.
  """
  @spec stub_running(Hourglass.WorkflowHandle.t()) :: any()
  def stub_running(_handle) do
    response = describe_response(:WORKFLOW_EXECUTION_STATUS_RUNNING, close_time: nil)
    empty_history = %History{events: []}

    Mox.stub(Hourglass.Client.Mock, :describe_workflow_execution, fn _h ->
      {:ok, response}
    end)

    # `Hourglass.await/2` on timeout calls `status(failures: :include)`
    # which fetches history; stub it as empty so the timeout-with-failures
    # branch resolves cleanly without an UnexpectedCallError.
    Mox.stub(Hourglass.Client.Mock, :fetch_history, fn _h -> {:ok, empty_history} end)
  end

  @doc """
  Stubs `describe_workflow_execution/1` for `:completed` state and
  `fetch_history/1` to include a `WorkflowExecutionCompleted` event
  carrying the JSON-encoded `result` payload (so
  `Hourglass.await/2` extracts it correctly).
  """
  @spec stub_completed(Hourglass.WorkflowHandle.t(), term()) :: any()
  def stub_completed(_handle, result) do
    desc =
      describe_response(:WORKFLOW_EXECUTION_STATUS_COMPLETED,
        close_time: now_proto_timestamp()
      )

    history = %History{
      events: [
        %HistoryEvent{
          event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
          attributes:
            {:workflow_execution_completed_event_attributes,
             %Temporal.Api.History.V1.WorkflowExecutionCompletedEventAttributes{
               result: %Payloads{
                 payloads: [
                   %Payload{
                     metadata: %{"encoding" => "json/plain"},
                     data: Jason.encode!(result)
                   }
                 ]
               }
             }}
        }
      ]
    }

    Mox.stub(Hourglass.Client.Mock, :describe_workflow_execution, fn _h -> {:ok, desc} end)
    Mox.stub(Hourglass.Client.Mock, :fetch_history, fn _h -> {:ok, history} end)
  end

  @doc """
  Stubs `describe_workflow_execution/1` for `:failed` state and
  `fetch_history/1` to include an `ActivityTaskFailed` event so the
  `failures: :include` branch of `Hourglass.status/2` populates
  `recent_failures`.

  ## Options

    * `:activity_type` (default `"TestActivity.fail_now"`) — name on
      the (synthetic) ActivityTaskScheduled event the failure links to.
    * `:reason` (default `"test failure"`) — message on the inner
      Failure proto.
  """
  @spec stub_failed(Hourglass.WorkflowHandle.t(), keyword()) :: any()
  def stub_failed(_handle, opts \\ []) do
    activity_type = Keyword.get(opts, :activity_type, "TestActivity.fail_now")
    reason = Keyword.get(opts, :reason, "test failure")

    desc =
      describe_response(:WORKFLOW_EXECUTION_STATUS_FAILED, close_time: now_proto_timestamp())

    history = failed_history(activity_type, reason)

    Mox.stub(Hourglass.Client.Mock, :describe_workflow_execution, fn _h -> {:ok, desc} end)
    Mox.stub(Hourglass.Client.Mock, :fetch_history, fn _h -> {:ok, history} end)
  end

  @doc """
  Stubs `start_workflow/3` to return a fresh `%WorkflowHandle{}`. The
  returned handle's `workflow_id` matches the `:workflow_id` opt the
  caller passed (production behavior); `run_id` is a generated UUID.
  Stub is many-times.
  """
  @spec stub_start_workflow() :: any()
  def stub_start_workflow do
    Mox.stub(Hourglass.Client.Mock, :start_workflow, fn _module, _args, opts ->
      workflow_id = Keyword.fetch!(opts, :workflow_id)

      {:ok,
       %Hourglass.WorkflowHandle{
         id: workflow_id,
         run_id: "mock-run-#{UUIDv7.generate()}"
       }}
    end)
  end

  # ---------------------------------------------------------------------------
  # Internals
  # ---------------------------------------------------------------------------

  defp describe_response(status, opts) do
    close_time = Keyword.get(opts, :close_time)

    %DescribeWorkflowExecutionResponse{
      workflow_execution_info: %WorkflowExecutionInfo{
        status: status,
        start_time: now_proto_timestamp(),
        close_time: close_time
      },
      pending_activities: []
    }
  end

  defp failed_history(activity_type, reason) do
    %History{
      events: [
        %HistoryEvent{
          event_id: 5,
          event_type: :EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
          attributes:
            {:activity_task_scheduled_event_attributes,
             %Temporal.Api.History.V1.ActivityTaskScheduledEventAttributes{
               activity_type: %Temporal.Api.Common.V1.ActivityType{name: activity_type}
             }}
        },
        %HistoryEvent{
          event_id: 6,
          event_type: :EVENT_TYPE_ACTIVITY_TASK_STARTED,
          attributes:
            {:activity_task_started_event_attributes,
             %Temporal.Api.History.V1.ActivityTaskStartedEventAttributes{attempt: 1}}
        },
        %HistoryEvent{
          event_id: 7,
          event_type: :EVENT_TYPE_ACTIVITY_TASK_FAILED,
          event_time: now_proto_timestamp(),
          attributes:
            {:activity_task_failed_event_attributes,
             %ActivityTaskFailedEventAttributes{
               scheduled_event_id: 5,
               started_event_id: 6,
               retry_state: :RETRY_STATE_NON_RETRYABLE_FAILURE,
               failure: %Temporal.Api.Failure.V1.Failure{message: reason}
             }}
        }
      ]
    }
  end

  defp now_proto_timestamp do
    now = DateTime.utc_now()

    %Google.Protobuf.Timestamp{
      seconds: DateTime.to_unix(now),
      nanos:
        now.microsecond
        |> elem(0)
        |> Kernel.*(1000)
    }
  end
end
