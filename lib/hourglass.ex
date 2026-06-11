# credo:disable-for-this-file Credo.Check.Refactor.ModuleDependencies
defmodule Hourglass do
  @moduledoc """
  Hourglass — a standalone Elixir SDK for Temporal (https://temporal.io).

  Workflow and activity definitions, a worker that polls a Temporal cluster,
  a client for starting and awaiting workflows, a deterministic replayer, and
  a Rust NIF bridge over `temporalio-sdk-core`.

  See `Hourglass.Workflow`, `Hourglass.Activity`, `Hourglass.Client`, and
  `Hourglass.Worker`.

  ## Top-level facade

  Top-level facade for the Hourglass SDK. `start/3`, `signal/3`, `cancel/2`,
  `status/1,2`, `history/1`, and `result/2` are the primary entry points for
  interacting with a Temporal cluster.

  All facade functions accept either a `%WorkflowHandle{}` or a bare
  workflow-id string — the string form is normalised to
  `%WorkflowHandle{id: id, run_id: ""}` before dispatch.

  ## Backend dispatch

  The cluster-facing operations resolve their backend via
  `Hourglass.Client.Backend.impl/0`:

    * Production / `:integration` tests → `Hourglass.Client.Real`
      (Bridge NIFs, real cluster).
    * Default-suite tests → `Hourglass.Client.Mock` (Mox), backed
      by `Hourglass.Test.HourglassCase` and friends.

  See `Hourglass.Client` for the lower-level Client helpers and
  `Hourglass.Bridge` for the NIF wrappers.

  ## Polling for a workflow result

  `result/2` polls `status/2` until the workflow closes (or the timeout
  budget expires) and extracts the close result from the workflow history.
  """

  alias Hourglass.Bridge
  alias Hourglass.Client.Backend
  alias Hourglass.Error
  alias Hourglass.WorkflowHandle
  alias Hourglass.WorkflowStatus
  alias Temporal.Api.History.V1.ActivityTaskFailedEventAttributes
  alias Temporal.Api.History.V1.ActivityTaskScheduledEventAttributes
  alias Temporal.Api.History.V1.ActivityTaskStartedEventAttributes
  alias Temporal.Api.History.V1.History
  alias Temporal.Api.History.V1.HistoryEvent
  alias Temporal.Api.Workflow.V1.PendingActivityInfo
  alias Temporal.Api.Workflow.V1.WorkflowExecutionInfo
  alias Temporal.Api.Workflowservice.V1.DescribeWorkflowExecutionResponse

  require Logger

  # ---------------------------------------------------------------------------
  # Handle normalisation
  # ---------------------------------------------------------------------------

  defp normalize_handle(%WorkflowHandle{} = h), do: h
  defp normalize_handle(id) when is_binary(id), do: %WorkflowHandle{id: id, run_id: ""}

  # ---------------------------------------------------------------------------
  # Public facade — start/3
  # ---------------------------------------------------------------------------

  @doc """
  Starts a new workflow execution.

  `id` is required (passed as `id: "some-id"` in `opts`). The `input` is
  cast+validated against the workflow module's declared input type before the
  backend is called. If validation fails the backend is **never** called and
  `{:error, %Ecto.Changeset{}}` is returned.

  ## Options

    * `:id` (required) — stable workflow ID.
    * `:task_queue` — defaults to `"default"`.
    * `:namespace` — defaults to the configured namespace.
  """
  @spec start(module(), term(), keyword()) ::
          {:ok, WorkflowHandle.t()} | {:error, Error.t()} | {:error, Ecto.Changeset.t()}
  def start(workflow_module, input, opts) do
    id = Keyword.fetch!(opts, :id)
    input_type = workflow_module.__workflow_input_type__()
    raw = if is_struct(input), do: Hourglass.Codec.dump(input_type, input), else: input

    case Hourglass.Codec.cast(input_type, raw) do
      {:ok, validated} ->
        dumped = Hourglass.Codec.dump(input_type, validated)

        Backend.impl().start_workflow(
          workflow_module,
          dumped,
          Keyword.put(opts, :workflow_id, id)
        )

      {:error, _changeset} = err ->
        err
    end
  end

  @doc """
  Sends a signal to a workflow execution.

  Accepts a `%WorkflowHandle{}` or a bare workflow-id string.
  """
  @spec signal(WorkflowHandle.t() | String.t(), String.t(), term()) ::
          :ok | {:error, Error.t()}
  def signal(handle_or_id, name, payload) do
    handle_or_id
    |> normalize_handle()
    |> Backend.impl().signal_workflow(name, payload)
  end

  @doc """
  Requests cancellation of a workflow execution.

  Accepts a `%WorkflowHandle{}` or a bare workflow-id string.
  The `reason` is optional (defaults to `""`).
  """
  @spec cancel(WorkflowHandle.t() | String.t(), String.t()) :: :ok | {:error, Error.t()}
  def cancel(handle_or_id, reason \\ "") do
    handle_or_id
    |> normalize_handle()
    |> Backend.impl().cancel_workflow(reason)
  end

  @doc """
  Fetches the full event history for a workflow execution.

  Accepts a `%WorkflowHandle{}` or a bare workflow-id string.
  """
  @spec history(WorkflowHandle.t() | String.t()) ::
          {:ok, Temporal.Api.History.V1.History.t()} | {:error, Error.t()}
  def history(handle_or_id) do
    handle_or_id
    |> normalize_handle()
    |> Backend.impl().fetch_history()
  end

  @doc """
  Polls `Hourglass.status/2` until the workflow closes or the budget
  expires, then extracts and returns the close result.

  Accepts a `%WorkflowHandle{}` or a bare workflow-id string.

  ## Options

    * `:timeout` — milliseconds (default `30_000`). The polling budget.
    * `:poll_interval` — milliseconds between status polls (default `100`).
    * `:as` — optional type. When provided, casts the raw result via
      `Hourglass.Codec.cast!(type, raw)` before returning.

  ## Return

    * `{:ok, result}` — workflow completed successfully.
    * `{:error, :timeout, status}` — budget expired before close.
    * `{:error, terminal, status}` — workflow closed in a non-success
      terminal state; `terminal` is one of `:failed`, `:canceled`,
      `:terminated`, `:continued_as_new`, `:timed_out`.
    * `{:error, reason}` — transport / Bridge error.
  """
  @spec result(WorkflowHandle.t() | String.t(), keyword()) ::
          {:ok, term()}
          | {:error,
             :timeout
             | :failed
             | :canceled
             | :terminated
             | :continued_as_new
             | :timed_out
             | :unknown, WorkflowStatus.t()}
          | {:error, Error.t()}
  def result(handle_or_id, opts \\ []) do
    handle = normalize_handle(handle_or_id)
    timeout = Keyword.get(opts, :timeout, 30_000)
    poll_interval = Keyword.get(opts, :poll_interval, 100)
    deadline = System.monotonic_time(:millisecond) + timeout
    as_type = Keyword.get(opts, :as)

    do_result(handle, deadline, poll_interval, as_type)
  end

  defp do_result(handle, deadline, poll_interval, as_type) do
    case status(handle, failures: :exclude) do
      {:ok, %WorkflowStatus{state: :running}} ->
        result_running(handle, deadline, poll_interval, as_type)

      {:ok, %WorkflowStatus{state: :completed}} ->
        fetch_completed_result(handle, as_type)

      {:ok, %WorkflowStatus{state: terminal}}
      when terminal in [:failed, :canceled, :terminated, :continued_as_new, :timed_out] ->
        terminal_with_failures(handle, terminal)

      {:ok, %WorkflowStatus{state: :unknown} = wstatus} ->
        {:error, :unknown, wstatus}

      {:error, _reason} = err ->
        err
    end
  end

  defp result_running(handle, deadline, poll_interval, as_type) do
    if System.monotonic_time(:millisecond) >= deadline do
      timeout_with_failures(handle)
    else
      Process.sleep(poll_interval)
      do_result(handle, deadline, poll_interval, as_type)
    end
  end

  defp timeout_with_failures(handle) do
    case status(handle, failures: :include) do
      {:ok, %WorkflowStatus{} = wstatus} -> {:error, :timeout, wstatus}
      {:error, _reason} = err -> err
    end
  end

  defp terminal_with_failures(handle, terminal) do
    case status(handle, failures: :include) do
      {:ok, %WorkflowStatus{} = wstatus} -> {:error, terminal, wstatus}
      {:error, _reason} = err -> err
    end
  end

  defp fetch_completed_result(%WorkflowHandle{} = handle, as_type) do
    with {:ok, %History{events: events}} <- Backend.impl().fetch_history(handle),
         %HistoryEvent{} = close <- Enum.find(events, &workflow_close_event?/1) do
      case extract_close_result(close) do
        {:ok, raw} when as_type != nil ->
          {:ok, Hourglass.Codec.cast!(as_type, raw)}

        other ->
          other
      end
    else
      nil -> {:error, Error.new(:not_found, "no close event in history")}
      {:error, _reason} = err -> err
    end
  end

  defp workflow_close_event?(%HistoryEvent{event_type: type}) do
    type in [
      :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
      :EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
      :EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
      :EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
      :EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
    ]
  end

  defp extract_close_result(%HistoryEvent{
         event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
         attributes:
           {:workflow_execution_completed_event_attributes,
            %Temporal.Api.History.V1.WorkflowExecutionCompletedEventAttributes{result: result}}
       }) do
    case result do
      %Temporal.Api.Common.V1.Payloads{
        payloads: [%Temporal.Api.Common.V1.Payload{data: data} | _rest]
      } ->
        {:ok, Jason.decode!(data)}

      _other ->
        {:ok, nil}
    end
  end

  defp extract_close_result(%HistoryEvent{event_type: type}) do
    {:error, Error.new(:rpc_error, type)}
  end

  @doc """
  Synchronous snapshot of a workflow execution's current state.

  Wraps `DescribeWorkflowExecution` (always) and
  `GetWorkflowExecutionHistory` (when `failures: :include`).

  ## Options

    * `:failures` — `:exclude` (default) | `:include`. When `:include`,
      walks the workflow's history for `ActivityTaskFailed` events and
      populates `WorkflowStatus.recent_failures`. Otherwise
      `recent_failures` is `nil`.

  `:exclude` issues exactly one Bridge call (cheap; suitable for
  production heartbeat polling). `:include` issues two Bridge calls
  (the second walks history; suitable for tests + on-demand operator
  tooling).
  """
  @spec status(WorkflowHandle.t() | String.t(), keyword()) ::
          {:ok, WorkflowStatus.t()} | {:error, Error.t()}
  def status(handle_or_id, opts \\ []) do
    handle = normalize_handle(handle_or_id)
    failures = Keyword.get(opts, :failures, :exclude)

    with {:ok, description} <- Backend.impl().describe_workflow_execution(handle) do
      build_status(handle, description, failures)
    end
  end

  # ---------------------------------------------------------------------------
  # status/2 internals
  # ---------------------------------------------------------------------------

  defp build_status(_handle, %DescribeWorkflowExecutionResponse{} = resp, :exclude) do
    {:ok, status_struct(resp, nil)}
  end

  defp build_status(handle, %DescribeWorkflowExecutionResponse{} = resp, :include) do
    failures =
      case fetch_recent_failures(handle) do
        {:ok, list} -> list
        {:error, _reason} -> []
      end

    {:ok, status_struct(resp, failures)}
  end

  defp status_struct(%DescribeWorkflowExecutionResponse{} = resp, recent_failures) do
    info = resp.workflow_execution_info

    %WorkflowStatus{
      state: state_from_info(info),
      pending_activities: pending_activities_from_response(resp),
      start_time: timestamp_to_datetime(info && info.start_time),
      close_time: timestamp_to_datetime(info && info.close_time),
      recent_failures: recent_failures
    }
  end

  defp state_from_info(nil), do: :unknown

  defp state_from_info(%WorkflowExecutionInfo{status: status}) do
    map_status(status)
  end

  defp map_status(:WORKFLOW_EXECUTION_STATUS_RUNNING), do: :running
  defp map_status(:WORKFLOW_EXECUTION_STATUS_COMPLETED), do: :completed
  defp map_status(:WORKFLOW_EXECUTION_STATUS_FAILED), do: :failed
  defp map_status(:WORKFLOW_EXECUTION_STATUS_CANCELED), do: :canceled
  defp map_status(:WORKFLOW_EXECUTION_STATUS_TERMINATED), do: :terminated
  defp map_status(:WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW), do: :continued_as_new
  defp map_status(:WORKFLOW_EXECUTION_STATUS_TIMED_OUT), do: :timed_out
  defp map_status(_status), do: :unknown

  defp pending_activities_from_response(%DescribeWorkflowExecutionResponse{
         pending_activities: list
       })
       when is_list(list) do
    Enum.map(list, &pending_activity_summary/1)
  end

  defp pending_activities_from_response(_resp), do: []

  defp pending_activity_summary(%PendingActivityInfo{} = info) do
    %{
      activity_type: activity_type_name(info.activity_type),
      attempt: info.attempt || 0,
      last_failure: info.last_failure
    }
  end

  defp activity_type_name(%Temporal.Api.Common.V1.ActivityType{name: name}) when is_binary(name),
    do: name

  defp activity_type_name(_type), do: ""

  defp timestamp_to_datetime(nil), do: nil

  defp timestamp_to_datetime(%Google.Protobuf.Timestamp{seconds: seconds, nanos: nanos}) do
    case DateTime.from_unix(seconds * 1_000_000_000 + nanos, :nanosecond) do
      {:ok, dt} -> dt
      {:error, _reason} -> nil
    end
  end

  # ---------------------------------------------------------------------------
  # Recent-failures (failures: :include)
  # ---------------------------------------------------------------------------

  defp fetch_recent_failures(%WorkflowHandle{} = handle) do
    case Backend.impl().fetch_history(handle) do
      {:ok, %History{events: events}} -> {:ok, walk_failures(events)}
      {:ok, nil} -> {:ok, []}
      {:error, _reason} = err -> err
    end
  end

  # Walks the history for ActivityTaskFailed events. For each failure, looks up
  # the corresponding ActivityTaskScheduled event (by scheduled_event_id) for
  # the activity_type, and the ActivityTaskStarted event (by started_event_id
  # on the failed event) for the actual attempt count. Returns most-recent
  # first.
  defp walk_failures(events) do
    scheduled_by_id = index_scheduled(events)
    started_by_id = index_started(events)

    events
    |> Enum.filter(&activity_task_failed?/1)
    |> Enum.map(&failure_event_summary(&1, scheduled_by_id, started_by_id))
    |> Enum.reverse()
  end

  defp index_scheduled(events) do
    Enum.reduce(events, %{}, fn
      %HistoryEvent{
        event_id: event_id,
        event_type: :EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
        attributes: {:activity_task_scheduled_event_attributes, scheduled}
      },
      acc ->
        Map.put(acc, event_id, scheduled)

      _event, acc ->
        acc
    end)
  end

  defp index_started(events) do
    Enum.reduce(events, %{}, fn
      %HistoryEvent{
        event_id: event_id,
        event_type: :EVENT_TYPE_ACTIVITY_TASK_STARTED,
        attributes: {:activity_task_started_event_attributes, started}
      },
      acc ->
        Map.put(acc, event_id, started)

      _event, acc ->
        acc
    end)
  end

  defp activity_task_failed?(%HistoryEvent{event_type: :EVENT_TYPE_ACTIVITY_TASK_FAILED}),
    do: true

  defp activity_task_failed?(_event), do: false

  defp failure_event_summary(
         %HistoryEvent{
           attributes: {:activity_task_failed_event_attributes, attrs},
           event_time: event_time
         },
         scheduled_by_id,
         started_by_id
       ) do
    %ActivityTaskFailedEventAttributes{
      failure: failure,
      scheduled_event_id: scheduled_event_id,
      started_event_id: started_event_id
    } = attrs

    scheduled = Map.get(scheduled_by_id, scheduled_event_id)
    started = Map.get(started_by_id, started_event_id)

    %{
      activity_type: scheduled_activity_type(scheduled),
      attempt: resolve_attempt(started, started_event_id),
      failure: failure,
      event_time: timestamp_to_datetime(event_time)
    }
  end

  defp scheduled_activity_type(%ActivityTaskScheduledEventAttributes{activity_type: type}) do
    activity_type_name(type)
  end

  defp scheduled_activity_type(_scheduled), do: ""

  # The ActivityTaskStarted event preceding a failure carries the attempt
  # number on its 4th field. We index started events by their own event_id
  # and look up the one referenced by the failed event's `started_event_id`.
  # Falls back to 1 with a warning only if the linkage is missing — should
  # never occur for a real failure but defends against malformed history.
  defp resolve_attempt(%ActivityTaskStartedEventAttributes{attempt: attempt}, _started_event_id)
       when is_integer(attempt) and attempt > 0,
       do: attempt

  defp resolve_attempt(_started, started_event_id) do
    Logger.warning(
      "Hourglass.status: ActivityTaskStarted event #{inspect(started_event_id)} " <>
        "not found in history (or missing :attempt); using attempt=1 fallback"
    )

    1
  end

  # ---------------------------------------------------------------------------
  # Bridge error classification (mirrors Hourglass.Client.classify/1)
  # ---------------------------------------------------------------------------

  @doc false
  # Public for unit testing. Translates a low-level Bridge.Error into a
  # facade-level Hourglass.Error. Not part of the public API.
  @spec classify_bridge_error(Bridge.Error.t()) :: Error.t()
  def classify_bridge_error(%Bridge.Error{kind: :tonic_error, detail: d}) do
    detail = d || ""

    if String.contains?(detail, "NotFound") or String.contains?(detail, "not found") do
      Error.new(:not_found, d)
    else
      Error.new(:rpc_error, d)
    end
  end

  def classify_bridge_error(%Bridge.Error{kind: :shutdown}),
    do: Error.new(:unreachable, :shutdown)

  def classify_bridge_error(%Bridge.Error{} = err), do: Error.new(:rpc_error, err)
end
