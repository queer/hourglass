defmodule Hourglass.WorkflowStatus do
  @moduledoc """
  Snapshot of a workflow execution's current state. Returned by
  `Hourglass.status/2`.

  Fields:

    * `state` — high-level lifecycle state.
    * `pending_activities` — activities currently scheduled or executing,
      with attempt count + last failure (if any). Always populated; cheap
      (single DescribeWorkflowExecution call).
    * `recent_failures` — list of activity-failure events from the
      workflow's history, ordered most-recent first. Only populated when
      `failures: :include` is passed to `status/2`; walks history (more
      expensive). `nil` when not requested; `[]` when requested + empty.
    * `start_time` — when the workflow started.
    * `close_time` — when the workflow completed/failed/was terminated;
      `nil` while running.

  ## Design

  User direction: "if the human has to look at temporal, we've failed."
  This struct is the production-safe observability surface — tests, ops
  tooling, and GenServers monitoring long-running workflows pull from
  here without needing the Temporal Web UI.
  """

  @enforce_keys [:state, :pending_activities, :start_time, :close_time]
  defstruct [
    :state,
    :pending_activities,
    :start_time,
    :close_time,
    recent_failures: nil
  ]

  @type state ::
          :running
          | :completed
          | :failed
          | :continued_as_new
          | :timed_out
          | :canceled
          | :terminated
          | :unknown

  @type pending_activity :: %{
          activity_type: String.t(),
          attempt: non_neg_integer(),
          last_failure: Temporal.Api.Failure.V1.Failure.t() | nil
        }

  @type failure_event :: %{
          activity_type: String.t(),
          attempt: non_neg_integer(),
          failure: Temporal.Api.Failure.V1.Failure.t(),
          event_time: DateTime.t()
        }

  @type t :: %__MODULE__{
          state: state(),
          pending_activities: [pending_activity()],
          start_time: DateTime.t() | nil,
          close_time: DateTime.t() | nil,
          recent_failures: [failure_event()] | nil
        }
end
