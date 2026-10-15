defmodule Hourglass.Telemetry do
  @moduledoc """
  Canonical `:telemetry` events emitted by Hourglass. Attach your own handlers
  to project these into your application's audit/event model. See
  `Hourglass.Telemetry.LoggerHandler` for an opt-in default.

  Events marked "reserved" are defined in the catalog for handler-attachment
  compatibility but are not currently emitted by any lib module.
  """

  @events [
    [:hourglass, :connection, :failed],
    [:hourglass, :worker, :registration_failed],
    # a stopping worker has stopped taking new tasks (Hourglass.Worker, "Graceful stop")
    [:hourglass, :worker, :stopping],
    # a stopping worker has released its handle; measurements: duration, abandoned_activities
    [:hourglass, :worker, :stopped],
    [:hourglass, :activity, :heartbeat],
    # reserved — not currently emitted; placeholder for future heartbeat tracking
    [:hourglass, :activity, :heartbeat_lost],
    [:hourglass, :activity, :failure],
    # reserved — not currently emitted; :failure carries classification as metadata
    [:hourglass, :activity, :failure, :unclassified],
    [:hourglass, :activity, :exception],
    [:hourglass, :activity, :cancel_received],
    [:hourglass, :activity, :dispatch_failed],
    # an activity's workflow run has closed, so it is cancelled
    # (Hourglass.Worker, "Activities of a closed workflow"); metadata: task_queue, workflow_id, run_id
    [:hourglass, :activity, :workflow_closed],
    # such an activity was still running once half the bound had passed, and was killed
    [:hourglass, :activity, :workflow_closed, :stopped],
    # a cancel was requested of the workflow run of an activity that ends on one
    # (use Hourglass.Activity, ends_on_cancel_request: true), so it is cancelled;
    # metadata: task_queue, workflow_id, run_id
    [:hourglass, :activity, :workflow_cancel_requested],
    # such an activity was still running once half the bound had passed, and was killed
    [:hourglass, :activity, :workflow_cancel_requested, :stopped],
    # an activity's run had ended when its task reached the worker, so it never started;
    # metadata: task_queue, workflow_id, run_id, reason (:workflow_closed | :workflow_cancel_requested)
    [:hourglass, :activity, :not_started],
    [:hourglass, :workflow, :exception],
    # workflow body raised → parked as a workflow-task failure; retried by the server
    [:hourglass, :workflow, :task_failed],
    # workflow body called fail/2,3 → terminal FailWorkflowExecution; not retried
    [:hourglass, :workflow, :failed],
    [:hourglass, :workflow, :unhandled_job_variant],
    [:hourglass, :bridge_holder, :activity_result_unrouted],
    # reserved — not currently emitted; intended for the replay CI gate mix task
    [:hourglass, :replay, :mismatch]
  ]

  @doc "All Hourglass telemetry event names."
  @spec events() :: [[atom()]]
  def events, do: @events
end
