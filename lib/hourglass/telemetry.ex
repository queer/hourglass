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
    [:hourglass, :activity, :heartbeat],
    # reserved — not currently emitted; placeholder for future heartbeat tracking
    [:hourglass, :activity, :heartbeat_lost],
    [:hourglass, :activity, :failure],
    # reserved — not currently emitted; :failure carries classification as metadata
    [:hourglass, :activity, :failure, :unclassified],
    [:hourglass, :activity, :exception],
    [:hourglass, :activity, :dispatch_failed],
    [:hourglass, :workflow, :exception],
    # workflow body raised → parked as a workflow-task failure; retried by the server
    [:hourglass, :workflow, :task_failed],
    [:hourglass, :workflow, :unhandled_job_variant],
    [:hourglass, :bridge_holder, :activity_result_unrouted],
    # reserved — not currently emitted; intended for the replay CI gate mix task
    [:hourglass, :replay, :mismatch]
  ]

  @doc "All Hourglass telemetry event names."
  @spec events() :: [[atom()]]
  def events, do: @events
end
