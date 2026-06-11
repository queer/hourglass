defmodule Hourglass.Activity.Info do
  @moduledoc """
  Per-dispatch context the runner threads through
  `Hourglass.Activity.info/0`.

  An `%Info{}` is set in the activity-task process dictionary by
  `Hourglass.ActivityRunner` immediately before each dispatch and
  cleared (regardless of success or raise) immediately after. The
  values come straight off the inbound `Coresdk.ActivityTask.Start`
  proto:

    * `workflow_id` — `start.workflow_execution.workflow_id`
    * `run_id` — `start.workflow_execution.run_id` (per-execution,
      stable across retries of the *same* attempt's activity)
    * `activity_id` — `start.activity_id` (server-assigned id, stable
      across retries of this scheduled activity)
    * `attempt` — `start.attempt`, normalised to a positive integer
      (an unset/zero uint32 clamps to 1; a dispatch is by definition
      an attempt)

  The `(run_id, activity_id)` pair is stable across retries of the
  same activity invocation, which is what the LLM call cache keys on
  to deduplicate retry double-spend.
  """

  @enforce_keys [:workflow_id, :run_id, :activity_id, :attempt]
  defstruct [:workflow_id, :run_id, :activity_id, :attempt]

  @type t :: %__MODULE__{
          workflow_id: String.t(),
          run_id: String.t(),
          activity_id: String.t(),
          attempt: pos_integer()
        }
end
