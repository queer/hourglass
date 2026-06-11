defmodule Hourglass.Activity.Behaviour do
  @moduledoc """
  Behaviour activities implement to be dispatched by
  `Hourglass.ActivityRunner`.

  ## Contract

  An activity module's `execute/1` callback receives the decoded input
  term and returns the output term directly. The runner handles
  encoding/decoding via `Hourglass.Codec` and the typed I/O accessors
  injected by `use Hourglass.Activity`.

  Raises (programming bugs, e.g. `FunctionClauseError`, `MatchError`)
  flow through the runner's rescue clause and are classified via
  `Hourglass.Activity.RetryClassifier.classify/1`. The classifier marks
  programming-bug exceptions as `:non_retryable` (see `RetryClassifier`'s
  `@programming_bug_exceptions`).

  ## Authoring

  Activity modules `use Hourglass.Activity` to inject the `@behaviour`
  declaration, the `__activity_retry_policy__/0`, `__activity_input_type__/0`,
  and `__activity_output_type__/0` helpers that the runner reads at dispatch
  time. See `Hourglass.Activity` for the `:input`, `:output`, and `:retry`
  options.
  """

  @callback execute(input :: term()) :: term()
end
