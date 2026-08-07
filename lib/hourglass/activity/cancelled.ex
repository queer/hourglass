# credo:disable-for-this-file Credo.Check.Consistency.ExceptionNames
#
# The two other exception modules here — `Hourglass.ActivityError` and
# `Hourglass.ChildWorkflowError` — carry the `Error` suffix, so the consistency
# check reads this one as the odd module out. It is deliberately not named for an
# error: the runner turns it into a Temporal Cancellation completion, which is a
# terminal outcome distinct from a failure, and Temporal treats the two
# differently (a cancellation is not retried as a failure would be). Naming it
# `CancelledError` would satisfy the check by asserting the opposite of what the
# runner does with it.
defmodule Hourglass.Activity.Cancelled do
  @moduledoc """
  Raised by `Hourglass.Activity.heartbeat!/0` when the running activity has been
  cancelled (heartbeat timeout, workflow cancel, or terminate). `Hourglass.ActivityRunner`
  recognises it and reports a Temporal Cancellation completion rather than a failure.

  Deliberately not named `…Error`: see the `credo:disable-for-this-file` note above
  the module for why the suffix would misdescribe it.
  """
  defexception [:reason]

  @impl Exception
  def message(%__MODULE__{reason: reason}),
    do: "activity cancelled (reason=#{inspect(reason)})"
end
