defmodule Hourglass.Test.UnloadedRetryActivity do
  @moduledoc """
  An activity that exists only to be UNLOADED.

  `Hourglass.Workflow.RetryPolicyLoadOrderTest` purges and deletes this
  module to reproduce the state every activity module is in on a fresh
  worker, then asks `Hourglass.Workflow.resolve_retry_policy/2` what it
  declares. It lives in `test/support` rather than inside the test file
  because a module defined by `defmodule` inside a test exists only in
  memory: deleting it makes it unrecoverable, and `Code.ensure_loaded?/1`
  — the thing under test — has no `.beam` to load it back from, so the
  reproduction would be of a different situation entirely.

  Nothing else may use it. A second user would be a test whose module got
  purged out from under it.

  The policy below is deliberately unlike `Hourglass.Activity`'s default
  (`[max_attempts: 1]`) in every field, so a resolution that fell back to
  the default cannot be mistaken for one that read this.
  """

  use Hourglass.Activity,
    input: :map,
    output: :map,
    retry: [
      max_attempts: 0,
      initial_interval: 1_000,
      backoff_coefficient: 2.0,
      max_interval: 60_000
    ]

  @impl true
  @spec execute(map(), keyword()) :: map()
  def execute(input, _opts \\ []), do: input
end
