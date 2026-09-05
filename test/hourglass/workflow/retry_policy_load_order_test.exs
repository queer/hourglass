defmodule Hourglass.Workflow.RetryPolicyLoadOrderTest do
  @moduledoc """
  An activity's declared retry policy is what gets scheduled, whether or
  not the VM has loaded that activity yet.

  ## The defect this is the regression test for

  `resolve_retry_policy/2` asked `function_exported?/3` whether the module
  declares `__activity_retry_policy__/0`. That reads the module's EXPORT
  TABLE, and a module the VM has not loaded has none — so it answered
  `false` for a declaration sitting in the source, and the resolution fell
  through to `Hourglass.Activity.default_retry_policy/0`: `max_attempts:
  1`, no retry at all.

  Under `:interactive` code loading — every `mix` invocation, so `mix
  test` and any `mix`-hosted worker — modules load on first CALL, and
  naming a module in `execute_activity/3` is not a call. On a fresh worker
  every activity module is therefore unloaded, and the FIRST schedule of
  each one silently lost its declared policy. The second and later ones
  got it, because running the activity is what loads the module. A
  load-order lottery, which is why it presented as a rare flake rather
  than as a constant.

  Found from a Temporal history: an activity declaring `max_attempts: 0`
  (unlimited) whose refusal its own classifier had ruled RETRYABLE, ending
  at `attempt: 1` with `retry_state: MAXIMUM_ATTEMPTS_REACHED`. A
  transient condition designed to heal by retry killed the run on first
  sight.

  ## Why the unloaded pole has to be produced rather than assumed

  A green suite cannot tell a guard that fires from one that is a no-op,
  and by the time any ordinary test asks about an activity, something has
  usually loaded it. So this purges and deletes the module outright,
  asserts the VM really has no export table for it — which is the exact
  input the old branch got wrong — and only then resolves.

  `async: false`: purging is VM-global.
  """

  use ExUnit.Case, async: false

  alias Hourglass.Activity
  alias Hourglass.Test.UnloadedRetryActivity
  alias Hourglass.Workflow

  @declared [
    max_attempts: 0,
    initial_interval: 1_000,
    backoff_coefficient: 2.0,
    max_interval: 60_000
  ]

  setup do
    # Left loaded afterwards, not left deleted: every later test in this VM
    # would otherwise inherit the very state this one manufactures.
    on_exit(fn -> Code.ensure_loaded!(UnloadedRetryActivity) end)
    :ok
  end

  describe "an activity module the VM has not loaded" do
    test "still resolves to the policy it declares" do
      unload!(UnloadedRetryActivity)

      # The pole. Both are the inputs the old implementation read, so a
      # future change that stops producing this state fails here rather
      # than quietly making the assertion below vacuous.
      assert :code.is_loaded(UnloadedRetryActivity) == false
      refute function_exported?(UnloadedRetryActivity, :__activity_retry_policy__, 0)

      assert Workflow.resolve_retry_policy(UnloadedRetryActivity, []) == @declared
    end

    test "does not resolve to the library default, which is the failure that shipped" do
      unload!(UnloadedRetryActivity)

      resolved = Workflow.resolve_retry_policy(UnloadedRetryActivity, [])

      refute resolved == Activity.default_retry_policy()
      assert Keyword.fetch!(resolved, :max_attempts) == 0
    end
  end

  describe "the loaded pole, so the two are known to agree" do
    test "resolves to the same policy once loaded" do
      Code.ensure_loaded!(UnloadedRetryActivity)

      assert Workflow.resolve_retry_policy(UnloadedRetryActivity, []) == @declared
    end

    test "a call-site override still tunes an unloaded activity's policy" do
      unload!(UnloadedRetryActivity)

      resolved =
        Workflow.resolve_retry_policy(UnloadedRetryActivity, retry_policy: [max_attempts: 3])

      assert Keyword.fetch!(resolved, :max_attempts) == 3
      # Merged over the DECLARED policy, not over the library default: the
      # untouched fields are the activity's own.
      assert Keyword.fetch!(resolved, :max_interval) == 60_000
    end
  end

  describe "a name that is not an activity" do
    test "an atom naming no module keeps answering the library default" do
      assert Workflow.resolve_retry_policy(:no_such_module_anywhere, []) ==
               Activity.default_retry_policy()
    end

    test "a module with no retry declaration keeps answering the library default" do
      assert Workflow.resolve_retry_policy(Enum, []) == Activity.default_retry_policy()
    end
  end

  # Purge, delete, purge: the first drops any old code, `delete` makes the
  # current version old, and the second drops that. Anything less leaves the
  # module callable and the pole unproduced.
  defp unload!(module) do
    :code.purge(module)
    :code.delete(module)
    :code.purge(module)
    :ok
  end
end
