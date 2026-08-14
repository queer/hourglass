defmodule Hourglass.Workflow.PatchReplayTest do
  # async: true — Temporal singletons live globally in test_helper.exs; each
  # test scopes its queue + workflow_id by UUID for cluster-side isolation.
  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.Client
  alias Hourglass.Replay.Mismatch
  alias Hourglass.Replayer
  alias Hourglass.TestSupport.EchoActivity

  @moduletag :temporal
  # The history these tests replay has to be RECORDED, not constructed: a
  # hand-built activation sequence proves the code does what its author
  # expected, and the property under test is that a history written before
  # the patch existed answers `patched?/1` `false`. Only a real pre-patch run
  # can produce one, so the recording step needs a cluster.
  @moduletag :integration
  @moduletag timeout: 120_000

  # ---------------------------------------------------------------------------
  # The three bodies: what shipped, the naive change, and the patched change.
  # ---------------------------------------------------------------------------

  # What was deployed when the history under test was written.
  defmodule PrePatchWorkflow do
    use Hourglass.Workflow, input: :map, output: :map

    @impl Hourglass.Workflow.Behaviour
    def run(args) do
      %{"first" => execute_activity!(EchoActivity, args, start_to_close_timeout: 30_000)}
    end
  end

  # The naive body change the epic was written about: one more step appended.
  # An execution started under PrePatchWorkflow replays into this and finds a
  # command the history does not have.
  defmodule UnguardedWorkflow do
    use Hourglass.Workflow, input: :map, output: :map

    @impl Hourglass.Workflow.Behaviour
    def run(args) do
      first = execute_activity!(EchoActivity, args, start_to_close_timeout: 30_000)
      second = execute_activity!(EchoActivity, args, start_to_close_timeout: 30_000)
      %{"first" => first, "second" => second}
    end
  end

  # The same change, asked of history first.
  defmodule PatchedWorkflow do
    use Hourglass.Workflow, input: :map, output: :map

    @impl Hourglass.Workflow.Behaviour
    def run(args) do
      first = execute_activity!(EchoActivity, args, start_to_close_timeout: 30_000)

      second =
        if patched?(:second_step) do
          execute_activity!(EchoActivity, args, start_to_close_timeout: 30_000)
        end

      %{"first" => first, "second" => second}
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Runs `module` to completion on a live cluster and returns
  # `{result, history_bytes}` — the proto-encoded
  # GetWorkflowExecutionHistoryResponse `Replayer.replay_history/2` expects.
  defp record(module, label) do
    queue = "#{label}-#{System.unique_integer([:positive])}"
    {:ok, _worker} = start_supervised({Hourglass.Worker, task_queue: queue}, id: queue)

    nonce = UUIDv7.generate()
    workflow_id = "#{label}:#{nonce}"

    {:ok, handle} =
      Hourglass.start(module, %{"nonce" => nonce}, id: workflow_id, task_queue: queue)

    {:ok, result} = Hourglass.result(handle, timeout: 60_000)
    {:ok, client} = Client.connect([])
    {:ok, history_bytes} = Client.fetch_history(client, workflow_id)

    {result, history_bytes}
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  test "a history recorded before the patch existed replays through the patched body" do
    {result, pre_patch_history} = record(PrePatchWorkflow, "patch-replay-pre")

    # The recording really is pre-patch: one step, and the body that wrote it
    # has no patch call in it at all.
    assert %{"first" => %{"nonce" => _nonce}} = result
    refute Map.has_key?(result, "second")

    # Without the patch, appending the step wedges this execution. Asserted by
    # message, not merely as "an error": the control is only a control if it
    # fails the way the epic says a body change fails, against the very
    # history the next assertion replays cleanly.
    assert {:error, %Mismatch{detail: detail}} =
             Replayer.replay_history(pre_patch_history, UnguardedWorkflow)

    assert detail =~ "TMPRL1100"
    assert detail =~ "Nondeterminism error"

    # With it, the same history replays through the changed body: patched?/1
    # answers false — there is no marker in this history to answer otherwise —
    # so the guarded region is skipped and the command sequence still matches.
    assert :ok = Replayer.replay_history(pre_patch_history, PatchedWorkflow)
  end

  test "an execution running the patched body for the first time takes the guarded branch" do
    {result, patched_history} = record(PatchedWorkflow, "patch-replay-new")

    # No history said otherwise, so the new branch ran.
    assert %{"first" => %{"nonce" => nonce}, "second" => %{"nonce" => nonce}} = result

    # And the decision it made is recorded: replaying its own history answers
    # the same way, so the second step is taken again and the sequence matches.
    assert :ok = Replayer.replay_history(patched_history, PatchedWorkflow)
  end
end
