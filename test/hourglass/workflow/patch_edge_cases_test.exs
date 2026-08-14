defmodule Hourglass.Workflow.PatchEdgeCasesTest do
  @moduledoc """
  The angles `Hourglass.Workflow.PatchTest` does not cover: where in the
  history a patch is first reached, async scopes, eviction, terminal
  finishers, ids that are not plain ASCII, and purity.

  Each of these is a way `patched?/1` could answer differently for the same
  execution at two moments, which is the one thing it must never do.
  """
  use ExUnit.Case, async: true

  alias Coresdk.WorkflowCommands.WorkflowCommand
  alias Coresdk.WorkflowCompletion.Success
  alias Coresdk.WorkflowCompletion.WorkflowActivationCompletion
  alias Hourglass.Workflow.Evaluator
  alias Hourglass.Workflow.State

  @moduletag capture_log: true

  defmodule A1 do
    def __activity_input_type__, do: :map
    def __activity_output_type__, do: :map
    def __activity_retry_policy__, do: []
  end

  defmodule A2 do
    def __activity_input_type__, do: :map
    def __activity_output_type__, do: :map
    def __activity_retry_policy__, do: []
  end

  defmodule A3 do
    def __activity_input_type__, do: :map
    def __activity_output_type__, do: :map
    def __activity_retry_policy__, do: []
  end

  # Patch reached only AFTER an activity resolves: the realistic shape where
  # the marker lands in the SECOND WFT, so Core's peekahead notifies on the
  # second activation.
  defmodule LateReach do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(input) do
      a = execute_activity!(A1, input)

      b =
        if patched?(:late) do
          execute_activity!(A2, input)
        else
          execute_activity!(A3, input)
        end

      %{"a" => a, "b" => b}
    end
  end

  defmodule InAsync do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(input) do
      c1 = async(fn -> if patched?(:p), do: execute_activity!(A1, input) end)
      c2 = async(fn -> if patched?(:p), do: execute_activity!(A2, input) end)
      %{"c1" => await(c1), "c2" => await(c2)}
    end
  end

  defmodule TwoPatches do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(input) do
      x = patched?(:alpha)
      y = patched?(:beta)
      a = execute_activity!(A1, input)
      %{"alpha" => x, "beta" => y, "a" => a}
    end
  end

  defmodule OddId do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(%{"id" => id}), do: %{"answer" => patched?(id)}
  end

  defmodule BadId do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input), do: %{"answer" => patched?(123)}
  end

  # -- helpers ----------------------------------------------------------------

  defp activation(jobs, opts \\ []) do
    %{
      timestamp: DateTime.utc_now(),
      is_replaying: Keyword.get(opts, :is_replaying, false),
      jobs: jobs,
      run_id: Keyword.get(opts, :run_id, "adv-run")
    }
  end

  defp init_job(input) do
    %{
      variant:
        {:initialize_workflow,
         %Coresdk.WorkflowActivation.InitializeWorkflow{
           workflow_type: "TestWf",
           workflow_id: "wf-1",
           arguments: [payload(input)]
         }}
    }
  end

  defp notify_job(id) do
    %{variant: {:notify_has_patch, %Coresdk.WorkflowActivation.NotifyHasPatch{patch_id: id}}}
  end

  defp resolve_job(seq, value) do
    %{
      variant:
        {:resolve_activity,
         %Coresdk.WorkflowActivation.ResolveActivity{
           seq: seq,
           result: %Coresdk.ActivityResult.ActivityResolution{
             status: {:completed, %Coresdk.ActivityResult.Success{result: payload(value)}}
           }
         }}
    }
  end

  defp evict_job do
    %{
      variant:
        {:remove_from_cache,
         %Coresdk.WorkflowActivation.RemoveFromCache{reason: :CACHE_FULL, message: "evicting"}}
    }
  end

  defp payload(data) do
    %Temporal.Api.Common.V1.Payload{
      metadata: %{"encoding" => "json/plain"},
      data: Jason.encode!(data)
    }
  end

  defp commands_of(%WorkflowActivationCompletion{
         status: {:successful, %Success{commands: commands}}
       }),
       do: commands

  defp acts(completion) do
    for %WorkflowCommand{variant: {:schedule_activity, sa}} <- commands_of(completion),
        do: {sa.seq, sa.activity_type}
  end

  defp markers(completion) do
    for %WorkflowCommand{variant: {:set_patch_marker, m}} <- commands_of(completion),
        do: m.patch_id
  end

  defp value(completion) do
    [cwe] =
      for %WorkflowCommand{variant: {:complete_workflow_execution, c}} <- commands_of(completion),
          do: c

    Jason.decode!(cwe.result.data)
  end

  defp fresh(run_id), do: State.new(run_id, "tq")

  # ---------------------------------------------------------------------------
  # Where in the history the patch is first reached
  # ---------------------------------------------------------------------------

  test "a patch first reached on activation 2, notified on activation 2, answers true" do
    s0 = fresh("adv-a")

    # Activation 1 (replaying): the body suspends at A1 before reaching the
    # patch, so nothing is memoised.
    {:ok, c1, s1} =
      Evaluator.evaluate(LateReach, activation([init_job(%{})], is_replaying: true), s0)

    assert [{1, _first_type}] = acts(c1)
    assert markers(c1) == []
    assert s1.patch_answers == %{}

    # Activation 2 (still replaying) carries both the resolution and the
    # peeked marker.
    {:ok, c2, s2} =
      Evaluator.evaluate(
        LateReach,
        activation([resolve_job(1, %{"ok" => 1}), notify_job("late")], is_replaying: true),
        s1
      )

    assert markers(c2) == ["late"]
    assert s2.patch_answers == %{"late" => true}
    assert [{3, type}] = acts(c2)
    assert type == Atom.to_string(A2)
  end

  test "A': the same shape with no marker in history takes the old branch" do
    s0 = fresh("adv-a2")

    {:ok, _c1, s1} =
      Evaluator.evaluate(LateReach, activation([init_job(%{})], is_replaying: true), s0)

    {:ok, c2, s2} =
      Evaluator.evaluate(
        LateReach,
        activation([resolve_job(1, %{"ok" => 1})], is_replaying: true),
        s1
      )

    assert markers(c2) == []
    assert s2.patch_answers == %{"late" => false}
    assert [{2, type}] = acts(c2)
    assert type == Atom.to_string(A3)
  end

  # ---------------------------------------------------------------------------
  # Async scopes
  # ---------------------------------------------------------------------------

  test "a patch asked inside two async scopes issues one marker and stable ids" do
    s0 = fresh("adv-b")
    {:ok, c1, s1} = Evaluator.evaluate(InAsync, activation([init_job(%{})]), s0)

    assert markers(c1) == ["p"]
    first = acts(c1)

    # Re-evaluate the SAME activation against the SAME prior state: pure.
    {:ok, c1b, _repeat_state} = Evaluator.evaluate(InAsync, activation([init_job(%{})]), s0)
    assert commands_of(c1) == commands_of(c1b)

    # One marker, then both scopes' activities: the patch is per execution,
    # not per scope, so the second scope reuses the answer without a command.
    assert [{2, _a1}, {3, _a2}] = first
    assert s1.command_id_to_seq == %{{[0], 1} => 1, {[0], 2} => 2, {[1], 2} => 3}

    # Resolving one of them issues nothing new — and no second marker.
    {:ok, c2, s2} = Evaluator.evaluate(InAsync, activation([resolve_job(2, %{"r" => 1})]), s1)
    assert markers(c2) == []
    assert acts(c2) == []
    assert s2.command_id_to_seq == s1.command_id_to_seq
  end

  # ---------------------------------------------------------------------------
  # Several patches, eviction, ids, terminal finishers, purity
  # ---------------------------------------------------------------------------

  test "two patch ids in one body each get a marker, in call order" do
    s0 = fresh("adv-c")
    {:ok, c1, s1} = Evaluator.evaluate(TwoPatches, activation([init_job(%{})]), s0)

    assert markers(c1) == ["alpha", "beta"]
    assert [{3, _act_type}] = acts(c1)
    assert s1.patch_answers == %{"alpha" => true, "beta" => true}

    {:ok, c2, _final_state} =
      Evaluator.evaluate(TwoPatches, activation([resolve_job(3, %{"r" => 1})]), s1)

    assert markers(c2) == []
    assert value(c2) == %{"alpha" => true, "beta" => true, "a" => %{"r" => 1}}
  end

  test "an eviction drops the memo, and the rebuilt run re-learns from history" do
    s0 = fresh("adv-d")
    {:ok, _c1, s1} = Evaluator.evaluate(TwoPatches, activation([init_job(%{})]), s0)
    assert s1.patch_answers != %{}

    {:ok, _evict, s2} = Evaluator.evaluate(TwoPatches, activation([evict_job()]), s1)
    assert s2.result == :evict

    # The worker deletes the cache entry on :evict, so the next activation
    # starts from State.new/2 and replays the whole history.
    rebuilt = fresh("adv-d")

    {:ok, c3, s3} =
      Evaluator.evaluate(
        TwoPatches,
        activation([init_job(%{}), notify_job("alpha"), notify_job("beta")], is_replaying: true),
        rebuilt
      )

    assert markers(c3) == ["alpha", "beta"]
    assert s3.patch_answers == %{"alpha" => true, "beta" => true}
  end

  for {label, id} <- [
        {"japanese", "日本語のパッチ"},
        {"emoji", "🚀-step"},
        {"rtl", "ملاحظات"},
        {"combining", "café́"},
        {"zwj", "a‍b"},
        {"long", String.duplicate("p", 4096)},
        {"newline", "a\nb"},
        {"nul-ish", "a b"}
      ] do
    test "a #{label} patch id survives the marker round trip unchanged" do
      id = unquote(id)

      {:ok, completion, _s} =
        Evaluator.evaluate(OddId, activation([init_job(%{"id" => id})]), fresh("adv-e"))

      assert markers(completion) == [id]
      assert value(completion) == %{"answer" => true}

      # And it survives protobuf encode/decode, which is where a non-UTF-8
      # byte or a length assumption would show up.
      encoded = Protobuf.encode(completion)
      decoded = WorkflowActivationCompletion.decode(encoded)
      assert markers(decoded) == [id]
    end

    test "a #{label} patch id is recognised when notified back" do
      id = unquote(id)

      {:ok, completion, _s} =
        Evaluator.evaluate(
          OddId,
          activation([init_job(%{"id" => id}), notify_job(id)], is_replaying: true),
          fresh("adv-e2")
        )

      assert value(completion) == %{"answer" => true}
    end
  end

  test "a non-atom, non-binary patch id parks rather than silently stringifying" do
    {:ok, completion, _s} = Evaluator.evaluate(BadId, activation([init_job(%{})]), fresh("adv-f"))
    assert %WorkflowActivationCompletion{status: {:failed, _f}} = completion
  end

  defmodule PatchThenContinue do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      if patched?(:can), do: continue_as_new(%{"n" => 2}), else: %{"old" => true}
    end
  end

  defmodule PatchThenFail do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      if patched?(:boom), do: fail("Boom", "patched branch failed"), else: %{"old" => true}
    end
  end

  test "a patch decided before continue_as_new is recorded ahead of it" do
    {:ok, completion, state} =
      Evaluator.evaluate(PatchThenContinue, activation([init_job(%{})]), fresh("adv-h1"))

    assert [
             %WorkflowCommand{variant: {:set_patch_marker, _m}},
             %WorkflowCommand{variant: {:continue_as_new_workflow_execution, _c}}
           ] = commands_of(completion)

    assert state.result == :continue_as_new
  end

  test "a patch decided before fail/2 is recorded ahead of it" do
    {:ok, completion, state} =
      Evaluator.evaluate(PatchThenFail, activation([init_job(%{})]), fresh("adv-h2"))

    assert [
             %WorkflowCommand{variant: {:set_patch_marker, _m}},
             %WorkflowCommand{variant: {:fail_workflow_execution, _f}}
           ] = commands_of(completion)

    # A redelivery after the terminal outcome re-emits the terminal command
    # and does not re-run the body.
    {:ok, again, _s} = Evaluator.evaluate(PatchThenFail, activation([]), state)
    assert [%WorkflowCommand{variant: {:fail_workflow_execution, _f2}}] = commands_of(again)
  end

  test "a patch answer does not leak between runs evaluated on one process" do
    # The evaluator runs the body inline on the calling process, so a second
    # run on the same process must not inherit the first's decisions.
    {:ok, first, _s1} =
      Evaluator.evaluate(OddId, activation([init_job(%{"id" => "shared"})]), fresh("adv-h3a"))

    assert value(first) == %{"answer" => true}

    {:ok, second, s2} =
      Evaluator.evaluate(
        OddId,
        activation([init_job(%{"id" => "shared"})], is_replaying: true),
        fresh("adv-h3b")
      )

    assert value(second) == %{"answer" => false}
    assert markers(second) == []
    assert s2.patch_answers == %{}
  end

  test "evaluating the same activation against the same state is byte-identical" do
    for id <- ["a", "日本語", "🚀"], replaying <- [true, false], notified <- [true, false] do
      jobs =
        [init_job(%{"id" => id})] ++ if notified, do: [notify_job(id)], else: []

      act = activation(jobs, is_replaying: replaying)
      {:ok, c1, s1} = Evaluator.evaluate(OddId, act, fresh("adv-g"))
      {:ok, c2, s2} = Evaluator.evaluate(OddId, act, fresh("adv-g"))

      assert Protobuf.encode(c1) == Protobuf.encode(c2)
      assert s1.patch_answers == s2.patch_answers
      assert s1.notified_patches == s2.notified_patches
    end
  end
end
