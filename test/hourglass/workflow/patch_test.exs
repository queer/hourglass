defmodule Hourglass.Workflow.PatchTest do
  # async: true — the pure-function evaluator owns no global resources; each
  # test builds its own activations + run_id.
  use ExUnit.Case, async: true

  alias Coresdk.WorkflowCommands.WorkflowCommand
  alias Coresdk.WorkflowCompletion.Success
  alias Coresdk.WorkflowCompletion.WorkflowActivationCompletion
  alias Hourglass.Workflow.Evaluator
  alias Hourglass.Workflow.State

  # The unhandled-job-variant path logs at :info; a malformed job is expected
  # to travel it rather than crash.
  @moduletag capture_log: true

  # ---------------------------------------------------------------------------
  # Stub activity modules — minimal shims so execute_activity/2,3 can call
  # __activity_input_type__ / __activity_output_type__ without a full
  # `use Hourglass.Activity` definition. Same shape as EvaluatorTest's.
  # ---------------------------------------------------------------------------

  defmodule MainAct do
    def __activity_input_type__, do: :map
    def __activity_output_type__, do: :map
    def __activity_retry_policy__, do: []
  end

  defmodule ExtraAct do
    def __activity_input_type__, do: :map
    def __activity_output_type__, do: :map
    def __activity_retry_policy__, do: []
  end

  defmodule OtherAct do
    def __activity_input_type__, do: :map
    def __activity_output_type__, do: :map
    def __activity_retry_policy__, do: []
  end

  # ---------------------------------------------------------------------------
  # Fixtures
  # ---------------------------------------------------------------------------

  defmodule PatchReadingWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      patches =
        Hourglass.Workflow.__notified_patches__()
        |> Map.keys()
        |> Enum.sort()

      %{"patches" => patches}
    end
  end

  defmodule RendezvousPatchWorkflow do
    use Hourglass.Workflow

    # Blocks the body until the test process has seen every concurrent
    # evaluation arrive, so they are provably in flight at the same instant —
    # the window in which a shared (rather than per-execution) patch set would
    # leak from one run into another. `Process.sleep/1` is banned inside a
    # workflow body by the determinism lint, and would only make the overlap
    # likely rather than certain in any case.
    @impl Hourglass.Workflow.Behaviour
    def run(%{"test_pid" => encoded}) do
      test_pid =
        encoded
        |> String.to_charlist()
        |> :erlang.list_to_pid()

      send(test_pid, {:arrived, self()})

      receive do
        :proceed -> :ok
      after
        5_000 -> raise "rendezvous timed out"
      end

      patches =
        Hourglass.Workflow.__notified_patches__()
        |> Map.keys()
        |> Enum.sort()

      %{"patches" => patches}
    end
  end

  # The body change this epic exists to make survivable: an extra step in
  # front of the one the pre-patch body already had, guarded by a patch.
  defmodule PatchGuardedWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(input) do
      if patched?(:extra_step) do
        %{"branch" => "new", "value" => execute_activity!(ExtraAct, input)}
      else
        %{"branch" => "old", "value" => execute_activity!(MainAct, input)}
      end
    end
  end

  # The pre-patch body PatchGuardedWorkflow was grown from — the baseline the
  # "issues exactly the commands it issued before" claim is measured against.
  defmodule PrePatchWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(input), do: %{"branch" => "old", "value" => execute_activity!(MainAct, input)}
  end

  defmodule DoubleAskWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(input) do
      first = patched?(:twice)
      a = execute_activity!(MainAct, input)
      second = patched?(:twice)
      b = execute_activity!(OtherAct, input)
      %{"first" => first, "second" => second, "a" => a, "b" => b}
    end
  end

  defmodule StringIdWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      %{"atom" => patched?(:extra_step), "string" => patched?("extra_step")}
    end
  end

  defmodule EmptyIdWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input), do: %{"patched" => patched?("")}
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp activation(jobs, opts \\ []) do
    %{
      timestamp: Keyword.get(opts, :timestamp, DateTime.utc_now()),
      is_replaying: Keyword.get(opts, :is_replaying, false),
      jobs: jobs,
      run_id: Keyword.get(opts, :run_id, "test-run")
    }
  end

  defp init_job(input) do
    %{
      variant:
        {:initialize_workflow,
         %Coresdk.WorkflowActivation.InitializeWorkflow{
           workflow_type: "TestWf",
           workflow_id: "wf-1",
           arguments: [synthetic_payload(input)]
         }}
    }
  end

  # The job Core sends pre-emptively when a history it is replaying records a
  # patch marker. Nothing in this SDK produced one before this epic.
  defp notify_has_patch_job(patch_id) do
    %{
      variant: {:notify_has_patch, %Coresdk.WorkflowActivation.NotifyHasPatch{patch_id: patch_id}}
    }
  end

  defp synthetic_payload(data) do
    %Temporal.Api.Common.V1.Payload{
      metadata: %{"encoding" => "json/plain"},
      data: Jason.encode!(data)
    }
  end

  defp commands_of(%WorkflowActivationCompletion{
         status: {:successful, %Success{commands: commands}}
       }),
       do: commands

  defp completed_value(completion) do
    [cwe] =
      for %WorkflowCommand{variant: {:complete_workflow_execution, cwe}} <-
            commands_of(completion),
          do: cwe

    Jason.decode!(cwe.result.data)
  end

  defp resolve_job(seq, value) do
    resolution = %Coresdk.ActivityResult.ActivityResolution{
      status: {:completed, %Coresdk.ActivityResult.Success{result: synthetic_payload(value)}}
    }

    %{
      variant:
        {:resolve_activity,
         %Coresdk.WorkflowActivation.ResolveActivity{seq: seq, result: resolution}}
    }
  end

  defp patch_markers(completion) do
    for %WorkflowCommand{variant: {:set_patch_marker, marker}} <- commands_of(completion),
        do: marker
  end

  defp scheduled_activities(completion) do
    for %WorkflowCommand{variant: {:schedule_activity, sa}} <- commands_of(completion), do: sa
  end

  defp fresh_state(run_id), do: State.new(run_id, "test-tq")

  # ---------------------------------------------------------------------------
  # An execution knows which patches its history records
  # ---------------------------------------------------------------------------

  test "a NotifyHasPatch job leaves the execution knowing that patch id" do
    {:ok, _completion, state} =
      Evaluator.evaluate(
        PatchReadingWorkflow,
        activation([init_job(nil), notify_has_patch_job("add-step")]),
        fresh_state("r-notify-1")
      )

    assert state.notified_patches == %{"add-step" => true}
  end

  test "an activation carrying no NotifyHasPatch job leaves the execution knowing none" do
    {:ok, _completion, state} =
      Evaluator.evaluate(
        PatchReadingWorkflow,
        activation([init_job(nil)]),
        fresh_state("r-notify-2")
      )

    assert state.notified_patches == %{}
  end

  test "the patch set is readable from inside the workflow body" do
    {:ok, completion, _state} =
      Evaluator.evaluate(
        PatchReadingWorkflow,
        activation([init_job(nil), notify_has_patch_job("beta"), notify_has_patch_job("alpha")]),
        fresh_state("r-notify-3")
      )

    assert completed_value(completion) == %{"patches" => ["alpha", "beta"]}
  end

  test "a patch notified on activation N is still known at activation N+1" do
    state0 = fresh_state("r-notify-4")

    {:ok, _c1, state1} =
      Evaluator.evaluate(
        PatchReadingWorkflow,
        activation([init_job(nil), notify_has_patch_job("add-step")]),
        state0
      )

    # Second activation carries no NotifyHasPatch job at all.
    {:ok, completion2, state2} = Evaluator.evaluate(PatchReadingWorkflow, activation([]), state1)

    assert state2.notified_patches == %{"add-step" => true}
    assert completed_value(completion2) == %{"patches" => ["add-step"]}
  end

  test "patches accumulate across activations rather than replacing each other" do
    state0 = fresh_state("r-notify-5")

    {:ok, _c1, state1} =
      Evaluator.evaluate(
        PatchReadingWorkflow,
        activation([init_job(nil), notify_has_patch_job("first")]),
        state0
      )

    {:ok, _c2, state2} =
      Evaluator.evaluate(
        PatchReadingWorkflow,
        activation([notify_has_patch_job("second")]),
        state1
      )

    assert state2.notified_patches == %{"first" => true, "second" => true}
  end

  test "two concurrently-evaluating workflows do not share or leak patch sets" do
    encoded_pid =
      self()
      |> :erlang.pid_to_list()
      |> List.to_string()

    runs = [{"r-concurrent-a", "patch-a"}, {"r-concurrent-b", "patch-b"}]

    tasks =
      Enum.map(runs, fn {run_id, patch_id} ->
        Task.async(fn ->
          {:ok, completion, state} =
            Evaluator.evaluate(
              RendezvousPatchWorkflow,
              activation(
                [
                  init_job(%{"test_pid" => encoded_pid}),
                  notify_has_patch_job(patch_id)
                ],
                run_id: run_id
              ),
              fresh_state(run_id)
            )

          {patch_id, completed_value(completion), state.notified_patches}
        end)
      end)

    arrived =
      Enum.map(runs, fn _run ->
        receive do
          {:arrived, pid} -> pid
        after
          5_000 -> flunk("a workflow body never reached the rendezvous")
        end
      end)

    Enum.each(arrived, &send(&1, :proceed))

    assert [
             {"patch-a", %{"patches" => ["patch-a"]}, set_a},
             {"patch-b", %{"patches" => ["patch-b"]}, set_b}
           ] = Task.await_many(tasks, 5_000)

    assert set_a == %{"patch-a" => true}
    assert set_b == %{"patch-b" => true}
  end

  test "a NotifyHasPatch job carrying an empty patch id records no patch" do
    {:ok, completion, state} =
      Evaluator.evaluate(
        PatchReadingWorkflow,
        activation([init_job(nil), notify_has_patch_job("")]),
        fresh_state("r-notify-empty")
      )

    assert state.notified_patches == %{}
    assert completed_value(completion) == %{"patches" => []}
  end

  test "a malformed notify_has_patch payload does not crash the evaluator" do
    malformed = %{variant: {:notify_has_patch, :not_a_struct}}

    {:ok, completion, state} =
      Evaluator.evaluate(
        PatchReadingWorkflow,
        activation([init_job(nil), malformed]),
        fresh_state("r-notify-malformed")
      )

    assert state.notified_patches == %{}
    assert completed_value(completion) == %{"patches" => []}
  end

  test "an unknown job variant does not crash the evaluator" do
    unknown = %{variant: {:some_future_job, %{whatever: true}}}

    {:ok, completion, state} =
      Evaluator.evaluate(
        PatchReadingWorkflow,
        activation([init_job(nil), unknown]),
        fresh_state("r-notify-unknown")
      )

    assert state.notified_patches == %{}
    assert completed_value(completion) == %{"patches" => []}
  end

  # ---------------------------------------------------------------------------
  # patched?/1 — a patched region is taken or skipped by replay
  # ---------------------------------------------------------------------------

  test "patched?/1 is true on an execution running the patched body for the first time" do
    {:ok, completion, state} =
      Evaluator.evaluate(
        PatchGuardedWorkflow,
        activation([init_job(%{"n" => 1})], is_replaying: false),
        fresh_state("r-patch-new")
      )

    assert [%Coresdk.WorkflowCommands.SetPatchMarker{patch_id: "extra_step", deprecated: false}] =
             patch_markers(completion)

    # The guarded branch is the one that ran: ExtraAct, not MainAct.
    assert [%{activity_type: activity_type}] = scheduled_activities(completion)
    assert activity_type == Atom.to_string(ExtraAct)

    assert state.patch_answers == %{"extra_step" => true}
  end

  test "patched?/1 is false when replaying a history recorded before the patch existed" do
    {:ok, completion, state} =
      Evaluator.evaluate(
        PatchGuardedWorkflow,
        activation([init_job(%{"n" => 1})], is_replaying: true),
        fresh_state("r-patch-old")
      )

    assert patch_markers(completion) == []

    assert [%{activity_type: activity_type}] = scheduled_activities(completion)
    assert activity_type == Atom.to_string(MainAct)

    assert state.patch_answers == %{"extra_step" => false}
  end

  test "patched?/1 is true when replaying a history that records the marker" do
    {:ok, completion, _state} =
      Evaluator.evaluate(
        PatchGuardedWorkflow,
        activation([init_job(%{"n" => 1}), notify_has_patch_job("extra_step")],
          is_replaying: true
        ),
        fresh_state("r-patch-replay-marked")
      )

    assert [%Coresdk.WorkflowCommands.SetPatchMarker{patch_id: "extra_step"}] =
             patch_markers(completion)

    assert [%{activity_type: activity_type}] = scheduled_activities(completion)
    assert activity_type == Atom.to_string(ExtraAct)
  end

  test "a patch answered false while replaying stays false once the execution catches up" do
    state0 = fresh_state("r-patch-catchup")

    {:ok, completion1, state1} =
      Evaluator.evaluate(
        PatchGuardedWorkflow,
        activation([init_job(%{"n" => 1})], is_replaying: true),
        state0
      )

    assert [%{seq: seq}] = scheduled_activities(completion1)

    # The execution has caught up: this activation is live, not replay. The
    # body re-executes from the top and reaches patched?/1 again — the moment
    # a recomputed-per-activation answer would flip to true, take the new
    # branch, and diverge from every command already issued.
    {:ok, completion2, state2} =
      Evaluator.evaluate(
        PatchGuardedWorkflow,
        activation([resolve_job(seq, %{"echo" => true})], is_replaying: false),
        state1
      )

    assert patch_markers(completion2) == []
    assert state2.patch_answers == %{"extra_step" => false}
    assert completed_value(completion2) == %{"branch" => "old", "value" => %{"echo" => true}}
  end

  test "asking the same patch id twice in one execution gives the same answer both times" do
    state0 = fresh_state("r-patch-twice")

    {:ok, completion1, state1} =
      Evaluator.evaluate(DoubleAskWorkflow, activation([init_job(%{})]), state0)

    # Exactly one marker, even though the body asks twice.
    assert [%Coresdk.WorkflowCommands.SetPatchMarker{patch_id: "twice"}] =
             patch_markers(completion1)

    assert [%{seq: main_seq}] = scheduled_activities(completion1)

    {:ok, completion2, state2} =
      Evaluator.evaluate(
        DoubleAskWorkflow,
        activation([resolve_job(main_seq, %{"a" => 1})]),
        state1
      )

    # Second activation: both asks are answered from the run's own state, so
    # no further marker is issued.
    assert patch_markers(completion2) == []
    assert [%{seq: other_seq}] = scheduled_activities(completion2)

    {:ok, completion3, _state3} =
      Evaluator.evaluate(
        DoubleAskWorkflow,
        activation([resolve_job(other_seq, %{"b" => 2})]),
        state2
      )

    assert completed_value(completion3) == %{
             "first" => true,
             "second" => true,
             "a" => %{"a" => 1},
             "b" => %{"b" => 2}
           }
  end

  test "a workflow that never calls patched?/1 issues the command sequence it issued before" do
    {:ok, completion, _state} =
      Evaluator.evaluate(
        PrePatchWorkflow,
        activation([init_job(%{"n" => 1})]),
        fresh_state("r-a")
      )

    assert [%{seq: 1, activity_id: "1", activity_type: activity_type}] =
             scheduled_activities(completion)

    assert activity_type == Atom.to_string(MainAct)
    assert patch_markers(completion) == []
  end

  test "a patch answered false costs the wire nothing — the old branch's seqs are unchanged" do
    {:ok, pre_patch, _pre_state} =
      Evaluator.evaluate(
        PrePatchWorkflow,
        activation([init_job(%{"n" => 1})], is_replaying: true),
        fresh_state("r-b")
      )

    {:ok, guarded, _guarded_state} =
      Evaluator.evaluate(
        PatchGuardedWorkflow,
        activation([init_job(%{"n" => 1})], is_replaying: true),
        fresh_state("r-b")
      )

    assert commands_of(pre_patch) == commands_of(guarded)
  end

  test "patched?/1 accepts an atom or a string for the same patch id" do
    {:ok, completion, _state} =
      Evaluator.evaluate(
        StringIdWorkflow,
        activation([init_job(%{})], is_replaying: true),
        %{fresh_state("r-patch-string") | notified_patches: %{"extra_step" => true}}
      )

    assert completed_value(completion) == %{"atom" => true, "string" => true}

    # One patch, asked twice under two spellings — one marker.
    assert [%Coresdk.WorkflowCommands.SetPatchMarker{patch_id: "extra_step"}] =
             patch_markers(completion)
  end

  test "a patch decided on the activation that ends the run still reaches history" do
    # The marker is the record of which branch ran. A run that decides and
    # then completes in the same activation would otherwise write a history
    # nothing can tell the two branches apart from.
    {:ok, completion, _state} =
      Evaluator.evaluate(
        StringIdWorkflow,
        activation([init_job(%{})], is_replaying: false),
        fresh_state("r-patch-terminal")
      )

    assert [
             %WorkflowCommand{variant: {:set_patch_marker, _marker}},
             %WorkflowCommand{variant: {:complete_workflow_execution, _cwe}}
           ] = commands_of(completion)
  end

  test "patched?/1 refuses an empty patch id" do
    {:ok, completion, _state} =
      Evaluator.evaluate(
        EmptyIdWorkflow,
        activation([init_job(%{})]),
        fresh_state("r-patch-empty")
      )

    assert %WorkflowActivationCompletion{status: {:failed, _failure}} = completion
  end

  test "patched?/1 raises outside a workflow evaluator" do
    assert_raise RuntimeError, ~r/outside a workflow evaluator/, fn ->
      Hourglass.Workflow.patched?(:nope)
    end
  end
end
