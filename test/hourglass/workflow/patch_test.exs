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
    [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] = commands_of(completion)
    Jason.decode!(cwe.result.data)
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
end
