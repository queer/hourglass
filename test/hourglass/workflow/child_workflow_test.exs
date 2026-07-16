defmodule Hourglass.Workflow.ChildWorkflowTest do
  # async: true — the pure-function evaluator owns no global resources; each
  # test builds its own activations + run_id.
  use ExUnit.Case, async: true

  alias Coresdk.WorkflowCommands.WorkflowCommand
  # Failure is needed for the raise-on-bad-:id assertion
  alias Coresdk.WorkflowCompletion.Failure
  alias Coresdk.WorkflowCompletion.Success
  alias Coresdk.WorkflowCompletion.WorkflowActivationCompletion
  alias Hourglass.Workflow.Evaluator
  alias Hourglass.Workflow.State

  @moduletag capture_log: true

  # A stub child workflow: the evaluator only reads the type callbacks off the
  # module, so this needs no `use Hourglass.Workflow` and no `run/1`. Mirrors
  # the stub-activity convention in evaluator_test.exs.
  defmodule MyChild do
    def __workflow_input_type__, do: :map
    def __workflow_output_type__, do: :map
  end

  defmodule SingleChild do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(input) do
      result = execute_child(MyChild, input)
      {:ok, result}
    end
  end

  # -- activation fabrication (mirrors test/hourglass/workflow/evaluator_test.exs) --

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

  defp child_started_job(seq, run_id) do
    %{
      variant:
        {:resolve_child_workflow_execution_start,
         %Coresdk.WorkflowActivation.ResolveChildWorkflowExecutionStart{
           seq: seq,
           status:
             {:succeeded,
              %Coresdk.WorkflowActivation.ResolveChildWorkflowExecutionStartSuccess{
                run_id: run_id
              }}
         }}
    }
  end

  defp child_completed_job(seq, value) do
    %{
      variant:
        {:resolve_child_workflow_execution,
         %Coresdk.WorkflowActivation.ResolveChildWorkflowExecution{
           seq: seq,
           result: %Coresdk.ChildWorkflow.ChildWorkflowResult{
             status:
               {:completed, %Coresdk.ChildWorkflow.Success{result: synthetic_payload(value)}}
           }
         }}
    }
  end

  defp synthetic_payload(data) do
    %Temporal.Api.Common.V1.Payload{
      metadata: %{"encoding" => "json/plain"},
      data: Jason.encode!(data)
    }
  end

  defp child_start_failed_job(seq) do
    %{
      variant:
        {:resolve_child_workflow_execution_start,
         %Coresdk.WorkflowActivation.ResolveChildWorkflowExecutionStart{
           seq: seq,
           status:
             {:failed,
              %Coresdk.WorkflowActivation.ResolveChildWorkflowExecutionStartFailure{
                workflow_id: "dupe-id",
                workflow_type: "MyChild",
                cause: :START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS
              }}
         }}
    }
  end

  defp child_start_cancelled_job(seq) do
    %{
      variant:
        {:resolve_child_workflow_execution_start,
         %Coresdk.WorkflowActivation.ResolveChildWorkflowExecutionStart{
           seq: seq,
           status:
             {:cancelled,
              %Coresdk.WorkflowActivation.ResolveChildWorkflowExecutionStartCancelled{
                failure: %Temporal.Api.Failure.V1.Failure{message: "start cancelled"}
              }}
         }}
    }
  end

  defp child_failed_job(seq, message) do
    %{
      variant:
        {:resolve_child_workflow_execution,
         %Coresdk.WorkflowActivation.ResolveChildWorkflowExecution{
           seq: seq,
           result: %Coresdk.ChildWorkflow.ChildWorkflowResult{
             status:
               {:failed,
                %Coresdk.ChildWorkflow.Failure{
                  failure: %Temporal.Api.Failure.V1.Failure{message: message}
                }}
           }
         }}
    }
  end

  defp commands_of(%WorkflowActivationCompletion{
         status: {:successful, %Success{commands: commands}}
       }),
       do: commands

  defp fresh_state(run_id), do: State.new(run_id, "test-tq")

  # -- tests --

  test "execute_child issues one StartChildWorkflowExecution and suspends" do
    state0 = fresh_state("r1")

    {:ok, completion, state1} =
      Evaluator.evaluate(SingleChild, activation([init_job(%{"msg" => "hi"})]), state0)

    assert [%WorkflowCommand{variant: {:start_child_workflow_execution, sc}}] =
             commands_of(completion)

    assert sc.seq == 1
    assert sc.workflow_type == Atom.to_string(MyChild)
    assert sc.task_queue == "test-tq"
    assert sc.namespace == ""
    assert sc.parent_close_policy == :PARENT_CLOSE_POLICY_TERMINATE
    assert sc.workflow_id_reuse_policy == :WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
    assert [%Temporal.Api.Common.V1.Payload{data: data}] = sc.input
    assert Jason.decode!(data) == %{"msg" => "hi"}

    # Derived id: non-empty, and NOT the parent's workflow id.
    assert is_binary(sc.workflow_id) and sc.workflow_id != ""
    refute sc.workflow_id == "wf-1"

    assert state1.next_global_seq == 2
    assert Map.has_key?(state1.pending_resolvers, 1)
  end

  test "start resolution alone does not complete execute_child; the result does" do
    state0 = fresh_state("r2")

    {:ok, completion1, state1} =
      Evaluator.evaluate(SingleChild, activation([init_job(%{"msg" => "hi"})]), state0)

    assert [%WorkflowCommand{variant: {:start_child_workflow_execution, sc}}] =
             commands_of(completion1)

    # Phase 1: started — the child is running, so the body must STILL suspend.
    {:ok, completion2, state2} =
      Evaluator.evaluate(
        SingleChild,
        activation([child_started_job(sc.seq, "child-run-1")]),
        state1
      )

    assert commands_of(completion2) == []
    assert state2.child_starts[sc.seq] == {:started, "child-run-1"}
    assert state2.result == nil

    # Phase 2: result — now the body completes.
    {:ok, completion3, state3} =
      Evaluator.evaluate(
        SingleChild,
        activation([child_completed_job(sc.seq, %{"msg" => "done"})]),
        state2
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, _cwe}}] =
             commands_of(completion3)

    assert {:completed, {:ok, {:ok, %{"msg" => "done"}}}} = state3.result
  end

  test "explicit :id overrides the derived child workflow id" do
    defmodule PinnedChild do
      use Hourglass.Workflow

      @impl Hourglass.Workflow.Behaviour
      def run(_input), do: execute_child(MyChild, %{}, id: "pinned-id")
    end

    {:ok, completion, _state} =
      Evaluator.evaluate(PinnedChild, activation([init_job(%{})]), fresh_state("r3"))

    assert [%WorkflowCommand{variant: {:start_child_workflow_execution, sc}}] =
             commands_of(completion)

    assert sc.workflow_id == "pinned-id"
  end

  test "derived child id is stable across re-execution but differs across run_ids" do
    command_id = {[], 1}

    assert Evaluator.derive_child_id("run-a", command_id) ==
             Evaluator.derive_child_id("run-a", command_id)

    refute Evaluator.derive_child_id("run-a", command_id) ==
             Evaluator.derive_child_id("run-b", command_id)
  end

  test "opts map onto the proto: task_queue, policies, timeouts, retry_policy" do
    defmodule OptsChild do
      use Hourglass.Workflow

      @impl Hourglass.Workflow.Behaviour
      def run(_input) do
        execute_child(MyChild, %{},
          task_queue: "other-tq",
          parent_close_policy: :abandon,
          workflow_id_reuse_policy: :reject_duplicate,
          workflow_execution_timeout: 60_000,
          workflow_run_timeout: 30_000,
          workflow_task_timeout: 10_000,
          retry_policy: [max_attempts: 5]
        )
      end
    end

    {:ok, completion, _state} =
      Evaluator.evaluate(OptsChild, activation([init_job(%{})]), fresh_state("r4"))

    assert [%WorkflowCommand{variant: {:start_child_workflow_execution, sc}}] =
             commands_of(completion)

    assert sc.task_queue == "other-tq"
    assert sc.parent_close_policy == :PARENT_CLOSE_POLICY_ABANDON
    assert sc.workflow_id_reuse_policy == :WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
    assert sc.workflow_execution_timeout == %Google.Protobuf.Duration{seconds: 60, nanos: 0}
    assert sc.workflow_run_timeout == %Google.Protobuf.Duration{seconds: 30, nanos: 0}
    assert sc.workflow_task_timeout == %Google.Protobuf.Duration{seconds: 10, nanos: 0}
    assert sc.retry_policy.maximum_attempts == 5
  end

  test "fan-out: two children via async/await_all issue two commands" do
    defmodule FanOutChild do
      use Hourglass.Workflow

      @impl Hourglass.Workflow.Behaviour
      def run(_input) do
        [a, b] =
          await_all([
            async(fn -> execute_child(MyChild, %{"n" => 1}) end),
            async(fn -> execute_child(MyChild, %{"n" => 2}) end)
          ])

        {a, b}
      end
    end

    {:ok, completion, _state} =
      Evaluator.evaluate(FanOutChild, activation([init_job(%{})]), fresh_state("r5"))

    commands = commands_of(completion)
    assert length(commands) == 2

    seqs =
      Enum.map(commands, fn %WorkflowCommand{variant: {:start_child_workflow_execution, sc}} ->
        sc.seq
      end)

    assert Enum.sort(seqs) == [1, 2]

    ids =
      Enum.map(commands, fn %WorkflowCommand{variant: {:start_child_workflow_execution, sc}} ->
        sc.workflow_id
      end)

    # Distinct command_ids ⇒ distinct derived child ids (no collision).
    assert length(Enum.uniq(ids)) == 2
  end

  # -------------------------------------------------------------------------
  # encode_input_payloads/1 fallback branches (child path) — mirrors the
  # activity-path tests in evaluator_test.exs, pinning the shared helper's
  # behaviour for both callers.
  # -------------------------------------------------------------------------

  test "execute_child with nil args encodes StartChildWorkflowExecution.input as []" do
    defmodule NilArgsChild do
      use Hourglass.Workflow

      @impl Hourglass.Workflow.Behaviour
      def run(_input), do: execute_child(MyChild, nil)
    end

    {:ok, completion, _state} =
      Evaluator.evaluate(NilArgsChild, activation([init_job(%{})]), fresh_state("r-nilargs"))

    assert [%WorkflowCommand{variant: {:start_child_workflow_execution, sc}}] =
             commands_of(completion)

    assert sc.input == []
  end

  test "execute_child with non-JSON-encodable args falls back to elixir/inspect encoding" do
    defmodule NonJsonArgsChild do
      use Hourglass.Workflow

      @impl Hourglass.Workflow.Behaviour
      def run(_input), do: execute_child(MyChild, {:not, :json, :encodable})
    end

    {:ok, completion, _state} =
      Evaluator.evaluate(
        NonJsonArgsChild,
        activation([init_job(%{})]),
        fresh_state("r-inspectargs")
      )

    assert [%WorkflowCommand{variant: {:start_child_workflow_execution, sc}}] =
             commands_of(completion)

    assert [
             %Temporal.Api.Common.V1.Payload{
               metadata: %{"encoding" => "elixir/inspect"},
               data: data
             }
           ] = sc.input

    assert data == inspect({:not, :json, :encodable})
  end

  # -------------------------------------------------------------------------
  # child_workflow_id/2: a present-but-non-binary :id must raise, not silently
  # fall back to a derived id.
  # -------------------------------------------------------------------------

  test "execute_child with a present but non-binary :id raises ArgumentError naming the value" do
    defmodule BadIdChild do
      use Hourglass.Workflow

      @impl Hourglass.Workflow.Behaviour
      def run(_input), do: execute_child(MyChild, %{}, id: 123)
    end

    {:ok, completion, state1} =
      Evaluator.evaluate(BadIdChild, activation([init_job(%{})]), fresh_state("r-bad-id"))

    # The raise happens inside run/1, so the evaluator's :error/:exit catch
    # parks the workflow as a workflow-task failure rather than propagating —
    # mirrors evaluator_test.exs's "raise inside run/1 parks the workflow" test.
    assert {:failed, %Failure{failure: %Temporal.Api.Failure.V1.Failure{message: msg}}} =
             completion.status

    assert msg =~ "ArgumentError"
    assert msg =~ "123"
    refute match?({:completed, _}, state1.result)
  end

  test "start failure resolves execute_child immediately — it does NOT wait for a result" do
    # The regression this design exists to prevent: a child that fails to start
    # gets a start resolution and NO result resolution, ever. A body that awaited
    # only the result would hang forever on a duplicate workflow id.
    state0 = fresh_state("r6")

    {:ok, completion1, state1} =
      Evaluator.evaluate(SingleChild, activation([init_job(%{})]), state0)

    assert [%WorkflowCommand{variant: {:start_child_workflow_execution, sc}}] =
             commands_of(completion1)

    {:ok, completion2, state2} =
      Evaluator.evaluate(SingleChild, activation([child_start_failed_job(sc.seq)]), state1)

    # Completed on the START resolution alone — no second resolution needed.
    assert [%WorkflowCommand{variant: {:complete_workflow_execution, _cwe}}] =
             commands_of(completion2)

    assert {:completed,
            {:ok,
             {:error,
              {:start_failed,
               :START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS}}}} =
             state2.result
  end

  test "start cancellation surfaces as {:cancelled, failure}" do
    state0 = fresh_state("r7")

    {:ok, completion1, state1} =
      Evaluator.evaluate(SingleChild, activation([init_job(%{})]), state0)

    assert [%WorkflowCommand{variant: {:start_child_workflow_execution, sc}}] =
             commands_of(completion1)

    {:ok, _completion2, state2} =
      Evaluator.evaluate(SingleChild, activation([child_start_cancelled_job(sc.seq)]), state1)

    assert {:completed, {:ok, {:error, {:cancelled, %Temporal.Api.Failure.V1.Failure{}}}}} =
             state2.result
  end

  test "a child that starts then fails surfaces the run Failure" do
    state0 = fresh_state("r8")

    {:ok, completion1, state1} =
      Evaluator.evaluate(SingleChild, activation([init_job(%{})]), state0)

    assert [%WorkflowCommand{variant: {:start_child_workflow_execution, sc}}] =
             commands_of(completion1)

    {:ok, _c2, state2} =
      Evaluator.evaluate(SingleChild, activation([child_started_job(sc.seq, "run-x")]), state1)

    {:ok, _c3, state3} =
      Evaluator.evaluate(SingleChild, activation([child_failed_job(sc.seq, "boom")]), state2)

    assert {:completed, {:ok, {:error, %Temporal.Api.Failure.V1.Failure{message: "boom"}}}} =
             state3.result
  end

  test "execute_child! raises ChildWorkflowError, parking the workflow task" do
    defmodule BangChild do
      use Hourglass.Workflow

      @impl Hourglass.Workflow.Behaviour
      def run(_input), do: execute_child!(MyChild, %{})
    end

    state0 = fresh_state("r9")

    {:ok, completion1, state1} =
      Evaluator.evaluate(BangChild, activation([init_job(%{})]), state0)

    assert [%WorkflowCommand{variant: {:start_child_workflow_execution, sc}}] =
             commands_of(completion1)

    {:ok, _c2, state2} =
      Evaluator.evaluate(BangChild, activation([child_started_job(sc.seq, "run-y")]), state1)

    # A raise inside the body is a workflow-task failure (park), not a completion.
    {:ok, completion3, _state3} =
      Evaluator.evaluate(BangChild, activation([child_failed_job(sc.seq, "kaboom")]), state2)

    assert %WorkflowActivationCompletion{status: {:failed, _failure}} = completion3
  end

  test "ChildWorkflowError message names the workflow and the reason" do
    error = %Hourglass.ChildWorkflowError{workflow: MyChild, reason: :nope}
    assert Exception.message(error) =~ "MyChild"
    assert Exception.message(error) =~ "nope"
  end
end
