defmodule Hourglass.Workflow.EvaluatorTest do
  # async: true — pure-function evaluator owns no global resources; each
  # test builds its own activations + run_id.
  use ExUnit.Case, async: true

  alias Coresdk.WorkflowCommands.FailWorkflowExecution
  alias Coresdk.WorkflowCommands.WorkflowCommand
  # Failure is needed for the park-on-raise assertions
  alias Coresdk.WorkflowCompletion.Failure
  alias Coresdk.WorkflowCompletion.Success
  alias Coresdk.WorkflowCompletion.WorkflowActivationCompletion
  alias Hourglass.Workflow.Evaluator
  alias Hourglass.Workflow.State
  # Not aliased as `Failure` — that name is already bound above to
  # Coresdk.WorkflowCompletion.Failure (the task-failure wrapper). This is
  # the *proto* Failure a FailWorkflowExecution command carries; referenced
  # fully-qualified below to avoid the collision.
  alias Temporal.Api.Failure.V1.ApplicationFailureInfo

  # CrashingAsync + RaiseInBody fixtures intentionally raise to verify
  # the catch path; the workflow_exception telemetry log is expected.
  @moduletag capture_log: true

  # -------------------------------------------------------------------------
  # Stub activity modules — minimal shims so execute_activity/2,3 can call
  # __activity_input_type__ and __activity_output_type__ without needing a
  # full `use Hourglass.Activity` definition.
  # -------------------------------------------------------------------------

  defmodule MyAct do
    def __activity_input_type__, do: :map
    def __activity_output_type__, do: :map
    def __activity_retry_policy__, do: []
  end

  defmodule WorkAct1 do
    def __activity_input_type__, do: :map
    def __activity_output_type__, do: :map
    def __activity_retry_policy__, do: []
  end

  defmodule WorkAct2 do
    def __activity_input_type__, do: :map
    def __activity_output_type__, do: :map
    def __activity_retry_policy__, do: []
  end

  defmodule WorkAct3 do
    def __activity_input_type__, do: :map
    def __activity_output_type__, do: :map
    def __activity_retry_policy__, do: []
  end

  # -------------------------------------------------------------------------
  # Fixture workflows
  # -------------------------------------------------------------------------

  defmodule ImmediateWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input), do: {:ok, :done}
  end

  defmodule SingleStep do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(input) do
      result = execute_activity(MyAct, input)
      {:ok, result}
    end
  end

  defmodule HeartbeatTimeoutWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(input) do
      result = execute_activity(MyAct, input, heartbeat_timeout: 30_000)
      {:ok, result}
    end
  end

  defmodule TwoStep do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(input) do
      a = execute_activity(MyAct, input)
      b = execute_activity(MyAct, a)
      {:ok, {a, b}}
    end
  end

  defmodule MultiAsync do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      c1 = async(fn -> execute_activity(WorkAct1, %{}) end)
      c2 = async(fn -> execute_activity(WorkAct2, %{}) end)
      c3 = async(fn -> execute_activity(WorkAct3, %{}) end)
      {:ok, {await(c1), await(c2), await(c3)}}
    end
  end

  defmodule CrashingAsync do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      c = async(fn -> raise "boom" end)
      await(c)
    end
  end

  defmodule RaiseInBody do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input), do: raise("workflow body explodes")
  end

  defmodule MatchedActivityFailure do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      case execute_activity(MyAct, %{}) do
        {:ok, value} -> {:ok, {:got, value}}
        {:error, reason} -> {:error, {:activity_failed, reason}}
      end
    end
  end

  # New fixture: uses execute_activity!/2 — raises on failure
  defmodule BangActivityWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      value = execute_activity!(MyAct, %{})
      {:ok, value}
    end
  end

  # fail/2,3 fixtures — a workflow body ending its run as genuinely failed
  # (distinct from CrashingAsync/RaiseInBody above, which park).
  defmodule FailingWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      fail("ParseFailed", "could not parse document", details: %{"stage" => "decompose"})
    end
  end

  defmodule FailNoOptsWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input), do: fail("Boom", "no details or overrides supplied")
  end

  defmodule FailRetryableWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input),
      do: fail("Transient", "let a workflow-level retry policy retry this", non_retryable: false)
  end

  # Proves a redelivered activation does not re-execute the body: run/1
  # raises if invoked a second time. Process.get/put is safe here only
  # because the pure-function evaluator runs run/1 inline in the calling
  # process (no Task spawn — see Evaluator's moduledoc), so the counter is
  # scoped to whichever ExUnit test process calls Evaluator.evaluate/3.
  defmodule FailOnceWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      key = {__MODULE__, :run_count}
      count = Process.get(key, 0)
      Process.put(key, count + 1)

      if count > 0 do
        raise "run/1 invoked again after a cached failure — redelivery must not re-run the body"
      end

      fail("DecomposeFailed", "decomposition failed on first attempt")
    end
  end

  defmodule BadFailArgWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    # :oops is not a binary — fail/2's guard clause rejects it, raising
    # FunctionClauseError, which run_body/2's generic :error clause parks
    # exactly like any other workflow-body bug. It must NOT reach
    # build_fail_command/1 with a malformed shape.
    def run(_input), do: fail(:oops, "message")
  end

  defmodule IOSchema do
    use Hourglass.Schema

    embedded_schema do
      field :n, :integer
    end
  end

  defmodule TypedIOWorkflow do
    use Hourglass.Workflow, input: IOSchema, output: IOSchema
    @impl Hourglass.Workflow.Behaviour
    def run(%IOSchema{n: n}), do: %IOSchema{n: n + 1}
  end

  # -------------------------------------------------------------------------
  # Helpers
  # -------------------------------------------------------------------------

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

  defp failed_resolution_job(seq, message) do
    resolution = %Coresdk.ActivityResult.ActivityResolution{
      status:
        {:failed,
         %Coresdk.ActivityResult.Failure{
           failure: %Temporal.Api.Failure.V1.Failure{message: message}
         }}
    }

    %{
      variant:
        {:resolve_activity,
         %Coresdk.WorkflowActivation.ResolveActivity{seq: seq, result: resolution}}
    }
  end

  defp evict_job do
    %{
      variant:
        {:remove_from_cache,
         %Coresdk.WorkflowActivation.RemoveFromCache{reason: :CACHE_FULL, message: "evicting"}}
    }
  end

  defp update_random_seed_job(seed) do
    %{
      variant:
        {:update_random_seed, %Coresdk.WorkflowActivation.UpdateRandomSeed{randomness_seed: seed}}
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

  defp fresh_state(run_id), do: State.new(run_id, "test-tq")

  # -------------------------------------------------------------------------
  # Tests
  # -------------------------------------------------------------------------

  test "init-only run completes immediately with CompleteWorkflowExecution" do
    {:ok, completion, new_state} =
      Evaluator.evaluate(ImmediateWorkflow, activation([init_job("hello")]), fresh_state("r1"))

    assert %WorkflowActivationCompletion{run_id: "r1"} = completion

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion)

    assert cwe.result != nil
    assert {:completed, {:ok, :done}} = new_state.result
  end

  test "single activity round-trip: init schedules, resolve completes" do
    state0 = fresh_state("r2")

    {:ok, completion1, state1} =
      Evaluator.evaluate(SingleStep, activation([init_job(%{"msg" => "hello"})]), state0)

    assert [%WorkflowCommand{variant: {:schedule_activity, sa}}] = commands_of(completion1)
    assert sa.seq == 1
    # activity_type is now Atom.to_string(module), NOT "Module.function"
    assert sa.activity_type == Atom.to_string(MyAct)
    assert state1.next_global_seq == 2
    assert Map.has_key?(state1.pending_resolvers, 1)

    {:ok, completion2, state2} =
      Evaluator.evaluate(SingleStep, activation([resolve_job(1, %{"msg" => "world"})]), state1)

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, _cwe}}] =
             commands_of(completion2)

    # result is wrapped in {:ok, {:ok, map}} — outer {:ok} from execute_activity, inner {:ok} from workflow body
    assert {:completed, {:ok, {:ok, %{"msg" => "world"}}}} = state2.result
  end

  test "two sequential activities yield seq=1 then seq=2" do
    state0 = fresh_state("r3")

    {:ok, c1, s1} = Evaluator.evaluate(TwoStep, activation([init_job(%{"v" => "input"})]), state0)
    assert [%WorkflowCommand{variant: {:schedule_activity, sa1}}] = commands_of(c1)
    assert sa1.seq == 1
    assert sa1.activity_type == Atom.to_string(MyAct)

    {:ok, c2, s2} = Evaluator.evaluate(TwoStep, activation([resolve_job(1, %{"v" => "r1"})]), s1)
    assert [%WorkflowCommand{variant: {:schedule_activity, sa2}}] = commands_of(c2)
    assert sa2.seq == 2
    assert sa2.activity_type == Atom.to_string(MyAct)

    {:ok, c3, _s3} = Evaluator.evaluate(TwoStep, activation([resolve_job(2, %{"v" => "r2"})]), s2)
    assert [%WorkflowCommand{variant: {:complete_workflow_execution, _cwe}}] = commands_of(c3)
  end

  test "out-of-order resolves still produce monotonic seqs from sorted command_ids" do
    # MultiAsync issues 3 children in source-position order. The evaluator
    # must assign seqs 1=WorkAct1, 2=WorkAct2, 3=WorkAct3 deterministically
    # regardless of the order resolves arrive.
    state0 = fresh_state("r4")

    {:ok, completion, state1} =
      Evaluator.evaluate(MultiAsync, activation([init_job(nil)]), state0)

    seqs =
      for %WorkflowCommand{variant: {:schedule_activity, sa}} <- commands_of(completion),
          do: {sa.seq, sa.activity_type}

    assert [{1, t1}, {2, t2}, {3, t3}] = seqs
    assert t1 == Atom.to_string(WorkAct1)
    assert t2 == Atom.to_string(WorkAct2)
    assert t3 == Atom.to_string(WorkAct3)
    assert state1.next_global_seq == 4

    # Resolves arrive out of order — but command_ids are bound to seqs from
    # activation 1 and don't depend on resolve arrival order.
    {:ok, c2, state2} =
      Evaluator.evaluate(
        MultiAsync,
        activation([resolve_job(3, %{"n" => "r3"}), resolve_job(1, %{"n" => "r1"})]),
        state1
      )

    # Still suspended on c2 (seq=2 unresolved).
    assert commands_of(c2) == []
    refute Map.has_key?(state2.resolved_results, 2)
    assert Map.fetch!(state2.resolved_results, 1) == {:ok, %{"n" => "r1"}}
    assert Map.fetch!(state2.resolved_results, 3) == {:ok, %{"n" => "r3"}}

    {:ok, c3, _state3} =
      Evaluator.evaluate(MultiAsync, activation([resolve_job(2, %{"n" => "r2"})]), state2)

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, _cwe}}] = commands_of(c3)
  end

  test "activity returning {:error, _} flows cleanly into matched workflow body" do
    state0 = fresh_state("r5")

    {:ok, _c1, s1} =
      Evaluator.evaluate(MatchedActivityFailure, activation([init_job(nil)]), state0)

    failure_resolution = %Coresdk.ActivityResult.ActivityResolution{
      status:
        {:failed,
         %Coresdk.ActivityResult.Failure{
           failure: %Temporal.Api.Failure.V1.Failure{message: "downstream"}
         }}
    }

    failure_job = %{
      variant:
        {:resolve_activity,
         %Coresdk.WorkflowActivation.ResolveActivity{seq: 1, result: failure_resolution}}
    }

    {:ok, c2, s2} =
      Evaluator.evaluate(MatchedActivityFailure, activation([failure_job]), s1)

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, _cwe}}] = commands_of(c2)
    assert {:completed, {:error, {:activity_failed, _reason}}} = s2.result
  end

  test "async + await: 3 children with deterministic global seqs" do
    state0 = fresh_state("r6")

    {:ok, completion, _s1} = Evaluator.evaluate(MultiAsync, activation([init_job(nil)]), state0)

    seqs =
      for %WorkflowCommand{variant: {:schedule_activity, sa}} <- commands_of(completion),
          do: sa.seq

    assert seqs == [1, 2, 3]
  end

  test "async with crashing inner fun parks the workflow (workflow-task failure)" do
    state0 = fresh_state("r7")

    {:ok, completion, state1} =
      Evaluator.evaluate(CrashingAsync, activation([init_job(nil)]), state0)

    assert {:failed, %Failure{failure: %Temporal.Api.Failure.V1.Failure{message: msg}}} =
             completion.status

    assert msg =~ "boom"
    # parked, not terminal -> next activation re-runs
    refute match?({:completed, _}, state1.result)
    refute match?({:failed, _}, state1.result)
  end

  test "raise inside run/1 parks the workflow (workflow-task failure, not terminal)" do
    state0 = fresh_state("r8")

    {:ok, completion, state1} =
      Evaluator.evaluate(RaiseInBody, activation([init_job(nil)]), state0)

    assert {:failed, %Failure{failure: %Temporal.Api.Failure.V1.Failure{message: msg}}} =
             completion.status

    assert msg =~ "workflow body explodes"
    # parked, not terminal -> next activation re-runs
    refute match?({:completed, _}, state1.result)
    refute match?({:failed, _}, state1.result)
  end

  # -------------------------------------------------------------------------
  # fail/2,3 — a workflow body ending its run as genuinely failed. New
  # coverage sits beside (never replaces) the park-on-raise tests above.
  # -------------------------------------------------------------------------

  test "fail/2 ends the run with a terminal FailWorkflowExecution inside a SUCCESSFUL completion" do
    state0 = fresh_state("r-fail-1")

    {:ok, completion, state1} =
      Evaluator.evaluate(FailingWorkflow, activation([init_job(nil)]), state0)

    # Protocol-level: the activation completion is {:successful, ...} —
    # fail_workflow_execution lives in the SAME command oneof as
    # complete_workflow_execution, never the {:failed, ...} task-status shape
    # parking uses (contrast the RaiseInBody test immediately above).
    assert {:successful, %Success{commands: [command]}} = completion.status
    assert %WorkflowCommand{variant: {:fail_workflow_execution, fwe}} = command
    assert %FailWorkflowExecution{failure: failure} = fwe
    assert %Temporal.Api.Failure.V1.Failure{} = failure
    assert failure.message == "could not parse document"
    assert failure.source == "ParseFailed"

    assert {:application_failure_info, %ApplicationFailureInfo{} = info} = failure.failure_info
    assert info.type == "ParseFailed"
    assert info.non_retryable == true
    assert %Temporal.Api.Common.V1.Payloads{payloads: [payload]} = info.details
    assert Jason.decode!(payload.data) == %{"stage" => "decompose"}

    # State carries the same decision — this is what a redelivered
    # activation re-emits without re-deciding (see the redelivery test below).
    assert state1.result ==
             {:failed,
              %{
                type: "ParseFailed",
                message: "could not parse document",
                details: %{"stage" => "decompose"},
                non_retryable: true
              }}
  end

  test "fail/2 (no opts) defaults details to nil and non_retryable to true" do
    state0 = fresh_state("r-fail-2")

    {:ok, completion, state1} =
      Evaluator.evaluate(FailNoOptsWorkflow, activation([init_job(nil)]), state0)

    assert {:successful,
            %Success{commands: [%WorkflowCommand{variant: {:fail_workflow_execution, fwe}}]}} =
             completion.status

    assert {:application_failure_info, %ApplicationFailureInfo{} = info} =
             fwe.failure.failure_info

    assert info.type == "Boom"
    assert info.non_retryable == true
    assert info.details == nil

    assert state1.result ==
             {:failed,
              %{
                type: "Boom",
                message: "no details or overrides supplied",
                details: nil,
                non_retryable: true
              }}
  end

  test "fail/3 :non_retryable override reaches ApplicationFailureInfo.non_retryable" do
    state0 = fresh_state("r-fail-3")

    {:ok, completion, _state1} =
      Evaluator.evaluate(FailRetryableWorkflow, activation([init_job(nil)]), state0)

    assert {:successful,
            %Success{commands: [%WorkflowCommand{variant: {:fail_workflow_execution, fwe}}]}} =
             completion.status

    assert {:application_failure_info, %ApplicationFailureInfo{non_retryable: false}} =
             fwe.failure.failure_info
  end

  test "fail/2's outcome is structurally distinct from an uncaught raise's park — same activation shape, different wire result" do
    # Two workflows, same shape of activation, deliberately different
    # terminal decisions. This is the adversarial check: prove the two paths
    # diverge on protocol (status variant / command variant), not merely that
    # "the workflow stopped" in both cases.
    {:ok, fail_completion, fail_state} =
      Evaluator.evaluate(
        FailingWorkflow,
        activation([init_job(nil)]),
        fresh_state("r-distinct-fail")
      )

    {:ok, raise_completion, raise_state} =
      Evaluator.evaluate(
        RaiseInBody,
        activation([init_job(nil)]),
        fresh_state("r-distinct-raise")
      )

    # fail/2: successful completion, fail_workflow_execution command, result cached as {:failed, _}.
    assert {:successful,
            %Success{commands: [%WorkflowCommand{variant: {:fail_workflow_execution, _fwe}}]}} =
             fail_completion.status

    assert match?({:failed, _}, fail_state.result)

    # raise: task-status failure, no commands at all, result stays nil (not terminal).
    assert {:failed, %Failure{}} = raise_completion.status
    refute match?({:completed, _}, raise_state.result)
    refute match?({:failed, _}, raise_state.result)

    # The two `status` shapes don't even share a variant tag: :successful vs :failed.
    assert elem(fail_completion.status, 0) == :successful
    assert elem(raise_completion.status, 0) == :failed
  end

  test "a redelivered activation after fail/2 re-emits the cached FailWorkflowExecution WITHOUT re-running the body" do
    state0 = fresh_state("r-fail-redeliver")

    {:ok, completion1, state1} =
      Evaluator.evaluate(FailOnceWorkflow, activation([init_job(nil)]), state0)

    assert {:successful,
            %Success{commands: [%WorkflowCommand{variant: {:fail_workflow_execution, fwe1}}]}} =
             completion1.status

    assert match?({:failed, _}, state1.result)

    # Redelivery: same workflow module, a fresh activation, the ALREADY-FAILED
    # state. If the body re-ran, FailOnceWorkflow's second invocation raises
    # (which would park, not re-emit) — so a non-parked, identical-failure
    # result here proves the cached branch short-circuited before run_body/2.
    {:ok, completion2, state2} =
      Evaluator.evaluate(FailOnceWorkflow, activation([init_job(nil)]), state1)

    assert {:successful,
            %Success{commands: [%WorkflowCommand{variant: {:fail_workflow_execution, fwe2}}]}} =
             completion2.status

    assert fwe1 == fwe2
    assert state2.result == state1.result

    # Direct proof the body ran exactly once, from the fixture's own counter.
    assert Process.get({FailOnceWorkflow, :run_count}) == 1
  end

  test "fail/2 requires a binary type and message — a caller mistake parks like any other bug" do
    {:ok, completion, state1} =
      Evaluator.evaluate(
        BadFailArgWorkflow,
        activation([init_job(nil)]),
        fresh_state("r-fail-badarg")
      )

    assert {:failed, %Failure{}} = completion.status
    refute match?({:completed, _}, state1.result)
    refute match?({:failed, _}, state1.result)
  end

  test "remove_from_cache yields an empty Success completion + :evict marker" do
    state0 = fresh_state("r9")

    {:ok, completion, new_state} =
      Evaluator.evaluate(SingleStep, activation([evict_job()]), state0)

    assert %WorkflowActivationCompletion{run_id: "r9"} = completion
    assert commands_of(completion) == []
    assert new_state.result == :evict
  end

  test "update_random_seed job is a no-op (does not crash the evaluator)" do
    # Regression: ErrorReporter.report/5 misuse on the unhandled-variant
    # path crashed the evaluator task on every UpdateRandomSeed activation
    # (which Core sends on every subsequent workflow activation),
    # freezing all in-flight workflows. Pin the no-op semantics here.
    state0 = fresh_state("r-rng")

    init = init_job("hello")
    rng = update_random_seed_job(0xCAFE_BABE)

    {:ok, %WorkflowActivationCompletion{} = completion, %{result: result}} =
      Evaluator.evaluate(ImmediateWorkflow, activation([init, rng]), state0)

    assert completion.run_id == "r-rng"
    # The unhandled variant did not flip into an :evict, and the workflow
    # body still ran to its normal completion.
    assert result != :evict
  end

  test "completion proto round-trips through encode + decode unchanged" do
    state0 = fresh_state("r10")

    {:ok, completion, _state1} =
      Evaluator.evaluate(SingleStep, activation([init_job("ping")]), state0)

    bytes = Protobuf.encode(completion)
    assert is_binary(bytes)
    assert WorkflowActivationCompletion.decode(bytes) == completion
  end

  # -------------------------------------------------------------------------
  # New tests for execute_activity/2,3 and execute_activity!/2,3
  # -------------------------------------------------------------------------

  test "EchoActivity-style round-trip: activity_type == Atom.to_string(module)" do
    # Uses the real EchoActivity fixture which has __activity_input_type__ etc.
    alias Hourglass.TestSupport.EchoActivity

    state0 = fresh_state("r-echo-1")

    {:ok, completion1, state1} =
      Evaluator.evaluate(
        Hourglass.TestSupport.EchoWorkflow,
        activation([init_job(%{"k" => "v"})]),
        state0
      )

    assert [%WorkflowCommand{variant: {:schedule_activity, sa}}] = commands_of(completion1)
    assert sa.seq == 1
    assert sa.activity_type == Atom.to_string(EchoActivity)

    # Second activation: resolve seq 1 with a plain-map success payload
    {:ok, completion2, state2} =
      Evaluator.evaluate(
        Hourglass.TestSupport.EchoWorkflow,
        activation([resolve_job(1, %{"k" => "v"})]),
        state1
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion2)

    assert cwe.result != nil
    # Body wraps result in %{"echoed" => ...}
    assert {:completed, %{"echoed" => %{"k" => "v"}}} = state2.result
  end

  test "execute_activity/2 returns {:ok, value} on success" do
    state0 = fresh_state("r-ok-1")

    # First activation: schedules the activity
    {:ok, _c1, s1} = Evaluator.evaluate(BangActivityWorkflow, activation([init_job(nil)]), state0)

    # Second activation: resolve with success (must be a map since MyAct output is :map)
    {:ok, c2, s2} =
      Evaluator.evaluate(
        BangActivityWorkflow,
        activation([resolve_job(1, %{"result" => true})]),
        s1
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, _cwe}}] = commands_of(c2)
    # execute_activity! returns the bare value; workflow wraps it in {:ok, _}
    assert {:completed, {:ok, %{"result" => true}}} = s2.result
  end

  test "execute_activity/2 returns {:error, _} on activity failure (matched workflow)" do
    state0 = fresh_state("r-err-1")

    {:ok, _c1, s1} =
      Evaluator.evaluate(MatchedActivityFailure, activation([init_job(nil)]), state0)

    {:ok, c2, s2} =
      Evaluator.evaluate(
        MatchedActivityFailure,
        activation([failed_resolution_job(1, "oops")]),
        s1
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, _cwe}}] = commands_of(c2)
    assert {:completed, {:error, {:activity_failed, _reason}}} = s2.result
  end

  test "execute_activity!/2 parks the workflow on activity failure (workflow-task failure)" do
    # BangActivityWorkflow uses execute_activity!/2 — failure propagates as
    # ActivityError which is caught by run_body and parks the workflow as a
    # workflow-task failure (server retries → deploy-to-resume).
    state0 = fresh_state("r-bang-fail-1")

    {:ok, _c1, s1} =
      Evaluator.evaluate(BangActivityWorkflow, activation([init_job(nil)]), state0)

    {:ok, c2, s2} =
      Evaluator.evaluate(BangActivityWorkflow, activation([failed_resolution_job(1, "boom")]), s1)

    # ActivityError is an exception caught by the :error branch → parked as workflow-task failure.
    assert {:failed, %Failure{failure: %Temporal.Api.Failure.V1.Failure{message: msg}}} =
             c2.status

    assert msg =~ "ActivityError"
    # parked, not terminal
    refute match?({:completed, _}, s2.result)
    refute match?({:failed, _}, s2.result)
  end

  test "scheduled ScheduleActivity command activity_type is module name only, no function suffix" do
    # Pin the wire format: activity_type is Atom.to_string(module) — no appended ".function".
    state0 = fresh_state("r-wire-1")

    {:ok, completion, _s1} =
      Evaluator.evaluate(SingleStep, activation([init_job(%{})]), state0)

    assert [%WorkflowCommand{variant: {:schedule_activity, sa}}] = commands_of(completion)
    # Must equal Atom.to_string(MyAct) — no extra ".step" or similar suffix.
    expected = Atom.to_string(Hourglass.Workflow.EvaluatorTest.MyAct)
    assert sa.activity_type == expected
    # No function suffix: the type must not end with ".something_lowercase"
    refute sa.activity_type =~ ~r/\.[a-z_]+$/
  end

  test "execute_activity/3 forwards heartbeat_timeout to ScheduleActivity command" do
    state0 = fresh_state("r-heartbeat-1")

    {:ok, completion, _s1} =
      Evaluator.evaluate(HeartbeatTimeoutWorkflow, activation([init_job(%{})]), state0)

    assert [%WorkflowCommand{variant: {:schedule_activity, sa}}] = commands_of(completion)
    assert sa.heartbeat_timeout == %Google.Protobuf.Duration{seconds: 30, nanos: 0}
  end

  test "execute_activity/2 omitting heartbeat_timeout yields nil on ScheduleActivity command" do
    state0 = fresh_state("r-heartbeat-nil-1")

    {:ok, completion, _s1} =
      Evaluator.evaluate(SingleStep, activation([init_job(%{})]), state0)

    assert [%WorkflowCommand{variant: {:schedule_activity, sa}}] = commands_of(completion)
    assert sa.heartbeat_timeout == nil
  end

  # -------------------------------------------------------------------------
  # encode_input_payloads/1 fallback branches (activity path), pinned here so
  # a future edit to the shared helper can't silently change the published
  # execute_activity wire format. Mirrored for the child path in
  # child_workflow_test.exs.
  # -------------------------------------------------------------------------

  defmodule NilArgsWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      result = execute_activity(MyAct, nil)
      {:ok, result}
    end
  end

  defmodule NonJsonArgsWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      result = execute_activity(MyAct, {:not, :json, :encodable})
      {:ok, result}
    end
  end

  test "execute_activity/2 with nil args encodes ScheduleActivity.arguments as []" do
    state0 = fresh_state("r-nilargs-1")

    {:ok, completion, _state1} =
      Evaluator.evaluate(NilArgsWorkflow, activation([init_job(nil)]), state0)

    assert [%WorkflowCommand{variant: {:schedule_activity, sa}}] = commands_of(completion)
    assert sa.arguments == []
  end

  test "execute_activity/2 with non-JSON-encodable args falls back to elixir/inspect encoding" do
    state0 = fresh_state("r-inspectargs-1")

    {:ok, completion, _state1} =
      Evaluator.evaluate(NonJsonArgsWorkflow, activation([init_job(nil)]), state0)

    assert [%WorkflowCommand{variant: {:schedule_activity, sa}}] = commands_of(completion)

    assert [
             %Temporal.Api.Common.V1.Payload{
               metadata: %{"encoding" => "elixir/inspect"},
               data: data
             }
           ] = sa.arguments

    assert data == inspect({:not, :json, :encodable})
  end

  # -------------------------------------------------------------------------
  # Durable timer (sleep/1)
  # -------------------------------------------------------------------------

  defmodule SleepWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      sleep({:sec, 5})
      %{"slept" => true}
    end
  end

  test "sleep/1 issues StartTimer on first activation, completes on fire_timer" do
    state0 = fresh_state("r-sleep-1")

    # First activation: init → should SUSPEND with a StartTimer command
    {:ok, completion1, state1} =
      Evaluator.evaluate(SleepWorkflow, activation([init_job(nil)]), state0)

    assert [
             %WorkflowCommand{
               variant:
                 {:start_timer,
                  %Coresdk.WorkflowCommands.StartTimer{
                    seq: seq,
                    start_to_fire_timeout: %Google.Protobuf.Duration{seconds: 5}
                  }}
             }
           ] = commands_of(completion1)

    # Second activation: fire_timer with the same seq → should COMPLETE
    fire_timer_job = %{
      variant: {:fire_timer, %Coresdk.WorkflowActivation.FireTimer{seq: seq}}
    }

    {:ok, completion2, state2} =
      Evaluator.evaluate(SleepWorkflow, activation([fire_timer_job]), state1)

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion2)

    assert cwe.result != nil
    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    assert decoded == %{"slept" => true}
    assert {:completed, %{"slept" => true}} = state2.result
  end

  test "typed workflow I/O: input is cast to schema struct, output is dumped to plain map" do
    # TypedIOWorkflow uses input: IOSchema, output: IOSchema.
    # The run/1 head matches %IOSchema{n: n} — if the evaluator passes the
    # raw map instead of a cast struct, it raises FunctionClauseError, so
    # a clean completion proves the cast happened.
    state0 = fresh_state("r-typed-io-1")

    {:ok, completion, state1} =
      Evaluator.evaluate(TypedIOWorkflow, activation([init_job(%{"n" => 1})]), state0)

    # Workflow should complete immediately (no activities).
    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion)

    assert cwe.result != nil
    # The result payload is json/plain — decode it and verify the dumped output.
    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    assert decoded == %{"n" => 2}

    # state.result holds the already-dumped value (atom-keyed from Ecto.embedded_dump).
    assert {:completed, %{n: 2}} = state1.result
  end

  # -------------------------------------------------------------------------
  # await_signal/1 tests
  # -------------------------------------------------------------------------

  defmodule AwaitOneSignal do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input), do: %{"got" => await_signal("go")}
  end

  defmodule AwaitTwoSignals do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input), do: %{"a" => await_signal("s"), "b" => await_signal("s")}
  end

  defp signal_job(name, payload) do
    %{
      variant:
        {:signal_workflow,
         %Coresdk.WorkflowActivation.SignalWorkflow{
           signal_name: name,
           input: [
             %Temporal.Api.Common.V1.Payload{
               metadata: %{"encoding" => "json/plain"},
               data: Jason.encode!(payload)
             }
           ]
         }}
    }
  end

  test "await_signal: suspends when no signal has arrived yet, completes on second activation" do
    state0 = fresh_state("r-sig-1")

    # Activation 1: init only, no signal — must SUSPEND with no commands
    {:ok, completion1, state1} =
      Evaluator.evaluate(AwaitOneSignal, activation([init_job(nil)]), state0)

    # No commands issued — suspended cleanly
    assert commands_of(completion1) == []
    refute match?({:completed, _}, state1.result)

    # Activation 2: deliver the "go" signal
    {:ok, completion2, state2} =
      Evaluator.evaluate(
        AwaitOneSignal,
        activation([signal_job("go", %{"v" => 1})]),
        state1
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion2)

    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    assert decoded == %{"got" => %{"v" => 1}}
    assert {:completed, %{"got" => %{"v" => 1}}} = state2.result
  end

  test "await_signal: two sequential awaits on same name consume signals in order" do
    state0 = fresh_state("r-sig-2")

    # Single activation: init + two signals for "s"
    {:ok, completion, state1} =
      Evaluator.evaluate(
        AwaitTwoSignals,
        activation([
          init_job(nil),
          signal_job("s", %{"n" => 1}),
          signal_job("s", %{"n" => 2})
        ]),
        state0
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion)

    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    assert decoded == %{"a" => %{"n" => 1}, "b" => %{"n" => 2}}
    assert {:completed, %{"a" => %{"n" => 1}, "b" => %{"n" => 2}}} = state1.result
  end

  test "await_signal: replay determinism — same activation re-evaluated from fresh state produces identical completion" do
    state0 = fresh_state("r-sig-3")

    jobs = [
      init_job(nil),
      signal_job("s", %{"n" => 1}),
      signal_job("s", %{"n" => 2})
    ]

    act = activation(jobs)

    {:ok, completion1, _state1} = Evaluator.evaluate(AwaitTwoSignals, act, state0)

    # Re-run the SAME activation from fresh state
    {:ok, completion2, _state2} = Evaluator.evaluate(AwaitTwoSignals, act, fresh_state("r-sig-3"))

    # Both completions must be byte-identical
    assert Protobuf.encode(completion1) == Protobuf.encode(completion2)
  end

  # -------------------------------------------------------------------------
  # cancelled?/0 cooperative cancellation tests
  # -------------------------------------------------------------------------

  defmodule CancelAwareWorkflow do
    use Hourglass.Workflow
    @impl Hourglass.Workflow.Behaviour
    def run(_input), do: %{"cancelled" => cancelled?()}
  end

  defp cancel_job(reason) do
    %{
      variant: {:cancel_workflow, %Coresdk.WorkflowActivation.CancelWorkflow{reason: reason}}
    }
  end

  test "cancelled?/0 returns false when no cancel_workflow job has arrived" do
    state0 = fresh_state("r-cancel-1")

    {:ok, completion, state1} =
      Evaluator.evaluate(CancelAwareWorkflow, activation([init_job(nil)]), state0)

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion)

    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    assert decoded == %{"cancelled" => false}
    assert {:completed, %{"cancelled" => false}} = state1.result
  end

  test "cancelled?/0 returns true when cancel_workflow job is delivered in the same activation" do
    state0 = fresh_state("r-cancel-2")

    {:ok, completion, state1} =
      Evaluator.evaluate(
        CancelAwareWorkflow,
        activation([init_job(nil), cancel_job("stop")]),
        state0
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion)

    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    assert decoded == %{"cancelled" => true}
    assert {:completed, %{"cancelled" => true}} = state1.result
  end

  # -------------------------------------------------------------------------
  # continue_as_new/1 tests
  # -------------------------------------------------------------------------

  defmodule CanWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input), do: continue_as_new(%{"n" => 2})
  end

  test "continue_as_new/1 emits a ContinueAsNewWorkflowExecution command and sets result :continue_as_new" do
    state0 = fresh_state("r-can-1")

    {:ok, completion, new_state} =
      Evaluator.evaluate(CanWorkflow, activation([init_job(nil)]), state0)

    assert [
             %WorkflowCommand{
               variant:
                 {:continue_as_new_workflow_execution,
                  %Coresdk.WorkflowCommands.ContinueAsNewWorkflowExecution{arguments: [arg]}}
             }
           ] = commands_of(completion)

    assert arg.metadata["encoding"] == "json/plain"
    assert {:ok, %{"n" => 2}} = Jason.decode(arg.data)
    assert new_state.result == :continue_as_new
  end

  # -------------------------------------------------------------------------
  # await_all/1 tests
  # -------------------------------------------------------------------------

  defmodule FanOutWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      results =
        await_all([
          async(fn -> execute_activity!(Hourglass.TestSupport.EchoActivity, %{"i" => 1}) end),
          async(fn -> execute_activity!(Hourglass.TestSupport.EchoActivity, %{"i" => 2}) end)
        ])

      %{"results" => results}
    end
  end

  test "await_all/1: first activation suspends with two schedule_activity commands" do
    state0 = fresh_state("r-fanout-1")

    {:ok, completion1, state1} =
      Evaluator.evaluate(FanOutWorkflow, activation([init_job(nil)]), state0)

    cmds = commands_of(completion1)
    assert length(cmds) == 2

    assert [
             %WorkflowCommand{variant: {:schedule_activity, sa1}},
             %WorkflowCommand{variant: {:schedule_activity, sa2}}
           ] = cmds

    assert sa1.seq == 1
    assert sa2.seq == 2

    echo_type = Atom.to_string(Hourglass.TestSupport.EchoActivity)
    assert sa1.activity_type == echo_type
    assert sa2.activity_type == echo_type

    refute match?({:completed, _}, state1.result)

    # Second activation: resolve both with echo payloads
    {:ok, completion2, state2} =
      Evaluator.evaluate(
        FanOutWorkflow,
        activation([resolve_job(1, %{"i" => 1}), resolve_job(2, %{"i" => 2})]),
        state1
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion2)

    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    assert decoded == %{"results" => [%{"i" => 1}, %{"i" => 2}]}
    assert {:completed, %{"results" => [%{"i" => 1}, %{"i" => 2}]}} = state2.result
  end

  # -------------------------------------------------------------------------
  # Typed signals (signals: %{name => Schema} declaration)
  # -------------------------------------------------------------------------

  defmodule Reply do
    use Hourglass.Schema

    embedded_schema do
      field :text, :string
    end
  end

  defmodule TypedSignalWorkflow do
    use Hourglass.Workflow, signals: %{reply: Reply}

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      r = await_signal(:reply)
      # r must be a %Reply{} struct — r.text works only if cast happened.
      # If r were a raw map (%{"text" => "hi"}), r.text would raise KeyError
      # and the workflow would park, not complete.
      %{"text" => r.text}
    end
  end

  test "typed signal: await_signal/1 casts payload to declared schema struct" do
    state0 = fresh_state("r-typed-sig-1")

    # Activation 1: init only, no signal — must suspend
    {:ok, completion1, state1} =
      Evaluator.evaluate(TypedSignalWorkflow, activation([init_job(nil)]), state0)

    assert commands_of(completion1) == []
    refute match?({:completed, _}, state1.result)

    # Activation 2: deliver "reply" signal with JSON payload
    {:ok, completion2, state2} =
      Evaluator.evaluate(
        TypedSignalWorkflow,
        activation([signal_job("reply", %{"text" => "hi"})]),
        state1
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion2)

    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    # Workflow body did r.text successfully — proves r was a %Reply{} struct
    assert decoded == %{"text" => "hi"}
    assert {:completed, %{"text" => "hi"}} = state2.result
  end

  test "untyped signal (no signals: declaration): await_signal/1 still returns raw map" do
    # AwaitOneSignal has no signals: declaration — raw payload must come through.
    state0 = fresh_state("r-untyped-sig-1")

    {:ok, _completion1, state1} =
      Evaluator.evaluate(AwaitOneSignal, activation([init_job(nil)]), state0)

    {:ok, completion2, state2} =
      Evaluator.evaluate(
        AwaitOneSignal,
        activation([signal_job("go", %{"v" => 42})]),
        state1
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion2)

    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    # Raw map returned — no cast applied
    assert decoded == %{"got" => %{"v" => 42}}
    assert {:completed, %{"got" => %{"v" => 42}}} = state2.result
  end

  # -------------------------------------------------------------------------
  # await_signal/2 with timeout: signal-vs-timer race
  # -------------------------------------------------------------------------

  defmodule AwaitSignalWithTimeout do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      case await_signal("go", timeout: {:sec, 30}) do
        {:ok, v} -> %{"got" => v}
        :timeout -> %{"got" => "TIMEOUT"}
      end
    end
  end

  defp fire_timer_job(seq) do
    %{
      variant: {:fire_timer, %Coresdk.WorkflowActivation.FireTimer{seq: seq}}
    }
  end

  test "await_signal/2 timeout: signal wins — activation 1 suspends with StartTimer, activation 2 delivers signal" do
    state0 = fresh_state("r-sig-timeout-1")

    # Activation 1: init only — must SUSPEND and issue exactly one StartTimer
    {:ok, completion1, state1} =
      Evaluator.evaluate(AwaitSignalWithTimeout, activation([init_job(nil)]), state0)

    # The race timer must have been issued
    timer_cmds =
      for %WorkflowCommand{variant: {:start_timer, st}} <- commands_of(completion1), do: st

    assert length(timer_cmds) == 1
    assert [%Coresdk.WorkflowCommands.StartTimer{seq: _timer_seq}] = timer_cmds
    refute match?({:completed, _}, state1.result)

    # Activation 2: deliver signal "go", do NOT fire the timer
    {:ok, completion2, state2} =
      Evaluator.evaluate(
        AwaitSignalWithTimeout,
        activation([signal_job("go", %{"v" => 1})]),
        state1
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion2)

    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    assert decoded == %{"got" => %{"v" => 1}}
    assert {:completed, %{"got" => %{"v" => 1}}} = state2.result
  end

  test "await_signal/2 timeout: timer wins — activation 1 suspends with StartTimer, activation 2 fires timer" do
    state0 = fresh_state("r-sig-timeout-2")

    # Activation 1: init only — capture the timer seq
    {:ok, completion1, state1} =
      Evaluator.evaluate(AwaitSignalWithTimeout, activation([init_job(nil)]), state0)

    timer_cmds =
      for %WorkflowCommand{variant: {:start_timer, st}} <- commands_of(completion1), do: st

    assert [%Coresdk.WorkflowCommands.StartTimer{seq: timer_seq}] = timer_cmds
    refute match?({:completed, _}, state1.result)

    # Activation 2: fire the timer, no signal — must complete with :timeout branch
    {:ok, completion2, state2} =
      Evaluator.evaluate(
        AwaitSignalWithTimeout,
        activation([fire_timer_job(timer_seq)]),
        state1
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion2)

    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    assert decoded == %{"got" => "TIMEOUT"}
    assert {:completed, %{"got" => "TIMEOUT"}} = state2.result
  end

  # Test 3: DETERMINISM — subsequent command after timeout-await resolves correctly in BOTH paths.
  # This test drives the signal-wins path and verifies that the execute_activity
  # command issued AFTER the await gets a consistent seq (not colliding with the timer's seq).
  defmodule AwaitSignalThenActivity do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      {:ok, r_payload} = await_signal("go", timeout: {:sec, 30})
      x = execute_activity!(Hourglass.Workflow.EvaluatorTest.MyAct, %{})
      %{"r" => r_payload, "x" => x}
    end
  end

  test "await_signal/2 determinism: subsequent execute_activity gets a non-colliding seq (signal-wins path)" do
    state0 = fresh_state("r-sig-det-1")

    # Activation 1: init → suspend with StartTimer (and no activity yet)
    {:ok, completion1, state1} =
      Evaluator.evaluate(AwaitSignalThenActivity, activation([init_job(nil)]), state0)

    # Must have exactly one StartTimer command (the race timer)
    timer_cmds =
      for %WorkflowCommand{variant: {:start_timer, st}} <- commands_of(completion1), do: st

    assert [%Coresdk.WorkflowCommands.StartTimer{seq: timer_seq}] = timer_cmds
    # No activity scheduled yet
    activity_cmds =
      for %WorkflowCommand{variant: {:schedule_activity, _sa}} <- commands_of(completion1),
          do: :sa

    assert activity_cmds == []
    refute match?({:completed, _}, state1.result)

    # Activation 2: deliver signal "go" → r resolves, body proceeds to issue execute_activity
    {:ok, completion2, state2} =
      Evaluator.evaluate(
        AwaitSignalThenActivity,
        activation([signal_job("go", %{"v" => 42})]),
        state1
      )

    # The activity must now be scheduled
    act_cmds =
      for %WorkflowCommand{variant: {:schedule_activity, sa}} <- commands_of(completion2), do: sa

    assert [sa] = act_cmds
    # The activity seq must NOT collide with the timer's seq
    assert sa.seq != timer_seq

    # The activity seq must be one higher than the timer seq (timer got seq 1, activity gets seq 2)
    assert sa.seq == timer_seq + 1

    refute match?({:completed, _}, state2.result)

    # Activation 3: resolve the activity → workflow completes with both r and x
    {:ok, completion3, state3} =
      Evaluator.evaluate(
        AwaitSignalThenActivity,
        activation([resolve_job(sa.seq, %{"n" => 99})]),
        state2
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion3)

    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    # r is {:ok, signal_payload}; x is the activity result
    assert decoded == %{"r" => %{"v" => 42}, "x" => %{"n" => 99}}
    assert {:completed, %{"r" => %{"v" => 42}, "x" => %{"n" => 99}}} = state3.result
  end

  # Test 4: typed cast still applies with timeout variant
  defmodule TypedSignalWithTimeout do
    use Hourglass.Workflow, signals: %{reply: Hourglass.Workflow.EvaluatorTest.Reply}

    @impl Hourglass.Workflow.Behaviour
    def run(_input) do
      case await_signal(:reply, timeout: {:sec, 5}) do
        {:ok, %Hourglass.Workflow.EvaluatorTest.Reply{} = r} -> %{"text" => r.text}
        :timeout -> %{"text" => "TIMEOUT"}
      end
    end
  end

  test "await_signal/2 with timeout: typed cast still applies when signal arrives" do
    state0 = fresh_state("r-typed-timeout-1")

    # Activation 1: init only → suspend
    {:ok, completion1, state1} =
      Evaluator.evaluate(TypedSignalWithTimeout, activation([init_job(nil)]), state0)

    assert [%WorkflowCommand{variant: {:start_timer, _st}}] = commands_of(completion1)
    refute match?({:completed, _}, state1.result)

    # Activation 2: deliver the typed "reply" signal
    {:ok, completion2, state2} =
      Evaluator.evaluate(
        TypedSignalWithTimeout,
        activation([signal_job("reply", %{"text" => "hello"})]),
        state1
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion2)

    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    # The pattern match in the workflow proves the cast happened (r.text accessed)
    assert decoded == %{"text" => "hello"}
    assert {:completed, %{"text" => "hello"}} = state2.result
  end

  # Test 5: await_signal/1 (no opts) unchanged — bare payload, no :ok wrapper
  test "await_signal/1 unchanged: returns bare payload (no :ok wrapper)" do
    state0 = fresh_state("r-sig-unchanged-1")

    {:ok, _c1, state1} =
      Evaluator.evaluate(AwaitOneSignal, activation([init_job(nil)]), state0)

    {:ok, completion2, state2} =
      Evaluator.evaluate(
        AwaitOneSignal,
        activation([signal_job("go", %{"v" => 7})]),
        state1
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion2)

    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    # await_signal/1 returns bare payload, not {:ok, payload}
    assert decoded == %{"got" => %{"v" => 7}}
    assert {:completed, %{"got" => %{"v" => 7}}} = state2.result
  end

  test "await_signal/2 with empty opts behaves like await_signal/1 (bare payload)" do
    # await_signal(name, []) must be identical to await_signal(name)
    defmodule AwaitSignalEmptyOpts do
      use Hourglass.Workflow

      @impl Hourglass.Workflow.Behaviour
      def run(_input), do: %{"got" => await_signal("go", [])}
    end

    state0 = fresh_state("r-sig-empty-opts-1")

    {:ok, _c1, state1} =
      Evaluator.evaluate(AwaitSignalEmptyOpts, activation([init_job(nil)]), state0)

    {:ok, completion2, state2} =
      Evaluator.evaluate(
        AwaitSignalEmptyOpts,
        activation([signal_job("go", %{"v" => 5})]),
        state1
      )

    assert [%WorkflowCommand{variant: {:complete_workflow_execution, cwe}}] =
             commands_of(completion2)

    assert cwe.result.metadata["encoding"] == "json/plain"
    assert {:ok, decoded} = Jason.decode(cwe.result.data)
    # Bare payload — no :ok wrapper
    assert decoded == %{"got" => %{"v" => 5}}
    assert {:completed, %{"got" => %{"v" => 5}}} = state2.result
  end
end
