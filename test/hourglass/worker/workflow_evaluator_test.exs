defmodule Hourglass.Worker.WorkflowEvaluatorTest do
  # async: true — the evaluator is per-activation ephemeral; each
  # test owns its own evaluator pid + run_id. The shared evaluator
  # DynSup (Hourglass.WorkflowEvaluator.DynamicSupervisor) is
  # started globally in test_helper.exs and tolerates multi-test
  # concurrency because each child is :temporary and keyed by pid.
  use ExUnit.Case, async: true

  alias Coresdk.WorkflowActivation.InitializeWorkflow
  alias Coresdk.WorkflowActivation.WorkflowActivation
  alias Coresdk.WorkflowActivation.WorkflowActivationJob
  alias Coresdk.WorkflowCompletion.WorkflowActivationCompletion

  alias Hourglass.Worker.WorkflowEvaluator
  alias Hourglass.WorkflowEvaluator.DynamicSupervisor

  # ---------------------------------------------------------------------------
  # BridgeHolder-stub strategy
  # ---------------------------------------------------------------------------
  #
  # The production `Hourglass.BridgeHolder.complete_workflow_activation/2`
  # mediates a real Rustler NIF that requires a live bridge worker. To
  # keep this unit test isolated from the bridge + Temporal cluster,
  # `WorkflowEvaluator` accepts an optional `:complete_fn` arg
  # documented on the module as a test affordance: it overrides the
  # default `&BridgeHolder.complete_workflow_activation/2` with a
  # caller-supplied 2-arity function. Production callers do not pass
  # this key.

  # ---------------------------------------------------------------------------
  # Fixture workflows
  # ---------------------------------------------------------------------------

  defmodule ImmediateWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input), do: {:ok, :done}
  end

  defmodule RaisingWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(_input), do: raise("workflow body explodes")
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp init_job(input) do
    %WorkflowActivationJob{
      variant:
        {:initialize_workflow,
         %InitializeWorkflow{
           workflow_type: "TestWf",
           workflow_id: "wf-1",
           arguments: [synthetic_payload(input)]
         }}
    }
  end

  defp synthetic_payload(data) do
    %Temporal.Api.Common.V1.Payload{
      metadata: %{"encoding" => "json/plain"},
      data: Jason.encode!(data)
    }
  end

  defp activation_bytes(jobs, opts) do
    Protobuf.encode(%WorkflowActivation{
      run_id: Keyword.get(opts, :run_id, "test-run"),
      is_replaying: Keyword.get(opts, :is_replaying, false),
      jobs: jobs
    })
  end

  defp send_complete_fn(target) do
    fn task_queue, bytes ->
      send(target, {:completion_shipped, task_queue, bytes})
      :ok
    end
  end

  defp constant_resolver(module), do: fn _activation -> module end

  # ---------------------------------------------------------------------------
  # 1. Happy path
  # ---------------------------------------------------------------------------

  test "happy path: decodes activation, runs workflow, ships completion bytes" do
    test_pid = self()
    task_queue = "tq-test-#{System.unique_integer([:positive])}"

    args = %{
      run_id: "happy-run-1",
      task_queue: task_queue,
      activation_bytes: activation_bytes([init_job("hello")], run_id: "happy-run-1"),
      workflow_module_resolver: constant_resolver(ImmediateWorkflow),
      complete_fn: send_complete_fn(test_pid)
    }

    {:ok, pid} = WorkflowEvaluator.start_link(args)
    ref = Process.monitor(pid)

    assert_receive {:completion_shipped, ^task_queue, bytes}, 1_000
    # :normal or :noproc — both indicate the evaluator finished cleanly
    # (Process.monitor synthesises :noproc if the pid already exited).
    assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 1_000
    assert reason in [:normal, :noproc]

    completion = WorkflowActivationCompletion.decode(bytes)
    assert %WorkflowActivationCompletion{run_id: "happy-run-1"} = completion

    assert {:successful, %Coresdk.WorkflowCompletion.Success{commands: [cmd]}} =
             completion.status

    assert {:complete_workflow_execution, _cwe} = cmd.variant
  end

  # ---------------------------------------------------------------------------
  # 2. Bridge-error path: log + exit :normal; Core's per-activation watchdog
  # timeout drives redelivery (no double-log from a raise + OTP Task crash dump)
  # ---------------------------------------------------------------------------

  test "Bridge error path: complete_fn returns :error → evaluator logs + exits :normal" do
    test_pid = self()
    task_queue = "tq-test-bridge-err-#{System.unique_integer([:positive])}"

    failing_complete_fn = fn ^task_queue, _bytes ->
      send(test_pid, :complete_called)
      {:error, :bridge_unavailable}
    end

    args = %{
      run_id: "bridge-err-run-1",
      task_queue: task_queue,
      activation_bytes: activation_bytes([init_job(nil)], run_id: "bridge-err-run-1"),
      workflow_module_resolver: constant_resolver(ImmediateWorkflow),
      complete_fn: failing_complete_fn
    }

    Process.flag(:trap_exit, true)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        {:ok, pid} = WorkflowEvaluator.start_link(args)
        ref = Process.monitor(pid)

        assert_receive :complete_called, 1_000
        # :normal or :noproc — both indicate the evaluator finished cleanly
        # (Process.monitor synthesises :noproc if the pid already exited).
        assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 1_000
        assert reason in [:normal, :noproc]
      end)

    assert log =~ "BridgeHolder.complete_workflow_activation failed"
    assert log =~ "bridge_unavailable"
    assert log =~ "Core will redeliver"
  end

  test "workflow body raises -> ships workflow-task failure (park) cleanly (does NOT crash evaluator)" do
    test_pid = self()
    task_queue = "tq-test-raise-#{System.unique_integer([:positive])}"

    args = %{
      run_id: "raise-run-1",
      task_queue: task_queue,
      activation_bytes: activation_bytes([init_job(nil)], run_id: "raise-run-1"),
      workflow_module_resolver: constant_resolver(RaisingWorkflow),
      complete_fn: send_complete_fn(test_pid)
    }

    {:ok, pid} = WorkflowEvaluator.start_link(args)
    ref = Process.monitor(pid)

    assert_receive {:completion_shipped, ^task_queue, bytes}, 1_000
    # :normal or :noproc — both indicate the evaluator finished cleanly
    # (Process.monitor synthesises :noproc if the pid already exited).
    assert_receive {:DOWN, ^ref, :process, ^pid, reason}, 1_000
    assert reason in [:normal, :noproc]

    completion = WorkflowActivationCompletion.decode(bytes)

    # Uncaught raise parks the workflow as a workflow-task failure (server retries).
    assert {:failed,
            %Coresdk.WorkflowCompletion.Failure{
              failure: %Temporal.Api.Failure.V1.Failure{message: msg}
            }} = completion.status

    assert msg =~ "workflow body explodes"
  end

  # ---------------------------------------------------------------------------
  # 3. Shared-DynSup spawning
  # ---------------------------------------------------------------------------

  test "DynSup.start_child/1 spawns an evaluator with the given args" do
    test_pid = self()
    task_queue = "tq-test-dynsup-#{System.unique_integer([:positive])}"

    args = %{
      run_id: "dynsup-run-1",
      task_queue: task_queue,
      activation_bytes: activation_bytes([init_job(:dynsup_input)], run_id: "dynsup-run-1"),
      workflow_module_resolver: constant_resolver(ImmediateWorkflow),
      complete_fn: send_complete_fn(test_pid)
    }

    {:ok, pid} = DynamicSupervisor.start_child(args)
    assert is_pid(pid)

    assert_receive {:completion_shipped, ^task_queue, bytes}, 1_000

    completion = WorkflowActivationCompletion.decode(bytes)
    assert %WorkflowActivationCompletion{run_id: "dynsup-run-1"} = completion
  end
end
