defmodule Hourglass.StatusTest do
  @moduledoc """
  Synchronous workflow-state query API.

  Verifies that `Hourglass.status/2` returns a populated
  `Hourglass.WorkflowStatus` for the four primary lifecycle states
  (running / completed / failed) and that the `failures: :include`
  opt-in correctly walks history for ActivityTaskFailed events.
  """

  # async: true — Hourglass.{Runtime, WorkerRegistry} are started
  # once globally in test_helper.exs. Per-test isolation
  # comes from unique task-queue + workflow_id; Temporal routes on
  # those independently of Elixir process identity, so concurrent
  # test files don't collide.
  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.Bridge
  alias Hourglass.Client
  alias Hourglass.Error
  alias Hourglass.TestSupport.EchoWorkflow
  alias Hourglass.WorkflowHandle
  alias Hourglass.WorkflowStatus
  alias Temporal.Api.Failure.V1.Failure

  @moduletag :temporal
  @moduletag timeout: 60_000
  # Several tests intentionally trigger workflow exceptions (FailingWorkflow,
  # RetryingFailingWorkflow). capture_log keeps that noise out of test
  # output unless a test fails.
  @moduletag capture_log: true

  # --------------------------------------------------------------------------
  # Test fixtures: a workflow that propagates activity failure to workflow
  # failure (EchoWorkflow swallows {:error, _}, which is wrong for testing
  # the :failed state).
  # --------------------------------------------------------------------------

  defmodule FailingActivity do
    @moduledoc false
    use Hourglass.Activity

    @impl Hourglass.Activity.Behaviour
    # :not_found is classified :non_retryable by RetryClassifier, so the
    # SDK won't reschedule the activity. The single ActivityTaskFailed event
    # is what `failures: :include` should surface.
    def execute(_args), do: {:error, :not_found}
  end

  defmodule FailingWorkflow do
    @moduledoc false
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(args) do
      # Under the park-not-die model, uncaught exceptions park the workflow
      # forever. Return a business-failure value instead so the workflow
      # completes; the ActivityTaskFailed event remains in history and is
      # surfaced via failures: :include.
      case execute_activity(
             Hourglass.StatusTest.FailingActivity,
             args,
             start_to_close_timeout: 30_000
           ) do
        {:ok, value} -> value
        {:error, reason} -> %{"ok" => false, "reason" => inspect(reason)}
      end
    end
  end

  defmodule RetryingFailingActivity do
    @moduledoc false
    use Hourglass.Activity

    @impl Hourglass.Activity.Behaviour
    # :rate_limited is classified :retryable, so the SDK will retry up to
    # the call-site `max_attempts`. With a tight initial_interval, the test
    # observes the FINAL ActivityTaskFailed event whose linked
    # ActivityTaskStarted carries the resolved attempt count.
    def execute(_args), do: {:error, :rate_limited}
  end

  defmodule RetryingFailingWorkflow do
    @moduledoc false
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(args) do
      # Same park-not-die idiom: return a business-failure value so the
      # workflow completes. The retry_policy drives 3 ActivityTaskFailed
      # events in history; attempt-count assertions still hold.
      case execute_activity(
             Hourglass.StatusTest.RetryingFailingActivity,
             args,
             start_to_close_timeout: 10_000,
             retry_policy: [
               max_attempts: 3,
               # Test asserts the FINAL attempt count, not backoff timing.
               # 1ms intervals keep retry semantics intact while removing the
               # ~200ms wall the test would otherwise pay between attempts.
               initial_interval: 1,
               backoff_coefficient: 1.0,
               max_interval: 1
             ]
           ) do
        {:ok, value} -> value
        {:error, reason} -> %{"ok" => false, "reason" => inspect(reason)}
      end
    end
  end

  # --------------------------------------------------------------------------
  # Tests
  # --------------------------------------------------------------------------

  test "status/2 reports :running for a started workflow with no worker" do
    # No worker is registered for this task queue, so the workflow stays
    # in WORKFLOW_EXECUTION_STATUS_RUNNING with a scheduled (un-picked-up)
    # workflow task. This is the fastest deterministic way to observe the
    # :running state.
    queue = "status-running-#{System.unique_integer([:positive])}"
    workflow_id = "status-running-#{UUIDv7.generate()}"

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(EchoWorkflow, %{nonce: "n"},
        id: workflow_id,
        task_queue: queue
      )

    assert {:ok, %WorkflowStatus{} = status} = Hourglass.status(handle)

    assert status.state == :running
    assert is_list(status.pending_activities)
    assert status.close_time == nil
    assert %DateTime{} = status.start_time
    assert status.recent_failures == nil
  end

  test "status/2 reports :completed after EchoWorkflow finishes" do
    queue = "status-completed-#{System.unique_integer([:positive])}"

    {:ok, _worker_pid} =
      start_supervised({Hourglass.Worker, task_queue: queue})

    nonce = UUIDv7.generate()
    workflow_id = "status-completed-#{nonce}"

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(EchoWorkflow, %{nonce: nonce},
        id: workflow_id,
        task_queue: queue
      )

    {:ok, _result} = Hourglass.result(handle, timeout: 30_000)

    assert {:ok, %WorkflowStatus{} = status} = Hourglass.status(handle)

    assert status.state == :completed
    assert %DateTime{} = status.start_time
    assert %DateTime{} = status.close_time
    assert status.pending_activities == []
    assert status.recent_failures == nil
  end

  test "status/2 completes with a business-failure result; the activity failure is surfaced via failures: :include" do
    # Under park-not-die, FailingWorkflow returns a business-failure map
    # instead of raising, so the workflow reaches :completed. The
    # ActivityTaskFailed event stays in history and is visible via
    # failures: :include.
    queue = "status-failed-#{System.unique_integer([:positive])}"

    {:ok, _worker_pid} =
      start_supervised({Hourglass.Worker, task_queue: queue})

    workflow_id = "status-failed-#{UUIDv7.generate()}"

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(FailingWorkflow, %{},
        id: workflow_id,
        task_queue: queue
      )

    _result = Hourglass.result(handle, timeout: 30_000)

    status = poll_until_closed(handle, 30_000)

    assert status.state == :completed
    assert %DateTime{} = status.close_time

    # Activity failure must be surfaced via failures: :include even though
    # the workflow itself completed successfully.
    assert {:ok, %WorkflowStatus{} = status_with_failures} =
             Hourglass.status(handle, failures: :include)

    assert is_list(status_with_failures.recent_failures)
    refute status_with_failures.recent_failures == []

    [failure | _rest] = status_with_failures.recent_failures
    assert failure.activity_type == Atom.to_string(FailingActivity)
  end

  test "status/2 default failures: :exclude leaves recent_failures nil" do
    # Behaviour test (we can't trivially intercept NIF calls): the default
    # path returns nil for recent_failures regardless of workflow state.
    # This pairs with the failures: :include test below to cover the
    # "doesn't walk history when not asked" contract.
    queue = "status-exclude-#{System.unique_integer([:positive])}"
    workflow_id = "status-exclude-#{UUIDv7.generate()}"

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(EchoWorkflow, %{nonce: "n"},
        id: workflow_id,
        task_queue: queue
      )

    assert {:ok, %WorkflowStatus{recent_failures: nil}} = Hourglass.status(handle)

    assert {:ok, %WorkflowStatus{recent_failures: nil}} =
             Hourglass.status(handle, failures: :exclude)
  end

  test "status/2 with failures: :include populates recent_failures for a workflow that handled a failing activity" do
    queue = "status-include-fail-#{System.unique_integer([:positive])}"

    {:ok, _worker_pid} =
      start_supervised({Hourglass.Worker, task_queue: queue})

    workflow_id = "status-include-fail-#{UUIDv7.generate()}"

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(FailingWorkflow, %{},
        id: workflow_id,
        task_queue: queue
      )

    _result = Hourglass.result(handle, timeout: 30_000)
    _status = poll_until_closed(handle, 30_000)

    assert {:ok, %WorkflowStatus{} = status} =
             Hourglass.status(handle, failures: :include)

    assert is_list(status.recent_failures)
    refute status.recent_failures == []

    # The new wire format encodes activity_type as Atom.to_string(module),
    # e.g. "Elixir.Hourglass.StatusTest.FailingActivity".
    [failure | _rest] = status.recent_failures

    expected_activity_type = Atom.to_string(FailingActivity)

    assert failure.activity_type == expected_activity_type,
           "expected activity_type #{inspect(expected_activity_type)}, " <>
             "got #{inspect(failure.activity_type)}"

    assert %DateTime{} = failure.event_time
    assert %Failure{} = failure.failure
  end

  test "status/2 with failures: :include returns [] for a successful workflow" do
    queue = "status-include-ok-#{System.unique_integer([:positive])}"

    {:ok, _worker_pid} =
      start_supervised({Hourglass.Worker, task_queue: queue})

    nonce = UUIDv7.generate()
    workflow_id = "status-include-ok-#{nonce}"

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(EchoWorkflow, %{nonce: nonce},
        id: workflow_id,
        task_queue: queue
      )

    {:ok, _result} = Hourglass.result(handle, timeout: 30_000)

    assert {:ok, %WorkflowStatus{} = status} =
             Hourglass.status(handle, failures: :include)

    assert status.state == :completed
    assert status.recent_failures == []
  end

  test "status/2 with failures: :include surfaces the final attempt count for retried activity" do
    # Activity always fails with a retryable shape; with max_attempts: 3 the
    # SDK reschedules twice before giving up. The resulting ActivityTaskFailed
    # event's linked ActivityTaskStarted event records attempt=3 — proves
    # resolve_attempt/2 is reading the real attempt from history, not a
    # hardcoded 1.
    queue = "status-retry-#{System.unique_integer([:positive])}"

    {:ok, _worker_pid} =
      start_supervised({Hourglass.Worker, task_queue: queue})

    workflow_id = "status-retry-#{UUIDv7.generate()}"

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(RetryingFailingWorkflow, %{},
        id: workflow_id,
        task_queue: queue
      )

    _result = Hourglass.result(handle, timeout: 30_000)
    _status = poll_until_closed(handle, 30_000)

    assert {:ok, %WorkflowStatus{recent_failures: failures}} =
             Hourglass.status(handle, failures: :include)

    assert is_list(failures)
    assert failures != []

    [%{attempt: attempt} | _rest] = failures
    assert attempt == 3, "expected final attempt count 3, got #{inspect(attempt)}"
  end

  test "status/2 returns {:error, :not_found} for a non-existent workflow" do
    handle = %WorkflowHandle{
      id: "does-not-exist-#{UUIDv7.generate()}",
      run_id: ""
    }

    assert {:error, %Error{reason: :not_found}} = Hourglass.status(handle)
  end

  # Sanity: Bridge.client_describe_workflow_execution is a valid, loaded NIF.
  # Catches accidental bridge regressions.
  test "Bridge.client_describe_workflow_execution/3 is loaded" do
    {:ok, client} = Client.connect()

    workflow_id = "bridge-describe-#{UUIDv7.generate()}"

    {:ok, _handle} =
      Hourglass.start(EchoWorkflow, %{nonce: "n"},
        id: workflow_id,
        task_queue: "bridge-describe-q"
      )

    assert {:ok, bytes} =
             Bridge.client_describe_workflow_execution(client, workflow_id, "")

    assert is_binary(bytes)
    assert byte_size(bytes) > 0
  end

  # --------------------------------------------------------------------------
  # Helpers
  # --------------------------------------------------------------------------

  # Polls status/2 until the workflow has a close_time (i.e. is no longer
  # running). Used in failure tests because await/2 may return before the
  # server-side state transitions to a non-running status.
  defp poll_until_closed(handle, deadline_ms) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms
    do_poll_closed(handle, deadline)
  end

  defp do_poll_closed(handle, deadline) do
    case Hourglass.status(handle) do
      {:ok, %WorkflowStatus{state: state} = s} when state != :running ->
        s

      {:ok, %WorkflowStatus{}} ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("workflow did not close within deadline")
        else
          Process.sleep(50)
          do_poll_closed(handle, deadline)
        end

      {:error, %Error{} = err} ->
        flunk("status/2 returned error: #{inspect(err)}")
    end
  end
end
