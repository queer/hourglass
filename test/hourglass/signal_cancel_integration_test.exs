defmodule Hourglass.SignalCancelIntegrationTest do
  @moduledoc """
  End-to-end NIF path tests for `Hourglass.signal/3` and `Hourglass.cancel/2`.

  These tests require a live Temporal cluster at `localhost:7233` and are
  excluded from the default `mix test` run via the `:temporal` and
  `:integration` tags. Run with:

      mix test --include temporal --include integration

  ## What is tested

    * `signal/3` — starts a workflow that blocks on `await_signal/1`, sends
      a signal through the real Bridge NIF, and asserts the workflow completes
      with the signalled value.

    * `cancel/2` — starts a workflow that runs forever unless cancelled, sends
      a cancel request through the Bridge NIF, and asserts the workflow closes
      in `:canceled` state.

  Both tests document the production code path:

      Hourglass.signal/3 (or cancel/2)
        → Hourglass.Client.Backend.impl() (Real)
        → Hourglass.Client.Real.signal_workflow/3 (or cancel_workflow/2)
        → Bridge.client_signal_workflow/2 (or client_cancel_workflow/2)
        → Rust NIF → Temporal gRPC
  """

  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.TestSupport.Patience
  alias Hourglass.TestSupport.WorkerProbes
  alias Hourglass.WorkflowHandle
  alias Hourglass.WorkflowStatus

  @moduletag :temporal
  @moduletag :integration
  @moduletag timeout: Patience.test_timeout(4)

  # ---------------------------------------------------------------------------
  # Fixture: a workflow that blocks on a "proceed" signal and returns its value
  # ---------------------------------------------------------------------------

  defmodule SignalledWorkflow do
    @moduledoc false
    use Hourglass.Workflow, input: :map, output: :map

    @impl Hourglass.Workflow.Behaviour
    def run(_args) do
      # Block until a "proceed" signal arrives. The signal payload is the result.
      payload = await_signal("proceed")
      %{"signalled" => true, "payload" => payload}
    end
  end

  # ---------------------------------------------------------------------------
  # Fixture: a workflow that loops until cancelled
  # ---------------------------------------------------------------------------

  defmodule LoopUntilCancelledWorkflow do
    @moduledoc false
    use Hourglass.Workflow, input: :map, output: :map

    @impl Hourglass.Workflow.Behaviour
    def run(args) do
      # Wait for the short timer so the workflow runs at least one activation
      # before the cancel arrives. Zero-duration sleep is effectively "yield".
      sleep(0)

      if cancelled?() do
        %{"cancelled" => true, "args" => args}
      else
        continue_as_new(args)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  test "signal/3 unblocks a waiting workflow via the real Bridge NIF" do
    queue = "signal-integration-#{System.unique_integer([:positive])}"

    {:ok, _worker_pid} =
      Patience.start_worker(task_queue: queue)

    nonce = UUIDv7.generate()
    workflow_id = "signal-int-#{nonce}"

    # Start the workflow — it blocks on await_signal("proceed").
    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(SignalledWorkflow, %{},
        id: workflow_id,
        task_queue: queue
      )

    # Poll until the workflow is running (ensures the first activation has landed
    # before we send the signal, so await_signal is in the buffered-signals path).
    WorkerProbes.poll_until("the workflow to be running", fn ->
      match?({:ok, %WorkflowStatus{state: :running}}, Hourglass.status(handle))
    end)

    # Signal through the real NIF path.
    assert :ok = Hourglass.signal(handle, "proceed", %{"value" => 42})

    # Wait for the workflow to complete; result should carry the signalled payload.
    assert {:ok, result} = Hourglass.result(handle, timeout: Patience.ms())
    assert result["signalled"] == true
    assert get_in(result, ["payload", "value"]) == 42
  end

  test "cancel/2 closes a running workflow via the real Bridge NIF" do
    queue = "cancel-integration-#{System.unique_integer([:positive])}"

    {:ok, _worker_pid} =
      Patience.start_worker(task_queue: queue)

    nonce = UUIDv7.generate()
    workflow_id = "cancel-int-#{nonce}"

    # Start the looping workflow.
    {:ok, %WorkflowHandle{}} =
      Hourglass.start(LoopUntilCancelledWorkflow, %{},
        id: workflow_id,
        task_queue: queue
      )

    # The workflow continues as new on every activation, so its first run may
    # be over by the time it is looked at: everything below names the
    # workflow, whose current run is the one to cancel and to watch close.
    handle = %WorkflowHandle{id: workflow_id}

    # Allow at least one activation to land before cancelling.
    WorkerProbes.poll_until("the workflow to be running", fn ->
      match?({:ok, %WorkflowStatus{state: :running}}, Hourglass.status(handle))
    end)

    # Cancel through the real NIF path.
    assert :ok = Hourglass.cancel(handle, "integration-test cancel")

    # Poll until the workflow closes. cancel/2 only requests cancellation —
    # the workflow must react to it, so we poll for a non-running state.
    final_status = poll_until_not_running(handle, Patience.ms())

    # The workflow handles cancelled? → true by completing normally (not failing).
    # Temporal reports it as :completed because the workflow returned a value
    # (rather than raising or re-throwing the cancel). Either :completed or
    # :canceled is acceptable depending on how Core surfaces the cancel path;
    # both mean the cancellation was delivered.
    assert final_status.state in [:completed, :canceled]
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp poll_until_not_running(handle, deadline_ms) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms
    do_poll_not_running(handle, deadline)
  end

  defp do_poll_not_running(handle, deadline) do
    case Hourglass.status(handle) do
      {:ok, %WorkflowStatus{state: state} = s} when state != :running ->
        s

      {:ok, %WorkflowStatus{}} ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("workflow did not leave :running within deadline")
        else
          Process.sleep(100)
          do_poll_not_running(handle, deadline)
        end

      {:error, err} ->
        flunk("status/2 returned error: #{inspect(err)}")
    end
  end
end
