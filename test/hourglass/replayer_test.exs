defmodule Hourglass.ReplayerTest do
  # async: true — Temporal singletons live globally in test_helper.exs; each
  # test scopes its queue + workflow_id by UUID for cluster-side isolation.
  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.Client
  alias Hourglass.Replay.Mismatch
  alias Hourglass.TestSupport.EchoWorkflow

  @moduletag :temporal
  # Real-cluster Hourglass.Replayer tests — capture history from a
  # live workflow run, then assert replay equivalence. The capture step
  # requires a real cluster; replay itself is pure but the captured-
  # history fixtures aren't checked into tree, so the test is end-to-end.
  # :integration so default suite stays fast.
  @moduletag :integration
  @moduletag timeout: 60_000

  # TamperedEchoWorkflow: executes the activity TWICE instead of once.
  # When replayed against a history that only recorded ONE activity execution,
  # the Temporal Core replayer detects the command-sequence divergence and
  # raises a nondeterminism error.
  defmodule TamperedEchoWorkflow do
    use Hourglass.Workflow

    @impl Hourglass.Workflow.Behaviour
    def run(args) do
      # First call (extra/tampered — not in the original single-activity history)
      _first =
        execute_activity!(Hourglass.TestSupport.EchoActivity, args,
          start_to_close_timeout: 30_000
        )

      # Second call mirrors what EchoWorkflow does — schedules a second activity
      # command that diverges from the single-activity history.
      value =
        execute_activity!(Hourglass.TestSupport.EchoActivity, args,
          start_to_close_timeout: 30_000
        )

      %{"ok" => true, "value" => value}
    end
  end

  # Fetches raw proto-encoded GetWorkflowExecutionHistoryResponse bytes for a
  # given workflow ID via a direct Client.connect + Client.fetch_history call.
  # This is the binary form that Hourglass.Replayer.replay_history/2 expects.
  defp fetch_history_bytes(workflow_id) do
    with {:ok, client} <- Client.connect([]) do
      Client.fetch_history(client, workflow_id)
    end
  end

  test "Replayer accepts a real Echo history" do
    queue = "replayer-ok-#{System.unique_integer([:positive])}"

    {:ok, _worker_pid} =
      start_supervised({Hourglass.Worker, task_queue: queue})

    nonce = UUIDv7.generate()
    workflow_id = "replay-echo:#{nonce}"

    {:ok, handle} =
      Hourglass.start(EchoWorkflow, %{nonce: nonce},
        id: workflow_id,
        task_queue: queue
      )

    {:ok, _result} = Hourglass.result(handle, timeout: 30_000)

    {:ok, history_bytes} = fetch_history_bytes(workflow_id)
    assert :ok = Hourglass.Replayer.replay_history(history_bytes, EchoWorkflow)
  end

  test "Replayer rejects a tampered workflow" do
    queue = "replayer-tamper-#{System.unique_integer([:positive])}"

    {:ok, _worker_pid} =
      start_supervised({Hourglass.Worker, task_queue: queue})

    nonce = UUIDv7.generate()
    workflow_id = "replay-tamper:#{nonce}"

    {:ok, handle} =
      Hourglass.start(EchoWorkflow, %{nonce: nonce},
        id: workflow_id,
        task_queue: queue
      )

    {:ok, _result} = Hourglass.result(handle, timeout: 30_000)

    # Fetch the EchoWorkflow history, but replay it against TamperedEchoWorkflow
    # which schedules two activities. The single-activity history causes a
    # command-sequence mismatch (nondeterminism), which the replayer detects.
    {:ok, history_bytes} = fetch_history_bytes(workflow_id)

    assert {:error, %Mismatch{}} =
             Hourglass.Replayer.replay_history(history_bytes, TamperedEchoWorkflow)
  end
end
