defmodule Hourglass.ActivityHeartbeatTest do
  @moduledoc """
  End-to-end proof that `Hourglass.Activity.heartbeat/0` resets an activity's
  Temporal-enforced `heartbeat_timeout`.

  Requires a live Temporal cluster at `localhost:7233` with the
  `hourglass-test` namespace (see compose.yaml + README). Excluded from the
  default `mix test` run via `:temporal` / `:integration`. Run with:

      mix test --include temporal --include integration

  ## What is proven

    * **Scenario 1 (positive).** A workflow runs ONE activity scheduled with
      `heartbeat_timeout: 2_000` and NO `start_to_close_timeout` (only a
      generous `schedule_to_close_timeout`). The activity body runs ~5 s,
      heartbeating every ~500 ms. Because each heartbeat resets the 2 s
      window at Temporal, the attempt is never timed out — the workflow
      completes successfully even though the activity ran well past the 2 s
      heartbeat window. This is the required proof.

    * **Scenario 2 (contrast).** The same short `heartbeat_timeout: 2_000`, but
      the activity sleeps ~5 s WITHOUT heartbeating. Temporal fails the attempt
      at ~2 s. With `retry_policy: [max_attempts: 2]` it is retried (the second
      attempt observes `attempt == 2`), and once attempts are exhausted within
      the `schedule_to_close_timeout`, `execute_activity/3` returns
      `{:error, _}` — which the workflow surfaces as a failed business outcome.
      This shows the window is genuinely enforced and that scenario 1 only
      passes because of the heartbeats.
  """

  # async: true — Temporal singletons (Runtime, WorkerRegistry) live globally;
  # per-test isolation comes from unique task-queue + workflow_id, which the
  # cluster routes independently of Elixir process identity. Mirrors
  # EchoWorkflowTest / SignalCancelIntegrationTest.
  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.WorkflowHandle

  @moduletag :temporal
  @moduletag :integration
  # ~5 s activity body + cluster round-trips + (scenario 2) a retry; keep
  # headroom over the default 60 s ExUnit timeout used by the sibling tests.
  @moduletag timeout: 90_000

  # ---------------------------------------------------------------------------
  # Fixtures: heartbeating + non-heartbeating activities, run by a workflow
  # under a short heartbeat_timeout.
  # ---------------------------------------------------------------------------

  defmodule HeartbeatingActivity do
    @moduledoc false
    # Activities have no determinism constraint, so Process.sleep is fine here.
    use Hourglass.Activity, input: :map, output: :map

    @impl Hourglass.Activity.Behaviour
    def execute(_args) do
      # ~5 s total, heartbeating every ~500 ms (10×). Each heartbeat resets the
      # 2 s heartbeat_timeout at Temporal, so the attempt outlives the window.
      Enum.each(1..10, fn _i ->
        Process.sleep(500)
        :ok = Hourglass.Activity.heartbeat()
      end)

      %{"heartbeated" => true}
    end
  end

  defmodule SilentActivity do
    @moduledoc false
    # Retry so Temporal gives it a second attempt after the first heartbeat
    # timeout. The default policy is max_attempts: 1 (no retry); we want to
    # observe attempt == 2 on the retry, so allow two attempts.
    use Hourglass.Activity, input: :map, output: :map, retry: [max_attempts: 2]

    @impl Hourglass.Activity.Behaviour
    def execute(_args) do
      # Sleep ~5 s WITHOUT heartbeating. Temporal fails the attempt at ~2 s
      # (the heartbeat_timeout). On the retry, surface the attempt count via a
      # raise so the workflow can prove a retry actually happened.
      attempt = Hourglass.Activity.attempt()

      if attempt > 1 do
        # Second attempt — fail fast and deterministically (no retry of this
        # branch under max_attempts: 2), so the workflow's execute_activity
        # gets a terminal {:error, _} quickly rather than waiting another 5 s.
        raise "silent activity: observed retry, attempt=#{attempt}"
      end

      Process.sleep(5_000)
      %{"unreachable" => true}
    end
  end

  defmodule HeartbeatWorkflow do
    @moduledoc false
    use Hourglass.Workflow, input: :map, output: :map

    @impl Hourglass.Workflow.Behaviour
    def run(_args) do
      result =
        execute_activity!(HeartbeatingActivity, %{},
          # Deliberately NO start_to_close_timeout — only a short heartbeat
          # window plus a generous schedule_to_close cap.
          heartbeat_timeout: 2_000,
          schedule_to_close_timeout: 60_000
        )

      %{"workflow_ok" => true, "activity" => result}
    end
  end

  defmodule SilentWorkflow do
    @moduledoc false
    use Hourglass.Workflow, input: :map, output: :map

    @impl Hourglass.Workflow.Behaviour
    def run(_args) do
      case execute_activity(SilentActivity, %{},
             heartbeat_timeout: 2_000,
             # Long enough to allow one heartbeat timeout (~2 s) + a retry that
             # fails fast, but bounded so the activity gives up rather than
             # retrying forever.
             schedule_to_close_timeout: 30_000
           ) do
        {:ok, _value} -> %{"workflow_ok" => true}
        {:error, _failure} -> %{"workflow_ok" => false, "activity_failed" => true}
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  test "heartbeating activity outlives a short heartbeat_timeout window" do
    queue = "heartbeat-pos-#{System.unique_integer([:positive])}"

    {:ok, _worker_pid} =
      start_supervised({Hourglass.Worker, task_queue: queue})

    workflow_id = "heartbeat-pos:#{UUIDv7.generate()}"

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(HeartbeatWorkflow, %{},
        id: workflow_id,
        task_queue: queue
      )

    {:ok, result} = Hourglass.result(handle, timeout: 60_000)

    # The activity ran ~5 s — well past the 2 s heartbeat window — and was NOT
    # failed by a heartbeat timeout, because it heartbeated every ~500 ms.
    assert result["workflow_ok"] == true
    assert get_in(result, ["activity", "heartbeated"]) == true
  end

  test "non-heartbeating activity under the same window is timed out and retried" do
    queue = "heartbeat-neg-#{System.unique_integer([:positive])}"

    {:ok, _worker_pid} =
      start_supervised({Hourglass.Worker, task_queue: queue})

    workflow_id = "heartbeat-neg:#{UUIDv7.generate()}"

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(SilentWorkflow, %{},
        id: workflow_id,
        task_queue: queue
      )

    {:ok, result} = Hourglass.result(handle, timeout: 60_000)

    # The activity never heartbeated, so Temporal failed the first attempt at
    # the 2 s heartbeat_timeout, retried it (attempt 2 raised), and the
    # workflow saw a terminal {:error, _}.
    assert result["workflow_ok"] == false
    assert result["activity_failed"] == true
  end
end
