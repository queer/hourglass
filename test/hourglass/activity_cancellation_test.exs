defmodule Hourglass.ActivityCancellationTest do
  @moduledoc """
  End-to-end proof that a CANCELLED activity STOPS (no zombie) — the anti-zombie regression for
  the cancellation-propagation feature, reproducing the real production trigger: HEARTBEAT TIMEOUT.

  The activity's tick interval (1.5 s) exceeds its `heartbeat_timeout` (1 s), so Temporal times the
  attempt out; on the next heartbeat the server reports the cancellation, Core delivers a Cancel
  activity task (same `task_token` as the Start), the runner marks it in
  `Hourglass.Activity.CancelRegistry`, and the activity's next `heartbeat!/0` raises `Cancelled`
  and the loop stops. Before this feature the abandoned attempt ran on as a zombie, bumping a
  side-effect counter long after Temporal gave up on it.

  This also verifies the design's keying assumption end-to-end: if the Cancel task's `task_token`
  did NOT equal the Start's, the registry mark would miss and the activity would keep ticking.

  Requires a live Temporal cluster on localhost:7233 with the `hourglass-test` namespace
  (compose.yaml). Excluded from the default lane via `:temporal` / `:integration`. Run with:

      mix test --include temporal --include integration test/hourglass/activity_cancellation_test.exs
  """
  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.WorkflowHandle

  @moduletag :temporal
  @moduletag :integration
  @moduletag timeout: 120_000

  # A side-effect counter (named Agent) proves whether the body kept running after cancellation.
  defmodule Marker do
    use Agent

    def start_link(_opts), do: Agent.start_link(fn -> 0 end, name: __MODULE__)
    def bump, do: Agent.update(__MODULE__, &(&1 + 1))
    def count, do: Agent.get(__MODULE__, & &1)
  end

  defmodule SlowHeartbeatActivity do
    # max_attempts: 1 — no retry, so the timed-out attempt is the only one running (a second
    # attempt would muddy the marker). schedule_to_close keeps the abandoned attempt's wall-clock
    # budget open so a zombie WOULD keep ticking absent cancellation.
    use Hourglass.Activity, input: :map, output: :map, retry: [max_attempts: 1]

    @impl Hourglass.Activity.Behaviour
    def execute(_args) do
      # 1.5 s tick > 1 s heartbeat_timeout → the attempt heartbeat-times-out, Core delivers a
      # Cancel task, and the next heartbeat!/0 raises Cancelled → the loop stops.
      Enum.each(1..200, fn _i ->
        Process.sleep(1_500)
        Hourglass.ActivityCancellationTest.Marker.bump()
        Hourglass.Activity.heartbeat!()
      end)

      %{"finished_without_cancel" => true}
    end
  end

  defmodule SlowHeartbeatWorkflow do
    use Hourglass.Workflow, input: :map, output: :map

    @impl Hourglass.Workflow.Behaviour
    def run(_args) do
      execute_activity(SlowHeartbeatActivity, %{},
        heartbeat_timeout: 1_000,
        schedule_to_close_timeout: 120_000
      )
    end
  end

  test "a heartbeat-timed-out activity is cancelled and stops (no zombie)" do
    {:ok, _pid} = start_supervised(Marker)

    # Capture the cancel_received telemetry so we can tell "Core never delivered a Cancel" apart
    # from "delivered but the registry keying missed".
    handler = "cancel-recv-#{System.unique_integer([:positive])}"
    me = self()

    :telemetry.attach(
      handler,
      [:hourglass, :activity, :cancel_received],
      fn _e, _m, meta, _config -> send(me, {:cancel_received, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    queue = "cancel-#{System.unique_integer([:positive])}"
    {:ok, _worker} = start_supervised({Hourglass.Worker, task_queue: queue})
    workflow_id = "cancel:#{UUIDv7.generate()}"

    {:ok, %WorkflowHandle{}} =
      Hourglass.start(SlowHeartbeatWorkflow, %{}, id: workflow_id, task_queue: queue)

    # Core delivered a Cancel task for the timed-out attempt (proves the trigger fired at all).
    assert_receive {:cancel_received, _},
                   30_000,
                   "Core never delivered a Cancel activity task for the heartbeat-timed-out attempt"

    # Give the activity a couple more tick intervals to observe the mark and unwind.
    Process.sleep(5_000)
    after_cancel = Marker.count()
    Process.sleep(5_000)

    assert Marker.count() == after_cancel,
           "activity kept ticking after cancellation (zombie): #{after_cancel} -> #{Marker.count()}"
  end
end
