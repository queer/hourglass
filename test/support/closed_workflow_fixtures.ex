defmodule Hourglass.TestSupport.EndsOnCancel do
  @moduledoc false
  # Runs one activity and returns its outcome. With `ends_on_cancel`, a cancel
  # ends the run at once (it completes, cancelled), leaving the activity
  # running; without it, the body never asks `cancelled?/0`, so a cancel leaves
  # the run waiting on the activity.
  #
  # `heeding` runs `HeedingActivity` in place of `GatedActivity`.
  use Hourglass.Workflow, input: :map, output: :map

  alias Hourglass.TestSupport.GatedActivity
  alias Hourglass.TestSupport.HeedingActivity

  @impl Hourglass.Workflow.Behaviour
  def run(%{"gate" => gate} = args) do
    if args["ends_on_cancel"] && cancelled?() do
      %{"cancelled" => true}
    else
      {activity, input} =
        if args["heeding"],
          do: {HeedingActivity, %{"gate" => gate, "cleanup_ms" => args["cleanup_ms"]}},
          else: {GatedActivity, %{"gate" => gate}}

      outcome = execute_activity(activity, input, start_to_close_timeout: 120_000)

      %{"outcome" => inspect(outcome)}
    end
  end
end

defmodule Hourglass.TestSupport.ClosingParent do
  @moduledoc false
  # Starts an `EndsOnCancel` child, whose activity a test holds, and ends in a
  # way that closes the child under it:
  #
  #   * "terminate" — the child is started with parent close policy TERMINATE,
  #     and the parent completes on a "close" signal, so Temporal terminates
  #     the child.
  #   * "time_out" — the child is started with a run timeout of `timeout_ms`
  #     and abandoned, so Temporal times it out.
  use Hourglass.Workflow, input: :map, output: :map

  alias Hourglass.TestSupport.EndsOnCancel

  @impl Hourglass.Workflow.Behaviour
  def run(%{"how" => "terminate", "child_id" => child_id} = args) do
    {:ok, _child} =
      start_child(EndsOnCancel, child_input(args), id: child_id, parent_close_policy: :terminate)

    %{"closed" => await_signal("close")}
  end

  def run(%{"how" => "time_out", "child_id" => child_id, "timeout_ms" => timeout_ms} = args) do
    {:ok, _child} =
      start_child(EndsOnCancel, child_input(args),
        id: child_id,
        parent_close_policy: :abandon,
        workflow_run_timeout: timeout_ms
      )

    %{"started" => child_id}
  end

  defp child_input(args), do: Map.take(args, ["gate", "heeding", "cleanup_ms"])
end

defmodule Hourglass.TestSupport.HeedingActivity do
  @moduledoc false
  # Heartbeats every 50 ms until it is told it is cancelled, then cleans up —
  # taking `cleanup_ms` (default 0) — and ends as cancelled. It reports to the
  # test, on the RaceGate key that is its gate, `{:running, pid, task_token}` as
  # it starts, `{:cancel_seen, reason}` once a heartbeat answers `:cancel`
  # (the reason `heartbeat!/0` raises with), and `:cleaned_up` once its
  # cleanup is done.
  use Hourglass.Activity, input: :map, output: :map

  alias Hourglass.TestSupport.RaceGate

  @impl Hourglass.Activity.Behaviour
  def execute(%{"gate" => gate} = args) do
    :ok = RaceGate.report(gate, {:running, self(), Hourglass.Activity.info().task_token})
    cancelled = await_cancel()
    :ok = RaceGate.report(gate, {:cancel_seen, cancelled.reason})
    Process.sleep(args["cleanup_ms"] || 0)
    :ok = RaceGate.report(gate, :cleaned_up)
    raise cancelled
  end

  defp await_cancel do
    Hourglass.Activity.heartbeat!()
    Process.sleep(50)
    await_cancel()
  rescue
    cancelled in Hourglass.Activity.Cancelled -> cancelled
  end
end

defmodule Hourglass.TestSupport.LongCall do
  @moduledoc false
  # Reports `{:running, run_id, pid}` to the test on the RaceGate key that is
  # its gate, then waits, never heartbeating, until the test sends it `:reply`.
  use Hourglass.Activity, input: :map, output: :map

  @impl Hourglass.Activity.Behaviour
  def execute(%{"gate" => gate}) do
    :ok =
      Hourglass.TestSupport.RaceGate.report(
        gate,
        {:running, Hourglass.Activity.info().run_id, self()}
      )

    receive do
      :reply -> %{"replied" => gate}
    end
  end
end

defmodule Hourglass.TestSupport.Generations do
  @moduledoc false
  # The first run starts a `LongCall` and, on a "next" signal, continues as new
  # with the call still running; the second run, of the same workflow id,
  # starts a `LongCall` of its own and returns its outcome.
  use Hourglass.Workflow, input: :map, output: :map

  alias Hourglass.TestSupport.LongCall

  @impl Hourglass.Workflow.Behaviour
  def run(%{"gate" => gate, "gen" => 1}) do
    _call =
      async(fn ->
        execute_activity(LongCall, %{"gate" => gate}, start_to_close_timeout: 120_000)
      end)

    _next = await_signal("next")
    continue_as_new(%{"gate" => gate, "gen" => 2})
  end

  def run(%{"gate" => gate, "gen" => 2}) do
    %{"gen2" => execute_activity!(LongCall, %{"gate" => gate}, start_to_close_timeout: 120_000)}
  end
end
