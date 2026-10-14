defmodule Hourglass.TestSupport.GatedWork do
  @moduledoc false
  # Runs one `GatedActivity` on the task queue the input names, which may not be
  # the workflow's own. On a "more" signal it also schedules an `EchoActivity`
  # on that queue while the first is still running, so a test can see whether
  # a worker that is stopping takes a task scheduled after it began to stop.
  #
  # `attempts` is the activity's `max_attempts` (default 1: no retry), so a test
  # can see whether a failure reported for the activity reached Temporal at once.
  # `stubborn` runs `StubbornGatedActivity` in place of `GatedActivity`.
  use Hourglass.Workflow, input: :map, output: :map

  alias Hourglass.TestSupport.EchoActivity
  alias Hourglass.TestSupport.GatedActivity
  alias Hourglass.TestSupport.StubbornGatedActivity

  @impl Hourglass.Workflow.Behaviour
  def run(%{"gate" => gate, "queue" => queue} = args) do
    activity = if args["stubborn"], do: StubbornGatedActivity, else: GatedActivity

    gated =
      async(fn ->
        execute_activity(activity, %{"gate" => gate},
          task_queue: queue,
          start_to_close_timeout: 120_000,
          retry_policy: [max_attempts: Map.get(args, "attempts", 1), initial_interval: 100]
        )
      end)

    _more = await_signal("more")

    echoed =
      execute_activity!(EchoActivity, %{"nonce" => gate},
        task_queue: queue,
        start_to_close_timeout: 120_000
      )

    %{"gated" => inspect(await(gated)), "echoed" => echoed}
  end
end

defmodule Hourglass.TestSupport.GatedDecision do
  @moduledoc false
  # Holds its first workflow task open at the gate, then completes. A test
  # stops the worker while the task is held to see whether the task's
  # completion still reaches Temporal.
  use Hourglass.Workflow, input: :map, output: :map

  alias Hourglass.TestSupport.RaceGate

  @impl Hourglass.Workflow.Behaviour
  def run(%{"gate" => gate}) do
    :ok = RaceGate.hold(gate)
    %{"decided" => gate}
  end
end

defmodule Hourglass.TestSupport.StubbornGatedActivity do
  @moduledoc false
  # `GatedActivity`, but trapping exits, so a `:shutdown` exit does not end it.
  use Hourglass.Activity, input: :map, output: :map

  alias Hourglass.TestSupport.RaceGate

  @impl Hourglass.Activity.Behaviour
  def execute(%{"gate" => gate}) do
    Process.flag(:trap_exit, true)
    :ok = RaceGate.hold({gate, :activity})
    %{"worked" => gate}
  end
end

defmodule Hourglass.TestSupport.FailsFirstAttempt do
  @moduledoc false
  # Fails its first attempt (retryably), and succeeds on any later one.
  use Hourglass.Activity, input: :map, output: :map

  @impl Hourglass.Activity.Behaviour
  def execute(_args) do
    case Hourglass.Activity.info().attempt do
      1 -> raise "the first attempt fails"
      attempt -> %{"attempt" => attempt}
    end
  end
end

defmodule Hourglass.TestSupport.RetriedWork do
  @moduledoc false
  # Runs `FailsFirstAttempt` on the queue the input names, retrying after a
  # pause long enough for a test to stop that queue's worker in between.
  use Hourglass.Workflow, input: :map, output: :map

  alias Hourglass.TestSupport.FailsFirstAttempt

  @impl Hourglass.Workflow.Behaviour
  def run(%{"queue" => queue}) do
    execute_activity!(FailsFirstAttempt, %{},
      task_queue: queue,
      start_to_close_timeout: 120_000,
      retry_policy: [max_attempts: 3, initial_interval: 3_000, backoff_coefficient: 1.0]
    )
  end
end
