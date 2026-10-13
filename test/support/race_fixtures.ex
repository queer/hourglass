defmodule Hourglass.TestSupport.RaceGate do
  @moduledoc false
  # A test-only latch that lets an integration test hold a workflow task (or an
  # activity) open at a chosen point, so an event the test sends lands INSIDE
  # that task rather than before or after it. Forcing a race this way makes it
  # deterministic: the test decides the interleaving instead of hoping for it.
  #
  # A gate is a `:global` name the test arms. `hold/1` blocks only while the
  # name is registered, so a body that passes the gate again after the test
  # disarmed it — a replay, say — runs straight through. A holder also stops
  # holding if the test process dies, so a failed test never strands a worker.
  #
  # `:global` lets one pid hold only one name and a test arms several gates, so
  # each name is held by a small proxy, linked to the test process, that knows
  # the test's pid.
  #
  # `report/2` uses the same lookup as a probe: a body sends what it saw to the
  # test that armed the key, and does nothing if none did.

  @spec arm(term()) :: :ok
  def arm(key) do
    owner = self()
    ack = make_ref()

    spawn_link(fn ->
      :yes = :global.register_name({__MODULE__, key}, self())
      send(owner, ack)
      proxy(owner)
    end)

    receive do
      ^ack -> :ok
    end
  end

  @spec disarm(term()) :: :ok
  def disarm(key) do
    case whereis(key) do
      :undefined ->
        :ok

      proxy ->
        # Unregister here, synchronously, so the very next `hold/1` already
        # finds the gate open.
        :global.unregister_name({__MODULE__, key})
        send(proxy, {__MODULE__, :stop})
        :ok
    end
  end

  defp proxy(owner) do
    receive do
      {__MODULE__, :stop} ->
        :ok

      {__MODULE__, :hold, key, holder, ref} ->
        send(holder, {__MODULE__, :owner, ref, owner})
        send(owner, {__MODULE__, :reached, key, holder, ref})
        proxy(owner)

      {__MODULE__, :observed, _key, _observation} = message ->
        send(owner, message)
        proxy(owner)
    end
  end

  @spec hold(term()) :: :ok
  def hold(key) do
    case whereis(key) do
      :undefined ->
        :ok

      proxy ->
        ref = make_ref()
        proxy_ref = Process.monitor(proxy)
        send(proxy, {__MODULE__, :hold, key, self(), ref})

        receive do
          {__MODULE__, :owner, ^ref, owner} ->
            Process.demonitor(proxy_ref, [:flush])
            await_release(ref, Process.monitor(owner))

          {:DOWN, ^proxy_ref, :process, _pid, _reason} ->
            # Disarmed between the lookup and the hold: the gate is open.
            :ok
        end
    end
  end

  defp await_release(ref, owner_ref) do
    receive do
      {__MODULE__, :release, ^ref} ->
        Process.demonitor(owner_ref, [:flush])
        :ok

      {:DOWN, ^owner_ref, :process, _pid, _reason} ->
        :ok
    end
  end

  # Test side: wait for a body to reach the gate. Returns the token `release/1`
  # takes. The gate stays armed until `disarm/1`, so whoever reaches it next
  # blocks too — disarm BEFORE releasing unless that is what the test wants.
  @spec await_held(term(), timeout()) :: {pid(), reference()}
  def await_held(key, timeout \\ 20_000) do
    receive do
      {__MODULE__, :reached, ^key, pid, ref} -> {pid, ref}
    after
      timeout -> raise "nothing reached race gate #{inspect(key)} within #{timeout}ms"
    end
  end

  @spec release({pid(), reference()}) :: :ok
  def release({pid, ref}) do
    send(pid, {__MODULE__, :release, ref})
    :ok
  end

  @spec report(term(), term()) :: :ok
  def report(key, observation) do
    case whereis(key) do
      :undefined -> :ok
      proxy -> send(proxy, {__MODULE__, :observed, key, observation})
    end

    :ok
  end

  defp whereis(key), do: :global.whereis_name({__MODULE__, key})
end

defmodule Hourglass.TestSupport.GatedActivity do
  @moduledoc false
  # Stays running for as long as the test holds its gate, then completes. The
  # test releases it at the moment it wants the completion to race a workflow
  # task.
  use Hourglass.Activity, input: :map, output: :map

  @impl Hourglass.Activity.Behaviour
  def execute(%{"gate" => gate}) do
    :ok = Hourglass.TestSupport.RaceGate.hold({gate, :activity})
    %{"worked" => gate}
  end
end

defmodule Hourglass.TestSupport.SignalledChild do
  @moduledoc false
  # A child that finishes when the test signals it, so the test chooses the
  # moment its completion lands on the parent.
  use Hourglass.Workflow, input: :map, output: :map

  @impl Hourglass.Workflow.Behaviour
  def run(_args), do: %{"go" => await_signal("go")}
end

defmodule Hourglass.TestSupport.RacedEnd do
  @moduledoc false
  # Ends its run as soon as ANY of these has happened — a "stop" signal
  # arrived, a cancel was requested, or one of the optional operations the
  # input asks for (an activity, a timer, a child workflow) finished — and
  # reports every one of them it saw. Holding the gate once it has decided to
  # end lets a test land a second event inside the workflow task that carries
  # the terminal command, which Temporal rejects as UnhandledCommand.
  #
  # Cancellation ends the run as FAILED (via fail/3), everything else as
  # completed, so the race is exercised against both terminal commands.
  use Hourglass.Workflow, input: :map, output: :map

  alias Hourglass.TestSupport.GatedActivity
  alias Hourglass.TestSupport.RaceGate
  alias Hourglass.TestSupport.SignalledChild

  @impl Hourglass.Workflow.Behaviour
  def run(%{"gate" => gate} = args) do
    scopes = start_operations(args)

    seen =
      scopes
      |> Map.new(fn {what, scope} -> {what, outcome(scope)} end)
      |> Map.put("cancelled", cancelled?())

    if Enum.all?(seen, fn {_what, saw} -> saw in [nil, false] end) do
      # Nothing has happened yet: suspend until something does.
      await(scopes["stop"])
    end

    :ok = RaceGate.hold(gate)

    if seen["cancelled"] do
      fail("Cancelled", "cancel requested", details: seen)
    else
      seen
    end
  end

  # Started one after another, in this order, so every activation issues the
  # same command ids.
  defp start_operations(%{"gate" => gate} = args) do
    work = if args["work"], do: async(fn -> run_gated_activity(gate) end)
    timer = if args["timer_ms"], do: async(fn -> sleep(args["timer_ms"]) end)
    child = if args["child"], do: async(fn -> run_child(args["child"]) end)
    stop = async(fn -> await_signal("stop") end)

    %{"work" => work, "timer" => timer, "child" => child, "stop" => stop}
  end

  defp run_gated_activity(gate) do
    execute_activity!(GatedActivity, %{"gate" => gate}, start_to_close_timeout: 60_000)
  end

  defp run_child(child_id), do: execute_child!(SignalledChild, %{}, id: child_id)

  defp outcome({:hourglass_temporal_async, {:done, value}}), do: value
  defp outcome(_pending_or_absent), do: nil
end

defmodule Hourglass.TestSupport.RacedContinue do
  @moduledoc false
  # Continues as new on a "stop" signal — unless a cancel had arrived, which
  # fails the run instead. The second generation completes at once.
  use Hourglass.Workflow, input: :map, output: :map

  alias Hourglass.TestSupport.RaceGate

  @impl Hourglass.Workflow.Behaviour
  def run(%{"gen" => 2, "carried" => carried}), do: %{"gen" => 2, "carried" => carried}

  def run(%{"gate" => gate}) do
    stop = await_signal("stop")
    cancelled = cancelled?()
    :ok = RaceGate.hold(gate)

    if cancelled do
      fail("Cancelled", "cancel requested", details: %{"stop" => stop})
    else
      continue_as_new(%{"gen" => 2, "carried" => stop})
    end
  end
end

defmodule Hourglass.TestSupport.ActivityThenSignal do
  @moduledoc false
  # Runs one activity, then waits for a signal. A worker that restarts in
  # between has to replay the activity from history.
  use Hourglass.Workflow, input: :map, output: :map

  @impl Hourglass.Workflow.Behaviour
  def run(%{"nonce" => nonce}) do
    echoed =
      execute_activity!(Hourglass.TestSupport.EchoActivity, %{"nonce" => nonce},
        start_to_close_timeout: 30_000
      )

    %{"echoed" => echoed, "next" => await_signal("next")}
  end
end

defmodule Hourglass.TestSupport.CancelObserver do
  @moduledoc false
  # Reports what cancelled?/0 answers each time the body reaches it, so a test
  # can compare what the original execution saw against what a replay of that
  # execution's history sees. Each "tick" signal moves the body one step on; it
  # ends at the first step that observes the cancel.
  use Hourglass.Workflow, input: :map, output: :map

  alias Hourglass.TestSupport.RaceGate

  @impl Hourglass.Workflow.Behaviour
  def run(%{"probe" => probe}), do: step(probe, 1)

  defp step(probe, n) do
    _tick = await_signal("tick")
    cancelled = cancelled?()
    :ok = RaceGate.report(probe, [n, cancelled])

    if cancelled do
      %{"steps" => n}
    else
      step(probe, n + 1)
    end
  end
end
