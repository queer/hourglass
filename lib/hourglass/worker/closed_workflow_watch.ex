# credo:disable-for-this-file Credo.Check.Refactor.ModuleDependencies
#
# It joins the executors, the cancel registry, the runner's reports and the
# Temporal client the Worker asks about runs: each is a dependency it has to
# have.
defmodule Hourglass.Worker.ClosedWorkflowWatch do
  @moduledoc false
  # Ends the activities a Worker runs whose workflow run has ended, within the
  # Worker's `:closed_workflow_bound`. `Hourglass.Worker`'s moduledoc
  # ("Activities of a closed workflow") is the contract, and says why the
  # Worker asks Temporal rather than waiting for Core to tell it.
  #
  # A run has ended for an activity once it has closed, or, for an activity
  # that opts in (`use Hourglass.Activity, ends_on_cancel_request: true`), once
  # a cancel is requested of it.
  #
  # Every quarter of the bound, one check asks Temporal about each run that has
  # an activity running on the queue (`Hourglass.Worker.ClosedRuns`); a check
  # still waiting for its answer when the next is due is left to finish, so
  # checks never pile up. Each activity of a run that has ended is marked
  # cancelled (its `heartbeat/0` answers `:cancel`), and one still running half
  # the bound later is killed and reported to Temporal.
  #
  # Before an activity starts, its executor asks about its run itself
  # (`run_state/2`), through the watch's client.

  use GenServer

  # credo:disable-for-next-line Credo.Check.Readability.AliasAs
  alias Hourglass.ActivityExecutor.DynamicSupervisor, as: Executors
  alias Hourglass.Activity.CancelRegistry
  alias Hourglass.ActivityRunner
  alias Hourglass.Client
  alias Hourglass.Worker.ClosedRuns

  require Logger

  # The bound when neither the Worker's options nor `config :hourglass,
  # Hourglass.Worker` set `:closed_workflow_bound`.
  @default_bound 5_000

  # The Worker asks Temporal every quarter of the bound; a quarter much under
  # this would hammer it.
  @min_bound 100

  # Half the bound is a timer's length, and a timer is at most 2^32 - 1 ms.
  @max_bound 2 * 4_294_967_295

  # However short the bound, Temporal is given this long to answer before a
  # describe (or opening the client) is given up on.
  @min_answer_time 1_000

  # How long an activity's start waits for Temporal to say whether its run has
  # ended (`run_state/2`): a start waits no longer than this, whatever the
  # bound; a run Temporal has not answered for by then has not ended.
  @ask_time 1_000

  # A check whose answer has not come this long after it was due (which only
  # a fault on the native side could cause) is given up on, so the watch never
  # stops checking.
  @answer_slack 5_000

  # The reason an activity is marked cancelled with when its run has closed,
  # and when (opting in) a cancel is requested of its run.
  @closed :workflow_closed
  @cancel_requested :workflow_cancel_requested

  @doc false
  @spec default_bound() :: pos_integer()
  def default_bound, do: @default_bound

  @doc false
  @spec min_bound() :: pos_integer()
  def min_bound, do: @min_bound

  @doc false
  @spec max_bound() :: pos_integer()
  def max_bound, do: @max_bound

  # The bound a Worker started with `opts` keeps: the option, else config,
  # else the default. Public as `Hourglass.Worker.closed_workflow_bound/1`.
  @spec bound(keyword()) :: pos_integer()
  def bound(opts) do
    bound =
      Keyword.get_lazy(opts, :closed_workflow_bound, fn ->
        :hourglass
        |> Application.get_env(Hourglass.Worker, [])
        |> Keyword.get(:closed_workflow_bound, @default_bound)
      end)

    if is_integer(bound) and bound >= @min_bound and bound <= @max_bound do
      bound
    else
      raise ArgumentError,
            ":closed_workflow_bound must be an integer of milliseconds, from " <>
              "#{@min_bound} to #{@max_bound}, got: #{inspect(bound)}"
    end
  end

  # The watch of the Worker for `task_queue`, started with the Worker's
  # `opts`, keeping `bound`. Linked to the caller.
  @spec start_link(String.t(), pos_integer(), keyword()) :: GenServer.on_start()
  def start_link(task_queue, bound, opts) do
    namespace = Keyword.get(opts, :namespace, Client.default_namespace())
    target_url = Keyword.get(opts, :target_url, Client.default_target_url())
    config = ClosedRuns.client_config(namespace, target_url)

    GenServer.start_link(__MODULE__, %{task_queue: task_queue, bound: bound, config: config})
  end

  @doc false
  # What Temporal says of `run` now, asked through the watch of
  # `task_queue`'s Worker before an activity of `run` starts. `:open` when
  # there is no watch, or Temporal has not answered within `@ask_time`
  # (a client still to be opened included), whatever the bound.
  @spec run_state(String.t(), ClosedRuns.run()) :: ClosedRuns.state()
  def run_state(task_queue, run) do
    give_up_at = now() + @ask_time

    with [{watch, _value} | _rest] <-
           Registry.lookup(Executors.registry(), {__MODULE__, task_queue}),
         {:ok, client} <- GenServer.call(watch, :client, @ask_time) do
      ClosedRuns.state(client, run, max(give_up_at - now(), 1))
    else
      _no_watch_or_client -> :open
    end
  catch
    :exit, _watch_gone -> :open
  end

  @impl GenServer
  def init(%{task_queue: task_queue, bound: bound, config: config}) do
    {:ok, _owner} = Registry.register(Executors.registry(), {__MODULE__, task_queue}, nil)
    check_every = div(bound, 4)

    state = %{
      task_queue: task_queue,
      check_every: check_every,
      stop_after: div(bound, 2),
      answer_time: max(check_every, @min_answer_time),
      config: config,
      # Opened when first needed, and kept.
      client: nil,
      # The client being opened, if it is: `{tag, give_up_at}`.
      opening: nil,
      # Who waits for the client being opened: callers of `run_state/2`, and
      # `:check` when a check is to follow.
      awaiting_client: [],
      # The check waiting for its answer, if any: `{tag, give_up_at}`.
      check: nil,
      # The activities marked cancelled and not yet stopped:
      # task token => {executor pid, workflow id, run id, reason}.
      ending: %{}
    }

    schedule_check(state)
    {:ok, state}
  end

  @impl GenServer
  def handle_call(:client, _from, %{client: client} = state) when client != nil,
    do: {:reply, {:ok, client}, state}

  def handle_call(:client, from, state), do: {:noreply, open(state, from)}

  @impl GenServer
  def handle_info(:check, state) do
    schedule_check(state)

    state =
      state
      |> give_up_overdue_check()
      |> give_up_overdue_open()

    {:noreply, check(state)}
  end

  def handle_info({:stop, token}, state) do
    {ending, rest} = Map.pop(state.ending, token)

    with {pid, workflow_id, run_id, reason} <- ending,
         true <- Executors.running_activity?(token, pid) do
      metadata = %{task_queue: state.task_queue, workflow_id: workflow_id, run_id: run_id}
      stop(token, pid, reason, metadata)
    end

    CancelRegistry.clear(token)
    {:noreply, %{state | ending: rest}}
  end

  def handle_info(message, state) do
    {:noreply, answered(ClosedRuns.answer(message), state)}
  end

  defp schedule_check(state), do: Process.send_after(self(), :check, state.check_every)

  defp give_up_overdue_check(%{check: {_tag, give_up_at}} = state) do
    if now() > give_up_at do
      Logger.warning(
        "closed-workflow check on task_queue=#{state.task_queue} got no answer; gave it up"
      )

      %{state | check: nil}
    else
      state
    end
  end

  defp give_up_overdue_check(state), do: state

  defp give_up_overdue_open(%{opening: {_tag, give_up_at}} = state) do
    if now() > give_up_at, do: opened(state, :error), else: state
  end

  defp give_up_overdue_open(state), do: state

  # Start a check, unless one is waiting for its answer or nothing is running.
  defp check(%{check: nil} = state) do
    case runs(state) do
      [] -> state
      runs -> ask(runs, state)
    end
  end

  defp check(state), do: state

  # With no client yet, open one first; the check is made once it is open.
  defp ask(_runs, %{client: nil} = state), do: open(state, :check)

  defp ask(runs, state) do
    case ClosedRuns.describe(state.client, runs, state.answer_time) do
      {:ok, tag} ->
        count = length(runs)
        within = ClosedRuns.answer_within(count, state.answer_time)
        %{state | check: {tag, now() + within + @answer_slack}}

      :error ->
        # A client the native side no longer knows (a NIF reload): open anew.
        %{state | client: nil}
    end
  end

  # Open the client for `waiter` (`:check`, or a caller of `run_state/2`),
  # unless it is already being opened.
  defp open(%{opening: nil} = state, waiter) do
    case ClosedRuns.open(state.config, state.answer_time) do
      {:ok, tag} ->
        opening = {tag, now() + state.answer_time + @answer_slack}
        %{state | opening: opening, awaiting_client: [waiter | state.awaiting_client]}

      :error ->
        opened(%{state | awaiting_client: [waiter | state.awaiting_client]}, :error)
    end
  end

  defp open(state, waiter), do: %{state | awaiting_client: [waiter | state.awaiting_client]}

  # The client opened, or could not be: answer the callers waiting for it,
  # and make the check waiting for it.
  defp opened(state, result) do
    {checks, callers} = Enum.split_with(state.awaiting_client, &(&1 == :check))

    client =
      case result do
        {:ok, client} -> client
        :error -> nil
      end

    state = %{state | client: client, opening: nil, awaiting_client: []}
    answer = if client, do: {:ok, client}, else: :error
    Enum.each(callers, &GenServer.reply(&1, answer))
    if client != nil and checks != [], do: check(state), else: state
  end

  defp answered({:opened, tag, result}, %{opening: {tag, _give_up_at}} = state),
    do: opened(state, result)

  defp answered({:described, tag, states}, %{check: {tag, _give_up_at}} = state) do
    %{state | check: nil, ending: cancel(states, state)}
  end

  # An answer to a check given up on, or a message that is no answer.
  defp answered(_other, state), do: state

  # The runs that have an activity running on the queue, but for those whose
  # activities are already being ended.
  defp runs(state) do
    for {_pid, token, run, _opted_in} <- Executors.running_activities(state.task_queue),
        not Map.has_key?(state.ending, token),
        uniq: true,
        do: run
  end

  # Mark each activity whose run has ended (`states`) cancelled, and have it
  # stopped if it is still running half the bound from now. Returns
  # `state.ending` with them added.
  defp cancel(states, state) do
    states = Map.new(states)

    for {pid, token, run, opted_in} <- Executors.running_activities(state.task_queue),
        reason = ended(Map.get(states, run), opted_in),
        reason != nil,
        not Map.has_key?(state.ending, token),
        into: state.ending do
      {workflow_id, run_id} = run
      CancelRegistry.mark(token, reason)
      metadata = %{task_queue: state.task_queue, workflow_id: workflow_id, run_id: run_id}
      :telemetry.execute([:hourglass, :activity, reason], %{count: 1}, metadata)
      Process.send_after(self(), {:stop, token}, state.stop_after)
      {token, {pid, workflow_id, run_id, reason}}
    end
  end

  @doc false
  # Whether an activity may start: it may unless its run has ended (`ended/2`),
  # which is asked of Temporal now (`run_state/2`). One that may not is
  # reported to Temporal as ended for that reason, and
  # `[:hourglass, :activity, :not_started]` is emitted.
  # (`Hourglass.ActivityExecutor.DynamicSupervisor`, "An activity of an ended
  # run never starts".) Returns, for one that may start, its run (nil if its
  # task names none) and whether it ends on a cancel request.
  @spec admit(String.t(), binary(), Coresdk.ActivityTask.Start.t()) ::
          {:start, ClosedRuns.run() | nil, boolean()} | :ended
  def admit(task_queue, token, start) do
    opted_in = ActivityRunner.ends_on_cancel_request?(start.activity_type)

    run = run_of(start)

    with {workflow_id, run_id} <- run,
         reason when reason != nil <- ended(run_state(task_queue, run), opted_in) do
      metadata = %{
        task_queue: task_queue,
        workflow_id: workflow_id,
        run_id: run_id,
        reason: reason
      }

      :telemetry.execute([:hourglass, :activity, :not_started], %{count: 1}, metadata)
      report_not_started(token, task_queue, reason)
      :ended
    else
      _not_ended -> {:start, run, opted_in}
    end
  end

  # The run (`{workflow_id, run_id}`) an activity task's `start` belongs to, or
  # nil if it names none.
  defp run_of(%{workflow_execution: %{workflow_id: workflow_id, run_id: run_id}})
       when workflow_id != "" and run_id != "",
       do: {workflow_id, run_id}

  defp run_of(_start), do: nil

  defp report_not_started(token, task_queue, reason) do
    case ActivityRunner.report_cancelled(token, task_queue, reason) do
      :ok ->
        :ok

      {:error, err} ->
        Logger.error(
          "reporting an activity that did not start failed: #{inspect(err)} " <>
            "(task_queue=#{task_queue})"
        )
    end
  end

  @doc false
  # Why an activity of a run in `state` has ended, if it has: its run closed,
  # or (when the activity opted in) a cancel was requested of it.
  @spec ended(ClosedRuns.state() | nil, boolean()) :: atom() | nil
  def ended(:closed, _opted_in), do: @closed
  def ended(:cancel_requested, true), do: @cancel_requested
  def ended(_state, _opted_in), do: nil

  # Kill the activity, which had its chance to end by itself, and report it
  # so Core lets go of the task. The report goes out in its own process: one
  # stuck on the network must not hold up the watch.
  defp stop(token, pid, reason, metadata) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    end

    :telemetry.execute([:hourglass, :activity, reason, :stopped], %{count: 1}, metadata)

    Logger.warning(
      "stopped an activity of the run #{metadata.run_id} of workflow " <>
        "#{metadata.workflow_id} on task_queue=#{metadata.task_queue} " <>
        "(#{inspect(reason)}): it was still running when half the worker's " <>
        "closed_workflow_bound had passed"
    )

    {:ok, _reporter} = Task.start(fn -> report_cancelled(token, metadata.task_queue, reason) end)
    :ok
  end

  defp report_cancelled(token, task_queue, reason) do
    ActivityRunner.report_cancelled(token, task_queue, reason)
  catch
    :exit, _holder_gone -> :ok
  end

  defp now, do: System.monotonic_time(:millisecond)
end
