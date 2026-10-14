# credo:disable-for-this-file Credo.Check.Refactor.ModuleDependencies
#
# It joins the executors, the cancel registry, the runner's reports and the
# Temporal client the Worker asks about runs: each is a dependency it has to
# have.
defmodule Hourglass.Worker.ClosedWorkflowWatch do
  @moduledoc false
  # Ends the activities a Worker runs whose workflow run has closed, within the
  # Worker's `:closed_workflow_bound`. `Hourglass.Worker`'s moduledoc
  # ("Activities of a closed workflow") is the contract, and says why the
  # Worker asks Temporal rather than waiting for Core to tell it.
  #
  # Every quarter of the bound, one check asks Temporal about each run that has
  # an activity running on the queue (`Hourglass.Worker.ClosedRuns`); a check
  # still waiting for its answer when the next is due is left to finish, so
  # checks never pile up. Each activity of a run that has closed is marked
  # cancelled (its `heartbeat/0` answers `:cancel`), and one still running half
  # the bound later is killed and reported to Temporal as cancelled.

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

  # A check whose answer has not come this long after it was due (which only
  # a fault on the native side could cause) is given up on, so the watch never
  # stops checking.
  @answer_slack 5_000

  # The reason an activity of a closed run is marked cancelled with, and
  # reported to Temporal as cancelled for.
  @reason :workflow_closed

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

  @impl GenServer
  def init(%{task_queue: task_queue, bound: bound, config: config}) do
    check_every = div(bound, 4)

    state = %{
      task_queue: task_queue,
      check_every: check_every,
      stop_after: div(bound, 2),
      answer_time: max(check_every, @min_answer_time),
      config: config,
      # Opened by the first check, and kept.
      client: nil,
      # The check waiting for its answer, if any: `{tag, give_up_at}`.
      check: nil,
      # The activities marked cancelled and not yet stopped:
      # task token => {executor pid, workflow id, run id}.
      ending: %{}
    }

    schedule_check(state)
    {:ok, state}
  end

  @impl GenServer
  def handle_info(:check, state) do
    schedule_check(state)
    state = give_up_overdue_check(state)
    {:noreply, check(state)}
  end

  def handle_info({:stop, token}, state) do
    {ending, rest} = Map.pop(state.ending, token)

    with {pid, workflow_id, run_id} <- ending,
         true <- Executors.running_activity?(token, pid) do
      stop(token, pid, %{task_queue: state.task_queue, workflow_id: workflow_id, run_id: run_id})
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

  # Start a check, unless one is waiting for its answer or nothing is running.
  defp check(%{check: nil} = state) do
    case runs(state) do
      [] -> state
      runs -> ask(runs, state)
    end
  end

  defp check(state), do: state

  # With no client yet, open one first; the check goes on once it is open.
  defp ask(_runs, %{client: nil} = state) do
    case ClosedRuns.open(state.config, state.answer_time) do
      {:ok, tag} -> %{state | check: {tag, now() + state.answer_time + @answer_slack}}
      :error -> state
    end
  end

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

  defp answered({:opened, tag, {:ok, client}}, %{check: {tag, _give_up_at}} = state) do
    check(%{state | client: client, check: nil})
  end

  defp answered({:opened, tag, :error}, %{check: {tag, _give_up_at}} = state) do
    %{state | check: nil}
  end

  defp answered({:described, tag, closed}, %{check: {tag, _give_up_at}} = state) do
    %{state | check: nil, ending: cancel(closed, state)}
  end

  # An answer to a check given up on, or a message that is no answer.
  defp answered(_other, state), do: state

  # The runs that have an activity running on the queue, but for those whose
  # activities are already being ended.
  defp runs(state) do
    for {_pid, token, workflow_id, run_id} <- Executors.running_activities(state.task_queue),
        not Map.has_key?(state.ending, token),
        uniq: true,
        do: {workflow_id, run_id}
  end

  # Mark each activity of the `closed` runs cancelled, and have it stopped if
  # it is still running half the bound from now. Returns `state.ending` with
  # them added.
  defp cancel(closed, state) do
    for {pid, token, workflow_id, run_id} <- Executors.running_activities(state.task_queue),
        {workflow_id, run_id} in closed,
        not Map.has_key?(state.ending, token),
        into: state.ending do
      CancelRegistry.mark(token, @reason)

      :telemetry.execute([:hourglass, :activity, :workflow_closed], %{count: 1}, %{
        task_queue: state.task_queue,
        workflow_id: workflow_id,
        run_id: run_id
      })

      Process.send_after(self(), {:stop, token}, state.stop_after)
      {token, {pid, workflow_id, run_id}}
    end
  end

  # Kill the activity, which had its chance to end by itself, and report it
  # cancelled so Core lets go of the task. The report goes out in its own
  # process: one stuck on the network must not hold up the watch.
  defp stop(token, pid, metadata) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    end

    :telemetry.execute([:hourglass, :activity, :workflow_closed, :stopped], %{count: 1}, metadata)

    Logger.warning(
      "stopped an activity of the closed run #{metadata.run_id} of workflow " <>
        "#{metadata.workflow_id} on task_queue=#{metadata.task_queue}: it was still " <>
        "running when the worker's closed_workflow_bound ran out"
    )

    {:ok, _reporter} = Task.start(fn -> report_cancelled(token, metadata.task_queue) end)
    :ok
  end

  defp report_cancelled(token, task_queue) do
    ActivityRunner.report_cancelled(token, task_queue, @reason)
  catch
    :exit, _holder_gone -> :ok
  end

  defp now, do: System.monotonic_time(:millisecond)
end
