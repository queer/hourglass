# credo:disable-for-this-file Credo.Check.Refactor.ModuleDependencies
#
# The Worker starts and stops everything a task queue's serving is made of
# (the handle, the poll loops, the closed-workflow watch, the graceful stop):
# each is a dependency it has to have.
defmodule Hourglass.Worker do
  @moduledoc """
  Per-task-queue Worker — a `GenServer` that owns the bridge-handle
  registration for one task queue and runs a small inner Supervisor
  hosting the two poll loops driving Temporal Core.

  ## Options

    * `:task_queue` (required) — the task queue to serve.
    * `:graceful_shutdown_period` — milliseconds a stopping worker lets
      the activities it is running finish (see "Graceful stop"). Falls
      back to `config :hourglass, Hourglass.Worker,
      graceful_shutdown_period: ms`, then to 20 seconds. `0` reports
      every running activity as failed by the shutdown at once.
    * `:closed_workflow_bound` — milliseconds within which an activity
      whose workflow has closed is ended (see "Activities of a closed
      workflow"). Falls back to `config :hourglass, Hourglass.Worker,
      closed_workflow_bound: ms`, then to
      #{Hourglass.Worker.ClosedWorkflowWatch.default_bound()} ms.
    * `:namespace`, `:target_url`, `:max_cached_workflows`,
      `:max_outstanding_workflow_tasks`, `:max_outstanding_activities`,
      `:max_outstanding_local_activities` — passed to the bridge
      worker; see `Hourglass.BridgeHolder.register_worker/2`.

  ## Activities of a closed workflow

  An activity belongs to one run of its workflow. Once that run has
  closed — completed, failed, timed out, been terminated, ended because
  a cancel was requested of it, or continued as new — or Temporal no
  longer knows it, nobody is waiting for the activity: Temporal drops
  its outcome. The Worker ends such an activity within
  `:closed_workflow_bound` of the run closing:

    1. Every quarter of the bound, the Worker asks Temporal
       (`DescribeWorkflowExecution`, through a client of its own
       namespace and target) about each run that has an activity
       running on its queue, up to sixteen runs at a time. A check
       still waiting for its answers when the next is due is left to
       finish.
    2. Each activity of a run that has closed is cancelled:
       `Hourglass.Activity.heartbeat/0` answers `:cancel` from then on,
       and `heartbeat!/0` raises `Hourglass.Activity.Cancelled` (reason
       `:workflow_closed`). An activity that heeds it has half the
       bound to clean up and end by itself, reporting as it ends as
       usual. `[:hourglass, :activity, :workflow_closed]` is emitted.
    3. One still running half the bound after it was cancelled is
       killed, and reported to Temporal as cancelled, so Core lets go
       of the task. `[:hourglass, :activity, :workflow_closed,
       :stopped]` is emitted, and a warning logged.

  So an activity of a closed run is gone within three quarters of the
  bound, plus the time Temporal takes to answer, and one that heeds the
  cancel sees it within a quarter of the bound, plus that time and its
  own heartbeat interval. Only a closed run ends its activities: a
  cancel requested of a run that goes on running, because its body does
  not end on `cancelled?/0`, ends nothing.

  Nor does a Temporal that cannot be asked. Temporal is given a quarter
  of the bound (at least a second) to answer for each run; a run it has
  not answered for by then is taken as still running, and asked about
  again at a later check. The asking waits on no scheduler thread, so a
  Temporal that is slow, or has gone quiet, holds nothing of the
  Worker's, and delays its stop not at all.

  Temporal Core would tell the activity itself only through a heartbeat
  it sends — answered as not found once the run has closed — and sends
  those at most once per 0.8 of the heartbeat timeout (or per 30
  seconds without one), and never for an activity that does not
  heartbeat; hence the Worker asks.

  ## Why a GenServer + inner Supervisor (not just `use Supervisor`)

  This module needs `init/1` and `terminate/2` callbacks so it can
  bracket the `BridgeHolder` registration with the lifetime of the
  Worker. `use Supervisor` doesn't expose terminate; wrapping a
  Supervisor inside a trap-exits GenServer gives us the hook.

  Sequence on graceful start:

    1. `init/1` calls `BridgeHolder.register_worker(task_queue, opts)`.
       The Application-level holder allocates a fresh bridge worker
       handle and stores it keyed by task_queue.
    2. `init/1` starts the inner Supervisor with the two poll loops.
       The poll loops call `BridgeHolder.poll_*` against the now-live
       handle.

  ## Graceful stop

  A Worker is stopped gracefully when its supervisor stops it — by
  `Worker.Supervisor.stop_worker/1`, by `stop_supervised/1`, or as the
  application or the node shuts down. It then:

    1. **Stops taking new tasks.** `terminate/2` calls
       `BridgeHolder.initiate_shutdown/1`: Core stops polling the
       server, and the server stops handing this worker tasks. The
       handle stays registered. `[:hourglass, :worker, :stopping]` is
       emitted.
    2. **Lets the work it holds finish and report.** The poll loops keep
       polling the draining handle. Workflow tasks already handed out
       are evaluated and completed as usual, and so are tasks a poll
       already in flight received as the stop began. Running activities
       go on running, and report their outcome — completion or failure
       — to Temporal through the still-held handle, exactly as they
       would have. Each loop exits once Core says its side has drained:
       every workflow task and activity it handed out has been
       completed.
    3. **Bounds the wait with the grace.** Once `:graceful_shutdown_period`
       has passed since step 1, Core cancels every activity still
       running with reason `WORKER_SHUTDOWN`. The runner marks each
       cancelled, so an activity that heartbeats with
       `Hourglass.Activity.heartbeat!/0` stops at its next heartbeat,
       and acknowledges the cancel at once; Core reports the activity
       to Temporal as a **retryable failure of type `"WorkerShutdown"`**
       ("Worker is shutting down and this activity did not complete in
       time"). Temporal retries it right away, on whichever worker
       serves the queue, as its retry policy allows — it does not wait
       out the activity's start-to-close timeout. (Under the default
       policy, `max_attempts: 1`, the activity fails at once, and the
       workflow sees the failure at once.)
    4. **Stops what is left.** With both loops gone — or once the grace
       and a five-second margin have passed — the inner Supervisor is
       stopped, and every activity executor still running for the queue
       is stopped: sent a `:shutdown` exit, and killed two seconds later
       if it traps exits. Stopping it keeps it from racing its own
       retry. Each activity stopped is then reported to Temporal as the
       same retryable `"WorkerShutdown"` failure, through the handle
       that handed it out
       (`Hourglass.ActivityRunner.report_worker_shutdown/3`). For one
       Core already reported in step 3 this second report names a task
       Core no longer holds, and is dropped; for one Core had not — the
       margin ran out first — it is the report. The reports go out in
       parallel, bounded by what is left of the shutdown budget (less six
       seconds kept for step 5): one stuck on the network is given up
       on, and its activity left to its timeout, rather than pushing the
       stop past its shutdown timeout.
    5. **Lets go of Temporal.** `BridgeHolder.unregister_worker/1` drops
       the handle, and aborts and waits out any poll still on it — one
       the margin cut off is still parked in Core — so a stopped worker
       holds no dirty-IO scheduler thread. `[:hourglass, :worker, :stopped]` is emitted, with
       the stop's `duration` and the number of `abandoned_activities`
       stopped in step 4, and `drained: false` if step 4 came at the
       margin's end rather than when Core had drained.

  Stopping is therefore quick when nothing is running, and otherwise
  takes as long as the longest running activity, up to the grace (plus
  the moment Core takes to report the cancelled ones). `child_spec/1`
  gives the Worker a shutdown timeout of the grace plus fifteen seconds,
  so its supervisor waits for all of it.

  Workflow tasks are not cut off by the grace: evaluating one is quick
  and deterministic, so one in flight is completed in step 2. One that
  has still not been completed when the margin runs out (a completion
  stuck on the network, say) is lost with the handle, and the server
  retries it after the workflow task timeout (ten seconds by default).

  A Worker that is not stopped gracefully — killed outright, or a node
  that crashes — cannot do any of this: its activities' outcomes wait
  out their timeouts. `BridgeHolder` still drops a killed Worker's
  handle, so the queue can be served again.

  A Worker that stops because its inner Supervisor crashed has no poll
  loops left to drain with, so Core can hand out nothing more — its
  `WORKER_SHUTDOWN` cancels included. Its running activities still
  finish and report through the held handle; in place of steps 2 and 3
  it waits until none is running, up to the grace, and then goes on to
  step 4, which reports each one still running as failed by the
  shutdown. Either way each activity is reported within the grace (and
  the moment step 4 takes), never left to its start-to-close timeout.
  A workflow task in flight in this case is completed if its
  evaluation finishes in time, and otherwise retried by the server
  after the workflow task timeout.

  ## Children of the inner Supervisor (`:one_for_one`)

    1. `Hourglass.Worker.WorkflowPollLoop` — drives
       `BridgeHolder.poll_workflow_activation/1`; dispatches each
       activation to the shared
       `Hourglass.WorkflowEvaluator.DynamicSupervisor`.

    2. `Hourglass.Worker.ActivityPollLoop` — drives
       `BridgeHolder.poll_activity_task/1`; dispatches each task to
       the shared `Hourglass.ActivityExecutor.DynamicSupervisor`.

  Loops are independent — neither holds state the other depends on —
  so `:one_for_one` is the natural fit. Both are `:transient`: a
  graceful `:normal` exit (the bridge returned `:shutdown`) does not
  trigger a restart; only abnormal exits do.

  ## Why bridge ownership lives in this Worker (not Worker.Supervisor)

  Putting `register_worker` in `init/1` (rather than in
  `Worker.Supervisor.start_worker/1`) makes the OTP shape symmetric:
  the same process that allocated the handle also frees it on exit.
  Direct callers of `Worker.start_link/1` (typical in tests via
  `start_supervised/1`) get correct lifecycle without having to
  remember the bracket.

  ## Cascade-restart safety

  In-flight evaluator and executor Tasks live under shared
  Application-level DynSups
  (`Hourglass.WorkflowEvaluator.DynamicSupervisor`,
  `Hourglass.ActivityExecutor.DynamicSupervisor`), not under
  this Worker's tree. A Worker child crash never destroys those
  Tasks. They survive, finish their work, and ship completions via
  `BridgeHolder` (which they reach by name, not by holding a
  reference). If the handle is gone by the time one completes — a
  `BridgeHolder` restart — the Task sees
  `{:error, :worker_not_registered}` and Temporal retries the task
  after its timeout.

  ## Naming

  `start_link/1` registers the GenServer under
  `Hourglass.WorkerRegistry.via(task_queue)` for `via`-name
  reachability. The poll loops do not need per-task-queue names —
  they receive `task_queue` as an arg and call BridgeHolder with it.
  """

  use GenServer

  alias Hourglass.BridgeHolder
  alias Hourglass.Worker.ActivityPollLoop
  alias Hourglass.Worker.ClosedWorkflowWatch
  alias Hourglass.Worker.GracefulStop
  alias Hourglass.Worker.WorkflowPollLoop
  alias Hourglass.WorkerRegistry

  @type opts :: [
          task_queue: String.t(),
          namespace: String.t(),
          max_cached_workflows: pos_integer(),
          target_url: String.t(),
          max_outstanding_workflow_tasks: non_neg_integer(),
          max_outstanding_activities: non_neg_integer(),
          max_outstanding_local_activities: non_neg_integer(),
          graceful_shutdown_period: non_neg_integer(),
          closed_workflow_bound: pos_integer()
        ]

  @spec start_link(opts()) :: GenServer.on_start()
  def start_link(opts) do
    task_queue = Keyword.fetch!(opts, :task_queue)
    GenServer.start_link(__MODULE__, opts, name: WorkerRegistry.via(task_queue))
  end

  @doc """
  Child spec for a Worker. Its `:shutdown` is the grace
  (`:graceful_shutdown_period`) plus fifteen seconds, so a supervisor
  stopping it waits for the whole graceful stop (see "Graceful stop").
  """
  @spec child_spec(opts()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: graceful_shutdown_period(opts) + GracefulStop.margins()
    }
  end

  @doc """
  The grace a Worker started with `opts` gives its running activities
  when it stops, in milliseconds: the `:graceful_shutdown_period`
  option, else `config :hourglass, Hourglass.Worker,
  graceful_shutdown_period: ms`, else 20 seconds. Raises
  `ArgumentError` unless it is a non-negative integer that fits in 32
  bits.
  """
  @spec graceful_shutdown_period(keyword()) :: non_neg_integer()
  defdelegate graceful_shutdown_period(opts), to: BridgeHolder

  @doc """
  Within how long a Worker started with `opts` ends an activity whose
  workflow run has closed, in milliseconds (see "Activities of a closed
  workflow"): the `:closed_workflow_bound` option, else `config
  :hourglass, Hourglass.Worker, closed_workflow_bound: ms`, else
  #{ClosedWorkflowWatch.default_bound()} ms. Raises `ArgumentError` unless it
  is an integer from #{ClosedWorkflowWatch.min_bound()} to
  #{ClosedWorkflowWatch.max_bound()}.
  """
  @spec closed_workflow_bound(keyword()) :: pos_integer()
  defdelegate closed_workflow_bound(opts), to: ClosedWorkflowWatch, as: :bound

  @doc """
  Returns the inner Supervisor pid for `task_queue`, primarily for
  test introspection (e.g. `Supervisor.which_children/1`).
  """
  @spec inner_supervisor(String.t()) :: pid()
  def inner_supervisor(task_queue) when is_binary(task_queue) do
    task_queue
    |> WorkerRegistry.via()
    |> GenServer.call(:inner_supervisor)
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  # Whitelist of opts keys forwarded from `start_link/1` (set by
  # `WorkerLauncher` from `:hourglass, Hourglass.Worker` config) into
  # `BridgeHolder.register_worker/2` and ultimately the proto wire.
  # If a key isn't here, it's silently stripped — production app-config
  # values stop reaching the Rust worker. `:graceful_shutdown_period` is
  # not listed: `init/1` always passes it, resolved by
  # `graceful_shutdown_period/1`, so Core's grace is the one this Worker's
  # stop waits on.
  @register_opts_keys [
    :namespace,
    :max_cached_workflows,
    :target_url,
    :max_outstanding_workflow_tasks,
    :max_outstanding_activities,
    :max_outstanding_local_activities
  ]

  @doc false
  @spec __register_opts_keys__() :: [atom()]
  def __register_opts_keys__, do: @register_opts_keys

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    task_queue = Keyword.fetch!(opts, :task_queue)
    grace = graceful_shutdown_period(opts)
    bound = closed_workflow_bound(opts)

    register_opts =
      opts
      |> Keyword.take(@register_opts_keys)
      |> Keyword.put(:graceful_shutdown_period, grace)

    with :ok <- BridgeHolder.register_worker(task_queue, register_opts),
         {:ok, sup_pid} <- start_inner_supervisor(task_queue),
         {:ok, watch_pid} <- ClosedWorkflowWatch.start_link(task_queue, bound, opts) do
      {:ok,
       %{
         task_queue: task_queue,
         sup_pid: sup_pid,
         watch_pid: watch_pid,
         graceful_shutdown_period: grace
       }}
    else
      {:error, reason} ->
        # Roll back the bridge allocation if the inner Supervisor or the
        # closed-workflow watch failed to start (either, started, is linked
        # to us and goes with us). Idempotent if registration itself failed.
        _result = BridgeHolder.unregister_worker(task_queue)
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call(:inner_supervisor, _from, %{sup_pid: sup_pid} = state) do
    {:reply, sup_pid, state}
  end

  # The inner Supervisor links to us. If it crashes we propagate
  # (let-it-crash); the parent DynamicSupervisor (Worker.Supervisor)
  # restarts this GenServer, which re-registers a fresh bridge handle
  # and starts a fresh inner Supervisor.
  @impl GenServer
  def handle_info({:EXIT, sup_pid, reason}, %{sup_pid: sup_pid} = state) do
    {:stop, reason, state}
  end

  # Likewise the closed-workflow watch, which links to us too.
  def handle_info({:EXIT, watch_pid, reason}, %{watch_pid: watch_pid} = state) do
    {:stop, reason, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # The graceful stop: see the moduledoc's "Graceful stop". The
  # closed-workflow watch goes on ending the activities of closed runs while
  # the stop drains, and is stopped once the stop is done.
  @impl GenServer
  def terminate(_reason, %{task_queue: task_queue, sup_pid: sup_pid} = state) do
    GracefulStop.run(task_queue, sup_pid, state.graceful_shutdown_period)
    stop_closed_workflow_watch(state.watch_pid)
  end

  # ---------------------------------------------------------------------------
  # Internals
  # ---------------------------------------------------------------------------

  defp start_inner_supervisor(task_queue) do
    children = [
      {WorkflowPollLoop, [task_queue: task_queue]},
      {ActivityPollLoop, [task_queue: task_queue]}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end

  # A watch that crashed (and so stops this Worker) is already gone.
  defp stop_closed_workflow_watch(watch_pid) do
    GenServer.stop(watch_pid, :normal)
  catch
    :exit, _gone -> :ok
  end
end
