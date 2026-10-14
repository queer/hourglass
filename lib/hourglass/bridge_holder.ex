# credo:disable-for-this-file Credo.Check.Refactor.ModuleDependencies
defmodule Hourglass.BridgeHolder do
  @moduledoc """
  Application-level resource manager for per-task-queue Temporal-bridge
  worker handles. A single long-lived `GenServer` started under
  `Hourglass.Application` that owns every bridge handle in the VM and
  mediates every NIF call against them.

  ## Why "Application-level"

  Previously, this module lived under `Hourglass.Worker.BridgeHolder`
  inside each per-task-queue Worker subtree, publishing a raw
  `reference()` to a shared ETS table. Poll loops, evaluators, and
  activity executors read the handle directly and called the NIF. That
  per-Worker placement (the deleted `Worker.BridgeHolder` module) had
  three problems dissolved by promoting it to an Application-level
  singleton:

    1. A poll-loop crash under `:rest_for_one` cascade-restarted the
       BridgeHolder, but in-flight evaluator/executor Tasks (spawned
       under sibling DynSups) survived holding stale handles —
       producing dirty NIF leaks once they tried to ship completions
       on the destroyed handle.

    2. `Bridge.worker_shutdown/1` had to be sequenced via a separate
       `ShutdownSentinel` child, because Supervisor reverse-order
       teardown didn't naturally line up with the bridge handle being
       drained before its consumers were killed.

    3. The cross-Worker test suite documented an "in-flight kill /
       cascade restart" gap (`cross_worker_affinity_test.exs`) that
       was a real partial-recovery limitation, not a flaw in the
       pure-function evaluator model.

  Pulling handle ownership out of the Worker tree dissolves all three.
  Worker child crashes can no longer destroy the handle. Poll loops,
  evaluators, and executors call this module's API instead of holding
  raw refs. Graceful shutdown of a Worker is ordered by
  `Worker.terminate/2`: `initiate_shutdown/1` first, then the drain,
  then `unregister_worker/1` (see "Shutting a handle down" below).

  ## Shutting a handle down: initiate, drain, release

  A handle goes through three states, which `handle_state/1` reports:

    * `:active` — registered and polling the server for new tasks.
    * `:draining` — `initiate_shutdown/1` has called
      `Bridge.worker_shutdown/1` (Core's `initiate_shutdown`), so Core no
      longer polls the server, but the handle is still held: polls,
      completions and heartbeats go on working through it. Core keeps
      handing out what it already holds — activations, activity cancels,
      and any task a poll already in flight received — and a poll returns
      `{:error, %Bridge.Error{kind: :shutdown}}` only once every workflow
      task and activity Core handed out has been completed. A poll loop
      that sees `:shutdown` while the handle is draining is done, and
      exits. `registered?/1` is still true.
    * gone — `unregister_worker/1` has dropped the handle. A completion
      for it now returns `{:error, :worker_not_registered}` and never
      reaches Temporal.

  `Hourglass.Worker` drives the three in that order, so the work a
  stopping worker holds reports its outcome before the handle goes. The
  grace that bounds the drain is Core's `graceful_shutdown_period`, set
  from the `:graceful_shutdown_period` register option: once it has
  elapsed since `initiate_shutdown/1`, Core cancels every activity still
  outstanding with reason `WORKER_SHUTDOWN`, and reports each one the
  runner acknowledges to the server as a retryable `"WorkerShutdown"`
  failure. See `Hourglass.Worker`'s "Graceful stop".

  A draining handle is never recycled (see "Poll-caller DOWN ⇒ handle
  recycle"): a fresh handle would poll the server for new tasks again.

  The process that registered a handle owns it: if it dies without
  unregistering — a Worker killed outright, say — the holder drops the
  handle itself, so the task queue can be registered again and no handle
  is leaked.

  ## Async-reply dispatch via Task.Supervisor (polls + register + unregister)

  `Bridge.worker_poll_*` is a dirty-NIF call that may block for many
  seconds. `Bridge.worker_new/2` and `Bridge.worker_shutdown/1` aren't
  long-polls but each connect to the Temporal cluster and routinely
  take 50–500ms. Running any of these inline in `handle_call` would
  serialise every other call (poll, complete, register, unregister)
  across every task queue in the VM behind it — and with async tests
  starting Workers concurrently, that turns parallel work into serial
  work and pushes a clean `mix test test/hourglass/temporal/` from
  ~3 seconds to 30+ seconds.

  The fix: a `Task.Supervisor` (started under this GenServer) spawns a
  child Task per slow request. The Task does the NIF call and replies
  to the original caller via `GenServer.reply(from, result)`. The
  GenServer mailbox returns immediately. Multiple concurrent calls run
  in parallel (each in its own Task). Caller blocks on `GenServer.call`
  until the Task replies, so the synchronous contract is preserved.

  Three call shapes use this pattern:

    * `poll_workflow_activation/1` + `poll_activity_task/1` — long-poll
      (seconds).
    * `register_worker/2` — `worker_new` (50–500ms). The Task sends
      the result back via an internal `:store_handle` message; a
      `pending_registrations` MapSet guards against duplicate-register
      races during the in-flight window.
    * `initiate_shutdown/1` and `unregister_worker/1` —
      `worker_shutdown` (50–500ms). `initiate_shutdown/1` marks the
      handle draining and keeps it; `unregister_worker/1` pops it from
      state synchronously (so subsequent `registered?/1` calls see the
      unregister immediately). Either way the `worker_shutdown` runs in
      the Task, which replies once it returns.

  ## Serialised workflow-activation completions

  `Bridge.worker_complete_workflow_activation/2` is a blocking NIF that
  SDK-Core's `complete_workflow_activation` future may take tens of
  seconds to resolve when the workflow has pending activity tasks (it
  waits for them to arrive via `complete_activity_task` before closing
  the workflow). Running it inline in `handle_call` deadlocks: the
  GenServer mailbox is blocked, so `complete_activity_task` calls can
  never be processed, and Core waits forever.

  The fix: run each `complete_workflow_activation` NIF in a Task (like
  the long-poll calls). The Task replies to the caller via
  `GenServer.reply/2` when the NIF returns, so the synchronous contract
  is preserved. The GenServer mailbox is free while the NIF runs,
  allowing `complete_activity_task` calls to proceed.

  However, SDK-Core's worker requires sequential (not concurrent)
  `complete_workflow_activation` calls: concurrent calls for the same
  worker handle can race inside Tokio and leave the workflow in an
  inconsistent state that Temporal Server never observes as closed.

  The solution: a per-task-queue FIFO queue stored in
  `completion_queue`. When a `complete_workflow_activation` call arrives:

    * If no Task is active for that task_queue: start one immediately,
      mark the queue as `:active` in `completion_active`.
    * If a Task is active: enqueue `{from, bytes}` in `completion_queue`.

  When a Task finishes it sends `{:workflow_activation_flushed, task_queue, from, result}`
  back to the GenServer, which dequeues the next waiting call (if any)
  and starts a new Task for it.

  ## Activity calls run off the holder

  `complete_activity_task/2` and `record_heartbeat/2` are not run inline
  in `handle_call` either. Completing an activity waits on Core's RPC to
  the server, and a completion stuck on the network would otherwise block
  the holder — and with it every other activity's completion and
  heartbeat on the node, on every task queue, and every poll and
  workflow completion besides. The holder only decides which handle the
  call goes to (the one that handed the task out: see "Handle recycle and
  the work it handed out"), and a Task under its Task.Supervisor makes
  the NIF call and replies. The task token is read from the proto in the
  caller's process, not the holder's. Activity calls are not serialised
  per queue, as workflow completions are: Core takes them concurrently.

  ## In-flight Task survival

  Evaluator and activity-executor Tasks live under shared
  Application-level DynSups, NOT under per-Worker subtrees. They
  survive Worker child crashes, and a gracefully stopping Worker keeps
  its handle until they have reported (see above). When one tries to
  ship a completion after the handle is gone — a holder restart, or a
  Worker that stopped without draining — the call returns
  `{:error, :worker_not_registered}`, the Task exits, and Temporal
  hears nothing of that task until its timeout (a workflow task's, or
  an activity's heartbeat or start-to-close timeout), then retries it.
  Idempotency for activities that have already done expensive work
  is the caller's responsibility — Hourglass provides no built-in
  deduplication cache.

  ## Poll-caller DOWN ⇒ handle recycle

  Dirty NIFs cannot be preempted from BEAM, and `temporalio-sdk-core`
  exposes no per-poll cancellation API — `Worker::shutdown()` is the
  only mechanism that makes in-flight polls return, and it cancels
  ALL polls on the handle.

  When a poll caller (a `WorkflowPollLoop` or `ActivityPollLoop`)
  is killed mid-poll, the orphaned Task in `task_sup` stays parked on
  its dirty NIF call. Once Core delivers an activation/task to it,
  `GenServer.reply/2` to the now-dead caller pid is silently dropped;
  Core sees the activation as delivered and does not redeliver within
  any reasonable test budget (observed: workflow stays parked at
  workflow-task-delivery for at least 5 minutes).

  The fix: `BridgeHolder` monitors every poll caller. If the caller
  process dies before the in-flight Task replies, the holder recycles
  the bridge handle for that task queue — `worker_shutdown` plus
  `Bridge.worker_abort_polls/1` (every in-flight poll on the old handle,
  the orphan included, returns at once, even while activities it handed
  out are still running) + `worker_new` (allocates a fresh handle from
  the original
  `register_opts`). Healthy parallel pollers see
  `{:error, %Bridge.Error{kind: :shutdown}}` and treat it as the
  normal transient-shutdown retry condition. The originally-affected
  workflow's activation gets redelivered by Core to the next poll on
  the new handle.

  Race ordering is one-sided in the holder's favor: the poll Task
  sends `{:poll_replied, caller_pid, task_pid}` BEFORE its `GenServer.reply`,
  so on a clean reply the cleanup `Process.demonitor(ref, [:flush])`
  always runs before any `:DOWN` could be observed (the caller cannot
  have died from observing the reply yet, because the reply hasn't
  arrived). On a real DOWN-while-parked the entry is still in the
  `pollers` map when the holder processes the `:DOWN`, triggering the
  recycle.

  ## Handle recycle and the work it handed out

  An activity the old handle handed out is outstanding in the OLD Core
  worker, and only that worker can report its outcome: sent to the
  fresh one, a completion or heartbeat names a task Core never handed
  out, and is dropped. So the holder remembers which handle handed each
  activity task out — the poll Task tells it, by task token
  (`:activity_handed_out`), before the poll's caller sees the task — and
  `complete_activity_task/2` and `record_heartbeat/2` go to that handle,
  recycled or not. The route goes with the activity's completion, so a
  retired handle is kept exactly as long as something it handed out is
  still running, then released. Releasing a queue (`unregister_worker/1`)
  drops its routes: nothing of a released handle reports.

  Recycle is async (`worker_shutdown` + `worker_new` together are
  100s of ms) — the DOWN handler returns `{:noreply, state}`
  immediately after dispatching a recycle Task under `task_sup`.
  The recycle Task sends `{:handle_recycled, task_queue, result}`
  back. If `task_queue` is no longer in `state.handles` when the
  result lands (an `unregister_worker` call won the race), the new
  handle is shut down on the spot and discarded.
  """

  use GenServer

  alias Hourglass.Bridge
  alias Hourglass.Client
  alias Hourglass.Runtime
  alias Hourglass.Worker.WorkflowStateCache

  require Logger

  # Default sticky-workflow-cache size when the caller doesn't set
  # `:max_cached_workflows`. This is the number of workflow runs Core keeps
  # "hot" (state cached) so it can deliver *incremental* activations instead
  # of replaying full history every task. When the live-workflow count
  # exceeds it, Core evicts LRU runs and re-delivers them non-sticky — extra
  # replay work, and (before the poll-loop recovery) the trigger for
  # sticky-cache-miss churn.
  #
  # Raised from the historical 10 to 100. Ten is far too small for any real
  # workload (Coffee runs many concurrent per-store projection workflows,
  # and its worker-task concurrency already defaults to 100); a cache smaller
  # than the outstanding-task concurrency guarantees thrash. 100 is a
  # conservative bump — each cached run holds State in Core plus the mirror
  # `WorkflowStateCache` ETS entry, so we don't default to the thousands some
  # SDKs use. Hosts that run more concurrent workflows raise it explicitly via
  # worker opts or `config :hourglass, Hourglass.Worker, max_cached_workflows: N`.
  @default_max_cached_workflows 100

  # The module whose `worker_complete_activity_task/2` and
  # `worker_record_activity_heartbeat/2` the holder calls: `Hourglass.Bridge`,
  # except in hourglass's own test build, where a stand-in lets a test stall
  # one of them (test/support/stallable_bridge.ex).
  @activity_bridge Application.compile_env(:hourglass, :activity_bridge, Hourglass.Bridge)

  # How long `unregister_worker/1` waits for the polls it aborted to return.
  @poll_return_timeout 5_000

  # How long `register_worker/2` waits for `worker_new`, which connects to
  # Temporal (the OS gives up on a TCP connect after about 75 s) and asks it
  # GetSystemInfo (Core gives up after 30 s). Longer than both, so a slow or
  # silent Temporal reaches the caller as the error `worker_new` returns, not
  # as the call timing out first.
  @register_timeout 120_000

  @doc false
  @spec default_max_cached_workflows() :: pos_integer()
  def default_max_cached_workflows, do: @default_max_cached_workflows

  # How long a stopping worker lets its running activities finish before
  # Core reports each as failed by the shutdown, when neither the worker's
  # options nor `config :hourglass, Hourglass.Worker` set
  # `:graceful_shutdown_period`. Twenty seconds, plus the drain's margin
  # (`Hourglass.Worker`), stays inside the thirty seconds an orchestrator
  # commonly allows a stopping node before it kills it.
  @default_graceful_shutdown_period 20_000

  @doc false
  @spec default_graceful_shutdown_period() :: non_neg_integer()
  def default_graceful_shutdown_period, do: @default_graceful_shutdown_period

  # Core takes the grace as a uint32 of milliseconds.
  @max_graceful_shutdown_period 4_294_967_295

  # The grace a Worker started with `opts` gives its running activities when it
  # stops: the option, else `config :hourglass, Hourglass.Worker`, else the
  # default. Public as `Hourglass.Worker.graceful_shutdown_period/1`; it lives
  # here, next to the default and the wire field it fills.
  @doc false
  @spec graceful_shutdown_period(keyword()) :: non_neg_integer()
  def graceful_shutdown_period(opts) do
    grace =
      Keyword.get_lazy(opts, :graceful_shutdown_period, fn ->
        :hourglass
        |> Application.get_env(Hourglass.Worker, [])
        |> Keyword.get(:graceful_shutdown_period, @default_graceful_shutdown_period)
      end)

    if is_integer(grace) and grace >= 0 and grace <= @max_graceful_shutdown_period do
      grace
    else
      raise ArgumentError,
            ":graceful_shutdown_period must be a non-negative integer of milliseconds " <>
              "(at most #{@max_graceful_shutdown_period}), got: #{inspect(grace)}"
    end
  end

  @typedoc "Options accepted by `register_worker/2`."
  @type register_opts :: [
          namespace: String.t(),
          max_cached_workflows: pos_integer(),
          target_url: String.t(),
          max_outstanding_workflow_tasks: non_neg_integer(),
          max_outstanding_activities: non_neg_integer(),
          max_outstanding_local_activities: non_neg_integer(),
          graceful_shutdown_period: non_neg_integer()
        ]

  @typedoc "Internal state."
  @type state :: %{
          handles: %{optional(String.t()) => reference()},
          pending_registrations: MapSet.t(String.t()),
          register_opts: %{optional(String.t()) => register_opts()},
          pollers: %{optional(pid()) => {String.t(), reference()}},
          # The Task running each poll in flight, with the task queue it polls.
          poll_tasks: %{optional(pid()) => String.t()},
          recycling: MapSet.t(String.t()),
          # The handle each outstanding activity task was handed out from, by
          # task token (see "Handle recycle and the work it handed out").
          activity_routes: %{optional(binary()) => {String.t(), reference()}},
          # Task queues whose handle `initiate_shutdown/1` has shut down but
          # which are still held, so their work can finish and report.
          draining: MapSet.t(String.t()),
          # The monitor on each handle's owner (the process that registered it).
          owners: %{optional(String.t()) => reference()},
          task_sup: pid(),
          # Per-task-queue FIFO queues for serialised workflow-activation completions.
          # `completion_active` holds task_queues with an in-flight Task.
          # `completion_queue` holds waiting {from, bytes} pairs.
          completion_active: MapSet.t(String.t()),
          completion_queue: %{optional(String.t()) => :queue.queue({term(), binary()})}
        }

  # ---------------------------------------------------------------------------
  # Lifecycle (Hourglass.Application starts this; Worker.Supervisor calls
  # register_worker/unregister_worker around its child lifecycle)
  # ---------------------------------------------------------------------------

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Allocate a bridge worker for `task_queue`. Synchronous: returns
  `:ok` once the underlying `Bridge.worker_new/2` succeeds and the
  handle is recorded in state. Returns `{:error, reason}` on NIF
  failure or if `task_queue` is already registered.
  """
  @spec register_worker(String.t(), register_opts()) :: :ok | {:error, term()}
  def register_worker(task_queue, opts \\ []) when is_binary(task_queue) do
    GenServer.call(__MODULE__, {:register_worker, task_queue, opts}, @register_timeout)
  end

  @doc """
  Stop the handle for `task_queue` polling the server for new tasks,
  and keep it, so the work it already handed out can finish and report
  through it. Calls `Bridge.worker_shutdown/1` (Core's
  `initiate_shutdown`) and marks the handle `:draining` (see "Shutting a
  handle down" in the moduledoc). Returns `:ok` once Core has stopped
  polling, and at once for a handle already draining. Returns
  `{:error, :worker_not_registered}` if `task_queue` has no handle, so
  there is nothing to drain.
  """
  @spec initiate_shutdown(String.t()) :: :ok | {:error, :worker_not_registered}
  def initiate_shutdown(task_queue) when is_binary(task_queue) do
    GenServer.call(__MODULE__, {:initiate_shutdown, task_queue}, 30_000)
  end

  @doc """
  Drop the handle for `task_queue` (calling `Bridge.worker_shutdown/1`
  on the way out). Idempotent: unknown task_queue returns `:ok`.

  Whatever the handle handed out and has not yet been completed can no
  longer report: a completion for it returns
  `{:error, :worker_not_registered}`. `Hourglass.Worker` therefore calls
  `initiate_shutdown/1` and drains before it calls this.

  Every poll still on the handle — including one whose caller died
  parked on it — is aborted (`Bridge.worker_abort_polls/1`), and this
  returns once they have returned (or after five seconds), so no poll
  on a released handle goes on holding a dirty-IO scheduler thread.
  """
  @spec unregister_worker(String.t()) :: :ok
  def unregister_worker(task_queue) when is_binary(task_queue) do
    GenServer.call(__MODULE__, {:unregister_worker, task_queue}, 30_000)
  end

  @doc """
  Returns true iff a handle is currently registered for `task_queue`,
  draining or not.
  """
  @spec registered?(String.t()) :: boolean()
  def registered?(task_queue) when is_binary(task_queue) do
    GenServer.call(__MODULE__, {:registered?, task_queue})
  end

  @doc """
  The state of the handle for `task_queue`: `:active` (polling for new
  tasks), `:draining` (`initiate_shutdown/1` has been called, the handle
  is still held), or `:none`. A poll loop that sees `:shutdown` from a
  poll uses this to tell a recycle (`:active`: poll again) from the end
  of its work (`:draining` or `:none`: exit).
  """
  @spec handle_state(String.t()) :: :active | :draining | :none
  def handle_state(task_queue) when is_binary(task_queue) do
    GenServer.call(__MODULE__, {:handle_state, task_queue})
  end

  # ---------------------------------------------------------------------------
  # Hot path: poll + complete
  # ---------------------------------------------------------------------------

  @doc """
  Long-poll for the next workflow activation on `task_queue`. Blocks
  the caller until Core delivers an activation or the bridge worker
  shuts down. The blocking work runs in a child Task under this
  module's Task.Supervisor so the GenServer mailbox stays responsive.
  """
  @spec poll_workflow_activation(String.t()) ::
          {:ok, binary()} | {:error, term()}
  def poll_workflow_activation(task_queue) when is_binary(task_queue) do
    GenServer.call(__MODULE__, {:poll_workflow_activation, task_queue}, :infinity)
  end

  @doc """
  Long-poll for the next activity task on `task_queue`. Same shape as
  `poll_workflow_activation/1`.
  """
  @spec poll_activity_task(String.t()) :: {:ok, binary()} | {:error, term()}
  def poll_activity_task(task_queue) when is_binary(task_queue) do
    GenServer.call(__MODULE__, {:poll_activity_task, task_queue}, :infinity)
  end

  @doc """
  Ship the workflow-activation completion bytes back to Core. Synchronous;
  the underlying NIF call is in-process to Core (no network round-trip).
  """
  @spec complete_workflow_activation(String.t(), binary()) :: :ok | {:error, term()}
  def complete_workflow_activation(task_queue, bytes)
      when is_binary(task_queue) and is_binary(bytes) do
    GenServer.call(__MODULE__, {:complete_workflow_activation, task_queue, bytes}, 30_000)
  end

  @doc """
  Ship the activity-task completion bytes back to Core. Same shape as
  `complete_workflow_activation/2`.
  """
  @spec complete_activity_task(String.t(), binary()) :: :ok | {:error, term()}
  def complete_activity_task(task_queue, bytes)
      when is_binary(task_queue) and is_binary(bytes) do
    # The task token is read here, in the caller, not in the holder.
    call = {:complete_activity_task, task_queue, completion_token(bytes), bytes}
    GenServer.call(__MODULE__, call, 30_000)
  end

  @doc """
  Record an activity heartbeat (liveness) for `task_queue`. Fast, in-process NIF call —
  fire-and-forget at Core. Unknown task_queue returns `{:error, :worker_not_registered}`
  (the heartbeat is best-effort; the caller swallows this).
  """
  @spec record_heartbeat(String.t(), binary()) :: :ok | {:error, term()}
  def record_heartbeat(task_queue, heartbeat_bin)
      when is_binary(task_queue) and is_binary(heartbeat_bin) do
    call = {:record_heartbeat, task_queue, heartbeat_token(heartbeat_bin), heartbeat_bin}
    GenServer.call(__MODULE__, call, 30_000)
  end

  # ---------------------------------------------------------------------------
  # GenServer callbacks
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init([]) do
    Process.flag(:trap_exit, true)

    # Task.Supervisor for long-poll children. Anonymous (no name) — we
    # only need its pid for `Task.Supervisor.start_child/2`. Linking to
    # the holder means a holder restart drops in-flight polls cleanly
    # (each Task's NIF call returns :shutdown when the worker handle
    # is destroyed by terminate/2).
    {:ok, task_sup} = Task.Supervisor.start_link()

    {:ok,
     %{
       handles: %{},
       pending_registrations: MapSet.new(),
       register_opts: %{},
       pollers: %{},
       poll_tasks: %{},
       recycling: MapSet.new(),
       activity_routes: %{},
       draining: MapSet.new(),
       owners: %{},
       task_sup: task_sup,
       completion_active: MapSet.new(),
       completion_queue: %{}
     }}
  end

  @impl GenServer
  def handle_call({:register_worker, task_queue, opts}, from, state) do
    cond do
      Map.has_key?(state.handles, task_queue) ->
        {:reply, {:error, :already_registered}, state}

      MapSet.member?(state.pending_registrations, task_queue) ->
        {:reply, {:error, :registration_in_flight}, state}

      true ->
        # Run worker_new in a Task — it connects to the Temporal cluster
        # and can take seconds. Running it inline would serialise every
        # other call (poll, complete, register, unregister) behind it,
        # which kills test-suite throughput when many Workers register
        # concurrently. The Task sends back {:store_handle, ...} on
        # success or replies to `from` directly on error.
        runtime = Runtime.handle()
        config_bin = build_worker_config(task_queue, opts)
        gs_pid = self()

        {:ok, _pid} =
          Task.Supervisor.start_child(state.task_sup, fn ->
            do_worker_new_async(runtime, config_bin, gs_pid, task_queue, opts, from)
          end)

        pending = MapSet.put(state.pending_registrations, task_queue)
        {:noreply, %{state | pending_registrations: pending}}
    end
  end

  @impl GenServer
  def handle_call({:initiate_shutdown, task_queue}, from, state) do
    case Map.fetch(state.handles, task_queue) do
      {:ok, handle} ->
        if MapSet.member?(state.draining, task_queue) do
          {:reply, :ok, state}
        else
          # Same throughput rationale as unregister: worker_shutdown runs in a
          # Task, which replies once Core has stopped polling. The handle stays
          # in `handles`, so the work Core already handed out completes through
          # it.
          shutdown_in_task(state, handle, task_queue, from)
          {:noreply, %{state | draining: MapSet.put(state.draining, task_queue)}}
        end

      :error ->
        {:reply, {:error, :worker_not_registered}, state}
    end
  end

  @impl GenServer
  def handle_call({:unregister_worker, task_queue}, from, state) do
    case Map.pop(state.handles, task_queue) do
      {nil, _handles} ->
        {:reply, :ok, state}

      {handle, _handles} when is_reference(handle) ->
        # Run worker_shutdown in a Task — same throughput rationale as
        # register. The Task also aborts every poll still on the handle and
        # waits for them to return, so once the caller's call returns no poll
        # on the released handle holds a dirty-IO thread.
        polls = for {pid, ^task_queue} <- state.poll_tasks, do: pid
        release_in_task(state, handle, task_queue, polls, from)
        {:noreply, release(state, task_queue)}
    end
  end

  @impl GenServer
  def handle_call({:registered?, task_queue}, _from, state) do
    {:reply, Map.has_key?(state.handles, task_queue), state}
  end

  @impl GenServer
  def handle_call({:handle_state, task_queue}, _from, state) do
    reply =
      cond do
        not Map.has_key?(state.handles, task_queue) -> :none
        MapSet.member?(state.draining, task_queue) -> :draining
        true -> :active
      end

    {:reply, reply, state}
  end

  @impl GenServer
  def handle_call({:poll_workflow_activation, task_queue}, from, state) do
    dispatch_poll(state, task_queue, from, &Bridge.worker_poll_workflow_activation/1)
  end

  @impl GenServer
  def handle_call({:poll_activity_task, task_queue}, from, state) do
    dispatch_poll(state, task_queue, from, &poll_and_route_activity_task(&1, task_queue))
  end

  @impl GenServer
  def handle_call({:complete_workflow_activation, task_queue, bytes}, from, state) do
    case Map.fetch(state.handles, task_queue) do
      {:ok, _handle} ->
        # Serialise completions per task_queue: only one Task may call
        # worker_complete_workflow_activation at a time on the same handle.
        # Concurrent calls can race inside SDK-Core's Tokio state machine and
        # produce a workflow that Temporal Server never observes as closed.
        # See moduledoc "Serialised workflow-activation completions".
        if MapSet.member?(state.completion_active, task_queue) do
          # Another Task is in-flight — enqueue this call for later.
          q = Map.get(state.completion_queue, task_queue, :queue.new())
          new_q = :queue.in({from, bytes}, q)
          new_state = put_in(state, [:completion_queue, task_queue], new_q)
          {:noreply, new_state}
        else
          new_state = %{
            state
            | completion_active: MapSet.put(state.completion_active, task_queue)
          }

          start_workflow_completion_task(new_state, task_queue, from, bytes)
          {:noreply, new_state}
        end

      :error ->
        {:reply, {:error, :worker_not_registered}, state}
    end
  end

  @impl GenServer
  def handle_call({:complete_activity_task, task_queue, token, bytes}, from, state) do
    # The activity is done with its route: pop it, so a retired handle is
    # released once nothing it handed out is still running.
    {handle, state} = pop_activity_handle(state, task_queue, token)

    case handle do
      handle when is_reference(handle) ->
        # In a Task, never inline: the NIF waits on Core's RPC to the server,
        # and a completion stuck on the network must hold up no other
        # activity's completion or heartbeat (see "Activity calls run off the
        # holder" in the moduledoc).
        activity_call_in_task(state, from, task_queue, "worker_complete_activity_task", fn ->
          @activity_bridge.worker_complete_activity_task(handle, bytes)
        end)

        {:noreply, state}

      nil ->
        # In-flight Task tried to ship a completion after the Worker was
        # unregistered. The result is "lost" from this Task's perspective —
        # Core will redeliver on the next poll, and the LLM cache absorbs
        # the redelivery double-spend for activities that did LLM work.
        # Emit telemetry so chronic reschedules become observable; without
        # this, operators are blind to the redelivery rate.
        :telemetry.execute(
          [:hourglass, :bridge_holder, :activity_result_unrouted],
          %{count: 1},
          %{task_queue: task_queue}
        )

        {:reply, {:error, :worker_not_registered}, state}
    end
  end

  @impl GenServer
  def handle_call({:record_heartbeat, task_queue, token, bytes}, from, state) do
    case activity_handle(state, task_queue, token) do
      handle when is_reference(handle) ->
        activity_call_in_task(state, from, task_queue, "worker_record_activity_heartbeat", fn ->
          @activity_bridge.worker_record_activity_heartbeat(handle, bytes)
        end)

        {:noreply, state}

      nil ->
        {:reply, {:error, :worker_not_registered}, state}
    end
  end

  # A poll Task handed an activity task out from `handle`: route the task's
  # completion and heartbeats back to that handle, even if it is recycled
  # meanwhile. Sent BEFORE the poll's reply, so it is in the mailbox before
  # any call the activity's executor can make. A queue released meanwhile is
  # not routed: nothing of a released handle may report.
  @impl GenServer
  def handle_info({:activity_handed_out, task_queue, token, handle}, state) do
    if Map.has_key?(state.handles, task_queue) do
      routes = Map.put(state.activity_routes, token, {task_queue, handle})
      {:noreply, %{state | activity_routes: routes}}
    else
      {:noreply, state}
    end
  end

  # A serialised workflow-activation-completion Task finished. Reply to the
  # caller, then dispatch the next waiting call for the same task_queue (if any).
  @impl GenServer
  def handle_info({:workflow_activation_flushed, task_queue, from, result}, state) do
    GenServer.reply(from, result)
    {:noreply, dispatch_next_completion(state, task_queue)}
  end

  # Async register-worker completion. Atomically transitions the task_queue
  # out of pending_registrations and into handles, captures the original
  # register_opts (needed by the poll-caller-DOWN recycle path), then
  # replies to the original GenServer.call from.
  @impl GenServer
  def handle_info({:store_handle, task_queue, handle, opts, from}, state) do
    # The registering process owns the handle: monitor it, so a handle whose
    # owner dies without unregistering is dropped rather than leaked.
    {owner, _tag} = from

    new_state = %{
      state
      | handles: Map.put(state.handles, task_queue, handle),
        register_opts: Map.put(state.register_opts, task_queue, opts),
        pending_registrations: MapSet.delete(state.pending_registrations, task_queue),
        owners: Map.put(state.owners, task_queue, Process.monitor(owner))
    }

    GenServer.reply(from, :ok)
    {:noreply, new_state}
  end

  # Async register-worker failure. The Task already replied to the caller
  # with the error; just clear the pending marker.
  def handle_info({:registration_failed, task_queue}, state) do
    pending = MapSet.delete(state.pending_registrations, task_queue)
    {:noreply, %{state | pending_registrations: pending}}
  end

  # Poll Task sent us its successful-cleanup ping (sent BEFORE its
  # `GenServer.reply` to ensure cleanup races are one-sided in our
  # favor — see moduledoc). Demonitor with `:flush` so any pending
  # `:DOWN` for this caller is removed from the mailbox before it can
  # trigger a spurious recycle.
  #
  # Tolerate missing entry: if the caller died parked on the NIF, the
  # `:DOWN` was processed first and already removed the entry (and
  # triggered a recycle).
  def handle_info({:poll_replied, caller_pid, task_pid}, state) do
    state = %{state | poll_tasks: Map.delete(state.poll_tasks, task_pid)}

    case Map.pop(state.pollers, caller_pid) do
      {nil, _pollers} ->
        {:noreply, state}

      {{_task_queue, ref}, pollers} ->
        Process.demonitor(ref, [:flush])
        {:noreply, %{state | pollers: pollers}}
    end
  end

  # Poll caller died parked on the dirty-NIF poll. Recycle the bridge
  # handle for its task queue: `worker_shutdown` + abort (every in-flight
  # poll on the old handle, including the orphan, returns) + `worker_new`
  # (fresh handle from the captured `register_opts`).
  #
  # Healthy parallel pollers on the same task queue see
  # `{:error, %Bridge.Error{kind: :shutdown}}` and retry — same contract
  # they handle for `unregister_worker`. The originally-affected
  # workflow's activation gets redelivered by Core to the next poll on
  # the new handle.
  def handle_info({:DOWN, ref, :process, caller_pid, _reason}, state) do
    case Map.get(state.pollers, caller_pid) do
      {task_queue, ^ref} ->
        pollers = Map.delete(state.pollers, caller_pid)
        new_state = %{state | pollers: pollers}

        if MapSet.member?(state.recycling, task_queue) or
             MapSet.member?(state.draining, task_queue) do
          # Recycle for this task queue is already in flight (another
          # poll caller died first), or the handle is draining, which a
          # fresh handle polling for new tasks would undo. Just drop this
          # caller from pollers.
          {:noreply, new_state}
        else
          maybe_dispatch_recycle(new_state, task_queue, caller_pid)
        end

      _other ->
        {:noreply, owner_down(state, ref)}
    end
  end

  # Async-recycle completion: install the new handle iff the old entry
  # is still present. Discard otherwise — `unregister_worker` won the
  # race, or the holder lost track of the old entry for some other
  # reason; either way installing a stale handle would leak it.
  def handle_info({:handle_recycled, task_queue, {:ok, new_handle}}, state) do
    recycling = MapSet.delete(state.recycling, task_queue)

    if Map.has_key?(state.handles, task_queue) do
      Logger.warning(
        "BridgeHolder recycled handle for task_queue=#{task_queue} after poll-caller DOWN"
      )

      shutdown_if_draining(state, task_queue, new_handle)

      {:noreply,
       %{state | handles: Map.put(state.handles, task_queue, new_handle), recycling: recycling}}
    else
      # `unregister_worker` won the race: state.handles has no entry
      # for this task queue. Shut down the just-allocated handle on
      # the spot so it doesn't leak.
      Logger.warning(
        "BridgeHolder discarding recycled handle for task_queue=#{task_queue} " <>
          "(unregister landed during recycle)"
      )

      Task.Supervisor.start_child(state.task_sup, fn ->
        _result = Bridge.worker_shutdown(new_handle)
      end)

      {:noreply, %{state | recycling: recycling}}
    end
  end

  def handle_info({:handle_recycled, task_queue, {:error, reason}}, state) do
    # `worker_new` failed during recycle. Drop the old handle entry so
    # subsequent polls return `:worker_not_registered` (the normal
    # transient-retry condition for poll loops) rather than dispatching
    # against a known-shutdown handle. Workers can re-register manually
    # via `Worker.terminate/2` + parent supervisor restart.
    Logger.error(
      "BridgeHolder recycle failed for task_queue=#{task_queue}: #{inspect(reason)}; " <>
        "dropping handle entry"
    )

    new_state =
      release(%{state | recycling: MapSet.delete(state.recycling, task_queue)}, task_queue)

    {:noreply, new_state}
  end

  # The Task.Supervisor is linked at init/1. If it crashes we cannot
  # recover — every in-flight long-poll Task it parents is dead, and
  # every caller blocked on `:infinity` GenServer.call would block
  # forever (the Task that was supposed to GenServer.reply no longer
  # exists). Stop the holder so its supervisor restarts it cleanly;
  # callers' calls fail fast with `:noproc` / exit on the way out.
  def handle_info({:EXIT, task_sup, reason}, %{task_sup: task_sup} = state) do
    Logger.error("BridgeHolder Task.Supervisor crashed: #{inspect(reason)}")
    {:stop, {:task_sup_crashed, reason}, state}
  end

  # Other linked exits (e.g. an aborted bridge handle resource emitting
  # a stray :EXIT). Log + continue: the holder's invariants are intact
  # and crashing here would be more disruptive than the leak.
  def handle_info({:EXIT, pid, reason}, state) do
    Logger.warning("BridgeHolder ignoring EXIT from #{inspect(pid)}: #{inspect(reason)}")
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, %{handles: handles}) do
    # Drain every still-registered handle so blocked NIF threads exit.
    # Tasks under task_sup link to us and are killed by the runtime;
    # destroying the handle here makes any in-flight NIF call on those
    # Tasks return :shutdown rather than leaking.
    Enum.each(handles, fn {task_queue, handle} ->
      _result = safe_worker_release(handle, task_queue)
    end)

    :ok
  end

  # ---------------------------------------------------------------------------
  # Internals
  # ---------------------------------------------------------------------------

  # Run `Bridge.worker_shutdown/1` on the holder's Task.Supervisor and reply
  # `:ok` to `from` once it returns. A stale handle (NIF reload) still
  # replies, so the caller never waits out its call timeout.
  defp shutdown_in_task(state, handle, task_queue, from) do
    Task.Supervisor.start_child(state.task_sup, fn ->
      _result = safe_worker_shutdown(handle, task_queue)
      GenServer.reply(from, :ok)
    end)
  end

  # Run an activity's bridge call (`fun`) in a Task and reply its result to
  # `from`. A stale handle (NIF reload) replies `{:error, :nif_reloaded}`
  # after signalling the cascade.
  defp activity_call_in_task(state, from, task_queue, call_site, fun) do
    Task.Supervisor.start_child(state.task_sup, fn ->
      reply =
        try do
          fun.()
        rescue
          err in ArgumentError ->
            :telemetry.execute(
              [:hourglass, :worker, :registration_failed],
              %{count: 1},
              %{
                failure_class: :nif_reload,
                task_queue: task_queue,
                detail: Exception.message(err)
              }
            )

            signal_nif_reload!(call_site)
            {:error, :nif_reloaded}
        end

      GenServer.reply(from, reply)
    end)
  end

  # Shut `handle` down and abort its polls (`safe_worker_release/2`), wait
  # for the poll Tasks still on it to return, and reply `:ok` to `from`.
  defp release_in_task(state, handle, task_queue, polls, from) do
    Task.Supervisor.start_child(state.task_sup, fn ->
      _result = safe_worker_release(handle, task_queue)
      deadline = System.monotonic_time(:millisecond) + @poll_return_timeout

      Enum.each(polls, fn pid ->
        ref = Process.monitor(pid)
        remaining = max(deadline - System.monotonic_time(:millisecond), 0)

        receive do
          {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
        after
          remaining -> Process.demonitor(ref, [:flush])
        end
      end)

      GenServer.reply(from, :ok)
    end)
  end

  # `initiate_shutdown` landed while a recycle was in flight: it shut the old
  # handle down, and the recycled one must not poll for new tasks either.
  defp shutdown_if_draining(state, task_queue, handle) do
    if MapSet.member?(state.draining, task_queue) do
      Task.Supervisor.start_child(state.task_sup, fn ->
        _result = safe_worker_shutdown(handle, task_queue)
      end)
    end
  end

  # Poll `handle` for an activity task, and tell the holder which handle
  # handed it out before the caller sees it (`:activity_handed_out`). Runs in
  # the poll Task.
  defp poll_and_route_activity_task(handle, task_queue) do
    case Bridge.worker_poll_activity_task(handle) do
      {:ok, bytes} = result ->
        send(__MODULE__, {:activity_handed_out, task_queue, task_token(bytes), handle})
        result

      error ->
        error
    end
  end

  # The handle to report an activity through: the one that handed it out, if
  # the holder routed it, else the queue's current handle. nil if neither.
  defp activity_handle(state, task_queue, token) do
    case Map.fetch(state.activity_routes, token) do
      {:ok, {_task_queue, handle}} -> handle
      :error -> Map.get(state.handles, task_queue)
    end
  end

  defp pop_activity_handle(state, task_queue, token) do
    handle = activity_handle(state, task_queue, token)
    {handle, %{state | activity_routes: Map.delete(state.activity_routes, token)}}
  end

  # Task tokens, read from the encoded protos. Bytes that do not decode are
  # left for the NIF to refuse (InvalidProto); they route by task queue.
  defp task_token(bytes), do: decoded_token(Coresdk.ActivityTask.ActivityTask, bytes)
  defp completion_token(bytes), do: decoded_token(Coresdk.ActivityTaskCompletion, bytes)
  defp heartbeat_token(bytes), do: decoded_token(Coresdk.ActivityHeartbeat, bytes)

  defp decoded_token(module, bytes) do
    module.decode(bytes).task_token
  rescue
    _error -> nil
  end

  # Forget every trace of `task_queue`'s handle — the worker's mirror of the
  # Core cache that goes with it included. Dropping register_opts too
  # means a recycle in flight detects the missing handle entry and discards
  # its new handle.
  defp release(state, task_queue) do
    {owner_ref, owners} = Map.pop(state.owners, task_queue)
    if owner_ref, do: Process.demonitor(owner_ref, [:flush])
    :ok = WorkflowStateCache.delete_task_queue(task_queue)

    routes =
      state.activity_routes
      |> Enum.reject(fn {_token, {routed_queue, _handle}} -> routed_queue == task_queue end)
      |> Map.new()

    poll_tasks =
      state.poll_tasks
      |> Enum.reject(fn {_pid, polled_queue} -> polled_queue == task_queue end)
      |> Map.new()

    %{
      state
      | handles: Map.delete(state.handles, task_queue),
        activity_routes: routes,
        poll_tasks: poll_tasks,
        register_opts: Map.delete(state.register_opts, task_queue),
        draining: MapSet.delete(state.draining, task_queue),
        owners: owners
    }
  end

  # The owner of a handle died without unregistering it (a Worker killed
  # outright, so its terminate/2 never ran): drop the handle, so the task
  # queue can be registered again and the handle is not leaked. A DOWN that
  # matches no owner is stale or unrelated, and ignored.
  defp owner_down(state, ref) do
    case Enum.find(state.owners, fn {_task_queue, owner_ref} -> owner_ref == ref end) do
      {task_queue, ^ref} ->
        Logger.warning(
          "BridgeHolder dropping the handle for task_queue=#{task_queue}: " <>
            "its owner exited without unregistering it"
        )

        handle = Map.fetch!(state.handles, task_queue)

        Task.Supervisor.start_child(state.task_sup, fn ->
          _result = safe_worker_release(handle, task_queue)
        end)

        release(state, task_queue)

      nil ->
        state
    end
  end

  defp dispatch_poll(state, task_queue, from, poll_fn) do
    case Map.fetch(state.handles, task_queue) do
      {:ok, handle} ->
        # Spawn a supervised child Task. The Task runs the dirty-NIF poll,
        # then sends `{:poll_replied, caller_pid, task_pid}` to the holder BEFORE
        # `GenServer.reply` so the cleanup demonitor races one-sided in
        # our favor (see moduledoc).
        #
        # We Process.monitor the caller pid so we observe a `:DOWN` if it
        # dies before the Task replies. The handler treats that as the
        # recycle trigger.
        #
        # Pattern-match failure on `{:error, :max_children}` crashes the
        # holder; the application supervisor restarts it. Acceptable —
        # `:max_children` is a deployment misconfiguration (Task.Supervisor
        # default is :infinity), not a runtime condition.
        {caller_pid, _tag} = from
        gs_pid = self()
        ref = Process.monitor(caller_pid)

        {:ok, task_pid} =
          Task.Supervisor.start_child(state.task_sup, fn ->
            result =
              try do
                poll_fn.(handle)
              rescue
                err in ArgumentError ->
                  # Worker poll on stale handle raised ArgumentError →
                  # emit telemetry before signalling the cascade.
                  # Covers both worker_poll_workflow_activation
                  # and worker_poll_activity_task call shapes (poll_fn).
                  :telemetry.execute(
                    [:hourglass, :worker, :registration_failed],
                    %{count: 1},
                    %{
                      failure_class: :nif_reload,
                      task_queue: task_queue,
                      detail: Exception.message(err)
                    }
                  )

                  signal_nif_reload!("worker_poll/dispatch_poll")
                  {:error, :nif_reloaded}
              end

            send(gs_pid, {:poll_replied, caller_pid, self()})
            GenServer.reply(from, result)
          end)

        pollers = Map.put(state.pollers, caller_pid, {task_queue, ref})
        poll_tasks = Map.put(state.poll_tasks, task_pid, task_queue)
        {:noreply, %{state | pollers: pollers, poll_tasks: poll_tasks}}

      :error ->
        {:reply, {:error, :worker_not_registered}, state}
    end
  end

  # Pop the next queued completion for `task_queue` and start it (the task_queue
  # stays active because a Task is now in flight); if nothing is queued, mark the
  # task_queue idle. A missing queue and a drained queue are the same "nothing
  # pending" case, so we normalise to an empty queue and let `:queue.out` decide.
  defp dispatch_next_completion(state, task_queue) do
    queue = Map.get(state.completion_queue, task_queue, :queue.new())

    case :queue.out(queue) do
      {{:value, {next_from, next_bytes}}, rest_q} ->
        new_state = %{
          state
          | completion_queue: put_pending(state.completion_queue, task_queue, rest_q)
        }

        start_workflow_completion_task(new_state, task_queue, next_from, next_bytes)
        new_state

      {:empty, _queue} ->
        %{
          state
          | completion_active: MapSet.delete(state.completion_active, task_queue),
            completion_queue: Map.delete(state.completion_queue, task_queue)
        }
    end
  end

  # Store the drained remainder back under `task_queue`, dropping the key once
  # the queue is empty so the map never accumulates stale empty queues.
  defp put_pending(completion_queue, task_queue, rest_q) do
    if :queue.is_empty(rest_q) do
      Map.delete(completion_queue, task_queue)
    else
      Map.put(completion_queue, task_queue, rest_q)
    end
  end

  # Spawn a Task that calls the `complete_workflow_activation` NIF and,
  # when done, sends `{:workflow_activation_flushed, task_queue, from, result}`
  # back to the holder (which replies to the caller and dequeues the next
  # waiting call). The handle is read from state at call time — the Task
  # does NOT capture it from a closure so it always uses the live handle.
  defp start_workflow_completion_task(state, task_queue, from, bytes) do
    handle = Map.fetch!(state.handles, task_queue)
    gs_pid = self()

    Task.Supervisor.start_child(state.task_sup, fn ->
      result =
        try do
          Bridge.worker_complete_workflow_activation(handle, bytes)
        rescue
          e in ArgumentError ->
            # Emit telemetry before signalling the cascade.
            :telemetry.execute(
              [:hourglass, :worker, :registration_failed],
              %{count: 1},
              %{failure_class: :nif_reload, task_queue: task_queue, detail: Exception.message(e)}
            )

            signal_nif_reload!("worker_complete_workflow_activation")
            {:error, {:nif_exception, e}}

          e ->
            {:error, {:nif_exception, e}}
        catch
          kind, value -> {:error, {:nif_exit, kind, value}}
        end

      send(gs_pid, {:workflow_activation_flushed, task_queue, from, result})
    end)
  end

  defp maybe_dispatch_recycle(state, task_queue, caller_pid) do
    case {Map.get(state.handles, task_queue), Map.get(state.register_opts, task_queue)} do
      {old_handle, opts} when is_reference(old_handle) and is_list(opts) ->
        Logger.warning(
          "BridgeHolder recycling handle for task_queue=#{task_queue} after poll-caller " <>
            "#{inspect(caller_pid)} died parked on dirty NIF"
        )

        runtime = Runtime.handle()
        config_bin = build_worker_config(task_queue, opts)
        gs_pid = self()

        Task.Supervisor.start_child(state.task_sup, fn ->
          _shutdown_result = safe_worker_release(old_handle, task_queue)
          result = recycle_new(runtime, config_bin, task_queue)
          send(gs_pid, {:handle_recycled, task_queue, result})
        end)

        {:noreply, %{state | recycling: MapSet.put(state.recycling, task_queue)}}

      _other ->
        # Caller died but its task queue is no longer registered (e.g.
        # `unregister_worker` ran while the caller was parked, or the
        # holder never had register_opts for it). Nothing to recycle.
        {:noreply, state}
    end
  end

  # `worker_shutdown` stops a handle polling the server. On a recycle's old
  # handle it makes every in-flight poll return :shutdown — the orphan from
  # the dead caller AND any healthy parallel poll — since nothing is
  # outstanding on it that Core waits for. A stale handle (NIF reload)
  # signals the cascade instead of raising into the caller.
  # Let go of a handle: shut it down, and abort every poll on it, which Core's
  # shutdown alone leaves parked — on a dirty-IO thread — for as long as
  # anything it handed out is outstanding. Activities it handed out still
  # complete through it (see "Handle recycle and the work it handed out").
  defp safe_worker_release(handle, task_queue) do
    result = safe_worker_shutdown(handle, task_queue)
    _aborted = Bridge.worker_abort_polls(handle)
    result
  rescue
    ArgumentError -> :nif_reloaded
  end

  defp safe_worker_shutdown(handle, task_queue) do
    Bridge.worker_shutdown(handle)
  rescue
    err in ArgumentError ->
      :telemetry.execute(
        [:hourglass, :worker, :registration_failed],
        %{count: 1},
        %{failure_class: :nif_reload, task_queue: task_queue, detail: Exception.message(err)}
      )

      signal_nif_reload!("worker_shutdown")
      :nif_reloaded
  end

  # Allocate a fresh bridge handle for the recycled task queue.
  defp recycle_new(runtime, config_bin, task_queue) do
    Bridge.worker_new(runtime, config_bin)
  rescue
    err in ArgumentError ->
      :telemetry.execute(
        [:hourglass, :worker, :registration_failed],
        %{count: 1},
        %{failure_class: :nif_reload, task_queue: task_queue, detail: Exception.message(err)}
      )

      signal_nif_reload!("worker_new/recycle")
      {:error, :nif_reloaded}
  end

  # Run Bridge.worker_new on the holder's Task.Supervisor and report the
  # result back to the holder via internal :store_handle / :registration_failed
  # messages. Extracted from the register_worker handle_call to keep that
  # clause shallow.
  defp do_worker_new_async(runtime, config_bin, gs_pid, task_queue, opts, from) do
    result =
      try do
        Bridge.worker_new(runtime, config_bin)
      rescue
        err in ArgumentError ->
          # Emit telemetry before cascading.
          :telemetry.execute(
            [:hourglass, :worker, :registration_failed],
            %{count: 1},
            %{failure_class: :nif_reload, task_queue: task_queue, detail: Exception.message(err)}
          )

          signal_nif_reload!("worker_new/register")
          {:error, :nif_reloaded}
      end

    case result do
      {:ok, handle} ->
        send(gs_pid, {:store_handle, task_queue, handle, opts, from})

      {:error, _reason} = err ->
        send(gs_pid, {:registration_failed, task_queue})
        GenServer.reply(from, err)
    end
  end

  # Delegate to the shared `Bridge.signal_nif_reload!/1`. `BridgeHolder`
  # has bespoke retry/recycle semantics per call shape (poll, complete,
  # recycle, register), so it can't blanket-wrap calls with
  # `Bridge.with_nif_reload_rescue/2` the way stateless callers do —
  # but the cascade-signal step itself is the same across the board.
  defp signal_nif_reload!(call_site), do: Bridge.signal_nif_reload!(call_site)

  @doc false
  @spec build_worker_config(String.t(), register_opts()) :: binary()
  def build_worker_config(task_queue, opts) do
    # Workers MUST poll the same namespace that workflow starts dispatch
    # to. Defer to Hourglass.Client.default_namespace/0 so app config
    # is the single source of truth — otherwise a worker registered without
    # explicit :namespace polls "default" while workflows go to whatever
    # the configured namespace is, and the two never meet.
    #
    # The three :max_outstanding_* opts default to 0, the wire-level
    # "unset" sentinel — the Rust side substitutes its hardcoded
    # DEFAULT_OUTSTANDING constant when it sees 0. Callers that want
    # bigger concurrency pass non-zero ints explicitly (typically pulled
    # from `Application.get_env(:hourglass, Hourglass.Worker, [])`
    # by `Hourglass.WorkerLauncher`).
    Protobuf.encode(%Hourglass.Proto.WorkerConfig{
      namespace: Keyword.get(opts, :namespace, Client.default_namespace()),
      task_queue: task_queue,
      max_cached_workflows:
        Keyword.get(opts, :max_cached_workflows, @default_max_cached_workflows),
      client_target_url: Keyword.get(opts, :target_url, Client.default_target_url()),
      max_outstanding_workflow_tasks: Keyword.get(opts, :max_outstanding_workflow_tasks, 0),
      max_outstanding_activities: Keyword.get(opts, :max_outstanding_activities, 0),
      max_outstanding_local_activities: Keyword.get(opts, :max_outstanding_local_activities, 0),
      graceful_shutdown_period_ms:
        Keyword.get(opts, :graceful_shutdown_period, @default_graceful_shutdown_period)
    })
  end
end
