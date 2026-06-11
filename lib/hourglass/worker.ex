defmodule Hourglass.Worker do
  @moduledoc """
  Per-task-queue Worker — a `GenServer` that owns the bridge-handle
  registration for one task queue and runs a small inner Supervisor
  hosting the two poll loops driving Temporal Core.

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

  Sequence on graceful stop:

    1. Caller invokes `Worker.Supervisor.stop_worker(task_queue)` —
       OR — Application shutdown reverse-walks children and stops
       this GenServer.
    2. `terminate/2` runs (we trap exits) and calls
       `BridgeHolder.unregister_worker(task_queue)`. The bridge handle
       is destroyed; any in-flight long-poll on it returns
       `{:error, %Bridge.Error{kind: :shutdown}}`.
    3. The poll loops see `:shutdown` and exit `:normal`. The inner
       Supervisor reaps them and exits `:normal`.
    4. `terminate/2` returns. The GenServer exits.

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
  reference). If the Worker exits between dispatch and completion,
  the in-flight Tasks see `{:error, :worker_not_registered}` and
  Core redelivers via heartbeat / start-to-close timeout.

  ## Naming

  `start_link/1` registers the GenServer under
  `Hourglass.WorkerRegistry.via(task_queue)` for `via`-name
  reachability. The poll loops do not need per-task-queue names —
  they receive `task_queue` as an arg and call BridgeHolder with it.
  """

  use GenServer

  alias Hourglass.BridgeHolder
  alias Hourglass.Worker.ActivityPollLoop
  alias Hourglass.Worker.WorkflowPollLoop
  alias Hourglass.WorkerRegistry

  @type opts :: [
          task_queue: String.t(),
          namespace: String.t(),
          max_cached_workflows: pos_integer(),
          target_url: String.t()
        ]

  @spec start_link(opts()) :: GenServer.on_start()
  def start_link(opts) do
    task_queue = Keyword.fetch!(opts, :task_queue)
    GenServer.start_link(__MODULE__, opts, name: WorkerRegistry.via(task_queue))
  end

  @doc false
  @spec child_spec(opts()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent,
      shutdown: 10_000
    }
  end

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
  # values stop reaching the Rust worker.
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
    register_opts = Keyword.take(opts, @register_opts_keys)

    with :ok <- BridgeHolder.register_worker(task_queue, register_opts),
         {:ok, sup_pid} <- start_inner_supervisor(task_queue) do
      {:ok, %{task_queue: task_queue, sup_pid: sup_pid}}
    else
      {:error, reason} ->
        # Roll back the bridge allocation if the inner Supervisor
        # failed to start. Idempotent if registration itself failed.
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

  def handle_info(_msg, state), do: {:noreply, state}

  @impl GenServer
  def terminate(_reason, %{task_queue: task_queue, sup_pid: sup_pid}) do
    # CRITICAL: unregister BEFORE the inner Supervisor's children are
    # killed. Destroying the bridge handle here is what makes the
    # poll loops' in-flight long-polls return :shutdown, so they exit
    # :normal instead of being brutal-killed by the Supervisor.
    #
    # `:noproc` exit means BridgeHolder is already gone — typically a
    # `:rest_for_one` cascade in `Hourglass.Subsystem` killed the
    # holder first; its own terminate already called
    # `Bridge.worker_shutdown` on every handle on the way out, so by
    # the time we're here the bridge is already drained and there's
    # nothing for `unregister_worker` to do. Treat as success rather
    # than letting the :noproc propagate (which would log a noisy
    # `[error] GenServer ... terminating` line on every Worker during
    # the cascade — exit-reason logging is suppressed as expected).
    try do
      BridgeHolder.unregister_worker(task_queue)
    catch
      :exit, {:noproc, _call} -> :ok
    end

    # Now stop the inner Supervisor, which reverse-walks the now-quiet
    # poll loops and reaps them. `Supervisor.stop/3` handles dead pids
    # cleanly (the `_ =` swallows :noproc), so no `Process.alive?`
    # guard — that check would be racy anyway.
    _result = Supervisor.stop(sup_pid, :shutdown, 5_000)

    :ok
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
end
