defmodule Hourglass.Subsystem do
  @moduledoc """
  `:rest_for_one` supervisor wrapping the five Temporal children whose
  lifetimes are tied to the bridge-handle registry:

    1. `Hourglass.Runtime` — owns the `CoreRuntimeResource` NIF
       resource ref. First child so a Runtime crash (notably via the
       `:nif_reloaded` exit signal that `BridgeHolder` raises when it
       detects an `ArgumentError` from `Bridge.worker_new/2` — the
       symptom of `Phoenix.CodeReloader` reloading the `Bridge` module
       and invalidating every pre-reload resource ref) cascades the
       entire subtree, dropping every stale handle in a single restart.
    2. `Hourglass.BridgeHolder` — owns every bridge handle in the VM.
    3. `Hourglass.WorkerRegistry` — `:via` registry mapping
       task-queue names to `Hourglass.Worker` GenServer pids.
    4. `Hourglass.Worker.Supervisor` — `DynamicSupervisor` of
       per-task-queue `Hourglass.Worker` GenServers.
    5. `Hourglass.WorkerLauncher` — boot child that registers the
       default `"default"`-task-queue Worker against `Worker.Supervisor`.
       The worker resolves workflow and activity modules structurally from
       the Temporal type name (no module-list config required).
       Inside the Subsystem so a `BridgeHolder` cascade re-runs it and
       re-establishes the default Worker against the fresh subtree.

  ## Why a dedicated subsystem?

  Previously these five were direct children of `Hourglass.Application`'s
  top-level `:one_for_one` supervisor. If `BridgeHolder` crashed
  (rare — `Logger.error :task_sup_crashed` exit, or other runtime
  bug), only `BridgeHolder` was restarted. Existing
  `Hourglass.Worker` GenServers (under `Worker.Supervisor`) had
  no idea their bridge-handle registrations were lost — their poll
  loops entered a 50 ms-sleep retry loop on
  `{:error, :worker_not_registered}` *forever*, requiring manual
  intervention to recover.

  Wrapping the five in a `:rest_for_one` supervisor makes a
  `BridgeHolder` crash cascade through `WorkerRegistry` and
  `Worker.Supervisor`. After the cascade, the new `BridgeHolder`
  starts with empty `handles`, the new `WorkerRegistry` is empty,
  and `Worker.Supervisor` is a fresh `DynamicSupervisor` with no
  children. Zombie state is gone.

  ## In-flight Task survival

  In-flight evaluator and activity-executor `Task`s live under
  *sibling* DynSups at `Hourglass.Application` scope:

    * `Hourglass.WorkflowEvaluator.DynamicSupervisor`
    * `Hourglass.ActivityExecutor.DynamicSupervisor`

  They are NOT children of this subsystem, so a `BridgeHolder` crash
  does not kill them. They keep running. Their next
  `BridgeHolder.complete_*` call returns
  `{:error, :worker_not_registered}` (the fresh `BridgeHolder` has
  no registration for their old task queue) — per
  `Hourglass.Worker.WorkflowEvaluator.run/1`'s error handling, that
  error is logged and the `Task` exits `:normal`. Core then
  redelivers the activation on the next poll on a fresh consumer.

  ## Recovery semantics — caller responsibility

  `Worker.Supervisor` is a `DynamicSupervisor`. Its children — the
  per-task-queue `Worker` GenServers — are dynamically registered
  via `Worker.Supervisor.start_worker/1`. Cascade-restarting
  `Worker.Supervisor` therefore terminates every `Worker` it was
  parenting; on restart, `Worker.Supervisor` is empty.

  After a `BridgeHolder` crash + cascade, the default `"default"`-task-
  queue Worker is re-registered automatically via `WorkerLauncher` (the
  5th child). Test-spawned per-test Workers use unique task queues and
  must be re-registered by the test harness — `BridgeHolder` crashes
  inside a test are rare enough that this is acceptable.

  ## Child ordering rationale

  `:rest_for_one` cascades restarts to every child *after* the one
  that died. Order matters:

    1. `Runtime` first — if it dies (notably via `BridgeHolder` raising
       `:nif_reloaded` after detecting an `ArgumentError` from
       `Bridge.worker_new/2`), cascade everything else. Every Bridge
       resource ref (CoreRuntime, Worker, Client) issued by the
       pre-reload NIF is now opaque; resetting the whole subtree
       re-acquires fresh refs against the freshly loaded NIF.

    2. `BridgeHolder` second — if it dies, cascade `WorkerRegistry`
       (its via-name entries are stale) and `Worker.Supervisor`
       (its Workers' bridge handles are gone).

    3. `WorkerRegistry` third — if it somehow dies independently
       of `BridgeHolder`, cascade only `Worker.Supervisor` (its
       Workers' via-names are gone). `BridgeHolder` keeps its
       handles, but every Worker is a fresh registration anyway.

    4. `Worker.Supervisor` last — its children are dynamic; if it
       dies on its own, the others are unaffected and the application
       restarts Workers via `start_worker/1`.

  ## Test-helper parity

  `test/test_helper.exs` boots the app-scope singletons; the
  `:temporal` integration suite brings up this subsystem (under
  `Hourglass.Application` with `:start_runtime` true, or via
  `start_supervised/1`) so the production-shape tests exercise the same
  supervision-tree structure. The cascade-restart regression test
  for `BridgeHolder` lives in
  `test/hourglass/subsystem_test.exs` (`async: false` because it
  deliberately crashes the globally-shared holder) and relies on this
  module to assert the correct cascade.
  """

  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, [], Keyword.put_new(opts, :name, __MODULE__))
  end

  @impl Supervisor
  def init(_arg) do
    children = [
      Hourglass.Runtime,
      Hourglass.BridgeHolder,
      Hourglass.WorkerRegistry,
      Hourglass.Worker.Supervisor,
      Hourglass.WorkerLauncher
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
