defmodule Hourglass.Application do
  @moduledoc """
  OTP Application entry point for the Hourglass Temporal SDK.

  The supervision tree is split into **local singletons** (always
  started) and the **cluster-facing runtime** (gated behind
  `config :hourglass, :start_runtime`). The split keeps the library
  inert by default: embedding it as a dependency, running a host's
  non-Temporal tests, or booting `iex -S mix` brings up only the cheap
  ETS/DynSup singletons — no pollers, no bridge handles, no cluster
  connection — until the host opts in.

  ## ETS table (created imperatively before the supervisor starts)

  `Hourglass.Worker.WorkflowStateCache` uses a `:named_table` ETS table
  created by calling `WorkflowStateCache.ensure_table/0` at the top of
  `start/2`, before `Supervisor.start_link/2`. The call is idempotent —
  it is safe to invoke from both `Application.start/2` and test setup.

  ## Local singletons (always-on)

  `local_singletons/0` returns two DynamicSupervisors started
  unconditionally because they are cheap, hold no cluster resources, and
  are needed the moment any Worker (default or per-test) spins up:

    * `Hourglass.WorkflowEvaluator.DynamicSupervisor` — shared DynSup
      for per-activation evaluator Tasks.
    * `Hourglass.ActivityExecutor.DynamicSupervisor` — shared DynSup
      for ephemeral activity-executor Tasks.

  These DynSups live at Application scope (siblings of the `Subsystem`,
  not children) precisely so in-flight Tasks survive a `BridgeHolder`
  crash + `:rest_for_one` cascade inside the Subsystem. See
  `Hourglass.Subsystem` for the cascade contract.

  ## Cluster-facing runtime (gated)

  Started only when `config :hourglass, :start_runtime` is `true`:

    * `Hourglass.Subsystem` — `:rest_for_one` group of `Runtime`,
      `BridgeHolder`, `WorkerRegistry`, `Worker.Supervisor`, and
      `WorkerLauncher`. The launcher registers the default
      `"default"`-task-queue Worker (when
      `config :hourglass, :start_default_worker` is `true`);
      modules are resolved structurally from the Temporal type name
      — no workflow/activity inventory config is required.
    * `Hourglass.NamespaceEnsurer` — runs after the Subsystem so
      `Runtime` is alive when it calls into the Temporal client.
      Returns `:ignore` once it succeeds; crashes the boot supervisor
      on connect/namespace-register failure so operators see the real
      error.

  ## The gate

  `config :hourglass, :start_runtime` defaults to `false`. Hosts that
  embed Hourglass and want the live worker tree set it `true`. The
  `:temporal` integration test suite either sets it `true` or brings
  up the `Subsystem` via `start_supervised/1` per test.
  """

  use Application

  alias Hourglass.Worker.WorkflowStateCache

  @impl Application
  def start(_type, _args) do
    # Per-run-id workflow state cache: create the ETS table before the
    # supervisor starts. ensure_table/0 is idempotent — safe to call here
    # and from test setup without raising.
    ensure_state_cache()

    children =
      local_singletons() ++ if start_runtime?(), do: runtime_children(), else: []

    Supervisor.start_link(children, strategy: :one_for_one, name: Hourglass.Supervisor)
  end

  # Cheap, cluster-free singletons every Worker needs. Always started so
  # the library is usable (per-test Workers, host startup) the instant a
  # task queue is served — independent of whether the gated cluster tree
  # is up.
  defp local_singletons do
    [
      Hourglass.WorkflowEvaluator.DynamicSupervisor,
      Hourglass.ActivityExecutor.DynamicSupervisor
    ]
  end

  # The cluster-facing tree. Subsystem groups Runtime + BridgeHolder +
  # WorkerRegistry + Worker.Supervisor + WorkerLauncher under a
  # :rest_for_one parent so a BridgeHolder crash drops every stale
  # bridge handle in a single cascade. NamespaceEnsurer runs last so
  # Runtime is alive when it talks to the cluster.
  defp runtime_children do
    [
      Hourglass.Subsystem,
      Hourglass.NamespaceEnsurer
    ]
  end

  defp start_runtime?, do: Application.get_env(:hourglass, :start_runtime, false)

  defp ensure_state_cache do
    WorkflowStateCache.ensure_table()
  end
end
