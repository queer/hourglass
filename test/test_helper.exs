ExUnit.start()
ExUnit.configure(exclude: [:temporal])

# Tolerate {:error, {:already_started, _}} so a re-run / partial-boot harness
# does not crash the whole suite.
ensure_started = fn start_fun ->
  case start_fun.() do
    {:ok, _pid} -> :ok
    {:error, {:already_started, _pid}} -> :ok
  end
end

# Credo has runtime: false in mix.exs (it's a dev/test tool, not a runtime dep),
# so its Application (and the GenServers it supervises — SourceFileAST, etc.)
# are not started automatically. Start it here so Credo.Test.Case helpers work.
ensure_started.(fn -> Credo.Application.start(:normal, []) end)

# Hourglass.Application.start/2 runs automatically under `mix test` and,
# with `config :hourglass, :start_runtime, false` (config/test.exs),
# brings up the cheap local singletons ONLY:
#
#   * Hourglass.Worker.WorkflowStateCache (ETS table)
#   * Hourglass.WorkflowEvaluator.DynamicSupervisor
#   * Hourglass.ActivityExecutor.DynamicSupervisor
#
# So those are intentionally NOT started here — doing so would double-start
# and raise {:error, {:already_started, _}}.
#
# The cluster-facing tree (Subsystem = Runtime + BridgeHolder +
# WorkerRegistry + Worker.Supervisor + WorkerLauncher, plus NamespaceEnsurer)
# stays DOWN because start_runtime is false. The `:temporal` integration
# suite (excluded by default above) brings up the full tree itself — either
# by configuring `start_runtime: true` or by `start_supervised(Hourglass.Subsystem)`
# per test — and uses unique task queues for per-test Workers.

# Is the `:temporal` integration suite being run (`mix test --include temporal`)?
temporal_included? =
  ExUnit.configuration()
  |> Keyword.get(:include, [])
  |> Enum.any?(&(&1 == :temporal or match?({:temporal, _opts}, &1)))

if temporal_included? do
  # Integration suite: bring up the full cluster-facing tree once for the run.
  # The Subsystem (:rest_for_one) owns Runtime + BridgeHolder + WorkerRegistry +
  # Worker.Supervisor + WorkerLauncher. WorkerLauncher does NOT auto-start a
  # default worker (`:start_default_worker` is false), so each test starts its
  # own Worker on a unique task queue; those register via WorkerRegistry and
  # connect through BridgeHolder.
  #
  # Requires a live Temporal cluster with the configured namespace
  # (`config :hourglass, Hourglass.Client` → "hourglass-test"). See compose.yaml
  # + README; set TEMPORAL_TARGET_URL if the frontend isn't on :7233.
  ensure_started.(fn -> Hourglass.Subsystem.start_link([]) end)
else
  # Default cluster-free suite: just the Runtime singleton (name-registered,
  # holds the CoreRuntime NIF resource) for runtime_test.exs (async: true).
  # Application does NOT start it when start_runtime is false.
  ensure_started.(fn -> Hourglass.Runtime.start_link([]) end)
end
