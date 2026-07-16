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
  # Cap test concurrency to what the BEAM's dirty-IO scheduler pool can sustain.
  #
  # Every :temporal test `start_supervised`s its own Hourglass.Worker, and each
  # Worker holds TWO blocking long-poll NIF calls (workflow + activity) parked on
  # dirty-IO schedulers for the life of the poll. ExUnit's default max_cases is
  # `schedulers_online * 2` (20 on a 10-core box) while the default dirty-IO pool
  # is 10 (see README, "Dirty-IO schedulers"). So the default lane wants ~40 slots
  # and gets 10: the pool starves, most Workers never poll at all, their workflows
  # sit Running forever, and `mix test.integration` HANGS rather than failing —
  # the worst failure mode, since a hang is indistinguishable from slow progress.
  #
  # Deriving the cap from the live pool keeps this correct on any machine, and
  # lets the README's own remedy pay off instead of fighting it: with `+SDio 128`
  # the computed cap lands above ExUnit's default, nothing is capped, and the
  # suite runs at full width. Measured: ~14s uncapped with +SDio 128, ~15s capped
  # on the default pool — both far better than the ~29s a serial `--trace` costs.
  #
  # The `- 1` is headroom: worker SHUTDOWN also calls into the NIF, and must not
  # deadlock against a pool fully occupied by polls.
  dirty_io = :erlang.system_info(:dirty_io_schedulers)
  supported_workers = max(1, div(dirty_io, 2) - 1)
  requested = ExUnit.configuration() |> Keyword.get(:max_cases, 1)

  if requested > supported_workers do
    IO.puts(
      "[hourglass] capping max_cases #{requested} -> #{supported_workers}: each Worker holds 2 " <>
        "blocking dirty-IO long-polls and this VM has only #{dirty_io} dirty-IO schedulers. " <>
        "Run with ERL_FLAGS=\"+SDio 128\" to lift the cap and run the suite at full width."
    )

    ExUnit.configure(max_cases: supported_workers)
  end

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
