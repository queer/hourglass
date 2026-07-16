# Hourglass

[![Hex.pm](https://img.shields.io/hexpm/v/hourglass.svg)](https://hex.pm/packages/hourglass)
[![Hexdocs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/hourglass)

Hourglass is a standalone Elixir SDK for [Temporal](https://temporal.io). It provides workflow and activity definitions, a worker that polls a Temporal cluster, a client for starting and observing workflows, a deterministic replayer, and a Rust NIF bridge over [`temporalio-sdk-core`](https://github.com/temporalio/sdk-core).

## ⚠️ WARNING ⚠️

This repo is entirely, 100% vibe-coded. A human has not read the code. It looks like it functions correctly, but user beware!

## Requirements

- Elixir `~> 1.15`
- Erlang/OTP 25+
- A Rust toolchain (stable; the NIF builds via [Rustler](https://github.com/rusterlium/rustler) at `mix deps.compile` time)
- A running Temporal cluster for workflow execution — the default test suite is cluster-free; integration tests require a cluster and are tagged `:temporal` / `:integration`

## Installation

Add `hourglass` to your dependencies in `mix.exs`:

```elixir
defp deps do
  [
    {:hourglass, "~> 0.5.0"}
  ]
end
```

Run `mix deps.get && mix deps.compile`. The Rustler build step compiles the NIF (`native/hourglass`) — this requires `cargo` on your `PATH`.

The `temporalio` Hex package supplies generated `Temporal.Api.*` protobuf modules. Generated proto modules are also committed under `lib/proto/` in this repo, so consumers do **not** need `protoc` or a proto compilation step.

## Defining a workflow

Declare `use Hourglass.Workflow, input: X, output: Y` and implement `run/1`:

```elixir
defmodule MyApp.Workflows.Hello do
  use Hourglass.Workflow, input: MyApp.Hello.Args, output: MyApp.Hello.Result

  @impl true
  def run(%MyApp.Hello.Args{name: name}) do
    greeting =
      execute_activity!(MyApp.Activities.Greet, %MyApp.Greet.In{name: name},
        start_to_close: {:sec, 10})

    %MyApp.Hello.Result{greeting: greeting}
  end
end
```

`input` and `output` are each a `Hourglass.Schema` module or a scalar atom (`:map`, `:string`, …). Both default to `:map`. An optional `signals:` map declares typed signal schemas — `signals: %{reply: MyApp.Reply}`.

`use Hourglass.Workflow` imports:

| Function | Purpose |
|---|---|
| `execute_activity/2,3` | Schedule an activity (durable); returns `{:ok, value} \| {:error, _}` |
| `execute_activity!/2,3` | Like `execute_activity/2,3` but returns the value directly, raises `Hourglass.ActivityError` on terminal failure |
| `async/1` | Spawn a concurrent scope (returns an opaque handle) |
| `await/1` | Resolve an `async/1` scope handle |
| `await_all/1` | Join a list of `async/1` handles, returning values in order |
| `sleep/1` | Durable timer; durations as `{:sec, n}` / `{:min, n}` / integer ms |
| `await_signal/1` | Block until a named signal arrives, return its payload |
| `cancelled?/0` | Returns `true` if a cancellation request has been delivered |
| `continue_as_new/1` | Emit `ContinueAsNewWorkflowExecution` — reset history and continue |
| `info/0` | Per-activation context (`run_id`, `task_queue`) |
| `uuid/0` | Deterministic UUID from the SDK |
| `random/1` | Deterministic random integer from the SDK |

Workflow code is **deterministic by re-execution** — the evaluator replays the body from the top on each activation. Non-deterministic primitives (`:rand`, `System.monotonic_time`, `DateTime.utc_now`, `Process.sleep`, …) cause a **compile error** via a `@before_compile` lint and are also flagged by the packaged `Hourglass.Check.WorkflowDeterminism` Credo check. Use `uuid/0` / `random/1` / `sleep/1` instead.

A workflow cannot author its own terminal failure — an uncaught exception **parks** the workflow as a workflow-task failure (the server retries; deploy a fix to resume). Business outcomes are return values. The event `[:hourglass, :workflow, :task_failed]` is emitted on each such park.

### Signals, timers & cancellation

```elixir
defmodule MyApp.Workflows.Approval do
  use Hourglass.Workflow,
    input: MyApp.Approval.Args,
    output: MyApp.Approval.Result,
    signals: %{approved: MyApp.Approval.Signal}

  @impl true
  def run(%MyApp.Approval.Args{} = args) do
    # Wait up to 24 hours for an approval signal
    sleep({:hour, 24})

    if cancelled?() do
      %MyApp.Approval.Result{status: :cancelled}
    else
      %MyApp.Approval.Signal{} = await_signal(:approved)
      %MyApp.Approval.Result{status: :approved}
    end
  end
end
```

From the client side:

```elixir
# Send a signal to a running workflow
:ok = Hourglass.signal(handle_or_id, "approved", %MyApp.Approval.Signal{by: "alice"})

# Request cancellation
:ok = Hourglass.cancel(handle_or_id, "operator requested")
```

### Child workflows

A workflow can orchestrate another workflow as a real Temporal child:

```elixir
# Await the child's result.
{:ok, result} = execute_child(MyApp.Workflows.Ingest, %{"url" => url})

# Or raise on failure.
result = execute_child!(MyApp.Workflows.Ingest, %{"url" => url})

# Fan out — no child-specific machinery; the ordinary scopes compose.
urls
|> Enum.map(fn u -> async(fn -> execute_child!(MyApp.Workflows.Ingest, %{"url" => u}) end) end)
|> await_all()

# Fire-and-forget. :parent_close_policy is required — see below.
{:ok, handle} =
  start_child(MyApp.Workflows.Ingest, %{"url" => url}, parent_close_policy: :abandon)
```

`start_child/3` requires `:parent_close_policy` because Temporal's default is
TERMINATE: a parent that completes right after a naive fire-and-forget would
silently kill its child mid-flight. Pass `:abandon` to let the child outlive the
parent, or `:terminate` to tie its lifetime to the parent's.

Child workflow ids default to a deterministic derivation from the parent's
`run_id` and the call's command id. Pin `:id` for singleton semantics (pair with
`workflow_id_reuse_policy: :reject_duplicate`). A random nonce or a timestamp is
illegal — workflow bodies must be deterministic; use `uuid/0` for a
readable-and-unique id.

Options: `:id`, `:task_queue`, `:parent_close_policy`, `:workflow_id_reuse_policy`,
`:workflow_execution_timeout`, `:workflow_run_timeout`, `:workflow_task_timeout`,
`:retry_policy`.

## Defining an activity

Declare `use Hourglass.Activity, input: X, output: Y, retry: [...]` and implement `execute/1`. Return the result value directly on success, or `{:error, reason}` / raise to fail:

```elixir
defmodule MyApp.Activities.Greet do
  use Hourglass.Activity, input: MyApp.Greet.In, output: :string, retry: [max_attempts: 3]

  @impl true
  def execute(%MyApp.Greet.In{name: name}), do: "Hello, #{name}!"
end
```

One activity module = one activity type. The `retry:` keyword (optional) sets the module-default Temporal `RetryPolicy`; omitting it gives `[max_attempts: 1]` (no retry).

Inside `execute/1`, call `Hourglass.Activity.info/0` for per-dispatch context:

```elixir
def execute(%MyApp.Greet.In{} = args) do
  ctx = Hourglass.Activity.info()
  # ctx.workflow_id, ctx.run_id, ctx.activity_id, ctx.attempt
  ...
end
```

### Activity heartbeat & cancellation

Long-running activities should heartbeat periodically. `Hourglass.Activity.heartbeat/0` resets the
activity's Temporal `heartbeat_timeout` and returns `:ok | :cancel` — `:cancel` once Temporal has
cancelled the activity. `heartbeat!/0` raises `Hourglass.Activity.Cancelled` on cancel instead of
returning it, and the runner reports a Temporal Cancellation completion. Check the return (or use
the bang form) at your loop's natural boundaries so a cancelled activity stops instead of running
on as a zombie after Temporal has abandoned it:

```elixir
def execute(_args) do
  Enum.each(work, fn item ->
    do_chunk(item)
    Hourglass.Activity.heartbeat!()   # raises Cancelled when cancelled → the activity unwinds
  end)
end
```

`heartbeat/0` is best-effort and never crashes the activity body (a missing/absent runtime degrades
to `:ok`).

> **Scope:** cancellation is delivered by Temporal Core as a Cancel activity task — currently on
> **heartbeat timeout** and server-side activity cancellation. Requesting cancellation of the parent
> *workflow* (`Hourglass.cancel/2`) does not yet propagate to in-flight activities (the evaluator
> flags the workflow cancelled but issues no activity-cancel command).

## Running a worker

Configure Hourglass in `config/runtime.exs` (or `config/config.exs`):

```elixir
# Start the Temporal runtime and default worker at application boot
config :hourglass, :start_runtime, true
config :hourglass, :start_default_worker, true
```

With `start_runtime: true` and `start_default_worker: true`, Hourglass starts its supervision tree (runtime, worker supervisor, poll loops) under `Hourglass.Application` when your OTP application boots. No additional start-up code is required.

The worker needs only a task queue — workflow and activity modules are resolved **structurally** at dispatch time. The Temporal type name on the wire is `Atom.to_string(module)` (e.g. `"Elixir.MyApp.Workflows.Hello"`), so the worker recovers the module atom via `String.to_existing_atom/1` and confirms it is a loaded Hourglass workflow or activity by checking for the `__workflow_input_type__/0` / `__activity_input_type__/0` marker generated by `use Hourglass.Workflow` / `use Hourglass.Activity`. There is no module inventory to configure.

Worker concurrency defaults (override as needed):

```elixir
config :hourglass, Hourglass.Worker,
  max_outstanding_workflow_tasks: 100,
  max_outstanding_activities: 100,
  max_outstanding_local_activities: 100
```

### Dirty-IO schedulers

Each worker holds two blocking long-poll calls (workflow + activity) into the
Rust NIF, scheduled on the BEAM's **dirty-IO schedulers**. The default pool is 10
(`erlang:system_info(dirty_io_schedulers)`), so a deployment running more than a
handful of workers — or that shuts workers down under load — should raise it so
poll and shutdown calls never starve each other. Set it at VM boot via
`vm.args`/`ERL_FLAGS`, sized to roughly `2 × (max concurrent workers) + headroom`:

```
+SDio 128
```

Starving this pool does **not** surface as an error: the polls simply never get a
scheduler, workflows sit `Running` forever, and the process looks slow rather than
broken. If workers appear to hang under concurrency, check this first.

The integration suite is subject to exactly this: every `:temporal` test starts its
own worker, so `test/test_helper.exs` caps ExUnit's `max_cases` to
`(dirty_io_schedulers / 2) - 1` and says so on stderr. Lift the cap — and run the
suite at full width — by giving the VM a bigger pool:

```
ERL_FLAGS="+SDio 128" mix test.integration
```

## Starting and observing workflows

```elixir
# Start a workflow — returns {:ok, %Hourglass.WorkflowHandle{}} or {:error, ...}
{:ok, handle} = Hourglass.start(MyApp.Workflows.Hello, %{"name" => "Alice"}, id: "my-run-1")

# Send a signal to a running workflow
:ok = Hourglass.signal(handle, "proceed", %{"value" => 42})

# Request cancellation of a running workflow
:ok = Hourglass.cancel(handle, "operator requested")

# Snapshot the current state — cheap, one RPC call
{:ok, status} = Hourglass.status(handle)
# status.state :: :running | :completed | :failed | :canceled | :terminated | ...

# Poll until the workflow closes and return the result
{:ok, result} = Hourglass.result(handle, timeout: 30_000)
```

`Hourglass.status/2` accepts `failures: :include` to also walk history and populate `status.recent_failures` with `ActivityTaskFailed` events — useful for operator tooling and test assertions.

`Hourglass.result/2` polls `status/2` until the workflow closes or the timeout budget expires. Production code should use it sparingly — long-running workflows may execute for hours or days. Prefer observing projections that workflows commit to, or poll `status/2` on demand.

## Telemetry

Hourglass emits the following `:telemetry` events:

| Event | Description |
|---|---|
| `[:hourglass, :connection, :failed]` | Temporal connection failed |
| `[:hourglass, :worker, :registration_failed]` | Worker registration failed |
| `[:hourglass, :activity, :failure]` | Activity returned `{:error, _}` or raised; metadata includes classification |
| `[:hourglass, :activity, :exception]` | Unhandled exception in activity dispatch |
| `[:hourglass, :activity, :dispatch_failed]` | Activity could not be dispatched |
| `[:hourglass, :activity, :heartbeat]` | Activity recorded a liveness heartbeat |
| `[:hourglass, :activity, :cancel_received]` | A Cancel activity task arrived (heartbeat timeout / server-side cancel); metadata includes `reason` |
| `[:hourglass, :activity, :heartbeat_lost]` | *(reserved — not yet emitted)* |
| `[:hourglass, :activity, :failure, :unclassified]` | *(reserved — not yet emitted)* |
| `[:hourglass, :workflow, :task_failed]` | Workflow-task parked as a failure (uncaught exception; server will retry on next activation) |
| `[:hourglass, :workflow, :exception]` | Unhandled exception in workflow evaluation |
| `[:hourglass, :workflow, :unhandled_job_variant]` | Unknown activation job variant |
| `[:hourglass, :bridge_holder, :activity_result_unrouted]` | Activity result had no waiting caller |
| `[:hourglass, :replay, :mismatch]` | *(reserved — intended for the replay CI gate)* |

`Hourglass.Telemetry.events/0` returns the full list at runtime.

**Opt-in default logger:** call `Hourglass.Telemetry.LoggerHandler.attach/0` (e.g. from your `Application.start/2`) to log all events at `info` level. Detach with `Hourglass.Telemetry.LoggerHandler.detach/0`. Most applications will want their own handlers that project events into metrics or structured audit logs.

## Custom retry classification

By default all activity failures are classified as `:retryable` (the `Hourglass.Activity.RetryClassifier.Default` module). Implement the `Hourglass.Activity.RetryClassifier` behaviour to add domain-specific rules:

```elixir
defmodule MyApp.RetryClassifier do
  @behaviour Hourglass.Activity.RetryClassifier

  @impl true
  def classify(%MyApp.PermanentError{} = err, _ctx),
    do: {:non_retryable, %{type: "PermanentError", message: err.message, details: nil}}

  def classify(err, ctx),
    do: Hourglass.Activity.RetryClassifier.Default.classify(err, ctx)
end
```

Then configure:

```elixir
config :hourglass, :retry_classifier, MyApp.RetryClassifier
```

The callback receives `{error, context}` where `context` is a map with optional keys `:activity_name` and `:caller` (`:rescue` | `:tuple_error`). Return `{classification, metadata}` where `classification` is `:retryable | :non_retryable | :unclassified`.

Note: retry eligibility (which error shapes retry) belongs to the classifier. Per-call `retry_policy` overrides on `execute_activity/4` may only tune quantity (`max_attempts`, `initial_interval`, `backoff_coefficient`, `max_interval`) — attempting to set `retryable_error_types` or `non_retryable_error_types` at the call site raises `ArgumentError`.

## Development

```bash
# Cluster-free tests (default suite)
mix test

# Integration / end-to-end tests against a real cluster (see below)
mix test.integration   # == mix test --include temporal --include integration

# Regenerate protobuf modules (requires protoc + protoc-gen-elixir)
mix hourglass.proto

# Static analysis
mix credo
mix dialyzer
```

### Integration tests

The `:temporal` / `:integration` tests run against a live Temporal cluster.
`compose.yaml` brings one up (Postgres + `temporalio/auto-setup`, plus the web UI
on http://localhost:8233) and registers the `hourglass-test` namespace the suite
uses:

```bash
podman compose up -d        # or: docker compose up -d
mix test.integration
podman compose down         # add -v to also wipe the database volume
```

The frontend binds the SDK default `localhost:7233`. To run the cluster on a
different host port (e.g. alongside another local Temporal), set
`HOURGLASS_TEMPORAL_PORT` for compose and point the tests at it with
`TEMPORAL_TARGET_URL`:

```bash
HOURGLASS_TEMPORAL_PORT=7333 HOURGLASS_UI_PORT=8234 podman compose up -d
TEMPORAL_TARGET_URL=http://localhost:7333 mix test.integration
```

The NIF is built in debug mode for all non-production Mix environments. If `cargo` is not on your `PATH`, prepend `~/.cargo/bin`:

```bash
PATH="$HOME/.cargo/bin:$PATH" mix compile
```

## License

Released under the [MIT License](LICENSE).
