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
    {:hourglass, "~> 0.9.0"}
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
        start_to_close_timeout: 10_000)

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
| `fail/2,3` | Emit `FailWorkflowExecution` — end the run as genuinely, terminally failed |
| `info/0` | Per-activation context (`run_id`, `task_queue`) |
| `patched?/1` | Ask whether this execution's history records a patch — change a body without wedging in-flight runs |
| `uuid/0` | Deterministic UUID from the SDK |
| `random/1` | Deterministic random integer from the SDK |

Workflow code is **deterministic by re-execution** — the evaluator replays the body from the top on each activation. Non-deterministic primitives (`:rand`, `System.monotonic_time`, `DateTime.utc_now`, `Process.sleep`, …) cause a **compile error** via a `@before_compile` lint and are also flagged by the packaged `Hourglass.Check.WorkflowDeterminism` Credo check. Use `uuid/0` / `random/1` / `sleep/1` instead.

An uncaught exception **parks** the workflow as a workflow-task failure (the server retries; deploy a fix to resume) — that is not a way to author a business failure, and stays true regardless of `fail/2,3` below. Business outcomes are ordinarily return values. The event `[:hourglass, :workflow, :task_failed]` is emitted on each park.

To end a run as **genuinely, terminally failed** instead — the server marks it `Failed` and does not retry — call `fail/2,3` with a type and a message:

```elixir
case execute_activity(Parse, input) do
  {:ok, result} -> {:ok, result}
  {:error, reason} -> fail("ParseFailed", "could not parse document", details: %{reason: inspect(reason)})
end
```

`fail/2,3` emits `FailWorkflowExecution` inside a *successful* activation completion — the same command oneof as `complete_workflow_execution` — never the workflow-task-failure status a raise produces. `:details` (optional, a JSON-encodable map) and `:non_retryable` (default `true`) round out the same `ApplicationFailureInfo` shape an activity failure carries. The event `[:hourglass, :workflow, :failed]` is emitted on each terminal failure. A redelivered activation after a run has failed re-emits the cached `FailWorkflowExecution` command rather than re-running the body — same handling as a redelivery after a normal completion.

### Changing a workflow body that has runs in flight

Editing a workflow body is not free: an execution started under the old body
replays its history against the new one, finds a command the history does not
have, and fails the workflow task with `[TMPRL1100] Nondeterminism error`.
Replay is deterministic, so it fails identically forever — and the wedged
execution still reports `Running`, so nothing downstream notices.

`patched?/1` is the way through. Guard the changed region with it:

```elixir
def run(input) do
  if patched?(:validate_before_write) do
    execute_activity!(MyApp.Activities.Validate, input, start_to_close_timeout: 30_000)
  end

  execute_activity!(MyApp.Activities.Write, input, start_to_close_timeout: 30_000)
end
```

An execution reaching that call for the first time under the new body answers
`true`, records a patch marker in its own history, and takes the new branch.
One replaying a history written before the patch existed answers `false`,
takes the old branch, and goes on answering `false` for the rest of its life —
including after it has caught up and is running live. Two executions on the
same worker at the same instant answer differently when their histories
differ; the answer is a fact about the execution, never about the deployed
code.

The id is yours to choose — an atom or a string, spelled either way for the
same patch — and yours to **keep stable**: it is the name the decision is
written into history under, so renaming it later makes every execution that
recorded the old name take the old branch again. Nothing polices uniqueness.

Not offered yet: `deprecate_patch`, Temporal's companion for retiring a patch
once no execution predates it.

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

To heartbeat from another process — a process linked to the activity, say, that keeps it alive
while the activity's own process is busy — capture a ref inside the activity with
`Hourglass.Activity.heartbeat_ref/0` and heartbeat with `Hourglass.Activity.heartbeat/1`. It returns
`:ok`, `:cancel` once Core has cancelled the activity, or `:ended`, sending nothing, once that
dispatch of the activity is over; it keeps reaching Temporal through a recycle of the worker's
bridge handle, and never heartbeats for a later attempt:

```elixir
def execute(args) do
  ref = Hourglass.Activity.heartbeat_ref()
  beating = spawn_link(fn -> keep_beating(ref) end)

  try do
    do_the_work(args)
  after
    Process.unlink(beating)
    Process.exit(beating, :kill)
  end
end

defp keep_beating(ref) do
  if Hourglass.Activity.heartbeat(ref) != :ended do
    Process.sleep(1_000)
    keep_beating(ref)
  end
end
```

> **Scope:** an activity is cancelled on **heartbeat timeout**, server-side activity cancellation,
> and a stopping worker's grace running out (see [Stopping a worker](#stopping-a-worker)) — each
> delivered by Temporal Core as a Cancel activity task — and once **its workflow has closed** (see
> [Activities of a closed workflow](#activities-of-a-closed-workflow)). Requesting cancellation of
> the *workflow* (`Hourglass.cancel/2`) cancels no activity by itself (the evaluator flags the
> workflow cancelled but issues no activity-cancel command) unless the activity opts in (see
> [Ending on a cancel request](#ending-on-a-cancel-request)); a workflow that ends on
> `cancelled?/0` closes, and that ends its activities.

## Running a worker

Configure Hourglass in `config/runtime.exs` (or `config/config.exs`):

```elixir
# Start the Temporal runtime and default worker at application boot
config :hourglass, :start_runtime, true
config :hourglass, :start_default_worker, true
```

With `start_runtime: true` and `start_default_worker: true`, Hourglass starts its supervision tree (runtime, worker supervisor, poll loops) under `Hourglass.Application` when your OTP application boots. No additional start-up code is required.

A worker that cannot reach Temporal when it starts — Temporal down or silent during a deploy or a
reboot — does not fail your application's start. It keeps trying to register, after a second,
then twice as long each time up to thirty seconds, logging a warning and emitting
`[:hourglass, :worker, :registration_failed]` for each failed attempt, and serves once Temporal
answers. `Hourglass.NamespaceEnsurer` likewise keeps trying to make its namespace exist. A
configuration Temporal will never accept, such as a namespace it does not know, still fails the
start. See `Hourglass.Worker`'s "Starting".

The worker needs only a task queue — workflow and activity modules are resolved **structurally** at dispatch time. The Temporal type name on the wire is `Atom.to_string(module)` (e.g. `"Elixir.MyApp.Workflows.Hello"`), so the worker recovers the module atom via `String.to_existing_atom/1` and confirms it is a loaded Hourglass workflow or activity by checking for the `__workflow_input_type__/0` / `__activity_input_type__/0` marker generated by `use Hourglass.Workflow` / `use Hourglass.Activity`. There is no module inventory to configure.

Worker concurrency defaults (override as needed):

```elixir
config :hourglass, Hourglass.Worker,
  max_outstanding_workflow_tasks: 100,
  max_outstanding_activities: 100,
  max_outstanding_local_activities: 100
```

### Stopping a worker

A worker stopped gracefully — by its supervisor, by
`Hourglass.Worker.Supervisor.stop_worker/1`, or as the application or node shuts
down — finishes the work it holds before it lets go of Temporal:

1. It stops polling, so Temporal hands it no new workflow or activity tasks.
2. The workflow tasks and activities it is already running finish and report
   their outcome (completion or failure) to Temporal as usual.
3. An activity still running once the **grace** has passed is cancelled
   (`heartbeat!/0` raises `Hourglass.Activity.Cancelled` at its next heartbeat)
   and reported to Temporal as a **retryable failure of type `"WorkerShutdown"`**,
   so Temporal retries it at once — on another worker, as its retry policy allows —
   instead of waiting out its start-to-close timeout. Its process is then stopped.
4. Only then is the worker's handle on Temporal released.

The same holds for a worker that stops because its poll loops' supervisor crashed:
its running activities finish and report through the held handle, and any still
running at the grace is stopped and reported as a `"WorkerShutdown"` failure.

So a stop takes as long as the longest running activity, up to the grace (and is
immediate when nothing is running). The grace defaults to 20 seconds; set it per
worker, or for every worker:

```elixir
# per worker
{Hourglass.Worker, task_queue: "my-queue", graceful_shutdown_period: 30_000}

# every worker
config :hourglass, Hourglass.Worker, graceful_shutdown_period: 30_000
```

`Hourglass.Worker.child_spec/1` sets the worker's shutdown timeout to the grace plus
fifteen seconds, so a supervisor waits for the whole stop. Keep the grace inside
whatever your platform allows a stopping node before it kills it. A worker that is
killed, or a node that crashes, cannot do any of this: its activities' outcomes wait
out their timeouts. See `Hourglass.Worker`'s "Graceful stop" for the details.

### Activities of a closed workflow

Once an activity's workflow has closed — completed, failed, timed out, terminated, ended on a
cancel, or continued as new — nobody is waiting for the activity, and Temporal drops its outcome.
The worker ends such an activity within its **closed-workflow bound**, which defaults to 5 seconds:

1. Every quarter of the bound, the worker asks Temporal about each workflow run that has an
   activity running on its queue, up to sixteen runs at a time.
2. Each activity of a run that has closed is cancelled: `heartbeat/0` answers `:cancel` from then
   on, and `heartbeat!/0` raises `Hourglass.Activity.Cancelled` (reason `:workflow_closed`). An
   activity that heeds it has half the bound to clean up and end by itself. The reason stays
   `:workflow_closed` whatever cancel Temporal Core sends the activity, before or after: one Core
   sends first is checked against Temporal, and reported as `:workflow_closed` if the run has
   closed.
3. One still running half the bound after it was cancelled is killed, and reported to Temporal as
   cancelled.

**What the bound promises, and when.** When Temporal answers each of the worker's checks within a
quarter of the bound — for every run the check asks about — an activity of a run that has closed is
gone within the bound of the run closing: the first check after the close starts within a quarter
of the bound, its answer comes within the next quarter, and the activity then has half the bound.
That holds for every bound the worker accepts. The worker opens its client to Temporal as it starts
and keeps it, so no check first waits for one to open. A Temporal slower than that still has its
answers used — the worker waits up to a quarter of the bound, and at least a second, for each
check — but the activity may then end later than the bound, by as long as the answers were late.

An activity whose run has already closed when its task reaches the worker never runs at all: its
`execute/1` is not called, and it is reported to Temporal as cancelled
(`[:hourglass, :activity, :not_started]`). Temporal hands out no task of a closed run, but Temporal
Core fetches tasks ahead of the worker asking for them, and a busy or paused worker can start one
long after Temporal handed it out; so before each activity starts, the worker asks Temporal about
its run. **Every activity's start waits for that answer**: one round trip to Temporal, and when
Temporal is slow, at most a second, whatever the bound, after which the activity starts.

An activity whose workflow is still running is never ended this way — including when a cancel of
the workflow was requested but its body goes on, unless the activity opts in (below). Nor is one
whose workflow Temporal cannot be asked about: an unreachable Temporal is not a closed workflow, and
a run Temporal has not answered for within a quarter of the bound (at least a second) is asked about
again at a later check. The periodic asking holds no scheduler thread, so a slow or silent Temporal
costs the worker nothing and does not delay its stop. Set the bound, in milliseconds (from 100 to
8589934590), per worker or for every worker:

```elixir
# per worker
{Hourglass.Worker, task_queue: "my-queue", closed_workflow_bound: 2_000}

# every worker
config :hourglass, Hourglass.Worker, closed_workflow_bound: 2_000
```

Temporal Core alone would tell an activity only through a heartbeat it sends, which it throttles
to one per 0.8 of the heartbeat timeout (or 30 seconds without one), and never for an activity
that does not heartbeat; hence the worker asks. See `Hourglass.Worker`'s "Activities of a closed
workflow" for the details.

#### Ending on a cancel request

A cancel requested of a workflow is only a request: its body decides what to do about it, and the
workflow's activities run on. An activity that should stop as soon as its workflow is asked to
cancel opts in:

```elixir
defmodule MyApp.Activities.Generate do
  use Hourglass.Activity, input: :map, output: :map, ends_on_cancel_request: true
  ...
end
```

The worker then treats a cancel requested of the activity's workflow as it treats the workflow
closing, within the same bound: an activity whose workflow has a cancel requested when its task
reaches the worker never runs, and a running one is cancelled — `heartbeat/0` answers `:cancel`,
`heartbeat!/0` raises `Hourglass.Activity.Cancelled` with reason `:workflow_cancel_requested`
(`[:hourglass, :activity, :workflow_cancel_requested]`) — and killed if it is still running half
the bound later. A workflow still running sees the activity fail, not retryably, with type
`"WorkflowCancelRequested"`: Temporal refuses a cancellation of an activity the workflow did not
ask to cancel, so reporting one would leave the workflow waiting out the activity's timeout.

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

hourglass keeps one connection to Temporal per target and namespace, made by the first call that
needs it and used by every call after: the calls below, and each worker's checks on its activities'
runs. A connection lost is made again by the next call. See `Hourglass.Client.connect/1`.

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

## Managing namespaces

`Hourglass.Client` registers, describes, updates, lists and deletes namespaces, each call
returning `{:error, %Hourglass.Error{}}` on failure as the client's other calls do:

```elixir
{:ok, client} = Hourglass.Client.connect()

# Register one, with data attached (string keys and values); one that exists is an error.
:ok = Hourglass.Client.register_namespace(client, "orders-2", data: %{"state" => "importing"})

# Or make one exist, registering it only if Temporal does not know it.
:ok = Hourglass.Client.ensure_namespace(client, "orders")

# Describe one: %{name: "orders-2", state: :registered, data: %{"state" => "importing"}}.
{:ok, namespace} = Hourglass.Client.describe_namespace(client, "orders-2")

# Merge into its data: the keys given are set, the others kept.
:ok = Hourglass.Client.update_namespace_data(client, "orders-2", %{"state" => "restored"})

# Every namespace Temporal knows, across all pages.
{:ok, namespaces} = Hourglass.Client.list_namespaces(client)

# Delete one, and everything in it; returns the name Temporal holds it under while removing it.
{:ok, _deleted_as} = Hourglass.Client.delete_namespace(client, "orders-2")
```

## Telemetry

Hourglass emits the following `:telemetry` events:

| Event | Description |
|---|---|
| `[:hourglass, :connection, :failed]` | Temporal connection failed |
| `[:hourglass, :worker, :registration_failed]` | Worker registration failed; for an attempt a starting worker makes again (Temporal unreachable), measurements `attempt` and `retry_in` (ms), metadata `failure_class`, `task_queue`, `detail` |
| `[:hourglass, :worker, :stopping]` | A stopping worker has stopped taking new tasks; metadata includes `task_queue` and `graceful_shutdown_period` |
| `[:hourglass, :worker, :stopped]` | A stopping worker has released its handle; measurements `duration` (ms) and `abandoned_activities`, metadata `task_queue` and `drained` |
| `[:hourglass, :activity, :failure]` | Activity returned `{:error, _}` or raised; metadata includes classification |
| `[:hourglass, :activity, :exception]` | Unhandled exception in activity dispatch |
| `[:hourglass, :activity, :dispatch_failed]` | Activity could not be dispatched |
| `[:hourglass, :activity, :workflow_closed]` | An activity's workflow run has closed, so its worker cancelled it; metadata `task_queue`, `workflow_id`, `run_id` |
| `[:hourglass, :activity, :workflow_closed, :stopped]` | Such an activity was still running half the closed-workflow bound later, and was killed; same metadata |
| `[:hourglass, :activity, :workflow_cancel_requested]` | A cancel was requested of the workflow of an activity that ends on one (`ends_on_cancel_request: true`), so its worker cancelled it; metadata `task_queue`, `workflow_id`, `run_id` |
| `[:hourglass, :activity, :workflow_cancel_requested, :stopped]` | Such an activity was still running half the closed-workflow bound later, and was killed; same metadata |
| `[:hourglass, :activity, :not_started]` | An activity's run had ended when its task reached the worker, so it never ran; metadata `task_queue`, `workflow_id`, `run_id`, `reason` (`:workflow_closed` or `:workflow_cancel_requested`) |
| `[:hourglass, :activity, :heartbeat]` | Activity recorded a liveness heartbeat |
| `[:hourglass, :activity, :cancel_received]` | A Cancel activity task arrived (heartbeat timeout / server-side cancel); metadata includes `reason` |
| `[:hourglass, :activity, :heartbeat_lost]` | *(reserved — not yet emitted)* |
| `[:hourglass, :activity, :failure, :unclassified]` | *(reserved — not yet emitted)* |
| `[:hourglass, :workflow, :task_failed]` | Workflow-task parked as a failure (uncaught exception; server will retry on next activation) |
| `[:hourglass, :workflow, :failed]` | Workflow body called `fail/2,3`; terminal `FailWorkflowExecution`, not retried |
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
# Before every change lands: Elixir and Rust formatting (checked, never
# rewritten), clippy and compile with warnings as errors, credo, the
# default suite
mix precommit

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

A Temporal shared with other suites on a busy machine can take tens of
seconds to answer, and past 30 s Core gives up on a call. The integration
tests wait out a slow Temporal for up to two minutes per wait, making a call
again when Core gave up on it, so they fail on what hourglass does rather than
on how busy the machine is; a hang still fails, after those two minutes. Set
`HOURGLASS_TEST_PATIENCE_MS` to change the wait.

If `cargo` is not on your `PATH`, prepend `~/.cargo/bin`:

```bash
PATH="$HOME/.cargo/bin:$PATH" mix compile
```

### NIF build mode

The NIF is built in release mode in every Mix environment (`use Rustler, mode: :release` in `Hourglass.Bridge`), so the NIF the suite tests is the one that ships. A debug build differs in more than speed: it turns `temporalio-sdk-core`'s internal debug assertions into panics, so tests would exercise a build production never runs. To debug the NIF locally, ask for one explicitly with `config :hourglass, Hourglass.Bridge, mode: :debug`, and never land that setting.

## License

Released under the [MIT License](LICENSE).
