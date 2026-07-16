# credo:disable-for-this-file Credo.Check.Refactor.ModuleDependencies
defmodule Hourglass.Workflow do
  @moduledoc """
  Workflow-side API. `use Hourglass.Workflow` declares the module is a
  workflow.

  ## Execution model

  Workflows execute via the pure-function `Hourglass.Workflow.Evaluator`.
  Each activation runs the workflow body inline on an ephemeral evaluator
  Task (spawned by `Hourglass.WorkflowEvaluator.DynamicSupervisor`)
  with state threaded through arguments. The Workflow API primitives
  consult a process-dict-keyed accumulator
  (`Hourglass.Workflow.CommandAccumulator`) and either return a
  resolved value or `throw` a sentinel that the evaluator catches.

  No GenServer. No `:persistent_term`. No `send + receive` suspension.

  Determinism comes from re-execution + sorted command_id ordering.

  ## Command identification

  Each command issued from workflow code is tagged with a **command_id**:
  a tuple `{child_index_path, local_seq}` where:

    * `child_index_path` is the lexicographic path identifying the issuing
      scope. The main workflow body has path `[]`. Direct async children
      get `[0]`, `[1]`, ... in the order `async/1` is called. Nested
      asyncs extend the path (e.g. `[1, 0]` is the first child of the
      second top-level child).
    * `local_seq` is a monotonic counter local to the issuing scope,
      incremented on each command.

  The evaluator accumulates commands by command_id, sorts by it at flush
  time (lexicographic `[]` < `[0]` < `[0,0]` < `[1]` < ...), and assigns
  monotonic global seqs in that deterministic order before sending to the
  SDK. This removes BEAM-scheduling non-determinism from the externally
  observable command sequence.
  """

  alias Hourglass.Activity
  alias Hourglass.Workflow.CommandAccumulator
  alias Hourglass.Workflow.Evaluator
  alias Hourglass.Workflow.Info

  defmacro __using__(opts) do
    input = Keyword.get(opts, :input, :map)
    output = Keyword.get(opts, :output, :map)
    signals_raw = Keyword.get(opts, :signals, %{})

    # signals_raw is a quoted map literal like {:%{}, _, [{:reply, {:__aliases__, _, [:Reply]}}]}.
    # We need to resolve each value alias at macro-expand time and store the
    # result as a map with STRING keys (to match inbound wire signal_name strings).
    signals_resolved =
      case signals_raw do
        {:%{}, _meta, pairs} ->
          Enum.map(pairs, fn {k, v_ast} -> {__normalize_signal_key__(k), v_ast} end)

        _other ->
          []
      end

    quote do
      @behaviour Hourglass.Workflow.Behaviour
      @before_compile Hourglass.Workflow

      @__workflow_input_type__ unquote(input)
      @__workflow_output_type__ unquote(output)

      def __workflow_input_type__, do: @__workflow_input_type__
      def __workflow_output_type__, do: @__workflow_output_type__

      def __workflow_signal_types__ do
        Map.new(unquote(signals_resolved))
      end

      import Hourglass.Workflow,
        only: [
          execute_activity: 2,
          execute_activity: 3,
          execute_activity!: 2,
          execute_activity!: 3,
          execute_child: 2,
          execute_child: 3,
          async: 1,
          await: 1,
          await_all: 1,
          await_signal: 1,
          await_signal: 2,
          cancelled?: 0,
          continue_as_new: 1,
          info: 0,
          uuid: 0,
          random: 1,
          sleep: 1
        ]
    end
  end

  @banned_mfa [
    {DateTime, :utc_now},
    {DateTime, :now},
    {NaiveDateTime, :utc_now},
    {NaiveDateTime, :local_now},
    {Date, :utc_today},
    {Time, :utc_now},
    {System, :system_time},
    {System, :os_time},
    {System, :monotonic_time},
    {Process, :sleep},
    {Process, :send_after},
    {:rand, :uniform},
    {:rand, :normal},
    {:crypto, :strong_rand_bytes},
    {:erlang, :now},
    {:erlang, :system_time},
    {:erlang, :monotonic_time},
    {:erlang, :unique_integer},
    {:os, :timestamp},
    {:os, :system_time}
  ]

  defmacro __before_compile__(env) do
    env.module
    |> Module.definitions_in()
    |> Enum.each(&__lint_definition__!(env, &1))

    :ok
  end

  @doc false
  @spec __normalize_signal_key__(atom() | binary() | term()) :: String.t()
  def __normalize_signal_key__(k) when is_atom(k), do: Atom.to_string(k)
  def __normalize_signal_key__(k) when is_binary(k), do: k
  def __normalize_signal_key__(k), do: to_string(k)

  @doc false
  @spec __lint_definition__!(Macro.Env.t(), {atom(), arity()}) :: :ok | nil
  def __lint_definition__!(env, {fun, arity}) do
    with {:v1, _meta, _kind, clauses} <- Module.get_definition(env.module, {fun, arity}) do
      Enum.each(clauses, &__lint_clause__!(&1, env))
    end
  end

  defp __lint_clause__!({_meta, _args, _guards, body}, env) do
    __lint_determinism__!(body, env)
  end

  @doc false
  @spec __lint_determinism__!(Macro.t(), Macro.Env.t()) :: Macro.t()
  def __lint_determinism__!(ast, env) do
    Macro.prewalk(ast, fn
      {{:., _dot_meta, [{:__aliases__, _alias_meta, parts}, fun]}, meta, _args} = node ->
        # Lints arbitrary user AST; the referenced module may not be a loaded
        # atom yet, so safe_concat/1 would raise. concat/1 is required here.
        # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
        __flag_if_banned__(Module.concat(parts), fun, meta, env)
        node

      {{:., _dot_meta, [mod, fun]}, meta, _args} = node when is_atom(mod) ->
        __flag_if_banned__(mod, fun, meta, env)
        node

      node ->
        node
    end)
  end

  @doc false
  @spec __flag_if_banned__(module(), atom(), keyword(), Macro.Env.t()) :: nil
  def __flag_if_banned__(mod, fun, meta, env) do
    if {mod, fun} in @banned_mfa do
      raise CompileError,
        file: env.file,
        line: meta[:line] || env.line,
        description:
          "non-deterministic call #{inspect(mod)}.#{fun} is not allowed in a workflow " <>
            "(determinism: workflow code replays). Move the side effect into an activity, " <>
            "or use Hourglass.Workflow.uuid/0 / random/1 for RNG-shaped needs."
    end
  end

  @spec sleep(non_neg_integer() | {atom(), non_neg_integer()}) :: :ok
  def sleep(duration) do
    ms = __duration_ms__(duration)
    command_id = CommandAccumulator.next_command_id()
    :fired = Evaluator.suspend_or_resolve(command_id, {:start_timer, %{duration_ms: ms}})
    :ok
  end

  @doc """
  Block (suspend) the workflow until a signal named `name` has arrived, then
  return its decoded payload.

  Signals are inbound `signal_workflow` activation jobs. The Nth call to
  `await_signal(name)` in a re-execution consumes `signals[name][N]` (0-based).
  The call counter resets at the start of every activation (via
  `CommandAccumulator.init/0`) so each re-execution of the workflow body
  deterministically re-consumes the same buffered signals.

  When called with `timeout: duration`, races the signal against a durable
  timer. Returns `{:ok, payload}` if the signal arrives first, or `:timeout`
  if the timer fires first. The timer command_id is allocated unconditionally
  every activation to preserve deterministic seq assignment.

  **Known limitation**: if a completion is not acknowledged by the server and
  the identical activation is redelivered after the signal has already been
  persisted to `state.signals`, the signal would be appended a second time
  (signals lack a seq-based idempotency key). Normal incremental and full-replay
  paths are correct.
  """
  @spec await_signal(String.t() | atom()) :: term()
  @spec await_signal(String.t() | atom(), keyword()) :: term() | {:ok, term()} | :timeout
  # credo:disable-for-next-line Credo.Check.Refactor.ABCSize
  def await_signal(name, opts \\ []) do
    state =
      CommandAccumulator.evaluator_state() ||
        raise "Hourglass.Workflow.await_signal/1 called outside a workflow evaluator"

    # Normalize name to a string for buffer lookup (buffer is keyed by wire signal_name string)
    name_str = to_string(name)

    case Keyword.fetch(opts, :timeout) do
      :error ->
        # Unbounded wait — original behavior: bare payload or suspend.
        k = CommandAccumulator.next_signal_index(name_str)
        buffered = Map.get(state.signals, name_str, [])

        if k < length(buffered) do
          cast_signal(state, name_str, Enum.at(buffered, k))
        else
          throw(:hourglass_temporal_suspend)
        end

      {:ok, dur} ->
        # Bounded wait — race a timer against the signal.
        # ALWAYS allocate the timer command_id FIRST (determinism: subsequent
        # command_ids must not shift between activations depending on signal presence).
        ms = __duration_ms__(dur)
        timer_id = CommandAccumulator.next_command_id()
        timer_status = Evaluator.peek_command(timer_id, {:start_timer, %{duration_ms: ms}})

        k = CommandAccumulator.next_signal_index(name_str)
        buffered = Map.get(state.signals, name_str, [])

        cond do
          k < length(buffered) ->
            {:ok, cast_signal(state, name_str, Enum.at(buffered, k))}

          timer_status == {:resolved, :fired} ->
            :timeout

          true ->
            throw(:hourglass_temporal_suspend)
        end
    end
  end

  # Cast the signal payload through the declared schema if present; otherwise
  # return the raw decoded value.
  defp cast_signal(state, name_str, raw) do
    schema =
      if function_exported?(state.workflow_module, :__workflow_signal_types__, 0) do
        Map.get(state.workflow_module.__workflow_signal_types__(), name_str)
      end

    if schema do
      Hourglass.Codec.cast!(schema, raw)
    else
      raw
    end
  end

  @doc """
  Returns `true` if a `cancel_workflow` activation job has been delivered to
  this workflow run, `false` otherwise.

  Non-blocking cooperative cancellation: the workflow checks `cancelled?/0` at
  safe points and decides what to do (e.g. skip remaining work and complete
  gracefully). The flag is set during job ingestion and persists across
  activations once set — once a cancel arrives it stays `true`.
  """
  @spec cancelled?() :: boolean()
  def cancelled? do
    state =
      CommandAccumulator.evaluator_state() ||
        raise "Hourglass.Workflow.cancelled?/0 called outside a workflow evaluator"

    state.cancel_requested
  end

  @doc false
  @spec __duration_ms__(non_neg_integer() | {atom(), non_neg_integer()}) :: non_neg_integer()
  def __duration_ms__(ms) when is_integer(ms) and ms >= 0, do: ms
  def __duration_ms__({:ms, n}), do: n
  def __duration_ms__({:sec, n}), do: n * 1_000
  def __duration_ms__({:min, n}), do: n * 60_000
  def __duration_ms__({:hour, n}), do: n * 3_600_000
  def __duration_ms__({:day, n}), do: n * 86_400_000

  @doc """
  Per-activation workflow context.

  Returns a `%Info{}` carrying the workflow's `run_id` + `task_queue`.
  Reads from the evaluator state stamped on the process dict by
  `Hourglass.Workflow.Evaluator.run_body/2`. Raises if no
  evaluator is active on the calling process — `info/0` is only
  meaningful inside a workflow body during evaluation.
  """
  @spec info() :: Info.t()
  def info do
    case CommandAccumulator.evaluator_state() do
      nil ->
        raise "Hourglass.Workflow.info/0 called outside a workflow evaluator"

      state ->
        %Info{run_id: state.run_id, task_queue: state.task_queue}
    end
  end

  # ---------------------------------------------------------------------------
  # Public API (callable only from inside an evaluator-hosted Task)
  # ---------------------------------------------------------------------------

  @spec execute_activity(module(), term()) :: {:ok, term()} | {:error, term()}
  @spec execute_activity(module(), term(), keyword()) :: {:ok, term()} | {:error, term()}
  def execute_activity(module, input, opts \\ []) do
    resolved = Keyword.put(opts, :retry_policy, resolve_retry_policy(module, opts))
    dumped = Hourglass.Codec.dump(module.__activity_input_type__(), input)

    case issue_activity_command(
           {:execute_activity, %{module: module, args: dumped, options: resolved}}
         ) do
      {:ok, decoded} -> {:ok, Hourglass.Codec.cast!(module.__activity_output_type__(), decoded)}
      {:error, _reason} = err -> err
    end
  end

  @spec execute_activity!(module(), term()) :: term()
  @spec execute_activity!(module(), term(), keyword()) :: term()
  def execute_activity!(module, input, opts \\ []) do
    case execute_activity(module, input, opts) do
      {:ok, value} -> value
      {:error, failure} -> raise Hourglass.ActivityError, activity: module, reason: failure
    end
  end

  @doc """
  Start a child workflow and await its result.

  Returns `{:ok, result}` with the child's output cast through its declared
  `__workflow_output_type__/0`, or `{:error, reason}` where reason is one of:

    * `{:start_failed, cause}` — the child could not be started (`cause` is a
      `Coresdk.ChildWorkflow.StartChildWorkflowExecutionFailedCause`, in practice
      `:START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS`).
      Kept distinct from a run failure on purpose: an "already exists" against a
      pinned singleton `:id` is usually expected rather than a fault.
    * `%Temporal.Api.Failure.V1.Failure{}` — the child started and its run failed.
    * `{:cancelled, failure}` — the child was cancelled.

  ## Options

    * `:id` — the child's workflow id. Defaults to a deterministic derivation
      from `{run_id, command_id}`. Pin it for singleton/dedup semantics (pair
      with `workflow_id_reuse_policy: :reject_duplicate`). A random nonce or a
      timestamp is **illegal** here — workflow bodies must be deterministic; use
      `uuid/0` if you want a readable-and-unique id.
    * `:task_queue` — defaults to the parent's task queue.
    * `:parent_close_policy` — `:terminate` (default) | `:abandon` | `:request_cancel`.
    * `:workflow_id_reuse_policy` — `:allow_duplicate` (default) |
      `:allow_duplicate_failed_only` | `:reject_duplicate` | `:terminate_if_running`.
    * `:workflow_execution_timeout` / `:workflow_run_timeout` /
      `:workflow_task_timeout` — millisecond integers.
    * `:retry_policy` — same keyword shape as activities.

  Fan out with the ordinary scopes — no child-specific machinery needed:

      urls
      |> Enum.map(fn u -> async(fn -> execute_child(Ingest, %{"url" => u}) end) end)
      |> await_all()
  """
  @spec execute_child(module(), term()) :: {:ok, term()} | {:error, term()}
  @spec execute_child(module(), term(), keyword()) :: {:ok, term()} | {:error, term()}
  def execute_child(module, input, opts \\ []) do
    {peeked, _workflow_id} = issue_child_command(module, input, opts)

    case peeked do
      # Issued but not started yet, or started but still running: in both cases
      # the result isn't in, so stop cleanly and ship the accumulated commands.
      :pending ->
        throw(:hourglass_temporal_suspend)

      {:started, _run_id} ->
        throw(:hourglass_temporal_suspend)

      {:started, _run_id, {:ok, raw}} ->
        {:ok, Hourglass.Codec.cast!(module.__workflow_output_type__(), raw)}

      {:started, _run_id, {:error, failure}} ->
        {:error, failure}

      {:start_failed, cause} ->
        {:error, {:start_failed, cause}}

      {:start_cancelled, failure} ->
        {:error, {:cancelled, failure}}
    end
  end

  # Allocate the command_id, resolve the child's workflow id, dump the input, and
  # peek both resolution phases. Shared by execute_child/3 and start_child/3 —
  # they differ only in which phase they stop at. Returns the workflow_id
  # alongside the peek result because `start_child/3` needs it for the handle.
  @spec issue_child_command(module(), term(), keyword()) :: {term(), String.t()}
  defp issue_child_command(module, input, opts) do
    command_id = CommandAccumulator.next_command_id()
    workflow_id = child_workflow_id(opts, command_id)
    dumped = Hourglass.Codec.dump(module.__workflow_input_type__(), input)

    peeked =
      Evaluator.peek_child(
        command_id,
        {:start_child, %{module: module, args: dumped, options: opts, workflow_id: workflow_id}}
      )

    {peeked, workflow_id}
  end

  # A child's workflow id must be deterministic across re-execution. Deriving from
  # the run_id (not the parent's workflow_id + seq) is what makes it survive
  # continue_as_new: seq restarts at 1 on each run, and continue_as_new begins a
  # fresh run under the SAME workflow id, so a seq-derived id would collide with
  # the previous generation's child. Every generation gets a fresh run_id.
  defp child_workflow_id(opts, command_id) do
    case Keyword.fetch(opts, :id) do
      {:ok, id} when is_binary(id) ->
        id

      {:ok, other} ->
        raise ArgumentError,
              "invalid :id #{inspect(other)}; expected a binary workflow id " <>
                "(or omit :id to let one be derived)"

      :error ->
        state =
          CommandAccumulator.evaluator_state() ||
            raise "Hourglass.Workflow child primitives called outside a workflow evaluator"

        Evaluator.derive_child_id(state.run_id, command_id)
    end
  end

  @doc """
  Spawn an asynchronous workflow scope.

  `async/1` runs `fun.()` inline within a child-path scope, swallowing any
  suspend sentinel so sibling asyncs can also contribute commands. The
  returned tag is opaque — pass it back to `await/1`.

  The externally observable command_id sequence is governed by
  `child_index_path ++ [child_index]`.
  """
  @spec async((-> any())) :: {:hourglass_temporal_async, term()}
  def async(fun) when is_function(fun, 0) do
    frame = CommandAccumulator.enter_async_child()

    promise =
      try do
        {:done, fun.()}
      catch
        :throw, :hourglass_temporal_suspend -> :suspended
        kind, reason -> {:raised, kind, reason, __STACKTRACE__}
      after
        CommandAccumulator.exit_async_child(frame)
      end

    {:hourglass_temporal_async, promise}
  end

  @doc """
  Await the result of an `async/1` scope. Returns the value the function
  returned, or raises / rethrows if the scope failed.

  Awaiting a scope whose body suspended (because it issued a command
  without a resolved result) re-throws the suspend sentinel — so the
  parent stops cleanly and the activation ships its accumulated
  commands.
  """
  @spec await({:hourglass_temporal_async, term()}) :: term()
  def await({:hourglass_temporal_async, {:done, result}}), do: result
  def await({:hourglass_temporal_async, :suspended}), do: throw(:hourglass_temporal_suspend)

  def await({:hourglass_temporal_async, {:raised, kind, reason, stacktrace}}),
    do: :erlang.raise(kind, reason, stacktrace)

  @doc """
  Join a list of `async/1` scopes, returning their values in order.

  Equivalent to mapping `await/1` over the list. If any scope has not yet
  resolved (i.e. it suspended waiting for a command result), `await/1`
  re-throws the suspend sentinel and the activation ships its accumulated
  commands — correct fan-out behaviour.
  """
  @spec await_all([{:hourglass_temporal_async, term()}]) :: [term()]
  def await_all(scopes) when is_list(scopes), do: Enum.map(scopes, &await/1)

  @doc """
  Terminal: stops the workflow body and emits a `ContinueAsNewWorkflowExecution`
  command carrying the serialised `input`. The Temporal server starts a new
  run of the same workflow type with fresh history, passing `input` as the
  initial argument.

  Raises if called outside a workflow evaluator.
  """
  @spec continue_as_new(term()) :: no_return()
  def continue_as_new(input) do
    state =
      CommandAccumulator.evaluator_state() ||
        raise "Hourglass.Workflow.continue_as_new/1 called outside a workflow evaluator"

    dumped = Hourglass.Codec.dump(state.workflow_module.__workflow_input_type__(), input)
    throw({:hourglass_temporal_continue_as_new, dumped})
  end

  @spec uuid() :: String.t()
  def uuid, do: synchronous_command({:uuid, %{}})

  @spec random(pos_integer()) :: non_neg_integer()
  def random(max), do: synchronous_command({:random, %{max: max}})

  # ---------------------------------------------------------------------------
  # Retry-policy resolution
  #
  # Merges the activity module's `__activity_retry_policy__/0` (declared via
  # `use Hourglass.Activity, retry: [...]`) with any per-call
  # `retry_policy:` option, validating the override against the call-site
  # allowlist before merging.
  #
  # Override allowlist (call site can tune QUANTITY only):
  #   :max_attempts, :initial_interval, :backoff_coefficient, :max_interval
  #
  # Override denylist (raises ArgumentError):
  #   :retryable_error_types, :non_retryable_error_types
  #
  # Per the "classifier owns eligibility, override tunes quantity" decision:
  # retry-eligibility per-error-shape comes from
  # `Hourglass.Activity.RetryClassifier` via
  # `Failure.application_failure_info.non_retryable` (populated by
  # ActivityRunner). Allowing a call-site override of
  # `(non_)retryable_error_types` would shadow the classifier in
  # unexpected ways.
  #
  # For cross-task-queue routed activities (sidecar case where the activity is
  # named by string/atom rather than module ref), the module default isn't
  # available; we use the library default retry policy plus the per-call
  # override.
  # ---------------------------------------------------------------------------

  @allowed_override_keys [
    :max_attempts,
    :initial_interval,
    :backoff_coefficient,
    :max_interval
  ]

  @disallowed_override_keys [
    :retryable_error_types,
    :non_retryable_error_types
  ]

  @doc false
  @spec resolve_retry_policy(module() | atom() | String.t(), keyword()) :: keyword()
  def resolve_retry_policy(activity_module, opts) when is_atom(activity_module) do
    default =
      if function_exported?(activity_module, :__activity_retry_policy__, 0) do
        activity_module.__activity_retry_policy__()
      else
        Activity.default_retry_policy()
      end

    override = Keyword.get(opts, :retry_policy, [])
    validate_override!(override)
    Keyword.merge(default, override)
  end

  def resolve_retry_policy(_string_activity_name, opts) do
    override = Keyword.get(opts, :retry_policy, [])
    validate_override!(override)
    Keyword.merge(Activity.default_retry_policy(), override)
  end

  defp validate_override!(override) when is_list(override) do
    Enum.each(override, fn
      {key, _value} when key in @allowed_override_keys ->
        :ok

      {key, _value} when key in @disallowed_override_keys ->
        raise ArgumentError, """
        retry_policy override key #{inspect(key)} is not allowed at the call site.

        The classifier (Hourglass.Activity.RetryClassifier) owns retry
        eligibility for known error shapes. Per-call retry_policy can only
        tune QUANTITY (#{inspect(@allowed_override_keys)}).
        """

      {key, _value} ->
        raise ArgumentError,
              "unknown retry_policy override key #{inspect(key)}. " <>
                "Allowed: #{inspect(@allowed_override_keys)}"

      other ->
        raise ArgumentError,
              "retry_policy override must be a keyword list, got entry: #{inspect(other)}"
    end)
  end

  defp validate_override!(other) do
    raise ArgumentError,
          "retry_policy override must be a keyword list, got: #{inspect(other)}"
  end

  # ---------------------------------------------------------------------------
  # Internal helpers — issue commands into the active evaluator.
  # ---------------------------------------------------------------------------

  defp issue_activity_command(command) do
    command_id = CommandAccumulator.next_command_id()
    Evaluator.suspend_or_resolve(command_id, command)
  end

  defp synchronous_command(command) do
    command_id = CommandAccumulator.next_command_id()
    Evaluator.synchronous(command_id, command)
  end
end
