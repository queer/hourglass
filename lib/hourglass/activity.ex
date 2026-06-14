defmodule Hourglass.Activity do
  @moduledoc """
  Activity-side `use` macro. Activities are plain Elixir modules —
  no determinism constraint. Each activity module implements
  `execute(input)` and is dispatched by the runner based on its
  module name (the activity_type). The runner calls `execute/1` with
  the deserialized input.

  ## Activity context

  Inside `execute/1`, an activity body can call `info/0` to read the
  per-dispatch context — `workflow_id`, `run_id`, `activity_id`,
  `attempt` — populated by `Hourglass.ActivityRunner` from the
  inbound `Coresdk.ActivityTask.Start`. `attempt/0` is a thin
  delegate that returns just the attempt count. Both raise when
  called outside an active dispatch.

  `try_info/0` is the non-raising sibling: returns the same struct
  inside an activity dispatch, `nil` outside.

  See `info/0` for field details and the `(run_id, activity_id)`
  retry-stability guarantee.

  ## Options

  `use Hourglass.Activity, input: X, output: Y, retry: [...]`
  declares input and output schemas and the module-default Temporal
  RetryPolicy applied by `Hourglass.ActivityRunner` when an activity
  returns `{:error, _}` or raises and the
  `Hourglass.Activity.RetryClassifier` classifies the failure
  as `:retryable`.

  * `:input` — a `Hourglass.Schema` module or scalar atom (e.g.
    `:map`, `:string`). Default: `:map`.
  * `:output` — a `Hourglass.Schema` module or scalar atom.
    Default: `:map`.
  * `:retry` — keyword list of retry policy options (optional).

  Allowed retry keys (mirror Temporal's `RetryPolicy` proto fields):

    * `:max_attempts` — non-negative integer. `0` means "unlimited"
      per Temporal's spec; positive values cap retries. Default: `1`
      (no retry).
    * `:initial_interval` — milliseconds before the first retry.
    * `:backoff_coefficient` — float `>= 1.0` controlling exponential
      backoff growth.
    * `:max_interval` — millisecond cap on the backoff interval.

  Disallowed keys: `:retryable_error_types` and
  `:non_retryable_error_types`. The classifier owns eligibility (which
  error shapes retry); the policy only controls quantity (how many
  times + how fast). Mixing both controls would let an activity
  silently widen retry eligibility around the classifier's audited
  taxonomy.

  Compile-time validation raises `CompileError` for unknown keys,
  for `:max_attempts < 0`, and for `:backoff_coefficient < 1.0`.

  ## Default policy

  Activities that omit `:retry` get
  `default_retry_policy/0` — `[max_attempts: 1]` (no retry). Retry is
  opt-in: the policy plus a `:retryable` classification are both
  required for the runner to retry.

  ## Example

      defmodule MyApp.Activities.Download do
        use Hourglass.Activity,
          input: MyApp.Download.Args,
          output: MyApp.Download.Result,
          retry: [
            max_attempts: 5,
            initial_interval: 1_000,
            backoff_coefficient: 2.0,
            max_interval: 30_000
          ]

        @impl true
        def execute(%MyApp.Download.Args{url: url}), do: do_fetch(url)
      end
  """

  @allowed_retry_policy_keys [
    :max_attempts,
    :initial_interval,
    :backoff_coefficient,
    :max_interval
  ]

  @disallowed_retry_policy_keys [
    :retryable_error_types,
    :non_retryable_error_types
  ]

  @doc """
  Default retry policy for activities that don't specify one.

  The default is "no retry" (`max_attempts: 1`). Activities that
  genuinely need retries opt in via `use Hourglass.Activity,
  retry: [...]`.

  The classifier still owns eligibility — even if `max_attempts` is
  set, the runner only retries when the classifier returns
  `:retryable`.
  """
  @spec default_retry_policy() :: keyword()
  def default_retry_policy, do: [max_attempts: 1]

  @doc """
  Returns the per-dispatch context as a `Hourglass.Activity.Info`
  struct. Fields: `workflow_id`, `run_id`, `activity_id`, `attempt`,
  `task_token`, `task_queue`.

  Read from a process-dictionary key (`{Hourglass.Activity, :info}`)
  that `Hourglass.ActivityRunner` sets immediately before each dispatch
  and clears (via `try ... after`) immediately after. Calling outside
  an active activity dispatch raises — there is no meaningful context
  outside that scope.

  The `(run_id, activity_id)` pair is stable across retries of the
  same activity invocation.
  """
  @spec info() :: Hourglass.Activity.Info.t()
  def info do
    Process.get({__MODULE__, :info}) ||
      raise "Hourglass.Activity.info/0 called outside an activity dispatch"
  end

  @doc """
  Non-raising variant of `info/0`: returns the per-dispatch
  `Hourglass.Activity.Info` struct inside an activity dispatch,
  `nil` outside.
  """
  @spec try_info() :: Hourglass.Activity.Info.t() | nil
  def try_info, do: Process.get({__MODULE__, :info})

  @doc """
  Returns the current attempt count for the running activity (1-based).

  `attempt` is `1` on the first dispatch, `2` on the first retry, and
  so on — Temporal Server increments the count per retry. Delegates
  to `info/0`; raises if called outside an active activity dispatch.
  """
  @spec attempt() :: pos_integer()
  def attempt, do: info().attempt

  @doc """
  Record a liveness heartbeat for the currently-running activity. Resets the activity's
  `heartbeat_timeout` at Temporal. Best-effort and side-effect-free on the activity body:

    * Outside an activity dispatch (or before `task_token` is populated) it is a `:ok` no-op.
    * Emits a `[:hourglass, :activity, :heartbeat]` telemetry event (observability + test hook).
    * Swallows a BridgeHolder `:exit` (holder absent or mid-recycle) — a heartbeat must never
      crash or fail the activity.

  Liveness only: no `details` payload (Hourglass does not surface heartbeat details on retry).
  """
  @spec heartbeat() :: :ok
  def heartbeat do
    case try_info() do
      %Hourglass.Activity.Info{task_token: token, task_queue: q}
      when is_binary(token) and token != "" ->
        :telemetry.execute([:hourglass, :activity, :heartbeat], %{count: 1}, %{task_queue: q})
        hb = %Coresdk.ActivityHeartbeat{task_token: token, details: []}
        _ = safe_record(q, Protobuf.encode(hb))
        :ok

      _ ->
        :ok
    end
  end

  defp safe_record(task_queue, bin) do
    Hourglass.BridgeHolder.record_heartbeat(task_queue, bin)
  catch
    :exit, _ -> {:error, :holder_unavailable}
  end

  defmacro __using__(opts) do
    input_raw = Keyword.get(opts, :input, :map)
    output_raw = Keyword.get(opts, :output, :map)

    input = Macro.expand(input_raw, __CALLER__)
    output = Macro.expand(output_raw, __CALLER__)

    retry = Keyword.get(opts, :retry) || default_retry_policy()

    __validate_retry_policy__(retry, __CALLER__)

    quote do
      @behaviour Hourglass.Activity.Behaviour

      @doc false
      def __activity_input_type__, do: unquote(Macro.escape(input))

      @doc false
      def __activity_output_type__, do: unquote(Macro.escape(output))

      @doc false
      def __activity_retry_policy__, do: unquote(Macro.escape(retry))

      defoverridable __activity_retry_policy__: 0
    end
  end

  @doc false
  @spec __validate_retry_policy__(term(), Macro.Env.t()) :: :ok
  def __validate_retry_policy__(retry_policy, caller) do
    unless Keyword.keyword?(retry_policy) do
      raise CompileError,
        file: caller.file,
        line: caller.line,
        description:
          "use Hourglass.Activity, retry: ... must be a keyword list, " <>
            "got: #{inspect(retry_policy)}"
    end

    Enum.each(retry_policy, fn {key, value} ->
      validate_key!(key, caller)
      validate_value!(key, value, caller)
    end)

    :ok
  end

  defp validate_key!(key, caller) when key in @disallowed_retry_policy_keys do
    raise CompileError,
      file: caller.file,
      line: caller.line,
      description:
        "retry key #{inspect(key)} is not allowed in `use Hourglass.Activity`. " <>
          "Eligibility (which error shapes retry) is owned by " <>
          "Hourglass.Activity.RetryClassifier; the policy only controls quantity. " <>
          "Allowed keys: #{inspect(@allowed_retry_policy_keys)}"
  end

  defp validate_key!(key, _caller) when key in @allowed_retry_policy_keys, do: :ok

  defp validate_key!(key, caller) do
    raise CompileError,
      file: caller.file,
      line: caller.line,
      description:
        "unknown retry key #{inspect(key)} in `use Hourglass.Activity`. " <>
          "Allowed keys: #{inspect(@allowed_retry_policy_keys)}"
  end

  defp validate_value!(:max_attempts, value, _caller) when is_integer(value) and value >= 0,
    do: :ok

  defp validate_value!(:max_attempts, value, caller) do
    raise CompileError,
      file: caller.file,
      line: caller.line,
      description:
        "retry :max_attempts must be a non-negative integer (0 = unlimited per Temporal " <>
          "spec), got: #{inspect(value)}"
  end

  defp validate_value!(:initial_interval, value, _caller)
       when is_integer(value) and value >= 0,
       do: :ok

  defp validate_value!(:initial_interval, value, caller) do
    raise CompileError,
      file: caller.file,
      line: caller.line,
      description:
        "retry :initial_interval must be a non-negative integer (milliseconds), " <>
          "got: #{inspect(value)}"
  end

  defp validate_value!(:backoff_coefficient, value, _caller)
       when (is_float(value) or is_integer(value)) and value >= 1,
       do: :ok

  defp validate_value!(:backoff_coefficient, value, caller) do
    raise CompileError,
      file: caller.file,
      line: caller.line,
      description: "retry :backoff_coefficient must be a number >= 1.0, got: #{inspect(value)}"
  end

  defp validate_value!(:max_interval, value, _caller) when is_integer(value) and value >= 0,
    do: :ok

  defp validate_value!(:max_interval, value, caller) do
    raise CompileError,
      file: caller.file,
      line: caller.line,
      description:
        "retry :max_interval must be a non-negative integer (milliseconds), " <>
          "got: #{inspect(value)}"
  end
end
