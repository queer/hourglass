defmodule Hourglass.TestSupport.Patience do
  @moduledoc """
  How long the integration suite waits on Temporal, and how it waits out a
  Temporal that is slow.

  The suite shares a Temporal dev server, in a VM, with other suites on a
  busy machine. There it can take tens of seconds to answer, past the 30 s
  after which Core gives up on a call (and Core does not retry a call it gave
  up on). So:

    * every wait on something Temporal does (a workflow reaching a state, an
      activity starting, a timer firing) waits up to `ms/0`;
    * a call Core gave up on is made again, until `ms/0` runs out (`retry/1`,
      `start_worker/1`, and `Hourglass.TestSupport.PatientBackend` for the
      `Hourglass` facade).

  A hang still fails: it fails after `ms/0` rather than after 30 s. Nothing
  here waits out anything but Temporal: what hourglass itself promises within
  a bound is asserted against hourglass's own telemetry.

  `HOURGLASS_TEST_PATIENCE_MS` overrides `ms/0`.
  """

  alias Hourglass.Client.Real
  alias Hourglass.Error
  alias Hourglass.WorkflowHandle

  @default_ms 120_000

  @doc "How long a test waits on anything Temporal does, in milliseconds."
  @spec ms() :: pos_integer()
  def ms do
    case System.get_env("HOURGLASS_TEST_PATIENCE_MS") do
      nil -> @default_ms
      value -> String.to_integer(value)
    end
  end

  @doc """
  An ExUnit test timeout that fits `waits` waits of `ms/0` each, with a
  minute to spare.
  """
  @spec test_timeout(pos_integer()) :: pos_integer()
  def test_timeout(waits), do: waits * ms() + 60_000

  @doc """
  Whether `result` is Core giving up on a Temporal call: tonic's
  "Timeout expired", however deep in an error it sits.
  """
  @spec temporal_timeout?(term()) :: boolean()
  def temporal_timeout?(result), do: inspect(result, limit: :infinity) =~ "Timeout expired"

  @doc """
  Calls `fun` until it returns something other than a Temporal timeout, or
  `ms/0` has gone by; returns the last result.

  Once Temporal has timed out every call of a retry for all of `ms/0`, later
  retries make their call once: a Temporal that has stopped answering then
  fails each test in one call's time rather than in `ms/0`. Its next answer
  ends that.
  """
  @spec retry((-> result)) :: result when result: term()
  def retry(fun), do: noted(retry(fun, deadline()))

  defp retry(fun, deadline) do
    result = fun.()

    if temporal_timeout?(result) and System.monotonic_time(:millisecond) < deadline do
      retry(fun, deadline)
    else
      result
    end
  end

  @unanswered {__MODULE__, :unanswered}

  defp deadline do
    now = System.monotonic_time(:millisecond)
    if :persistent_term.get(@unanswered, false), do: now, else: now + ms()
  end

  # Record whether Temporal answered `result`'s call; returns `result`.
  defp noted(result) do
    unanswered? = temporal_timeout?(result)

    if :persistent_term.get(@unanswered, false) != unanswered? do
      :persistent_term.put(@unanswered, unanswered?)
    end

    result
  end

  @doc """
  Runs `fun` until it returns without raising, for up to `ms/0`; past that,
  runs it once more and lets it raise. For assertions on a state Temporal
  reaches in its own time.
  """
  @spec eventually((-> result)) :: result when result: term()
  def eventually(fun), do: eventually(fun, System.monotonic_time(:millisecond) + ms())

  defp eventually(fun, deadline) do
    fun.()
  rescue
    error ->
      if System.monotonic_time(:millisecond) < deadline do
        Process.sleep(100)
        eventually(fun, deadline)
      else
        reraise error, __STACKTRACE__
      end
  end

  @doc """
  Starts a workflow under `workflow_id` with `fun`, starting it again while
  Temporal times the start out. A start made again that Temporal answers
  "already started" did start: the first try reached Temporal and only its
  answer was lost. Every test starts under an id of its own, so the run under
  that id is the one this start began, and its handle is returned.
  """
  @spec start((-> {:ok, WorkflowHandle.t()} | {:error, term()}), String.t()) ::
          {:ok, WorkflowHandle.t()} | {:error, term()}
  def start(fun, workflow_id), do: noted(start(fun, workflow_id, deadline(), false))

  defp start(fun, workflow_id, deadline, timed_out?) do
    result = fun.()

    cond do
      temporal_timeout?(result) and System.monotonic_time(:millisecond) < deadline ->
        start(fun, workflow_id, deadline, true)

      timed_out? and match?({:error, %Error{reason: :already_started}}, result) ->
        started(workflow_id)

      true ->
        result
    end
  end

  # The handle of the run under `workflow_id`.
  defp started(workflow_id) do
    handle = %WorkflowHandle{id: workflow_id}

    with {:ok, described} <- retry(fn -> Real.describe_workflow_execution(handle) end) do
      {:ok, %{handle | run_id: described.workflow_execution_info.execution.run_id}}
    end
  end

  @doc """
  `start_supervised/1` of an `Hourglass.Worker` with `opts`, registered
  again while Temporal times out its GetSystemInfo. Call it from the test
  process.
  """
  @spec start_worker(keyword()) :: Supervisor.on_start_child()
  def start_worker(opts) do
    retry(fn -> ExUnit.Callbacks.start_supervised({Hourglass.Worker, opts}) end)
  end

  @doc "Like `start_worker/1`, returning the pid or raising."
  @spec start_worker!(keyword()) :: pid()
  def start_worker!(opts) do
    case start_worker(opts) do
      {:ok, pid} -> pid
      other -> raise "Hourglass.Worker #{inspect(opts)} did not start: #{inspect(other)}"
    end
  end
end
