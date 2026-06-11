defmodule Hourglass.Client.Backend do
  @moduledoc """
  Behaviour describing the cluster-facing operations Hourglass code issues
  through `Hourglass.Client`.

  Two implementations:

    * `Hourglass.Client.Real` — production. Wraps the Bridge NIFs
      and round-trips real Temporal Server.
    * `Hourglass.Client.Mock` — Mox-defined in `test/support/client_mock.ex`.
      Returns canned responses for behavioral tests so the default
      suite runs in-process with no cluster.

  The active backend is resolved at call time via
  `Application.fetch_env!(:hourglass, :client_backend)`. Production
  config sets it to `Real`; `config/test.exs` sets it to `Mock`.

  ## Callback shape

  No client ref leaks through the contract — each backend manages its
  own connection internally. Production opens the bridge client on
  demand inside each callback (matching the per-call connect pattern
  the codebase already uses); the mock has nothing to connect to.

  Callbacks return Hourglass-typed values + decoded proto messages, never
  raw bytes — the Bridge's bytes-in/bytes-out contract is a NIF
  implementation detail, not part of this surface. That makes the mock
  cheap (no protobuf round-trip) and the production-impl boundary
  explicit (decode once at the boundary).

  ## What the backend does NOT expose

    * **`connect`** — connection management is per-impl, not part of
      the contract.
    * **`await_workflow`** — `Hourglass.await/2` is a polling loop
      on `status/2`; it doesn't need a separate cluster hook. Mock-
      backed `describe_workflow_execution` covers it.
    * **`query_workflow`, `terminate_workflow`** —
      not used by Hourglass today. Add when needed.
  """

  alias Hourglass.Error
  alias Hourglass.WorkflowHandle
  alias Temporal.Api.History.V1.History
  alias Temporal.Api.Workflowservice.V1.DescribeWorkflowExecutionResponse

  @doc """
  Starts a new workflow execution. Returns a `WorkflowHandle` containing
  the cluster-assigned `run_id`.

  Required `opts`:

    * `:workflow_id` — caller-supplied stable id.

  Optional:

    * `:task_queue` — defaults to `"default"`.
    * `:namespace` — defaults to `Hourglass.Client.default_namespace/0`.
  """
  @callback start_workflow(workflow_module :: module(), args :: term(), opts :: keyword()) ::
              {:ok, WorkflowHandle.t()} | {:error, Error.t()}

  @doc """
  Returns a snapshot of the workflow's current execution state. Maps to
  Temporal's `DescribeWorkflowExecution` RPC; the caller (`Hourglass.status/2`)
  decodes the response into a `WorkflowStatus` struct.
  """
  @callback describe_workflow_execution(handle :: WorkflowHandle.t()) ::
              {:ok, DescribeWorkflowExecutionResponse.t()} | {:error, Error.t()}

  @doc """
  Returns the workflow's full event history. Maps to
  `GetWorkflowExecutionHistory` (with `wait_new_event: false` semantics).
  Used by `Hourglass.status/2` (when `failures: :include`) and by
  `Hourglass.await/2`'s test-only completed-result extraction.
  """
  @callback fetch_history(handle :: WorkflowHandle.t()) ::
              {:ok, History.t()} | {:error, Error.t()}

  @doc """
  Ensures the named namespace exists in the cluster. Idempotent: if the
  namespace already exists, returns `:ok` without re-registering.
  """
  @callback ensure_namespace(name :: String.t()) ::
              :ok | {:error, Error.t()}

  @doc """
  Sends a signal to the identified workflow execution.
  """
  @callback signal_workflow(handle :: WorkflowHandle.t(), name :: String.t(), payload :: term()) ::
              :ok | {:error, Error.t()}

  @doc """
  Requests cancellation of the identified workflow execution.
  """
  @callback cancel_workflow(handle :: WorkflowHandle.t(), reason :: String.t()) ::
              :ok | {:error, Error.t()}

  @doc """
  Resolves the active backend module.

  Lookup order:
    1. Process dictionary (`set_impl/1`) — per-process override, used
       by test setup to flip to the Mox mock for a single test without
       affecting other concurrent tests.
    2. Application config (`:client_backend`) — process-wide
       default. Production sets it to `Real`; tests use `Mock`.

  Production callers call this on every cluster operation. The cost
  is two map lookups — negligible compared to the cluster round-trip
  the call performs.
  """
  @spec impl() :: module()
  def impl do
    Process.get({__MODULE__, :impl}) || Application.fetch_env!(:hourglass, :client_backend)
  end

  @doc """
  Sets a per-process backend override. Returns the previous value (or
  `nil` if there was none) so callers can restore it.

  Used by the test case template; production code should not call this.
  """
  @spec set_impl(module()) :: module() | nil
  def set_impl(module) when is_atom(module) do
    Process.put({__MODULE__, :impl}, module)
  end
end
