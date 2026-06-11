# credo:disable-for-this-file Credo.Check.Refactor.ModuleDependencies
defmodule Hourglass.Client do
  @moduledoc """
  Client-side workflow operations. Wraps `Hourglass.Bridge.client_*`
  NIFs; translates `%Bridge.Error{}` to higher-level `%Error{}` reasons.
  """

  alias Hourglass.Bridge
  alias Hourglass.Error
  alias Hourglass.Runtime
  alias Hourglass.WorkflowHandle
  alias Temporal.Api.Workflowservice.V1.GetWorkflowExecutionHistoryResponse
  alias Temporal.Api.Workflowservice.V1.StartWorkflowExecutionResponse

  @type t :: reference()

  @spec connect(keyword()) :: {:ok, t()} | {:error, Error.t()}
  def connect(opts \\ []) do
    runtime = Runtime.handle()

    config_bin =
      Protobuf.encode(%Hourglass.Proto.ClientConfig{
        target_url: Keyword.get(opts, :target_url, default_target_url()),
        namespace: Keyword.get(opts, :namespace, default_namespace())
      })

    # Wrap `Bridge.client_new/2` so a post-hot-reload `ArgumentError`
    # (stale runtime ref decoded against a freshly loaded NIF resource
    # registry) cascades a Subsystem restart instead of crashing the
    # caller opaquely. See `Bridge.with_nif_reload_rescue/2` moduledoc.
    case Bridge.with_nif_reload_rescue("client_new/connect", fn ->
           Bridge.client_new(runtime, config_bin)
         end) do
      {:ok, client} ->
        {:ok, client}

      {:error, :nif_reloaded} ->
        # Telemetry emit already landed inside `with_nif_reload_rescue`.
        {:error, Error.new(:unreachable, :nif_reloaded)}

      {:error, %Bridge.Error{} = err} ->
        # Non-NIF-reload connect failure. Classify the Bridge error shape into
        # the failure_class vocabulary and emit telemetry before returning so
        # operators can see what's wrong with the Temporal cluster reachability.
        :telemetry.execute(
          [:hourglass, :connection, :failed],
          %{count: 1},
          %{
            failure_class: classify_connect_error(err),
            call_site: "client_new/connect",
            detail: inspect(err)
          }
        )

        {:error, classify(err)}
    end
  end

  # Map a `%Bridge.Error{}` into a `failure_class` atom for telemetry.
  # The Bridge error surface (tonic_error / shutdown / unknown) is coarse —
  # gRPC reachability failures land in `:tonic_error`, so the default
  # classification for cluster-reachability failures is `:tcp_reset`.
  @spec classify_connect_error(Bridge.Error.t()) :: atom()
  defp classify_connect_error(%Bridge.Error{kind: :tonic_error, detail: detail}),
    do: classify_tonic_detail(detail)

  defp classify_connect_error(%Bridge.Error{}), do: :tcp_reset

  # Walk a small fingerprint→class lookup so each substring check is a
  # single Enum step rather than a `cond` arm that credo counts as
  # cyclomatic complexity.
  @connect_error_fingerprints [
    {"deadline", :timeout},
    {"timeout", :timeout},
    {"Unauthenticated", :auth_failed},
    {"unauthenticated", :auth_failed},
    {"dns", :dns_failure},
    {"resolve", :dns_failure}
  ]

  defp classify_tonic_detail(detail) when is_binary(detail) do
    Enum.find_value(@connect_error_fingerprints, :tcp_reset, fn {needle, class} ->
      if String.contains?(detail, needle), do: class
    end)
  end

  defp classify_tonic_detail(_detail), do: :tcp_reset

  @spec ensure_namespace(t(), String.t()) :: :ok | {:error, Error.t()}
  def ensure_namespace(client, name) do
    case Bridge.client_describe_namespace(client, name) do
      {:ok, _info} -> :ok
      {:error, %Bridge.Error{kind: :tonic_error}} -> register_namespace(client, name)
      {:error, %Bridge.Error{} = err} -> {:error, classify(err)}
    end
  end

  @spec health(t()) :: :ok | {:error, Error.t()}
  def health(_client) do
    # client_new succeeding already proved the cluster is reachable. For v0
    # this is a no-op success. A future pass can ping a gRPC health endpoint.
    :ok
  end

  @spec start_workflow(t(), module(), term(), keyword()) ::
          {:ok, WorkflowHandle.t()} | {:error, Error.t()}
  def start_workflow(client, workflow_module, args, opts) do
    workflow_id = Keyword.fetch!(opts, :workflow_id)
    task_queue = Keyword.get(opts, :task_queue, "default")
    namespace = Keyword.get(opts, :namespace, default_namespace())

    request =
      %Temporal.Api.Workflowservice.V1.StartWorkflowExecutionRequest{
        namespace: namespace,
        workflow_id: workflow_id,
        workflow_type: %Temporal.Api.Common.V1.WorkflowType{
          name: Atom.to_string(workflow_module)
        },
        task_queue: %Temporal.Api.Taskqueue.V1.TaskQueue{name: task_queue},
        input: %Temporal.Api.Common.V1.Payloads{
          payloads: [
            %Temporal.Api.Common.V1.Payload{
              metadata: %{"encoding" => "json/plain"},
              data: Jason.encode!(args)
            }
          ]
        },
        request_id: UUIDv7.generate(),
        workflow_id_reuse_policy: :WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
      }

    case Bridge.client_start_workflow(client, Protobuf.encode(request)) do
      {:ok, resp_bin} ->
        resp = StartWorkflowExecutionResponse.decode(resp_bin)

        {:ok, %WorkflowHandle{id: workflow_id, run_id: resp.run_id}}

      {:error, %Bridge.Error{} = err} ->
        {:error, classify(err)}
    end
  end

  @spec await_workflow(t(), WorkflowHandle.t(), keyword()) ::
          {:ok, term()} | {:error, Error.t()}
  def await_workflow(client, %WorkflowHandle{id: id}, opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout, 60_000)
    namespace = Keyword.get(opts, :namespace, default_namespace())

    request =
      %Temporal.Api.Workflowservice.V1.GetWorkflowExecutionHistoryRequest{
        namespace: namespace,
        execution: %Temporal.Api.Common.V1.WorkflowExecution{workflow_id: id},
        wait_new_event: true,
        history_event_filter_type: :HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
      }

    case Bridge.client_await_workflow(client, Protobuf.encode(request), timeout_ms) do
      {:ok, resp_bin} ->
        resp = GetWorkflowExecutionHistoryResponse.decode(resp_bin)

        decode_close_result(resp.history)

      {:error, %Bridge.Error{} = err} ->
        {:error, classify(err)}
    end
  end

  @spec fetch_history(t(), String.t()) :: {:ok, binary()} | {:error, Error.t()}
  def fetch_history(client, workflow_id) do
    case Bridge.client_fetch_history(client, workflow_id) do
      {:ok, bytes} -> {:ok, bytes}
      {:error, %Bridge.Error{} = err} -> {:error, classify(err)}
    end
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  defp register_namespace(client, name) do
    case Bridge.client_register_namespace(client, name) do
      :ok -> :ok
      {:error, %Bridge.Error{} = err} -> {:error, classify(err)}
    end
  end

  @doc """
  Returns the cluster default namespace as resolved from
  `:hourglass, Hourglass.Client` Application config (`:namespace`),
  the `TEMPORAL_NAMESPACE` env var, or `"default"`.

  Public so call sites that build their own gRPC requests outside
  `connect/1` (e.g. `Hourglass.BridgeHolder.build_worker_config/2`)
  can share the same source of truth — preventing
  worker-vs-workflow-namespace skew where one side uses the configured
  namespace and the other falls back to `"default"`.
  """
  @spec default_namespace() :: String.t()
  def default_namespace do
    :hourglass
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:namespace, System.get_env("TEMPORAL_NAMESPACE", "default"))
  end

  @doc """
  Returns the cluster gRPC target URL from app config / env / default.
  Public for the same reason as `default_namespace/0`.
  """
  @spec default_target_url() :: String.t()
  def default_target_url do
    :hourglass
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(
      :target_url,
      System.get_env("TEMPORAL_TARGET_URL") || System.get_env("TEMPORAL_ADDRESS") ||
        "http://localhost:7233"
    )
  end

  defp classify(%Bridge.Error{} = err), do: Error.from_bridge_error(err)

  defp decode_close_result(%Temporal.Api.History.V1.History{events: events}) do
    case Enum.find(events, &workflow_close_event?/1) do
      nil ->
        {:error, Error.new(:not_found, "no close event in history")}

      event ->
        extract_close_result(event)
    end
  end

  defp decode_close_result(nil) do
    {:error, Error.new(:not_found, "empty history")}
  end

  defp workflow_close_event?(%Temporal.Api.History.V1.HistoryEvent{event_type: type}) do
    type in [
      :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
      :EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
      :EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
      :EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
      :EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
    ]
  end

  defp extract_close_result(%Temporal.Api.History.V1.HistoryEvent{
         event_type: :EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
         attributes:
           {:workflow_execution_completed_event_attributes,
            %Temporal.Api.History.V1.WorkflowExecutionCompletedEventAttributes{result: result}}
       }) do
    case result do
      %Temporal.Api.Common.V1.Payloads{
        payloads: [%Temporal.Api.Common.V1.Payload{data: data} | _rest]
      } ->
        {:ok, Jason.decode!(data)}

      _other ->
        {:ok, nil}
    end
  end

  defp extract_close_result(%Temporal.Api.History.V1.HistoryEvent{event_type: type}) do
    {:error, Error.new(:rpc_error, type)}
  end
end
