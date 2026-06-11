# credo:disable-for-this-file Credo.Check.Refactor.ModuleDependencies
defmodule Hourglass.Client.Real do
  @moduledoc """
  Production `Hourglass.Client.Backend` implementation. Wraps the
  Bridge NIFs to round-trip real Temporal Server.

  Each callback opens its own bridge client lazily — matches the
  pre-existing per-call connect pattern (`Hourglass.Client.connect/1`
  did `Client.connect()` then `Client.start_workflow(client, ...)` on
  every call). The cost is negligible compared to the gRPC round-trip
  the call itself does.
  """

  @behaviour Hourglass.Client.Backend

  alias Hourglass.Bridge
  alias Hourglass.Client
  alias Hourglass.Error
  alias Hourglass.Runtime
  alias Hourglass.WorkflowHandle

  alias Temporal.Api.Common.V1.Payload
  alias Temporal.Api.Common.V1.Payloads
  alias Temporal.Api.Common.V1.WorkflowExecution
  alias Temporal.Api.Common.V1.WorkflowType
  alias Temporal.Api.History.V1.History
  alias Temporal.Api.Taskqueue.V1.TaskQueue
  alias Temporal.Api.Workflowservice.V1.DescribeWorkflowExecutionResponse
  alias Temporal.Api.Workflowservice.V1.GetWorkflowExecutionHistoryResponse
  alias Temporal.Api.Workflowservice.V1.RequestCancelWorkflowExecutionRequest
  alias Temporal.Api.Workflowservice.V1.SignalWorkflowExecutionRequest
  alias Temporal.Api.Workflowservice.V1.StartWorkflowExecutionRequest
  alias Temporal.Api.Workflowservice.V1.StartWorkflowExecutionResponse

  @impl Hourglass.Client.Backend
  def start_workflow(workflow_module, args, opts) do
    workflow_id = Keyword.fetch!(opts, :workflow_id)
    task_queue = Keyword.get(opts, :task_queue, "default")
    namespace = Keyword.get(opts, :namespace, Client.default_namespace())
    reuse_policy = id_reuse_policy_enum(Keyword.get(opts, :id_reuse_policy, :reject_duplicate))

    request = %StartWorkflowExecutionRequest{
      namespace: namespace,
      workflow_id: workflow_id,
      workflow_type: %WorkflowType{name: Atom.to_string(workflow_module)},
      task_queue: %TaskQueue{name: task_queue},
      input: %Payloads{
        payloads: [
          %Payload{
            metadata: %{"encoding" => "json/plain"},
            data: Jason.encode!(args)
          }
        ]
      },
      request_id: UUIDv7.generate(),
      workflow_id_reuse_policy: reuse_policy
    }

    with {:ok, client} <- connect(opts),
         {:ok, resp_bin} <-
           classify(Bridge.client_start_workflow(client, Protobuf.encode(request))) do
      resp = StartWorkflowExecutionResponse.decode(resp_bin)
      {:ok, %WorkflowHandle{id: workflow_id, run_id: resp.run_id}}
    end
  end

  @impl Hourglass.Client.Backend
  def describe_workflow_execution(%WorkflowHandle{id: id, run_id: run_id}) do
    with {:ok, client} <- connect([]),
         {:ok, bytes} <-
           classify(Bridge.client_describe_workflow_execution(client, id, run_id || "")) do
      {:ok, DescribeWorkflowExecutionResponse.decode(bytes)}
    end
  end

  @impl Hourglass.Client.Backend
  def fetch_history(%WorkflowHandle{id: id}) do
    with {:ok, client} <- connect([]),
         {:ok, bytes} <- classify(Bridge.client_fetch_history(client, id)) do
      case GetWorkflowExecutionHistoryResponse.decode(bytes) do
        %GetWorkflowExecutionHistoryResponse{history: %History{} = h} -> {:ok, h}
        %GetWorkflowExecutionHistoryResponse{history: nil} -> {:ok, %History{events: []}}
      end
    end
  end

  @impl Hourglass.Client.Backend
  def ensure_namespace(name) do
    with {:ok, client} <- connect([]) do
      case Bridge.client_describe_namespace(client, name) do
        {:ok, _info} ->
          :ok

        {:error, %Bridge.Error{kind: :tonic_error}} ->
          register_namespace(client, name)

        {:error, %Bridge.Error{} = err} ->
          {:error, classify_bridge_error(err)}
      end
    end
  end

  @impl Hourglass.Client.Backend
  def signal_workflow(%WorkflowHandle{} = handle, name, payload) do
    namespace = Client.default_namespace()

    with {:ok, client} <- connect([]) do
      req = build_signal_request(namespace, handle, name, payload)

      classify_unit(Bridge.client_signal_workflow(client, Protobuf.encode(req)))
    end
  end

  @impl Hourglass.Client.Backend
  def cancel_workflow(%WorkflowHandle{} = handle, reason) do
    namespace = Client.default_namespace()

    with {:ok, client} <- connect([]) do
      req = build_cancel_request(namespace, handle, reason)

      classify_unit(Bridge.client_cancel_workflow(client, Protobuf.encode(req)))
    end
  end

  @doc false
  @spec build_signal_request(String.t(), WorkflowHandle.t(), String.t(), term()) ::
          SignalWorkflowExecutionRequest.t()
  def build_signal_request(namespace, %WorkflowHandle{id: id, run_id: run_id}, name, payload) do
    %SignalWorkflowExecutionRequest{
      namespace: namespace,
      workflow_execution: %WorkflowExecution{
        workflow_id: id,
        run_id: run_id || ""
      },
      signal_name: name,
      input: %Payloads{
        payloads: [
          %Payload{
            metadata: %{"encoding" => "json/plain"},
            data: Jason.encode!(payload)
          }
        ]
      },
      request_id: UUIDv7.generate()
    }
  end

  @doc false
  @spec build_cancel_request(String.t(), WorkflowHandle.t(), String.t()) ::
          RequestCancelWorkflowExecutionRequest.t()
  def build_cancel_request(namespace, %WorkflowHandle{id: id, run_id: run_id}, reason) do
    %RequestCancelWorkflowExecutionRequest{
      namespace: namespace,
      workflow_execution: %WorkflowExecution{
        workflow_id: id,
        run_id: run_id || ""
      },
      reason: reason,
      request_id: UUIDv7.generate()
    }
  end

  # ---------------------------------------------------------------------------
  # Internal helpers
  # ---------------------------------------------------------------------------

  defp connect(opts) do
    runtime = Runtime.handle()

    config_bin =
      Protobuf.encode(%Hourglass.Proto.ClientConfig{
        target_url: Keyword.get(opts, :target_url, Client.default_target_url()),
        namespace: Keyword.get(opts, :namespace, Client.default_namespace())
      })

    # Wrap `Bridge.client_new/2` so a post-hot-reload `ArgumentError`
    # cascades a Subsystem restart. See `Bridge.with_nif_reload_rescue/2`
    # moduledoc and `Client.connect/1` for the same pattern.
    case Bridge.with_nif_reload_rescue("client_new/real_connect", fn ->
           Bridge.client_new(runtime, config_bin)
         end) do
      {:ok, client} ->
        {:ok, client}

      {:error, :nif_reloaded} ->
        # Telemetry emit already landed inside `with_nif_reload_rescue`.
        {:error, Error.new(:unreachable, :nif_reloaded)}

      {:error, %Bridge.Error{} = err} ->
        # Non-NIF-reload Real backend connect failure.
        # Classify + emit telemetry before returning so operators can see
        # what's wrong with the Temporal cluster reachability.
        :telemetry.execute(
          [:hourglass, :connection, :failed],
          %{count: 1},
          %{
            failure_class: classify_connect_error_class(err),
            call_site: "client_new/real_connect",
            detail: inspect(err)
          }
        )

        {:error, classify_bridge_error(err)}
    end
  end

  # Mirror classify_connect_error/1 locally — kept in the Real backend so the
  # classification logic for the production connect path lives next to its consumer.
  @spec classify_connect_error_class(Bridge.Error.t()) :: atom()
  defp classify_connect_error_class(%Bridge.Error{kind: :tonic_error, detail: detail}),
    do: classify_tonic_detail(detail)

  defp classify_connect_error_class(%Bridge.Error{}), do: :tcp_reset

  # Same fingerprint→class lookup as Hourglass.Client to keep the two
  # connect paths classifying identically.
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

  defp register_namespace(client, name) do
    case Bridge.client_register_namespace(client, name) do
      :ok -> :ok
      {:error, %Bridge.Error{} = err} -> {:error, classify_bridge_error(err)}
    end
  end

  # Pipeline-friendly wrapper: turns `{:ok, _}` straight through and
  # converts a `Bridge.Error` into a `Hourglass.Error` for the
  # caller's `with` chain.
  defp classify({:ok, _result} = ok), do: ok
  defp classify({:error, %Bridge.Error{} = err}), do: {:error, classify_bridge_error(err)}

  # Like `classify/1` but for NIFs that return `:ok` (not `{:ok, binary}`).
  defp classify_unit(:ok), do: :ok
  defp classify_unit({:error, %Bridge.Error{} = err}), do: {:error, classify_bridge_error(err)}

  # Map the friendly `:id_reuse_policy` atom callers pass through
  # `Hourglass.start_workflow/3` onto the Temporal proto enum value.
  defp id_reuse_policy_enum(:reject_duplicate),
    do: :WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE

  defp id_reuse_policy_enum(:allow_duplicate),
    do: :WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE

  defp id_reuse_policy_enum(:allow_duplicate_failed_only),
    do: :WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY

  defp id_reuse_policy_enum(:terminate_if_running),
    do: :WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING

  defp classify_bridge_error(%Bridge.Error{} = err), do: Error.from_bridge_error(err)
end
