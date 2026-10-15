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
  alias Temporal.Api.Workflowservice.V1.DescribeNamespaceResponse
  alias Temporal.Api.Workflowservice.V1.GetWorkflowExecutionHistoryResponse
  alias Temporal.Api.Workflowservice.V1.ListNamespacesResponse
  alias Temporal.Api.Workflowservice.V1.StartWorkflowExecutionResponse

  @type t :: reference()

  @doc """
  A client of the namespace `opts[:namespace]` at `opts[:target_url]`
  (by default `default_namespace/0` and `default_target_url/0`).

  hourglass keeps one connection per target and namespace, made by the
  first call that needs it (Core asks Temporal's GetSystemInfo then, once)
  and used by every call after, the facade's (`Hourglass.start/3` and the
  rest) and each worker's closed-workflow watch included. A connection
  that fails to be made is made again by the next call; one lost later is
  made again by the next call that uses it, which alone may fail for it.
  A worker's own connection to Temporal (`Hourglass.Worker`) is its own:
  Core refuses two workers of one namespace and task queue on one
  connection, which replacing a worker's handle needs.
  """
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
  # A connect failure is `:unreachable` (Temporal not reached, or silent) or
  # `:tonic_error` (refused); either is classed by its detail, and anything
  # the detail does not name is `:tcp_reset`.
  @spec classify_connect_error(Bridge.Error.t()) :: atom()
  defp classify_connect_error(%Bridge.Error{kind: kind, detail: detail})
       when kind in [:tonic_error, :unreachable],
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

  @doc """
  Makes the namespace `name` exist: registers it, with a day's retention,
  only if Temporal says it does not know it. Any other failure to describe
  it (Temporal not answering, say: reason `:unreachable`) is returned as
  that failure, and nothing is registered. A namespace another caller
  registered between the two is there, as asked: `:ok`.
  """
  @spec ensure_namespace(t(), String.t()) :: :ok | {:error, Error.t()}
  def ensure_namespace(client, name) do
    case Bridge.client_describe_namespace(client, name) do
      {:ok, _info} ->
        :ok

      {:error, %Bridge.Error{kind: :namespace_not_found}} ->
        exists_after(register_namespace(client, name))

      {:error, %Bridge.Error{} = err} ->
        {:error, classify(err)}
    end
  end

  # A namespace another caller registered first exists, as ensured.
  defp exists_after({:error, %Error{reason: :already_started}}), do: :ok
  defp exists_after(registered), do: registered

  @doc """
  Deletes the namespace `name`, and everything in it. Temporal renames it at
  once, so the name is free again, and removes it in the background; the
  name it holds it under meanwhile is returned.
  """
  @spec delete_namespace(t(), String.t()) :: {:ok, String.t()} | {:error, Error.t()}
  def delete_namespace(client, name) do
    case Bridge.client_delete_namespace(client, name) do
      {:ok, deleted_name} -> {:ok, deleted_name}
      {:error, %Bridge.Error{} = err} -> {:error, classify(err)}
    end
  end

  @typedoc """
  A namespace as Temporal describes it: its name, its state, and the data
  attached to it (string keys and values, set when it is registered or by
  `update_namespace_data/3`).
  """
  @type namespace :: %{
          name: String.t(),
          state: :registered | :deprecated | :deleted | :unspecified,
          data: %{optional(String.t()) => String.t()}
        }

  @doc """
  Registers the namespace `name`, with a day's retention. Options:

    * `:data` — string keys and values to attach to it (`describe_namespace/2`
      reads them back). Default: none.

  It returns once Temporal serves the namespace: for some seconds after a
  registration, Temporal describes a namespace but refuses every other call
  naming it (a workflow start, a worker's poll) as if it did not exist, and
  this waits that out, for up to a minute. A namespace that exists already
  is an error (reason `:already_started`).
  """
  @spec register_namespace(t(), String.t(), keyword()) :: :ok | {:error, Error.t()}
  def register_namespace(client, name, opts \\ []) do
    data = string_map!(Keyword.get(opts, :data, %{}))

    case Bridge.client_register_namespace(client, name, data) do
      :ok -> :ok
      {:error, %Bridge.Error{} = err} -> {:error, classify(err)}
    end
  end

  @doc """
  The namespace `name`, with its data. One Temporal does not know is an
  error with reason `:not_found`.
  """
  @spec describe_namespace(t(), String.t()) :: {:ok, namespace()} | {:error, Error.t()}
  def describe_namespace(client, name) do
    case Bridge.client_describe_namespace(client, name) do
      {:ok, bytes} -> {:ok, namespace(DescribeNamespaceResponse.decode(bytes))}
      {:error, %Bridge.Error{} = err} -> {:error, classify(err)}
    end
  end

  @doc """
  Merges `data` into the namespace `name`'s data: each key given is set to
  its value, and the keys not given are left as they are.
  """
  @spec update_namespace_data(t(), String.t(), %{optional(String.t()) => String.t()}) ::
          :ok | {:error, Error.t()}
  def update_namespace_data(client, name, data) do
    case Bridge.client_update_namespace_data(client, name, string_map!(data)) do
      :ok -> :ok
      {:error, %Bridge.Error{} = err} -> {:error, classify(err)}
    end
  end

  @doc """
  Every namespace Temporal knows, with its data, read page by page until the
  last. Options:

    * `:page_size` — how many to ask for at a time. Default: the server's.
  """
  @spec list_namespaces(t(), keyword()) :: {:ok, [namespace()]} | {:error, Error.t()}
  def list_namespaces(client, opts \\ []) do
    list_namespaces(client, Keyword.get(opts, :page_size, 0), "", [])
  end

  defp list_namespaces(client, page_size, token, listed) do
    case Bridge.client_list_namespaces(client, page_size, token) do
      {:ok, bytes} ->
        page = ListNamespacesResponse.decode(bytes)

        listed =
          page.namespaces
          |> Enum.map(&namespace/1)
          |> Enum.reverse(listed)

        if page.next_page_token in ["", nil],
          do: {:ok, Enum.reverse(listed)},
          else: list_namespaces(client, page_size, page.next_page_token, listed)

      {:error, %Bridge.Error{} = err} ->
        {:error, classify(err)}
    end
  end

  # Namespace data is strings to strings, as Temporal keeps it.
  defp string_map!(data) do
    if is_map(data) and
         Enum.all?(data, fn {key, value} -> is_binary(key) and is_binary(value) end),
       do: data,
       else:
         raise(ArgumentError, "namespace data must map strings to strings, got: #{inspect(data)}")
  end

  defp namespace(%{namespace_info: info}) do
    %{name: info.name, state: namespace_state(info.state), data: Map.new(info.data)}
  end

  defp namespace_state(:NAMESPACE_STATE_REGISTERED), do: :registered
  defp namespace_state(:NAMESPACE_STATE_DEPRECATED), do: :deprecated
  defp namespace_state(:NAMESPACE_STATE_DELETED), do: :deleted
  defp namespace_state(_unspecified), do: :unspecified

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

  @doc """
  Returns the cluster default namespace as resolved from
  `:hourglass, Hourglass.Client` Application config (`:namespace`),
  the `TEMPORAL_NAMESPACE` env var, or `"default"`.

  Public so call sites that build their own gRPC requests outside
  `connect/1` (e.g. the worker config the bridge holder builds)
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
