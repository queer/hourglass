defmodule Hourglass.TestSupport.PatientBackend do
  @moduledoc """
  The `Hourglass.Client.Real` backend, for the integration suite, making a
  call again while Temporal times it out (`Hourglass.TestSupport.Patience`).

  Making a call again must not do twice what Temporal did once:

    * a signal or a cancel is sent again as the same request, so Temporal
      takes it once (it dedupes on the request id);
    * a start made again is `Hourglass.TestSupport.Patience.start/2`'s.
  """

  @behaviour Hourglass.Client.Backend

  alias Hourglass.Bridge
  alias Hourglass.Client
  alias Hourglass.Client.Real
  alias Hourglass.Error
  alias Hourglass.TestSupport.Patience
  alias Hourglass.WorkflowHandle

  @impl Hourglass.Client.Backend
  def start_workflow(workflow_module, args, opts) do
    Patience.start(
      fn -> Real.start_workflow(workflow_module, args, opts) end,
      Keyword.fetch!(opts, :workflow_id)
    )
  end

  @impl Hourglass.Client.Backend
  def describe_workflow_execution(handle),
    do: Patience.retry(fn -> Real.describe_workflow_execution(handle) end)

  @impl Hourglass.Client.Backend
  def fetch_history(handle), do: Patience.retry(fn -> Real.fetch_history(handle) end)

  @impl Hourglass.Client.Backend
  def ensure_namespace(name), do: Patience.retry(fn -> Real.ensure_namespace(name) end)

  @impl Hourglass.Client.Backend
  def signal_workflow(%WorkflowHandle{} = handle, name, payload) do
    request = Real.build_signal_request(Client.default_namespace(), handle, name, payload)
    send_again(request, &Bridge.client_signal_workflow/2)
  end

  @impl Hourglass.Client.Backend
  def cancel_workflow(%WorkflowHandle{} = handle, reason) do
    request = Real.build_cancel_request(Client.default_namespace(), handle, reason)
    send_again(request, &Bridge.client_cancel_workflow/2)
  end

  @impl Hourglass.Client.Backend
  def terminate_workflow(%WorkflowHandle{} = handle, reason) do
    request = Real.build_terminate_request(Client.default_namespace(), handle, reason)
    send_again(request, &Bridge.client_terminate_workflow/2)
  end

  # The same request bytes on every try, over a fresh connection each time.
  defp send_again(request, call) do
    bytes = Protobuf.encode(request)

    Patience.retry(fn ->
      with {:ok, client} <- connect(), do: unit(call.(client, bytes))
    end)
  end

  defp unit(:ok), do: :ok
  defp unit({:error, %Bridge.Error{} = err}), do: {:error, Error.from_bridge_error(err)}

  defp connect do
    config =
      Protobuf.encode(%Hourglass.Proto.ClientConfig{
        target_url: Client.default_target_url(),
        namespace: Client.default_namespace()
      })

    case Bridge.client_new(Hourglass.Runtime.handle(), config) do
      {:ok, client} -> {:ok, client}
      {:error, %Bridge.Error{} = err} -> {:error, Error.from_bridge_error(err)}
    end
  end
end
