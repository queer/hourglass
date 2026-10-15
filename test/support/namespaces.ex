defmodule Hourglass.TestSupport.Namespaces do
  @moduledoc """
  Namespaces a test creates on the Temporal it runs against, and leaves
  none of behind.

  `scratch!/1` names a namespace for the calling test to create, and has it
  deleted when the test ends. The test then fails unless Temporal has let go
  of it entirely: neither its name nor the one Temporal holds it under while
  removing it is known. The shared dev server once had about 1,130
  namespaces, one per run of a test that never deleted its own.

  It is the only way a test may create a namespace:
  `Hourglass.TestSupport.NamespaceGuard` fails the run if anything
  registers a name it did not hand out.
  """

  import ExUnit.Assertions

  alias Hourglass.Bridge
  alias Hourglass.Client
  alias Hourglass.TestSupport.NamespaceGuard
  alias Hourglass.TestSupport.Patience
  alias Hourglass.TestSupport.WorkerProbes

  @doc """
  A fresh namespace name starting `prefix`, deleted when the calling test
  ends, failing the test if Temporal still knows it then.
  """
  @spec scratch!(String.t()) :: String.t()
  def scratch!(prefix) do
    name = "#{prefix}-#{UUIDv7.generate()}"
    :ok = NamespaceGuard.allow(name)
    ExUnit.Callbacks.on_exit(fn -> leave_none!(name) end)
    name
  end

  @doc "Delete `name` if Temporal knows it, and wait until it knows it under no name."
  @spec leave_none!(String.t()) :: :ok
  def leave_none!(name) do
    {:ok, client} = Patience.retry(fn -> Client.connect() end)

    names = if known?(client, name), do: [name, delete!(client, name)], else: [name]
    gone!(client, names)
  end

  @doc "Wait until Temporal knows none of `names`."
  @spec gone!(Client.t(), [String.t()]) :: :ok
  def gone!(client, names) do
    WorkerProbes.poll_until("Temporal to let go of namespace #{inspect(names)}", fn ->
      not Enum.any?(names, &known?(client, &1))
    end)
  end

  @doc """
  Delete `name`, which the test has just registered, and return the name
  Temporal holds it under while removing it. Temporal's frontend learns of a
  new namespace some seconds after registering it, and until then answers a
  delete that it does not know the namespace; the delete is asked again
  meanwhile, for as long as the suite waits on Temporal.
  """
  @spec delete!(Client.t(), String.t()) :: String.t()
  def delete!(client, name) do
    Patience.eventually(fn ->
      assert {:ok, deleted} = Patience.retry(fn -> Client.delete_namespace(client, name) end)
      deleted
    end)
  end

  defp known?(client, name) do
    case Patience.retry(fn -> Bridge.client_describe_namespace(client, name) end) do
      {:ok, _info} ->
        true

      {:error, %Bridge.Error{detail: detail}} ->
        refute Patience.temporal_timeout?(detail), "Temporal did not answer for #{name}"
        not String.contains?(detail, "not found")
    end
  end
end
