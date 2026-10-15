defmodule Hourglass.ClientBridgeTest do
  # async: true — Runtime lives globally in test_helper.exs; tests use unique
  # namespace names for concurrent isolation on the Temporal cluster.
  use ExUnit.Case, async: true

  alias Hourglass.Bridge
  alias Hourglass.Runtime
  alias Hourglass.TestSupport.Patience

  @moduletag :temporal

  # Bridge-NIF contract test — exercises real client_new / describe_namespace
  # round-trips against the server's own "default" namespace. Cannot be mocked:
  # proves the NIF transport. Registering is ClientTest's, through
  # Namespaces.scratch!/1. Default `mix test` excludes :temporal.

  setup do
    runtime = Runtime.handle()

    config_bin =
      Protobuf.encode(%Hourglass.Proto.ClientConfig{
        target_url: "http://localhost:7233",
        namespace: "default"
      })

    {:ok, client} = Patience.retry(fn -> Bridge.client_new(runtime, config_bin) end)

    [client: client]
  end

  test "client_new returns a usable resource", %{client: client} do
    refute is_nil(client)
  end

  test "describe_namespace returns proto bytes for the default namespace", %{client: client} do
    assert {:ok, info_bin} =
             Patience.retry(fn -> Bridge.client_describe_namespace(client, "default") end)

    assert is_binary(info_bin)
    refute info_bin == <<>>
  end

  test "describe_namespace on missing namespace returns namespace_not_found", %{client: client} do
    name = "missing-#{System.unique_integer([:positive])}"

    assert {:error, %Bridge.Error{kind: :namespace_not_found, detail: detail}} =
             Patience.retry(fn -> Bridge.client_describe_namespace(client, name) end)

    assert detail =~ "not found"
  end
end
