defmodule Hourglass.ClientBridgeTest do
  # async: true — Runtime lives globally in test_helper.exs; tests use unique
  # namespace names for concurrent isolation on the Temporal cluster.
  use ExUnit.Case, async: true

  alias Hourglass.Bridge
  alias Hourglass.Runtime

  @moduletag :temporal

  # Bridge-NIF contract test — exercises real client_new / register_namespace
  # / describe_namespace round-trips. Cannot be mocked: proves the NIF
  # transport. Default `mix test` excludes :temporal.

  setup do
    runtime = Runtime.handle()

    config_bin =
      Protobuf.encode(%Hourglass.Proto.ClientConfig{
        target_url: "http://localhost:7233",
        namespace: "default"
      })

    {:ok, client} = Bridge.client_new(runtime, config_bin)

    # Ensure the "default" namespace exists; register_namespace is idempotent —
    # already-exists is a tonic_error which we ignore here.
    Bridge.client_register_namespace(client, "default")

    [client: client]
  end

  test "client_new returns a usable resource", %{client: client} do
    refute is_nil(client)
  end

  test "describe_namespace returns proto bytes for the default namespace", %{client: client} do
    assert {:ok, info_bin} = Bridge.client_describe_namespace(client, "default")
    assert is_binary(info_bin)
    refute info_bin == <<>>
  end

  test "describe_namespace on missing namespace returns tonic error", %{client: client} do
    name = "missing-#{System.unique_integer([:positive])}"

    assert {:error, %Bridge.Error{kind: :tonic_error}} =
             Bridge.client_describe_namespace(client, name)
  end
end
