defmodule Hourglass.ClientConnectionIntegrationTest do
  @moduledoc """
  hourglass keeps one connection to Temporal per client configuration (target
  and namespace), and every call with that configuration uses it: no call
  opens a connection of its own.

  Temporal is reached through a proxy that counts the connections made to
  it. Core asks Temporal's GetSystemInfo only as it connects, once per
  connection, so one connection is one GetSystemInfo.

  `async: false`: the facade's calls go to the target in application config,
  which these tests point at the proxy.
  """

  use ExUnit.Case, async: false
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.Client
  alias Hourglass.Error
  alias Hourglass.TestSupport.FreezableProxy
  alias Hourglass.TestSupport.Patience

  @moduletag :temporal
  @moduletag :integration
  @moduletag timeout: Patience.test_timeout(2)

  setup do
    # Both proxies forward to Temporal at the configured target, so both
    # start before the config points at one of them.
    proxy = start_supervised!(FreezableProxy)
    other = start_supervised!(FreezableProxy, id: :other)
    original = Application.get_env(:hourglass, Hourglass.Client, [])
    url = FreezableProxy.url(proxy)
    Application.put_env(:hourglass, Hourglass.Client, Keyword.put(original, :target_url, url))
    on_exit(fn -> Application.put_env(:hourglass, Hourglass.Client, original) end)
    %{proxy: proxy, other: other, url: url}
  end

  test "repeated facade calls with one configuration make one connection, and one GetSystemInfo",
       %{proxy: proxy} do
    for _call <- 1..5 do
      assert {:error, %Error{reason: :not_found}} = Hourglass.status(no_such_workflow())
    end

    assert FreezableProxy.connections(proxy) == 1
  end

  test "a connection lost mid-life is made again on the next call, which alone may fail for it",
       %{proxy: proxy} do
    assert {:error, %Error{reason: :not_found}} = Hourglass.status(no_such_workflow())
    assert FreezableProxy.connections(proxy) == 1

    :ok = FreezableProxy.cut(proxy)

    # The call after the loss may see it, or not (Core tries it again); the
    # one after that does not.
    _after_the_loss = Hourglass.status(no_such_workflow())
    assert {:error, %Error{reason: :not_found}} = Hourglass.status(no_such_workflow())
    assert FreezableProxy.connections(proxy) == 2
  end

  test "another namespace, or another endpoint, gets a connection of its own", %{
    proxy: proxy,
    other: other,
    url: url
  } do
    namespace = Client.default_namespace()
    assert {:ok, _client} = Patience.retry(fn -> Client.connect(target_url: url) end)
    assert {:ok, _client} = Patience.retry(fn -> Client.connect(target_url: url) end)
    assert FreezableProxy.connections(proxy) == 1

    assert {:ok, _client} =
             Patience.retry(fn ->
               Client.connect(target_url: url, namespace: "#{namespace}-b")
             end)

    assert FreezableProxy.connections(proxy) == 2

    assert {:ok, _client} =
             Patience.retry(fn -> Client.connect(target_url: FreezableProxy.url(other)) end)

    assert FreezableProxy.connections(other) == 1
    assert FreezableProxy.connections(proxy) == 2
  end

  defp no_such_workflow, do: "no-such-workflow-#{UUIDv7.generate()}"
end
