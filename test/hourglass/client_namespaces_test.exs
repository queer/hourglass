defmodule Hourglass.ClientNamespacesTest do
  @moduledoc """
  Managing namespaces through `Hourglass.Client`, against real Temporal:
  registering one with data, describing it with its data, updating its data,
  and listing every namespace, across pages. Failures come back as
  `{:error, %Hourglass.Error{}}`, as from the client's other calls.

  Every namespace a test creates is a scratch one, deleted when the test
  ends (`Hourglass.TestSupport.Namespaces`).
  """

  use ExUnit.Case, async: true

  alias Hourglass.Client
  alias Hourglass.Error
  alias Hourglass.TestSupport.Namespaces
  alias Hourglass.TestSupport.Patience

  @moduletag :temporal

  setup do
    {:ok, client} = Patience.retry(fn -> Client.connect() end)
    %{client: client}
  end

  test "a namespace registered with data is described with it", %{client: client} do
    name = Namespaces.scratch!("ns-data")

    assert :ok =
             Patience.retry(fn ->
               Client.register_namespace(client, name, data: %{"state" => "importing"})
             end)

    assert {:ok, %{name: ^name, state: :registered, data: %{"state" => "importing"}}} =
             Patience.retry(fn -> Client.describe_namespace(client, name) end)
  end

  test "a namespace registered is served once the call returns: a workflow starts in it at once",
       %{client: client} do
    name = Namespaces.scratch!("ns-served")
    :ok = Patience.retry(fn -> Client.register_namespace(client, name) end)

    assert {:ok, %Hourglass.WorkflowHandle{}} =
             Client.start_workflow(client, NoSuchWorkflow, %{},
               workflow_id: "served-#{UUIDv7.generate()}",
               task_queue: "nobody",
               namespace: name
             )
  end

  test "updating a namespace's data replaces the keys given and keeps the others", %{
    client: client
  } do
    name = Namespaces.scratch!("ns-update")
    data = %{"state" => "importing", "target" => "t1"}
    :ok = Patience.retry(fn -> Client.register_namespace(client, name, data: data) end)

    assert :ok =
             Patience.retry(fn ->
               Client.update_namespace_data(client, name, %{"state" => "restored"})
             end)

    assert {:ok, %{data: %{"state" => "restored", "target" => "t1"}}} =
             Patience.retry(fn -> Client.describe_namespace(client, name) end)
  end

  test "listing returns every namespace, from every page", %{client: client} do
    names = for n <- 1..3, do: Namespaces.scratch!("ns-list-#{n}")

    for name <- names do
      :ok =
        Patience.retry(fn -> Client.register_namespace(client, name, data: %{"n" => name}) end)
    end

    # Two to a page: the server's own namespaces and these three span pages.
    assert {:ok, listed} = Patience.retry(fn -> Client.list_namespaces(client, page_size: 2) end)
    assert length(listed) > 2

    for name <- names do
      assert [%{state: :registered, data: %{"n" => ^name}}] =
               Enum.filter(listed, &(&1.name == name))
    end

    assert {:ok, all_at_once} = Patience.retry(fn -> Client.list_namespaces(client) end)
    assert MapSet.new(listed, & &1.name) == MapSet.new(all_at_once, & &1.name)
  end

  test "a namespace Temporal does not know is a not_found error from each call", %{
    client: client
  } do
    name = "no-such-namespace-#{UUIDv7.generate()}"

    assert {:error, %Error{reason: :not_found}} =
             Patience.retry(fn -> Client.describe_namespace(client, name) end)

    assert {:error, %Error{reason: :not_found}} =
             Patience.retry(fn -> Client.update_namespace_data(client, name, %{"a" => "b"}) end)
  end

  test "namespace data that is not strings to strings is refused before it is sent", %{
    client: client
  } do
    for data <- [%{"a" => 1}, %{a: "b"}, [{"a", "b"}]] do
      assert_raise ArgumentError, ~r/must map strings to strings/, fn ->
        Client.register_namespace(client, "never-sent", data: data)
      end

      assert_raise ArgumentError, ~r/must map strings to strings/, fn ->
        Client.update_namespace_data(client, "never-sent", data)
      end
    end
  end

  test "registering a namespace that exists is a namespace_exists error, not a workflow's", %{
    client: client
  } do
    name = Namespaces.scratch!("ns-twice")
    :ok = Patience.retry(fn -> Client.register_namespace(client, name) end)

    assert {:error, %Error{reason: :namespace_exists}} =
             Patience.retry(fn -> Client.register_namespace(client, name) end)
  end
end
