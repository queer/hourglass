defmodule Hourglass.ClientTest do
  # async: true — Temporal Runtime lives globally in test_helper.exs; each
  # test scopes its own workflow_id + namespace name, so concurrent tests
  # don't collide on Temporal-side state.
  use ExUnit.Case, async: true

  alias Hourglass.Bridge
  alias Hourglass.Client
  alias Hourglass.Client.Backend
  alias Hourglass.Client.Real
  alias Hourglass.Error
  alias Hourglass.TestSupport.FreezableProxy
  alias Hourglass.TestSupport.Namespaces
  alias Hourglass.TestSupport.Patience
  alias Hourglass.TestSupport.Runs
  alias Hourglass.WorkflowHandle

  @moduletag :temporal

  # Real-cluster Hourglass.Client tests. Requires a running Temporal cluster.
  # Excluded from the default suite via `ExUnit.configure(exclude: [:temporal])`.
  setup do
    Backend.set_impl(Real)
    on_exit(fn -> Process.delete({Backend, :impl}) end)
    :ok
  end

  test "Client.connect/0 returns a usable client" do
    assert {:ok, _client} = Patience.retry(fn -> Client.connect() end)
  end

  test "Client.health/1 returns :ok" do
    {:ok, client} = Patience.retry(fn -> Client.connect() end)
    assert :ok = Client.health(client)
  end

  test "Client.ensure_namespace/2 creates if missing" do
    {:ok, client} = Patience.retry(fn -> Client.connect() end)
    # Deleted when the test ends; the test fails if Temporal still has it.
    name = Namespaces.scratch!("ensure-test")
    trace_registers()
    assert :ok = Patience.retry(fn -> Client.ensure_namespace(client, name) end)

    assert_receive {:trace, _self, :call,
                    {Bridge, :client_register_namespace, [_client, ^name, _data]}}

    # Re-running should still return :ok (already-exists is fine)
    assert :ok = Patience.retry(fn -> Client.ensure_namespace(client, name) end)
  end

  test "Client.ensure_namespace/2 by many callers at once makes it exist for each" do
    {:ok, client} = Patience.retry(fn -> Client.connect() end)
    name = Namespaces.scratch!("ensure-racing")

    results =
      1..8
      |> Enum.map(fn _n ->
        Task.async(fn -> Patience.retry(fn -> Client.ensure_namespace(client, name) end) end)
      end)
      |> Task.await_many(Patience.ms())

    assert Enum.uniq(results) == [:ok]
  end

  # Core gives up on a call after 30 s.
  @tag timeout: Patience.test_timeout(2)
  test "Client.ensure_namespace/2 returns a describe Temporal does not answer, and registers nothing" do
    proxy = start_supervised!(FreezableProxy)

    {:ok, client} =
      Patience.retry(fn -> Client.connect(target_url: FreezableProxy.url(proxy)) end)

    name = Namespaces.scratch!("ensure-silent")
    trace_registers()
    :ok = FreezableProxy.freeze(proxy)

    assert {:error, %Error{reason: :unreachable, detail: detail}} =
             Client.ensure_namespace(client, name)

    assert detail =~ "Timeout expired"
    refute_receive {:trace, _self, :call, {Bridge, :client_register_namespace, _args}}, 1_000
  end

  test "Client.delete_namespace/2 frees the name at once" do
    {:ok, client} = Patience.retry(fn -> Client.connect() end)
    name = Namespaces.scratch!("delete-test")
    :ok = Patience.retry(fn -> Client.ensure_namespace(client, name) end)

    deleted = Namespaces.delete!(client, name)
    assert deleted != name

    assert {:error, %Hourglass.Bridge.Error{detail: detail}} =
             Patience.retry(fn -> Hourglass.Bridge.client_describe_namespace(client, name) end)

    assert detail =~ "not found"
  end

  # Trace the calling process's calls that register a namespace, to it,
  # through a relay (a process is not its own tracer).
  defp trace_registers do
    test = self()
    relay = spawn_link(fn -> relay(test) end)
    session = :trace.session_create(__MODULE__, relay, [])
    on_exit(fn -> :trace.session_destroy(session) end)
    1 = :trace.process(session, test, true, [:call])
    1 = :trace.function(session, {Bridge, :client_register_namespace, 3}, true, [:global])
    :ok
  end

  defp relay(test) do
    receive do
      message ->
        send(test, message)
        relay(test)
    end
  end

  test "Client.start_workflow returns a %WorkflowHandle{}" do
    {:ok, client} = Patience.retry(fn -> Client.connect() end)
    workflow_id = "facade-test-#{UUIDv7.generate()}"
    :ok = Runs.end_on_exit(workflow_id)

    # Temporal accepts the start regardless of whether a worker is registered
    # for the workflow_type — the workflow won't run, but the start call
    # itself succeeds and returns a handle.
    assert {:ok, %WorkflowHandle{id: ^workflow_id, run_id: run_id}} =
             Patience.start(
               fn ->
                 Client.start_workflow(client, FacadeTest.NoSuchWorkflow, %{},
                   workflow_id: workflow_id,
                   task_queue: "facade-test-queue"
                 )
               end,
               workflow_id
             )

    assert is_binary(run_id)
  end

  test "Client.start_workflow on duplicate workflow_id returns :already_started" do
    {:ok, client} = Patience.retry(fn -> Client.connect() end)
    workflow_id = "dupe-test-#{UUIDv7.generate()}"
    :ok = Runs.end_on_exit(workflow_id)

    start = fn ->
      Client.start_workflow(client, FacadeTest.NoSuchWorkflow, %{},
        workflow_id: workflow_id,
        task_queue: "facade-test-queue"
      )
    end

    {:ok, _handle} = Patience.start(start, workflow_id)
    assert {:error, %Error{reason: :already_started}} = Patience.retry(start)
  end
end
