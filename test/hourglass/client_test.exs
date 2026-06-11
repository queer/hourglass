defmodule Hourglass.ClientTest do
  # async: true — Temporal Runtime lives globally in test_helper.exs; each
  # test scopes its own workflow_id + namespace name, so concurrent tests
  # don't collide on Temporal-side state.
  use ExUnit.Case, async: true

  alias Hourglass.Client
  alias Hourglass.Client.Backend
  alias Hourglass.Client.Real
  alias Hourglass.Error
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
    assert {:ok, _client} = Client.connect()
  end

  test "Client.health/1 returns :ok" do
    {:ok, client} = Client.connect()
    assert :ok = Client.health(client)
  end

  test "Client.ensure_namespace/2 creates if missing" do
    {:ok, client} = Client.connect()
    name = "ensure-test-#{UUIDv7.generate()}"
    assert :ok = Client.ensure_namespace(client, name)
    # Re-running should still return :ok (already-exists is fine)
    assert :ok = Client.ensure_namespace(client, name)
  end

  test "Client.start_workflow returns a %WorkflowHandle{}" do
    {:ok, client} = Client.connect()
    workflow_id = "facade-test-#{UUIDv7.generate()}"

    # Temporal accepts the start regardless of whether a worker is registered
    # for the workflow_type — the workflow won't run, but the start call
    # itself succeeds and returns a handle.
    assert {:ok, %WorkflowHandle{id: ^workflow_id, run_id: run_id}} =
             Client.start_workflow(client, FacadeTest.NoSuchWorkflow, %{},
               workflow_id: workflow_id,
               task_queue: "facade-test-queue"
             )

    assert is_binary(run_id)
  end

  test "Client.start_workflow on duplicate workflow_id returns :already_started" do
    {:ok, client} = Client.connect()
    workflow_id = "dupe-test-#{UUIDv7.generate()}"

    {:ok, _handle} =
      Client.start_workflow(client, FacadeTest.NoSuchWorkflow, %{},
        workflow_id: workflow_id,
        task_queue: "facade-test-queue"
      )

    assert {:error, %Error{reason: :already_started}} =
             Client.start_workflow(client, FacadeTest.NoSuchWorkflow, %{},
               workflow_id: workflow_id,
               task_queue: "facade-test-queue"
             )
  end
end
