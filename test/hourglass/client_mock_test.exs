defmodule Hourglass.ClientMockTest do
  @moduledoc """
  Mock-backed default-suite counterpart to `Hourglass.ClientTest`
  for the operations that flow through the configured backend
  (`Hourglass.start/3`).

  The integration version covers `Client.connect/1` + `Client.health/1`
  + `Client.ensure_namespace/2` directly — those are real-NIF code paths
  that don't have a mock counterpart by design.
  """

  use Hourglass.Test.HourglassCase, async: true

  alias Hourglass.Error

  defmodule SomeWorkflow do
    @moduledoc false
    use Hourglass.Workflow, input: :map, output: :map

    @impl Hourglass.Workflow.Behaviour
    def run(_args), do: %{}
  end

  test "Hourglass.start/3 returns a WorkflowHandle on success" do
    stub_start_workflow()

    workflow_id = "client-mock-#{UUIDv7.generate()}"

    assert {:ok, %WorkflowHandle{id: ^workflow_id, run_id: run_id}} =
             Hourglass.start(SomeWorkflow, %{nonce: "n"},
               id: workflow_id,
               task_queue: "test-queue"
             )

    assert is_binary(run_id)
  end

  test "Hourglass.start/3 surfaces :already_started for duplicate IDs" do
    Mox.stub(TemporalMock, :start_workflow, fn _module, _args, _opts ->
      {:error, Error.new(:already_started, "duplicate workflow_id")}
    end)

    assert {:error, %Error{reason: :already_started}} =
             Hourglass.start(SomeWorkflow, %{nonce: "n"},
               id: "dup",
               task_queue: "test-queue"
             )
  end

  test "Hourglass.start/3 propagates rpc transport errors" do
    Mox.stub(TemporalMock, :start_workflow, fn _module, _args, _opts ->
      {:error, Error.new(:rpc_error, :timeout)}
    end)

    assert {:error, %Error{reason: :rpc_error}} =
             Hourglass.start(SomeWorkflow, %{},
               id: "x",
               task_queue: "q"
             )
  end
end
