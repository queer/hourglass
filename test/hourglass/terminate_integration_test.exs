defmodule Hourglass.TerminateIntegrationTest do
  @moduledoc """
  `Hourglass.terminate/2` against real Temporal: a workflow terminated closes
  at once, as terminated, though nothing serves it; one already closed, or
  unknown, is `:not_found`.
  """

  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.Error
  alias Hourglass.TestSupport.EchoWorkflow
  alias Hourglass.TestSupport.Patience
  alias Hourglass.WorkflowStatus

  @moduletag :temporal

  test "a workflow nothing serves is terminated by its handle" do
    handle = start!()

    assert :ok = Hourglass.terminate(handle, "a test")

    assert {:ok, %WorkflowStatus{state: :terminated, close_time: %DateTime{}}} =
             Hourglass.status(handle)

    assert {:error, %Error{reason: :not_found}} = Hourglass.terminate(handle, "again")
  end

  test "a workflow is terminated by its id alone" do
    handle = start!()

    assert :ok = Hourglass.terminate(handle.id)
    assert {:ok, %WorkflowStatus{state: :terminated}} = Hourglass.status(handle)
  end

  test "a workflow Temporal does not know is not_found" do
    assert {:error, %Error{reason: :not_found}} =
             Hourglass.terminate("no-such-workflow-#{UUIDv7.generate()}")
  end

  # A workflow started on a queue no worker serves: it goes on running until
  # something ends it.
  defp start! do
    id = "terminate-#{UUIDv7.generate()}"

    {:ok, handle} =
      Patience.start(
        fn ->
          Hourglass.start(EchoWorkflow, %{nonce: id},
            id: id,
            task_queue: "terminate-nobody-#{System.unique_integer([:positive])}"
          )
        end,
        id
      )

    handle
  end
end
