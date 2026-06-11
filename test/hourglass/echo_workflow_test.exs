defmodule Hourglass.EchoWorkflowTest do
  # async: true — Temporal singletons (Runtime, WorkerRegistry) live
  # globally; per-test isolation comes from unique task-queue +
  # workflow_id, which the Temporal cluster routes by independently of
  # Elixir process identity.
  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.TestSupport.EchoWorkflow
  alias Hourglass.WorkflowHandle

  @moduletag :temporal
  # End-to-end cluster round-trip — proves the Hourglass Worker + Evaluator
  # + Bridge stack actually executes a workflow against real Temporal.
  # Cannot be mocked. Default `mix test` excludes :integration.
  @moduletag :integration
  @moduletag timeout: 60_000

  test "Echo workflow round-trips end-to-end" do
    queue = "echo-test-#{System.unique_integer([:positive])}"

    {:ok, _worker_pid} =
      start_supervised({Hourglass.Worker, task_queue: queue})

    nonce = UUIDv7.generate()
    workflow_id = "echo:#{nonce}"

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(EchoWorkflow, %{nonce: nonce},
        id: workflow_id,
        task_queue: queue
      )

    {:ok, result} = Hourglass.result(handle, timeout: 30_000)

    # EchoWorkflow returns %{"echoed" => %{"nonce" => nonce}}.
    # The activity receives JSON-decoded args (atom keys become string keys),
    # echoes them, and the workflow wraps the result in %{"echoed" => ...}.
    assert result["echoed"]["nonce"] == nonce
  end
end
