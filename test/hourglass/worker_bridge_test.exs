defmodule Hourglass.WorkerBridgeTest do
  # async: true — Runtime lives globally in test_helper.exs; queue name is
  # uniquified per test so concurrent tests don't share Temporal-side state.
  use ExUnit.Case, async: true

  alias Hourglass.Bridge
  alias Hourglass.Runtime

  @moduletag :temporal
  # Bridge-NIF contract test — exercises real worker_new / poll / shutdown.
  # Cannot be mocked: proves the NIF transport. Default `mix test`
  # excludes :integration.
  @moduletag :integration

  setup do
    runtime = Runtime.handle()
    queue = "wbtest-#{System.unique_integer([:positive])}"

    config_bin =
      Protobuf.encode(%Hourglass.Proto.WorkerConfig{
        namespace: "default",
        task_queue: queue,
        max_cached_workflows: 5,
        client_target_url: "http://localhost:7233"
      })

    {:ok, worker} = Bridge.worker_new(runtime, config_bin)
    [worker: worker, queue: queue]
  end

  test "shutdown returns :ok", %{worker: worker} do
    assert :ok = Bridge.worker_shutdown(worker)
  end

  test "post-shutdown poll returns shutdown error", %{worker: worker} do
    :ok = Bridge.worker_shutdown(worker)

    assert {:error, %Bridge.Error{kind: :shutdown}} =
             Bridge.worker_poll_workflow_activation(worker)
  end
end
