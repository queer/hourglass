defmodule Hourglass.BridgeHolderWorkerConfigTest do
  use ExUnit.Case, async: true

  alias Hourglass.Proto.WorkerConfig

  test "encodes concurrency opts into the proto" do
    bin =
      Hourglass.BridgeHolder.build_worker_config("default",
        max_outstanding_workflow_tasks: 50,
        max_outstanding_activities: 100,
        max_outstanding_local_activities: 25
      )

    decoded = WorkerConfig.decode(bin)
    assert decoded.max_outstanding_workflow_tasks == 50
    assert decoded.max_outstanding_activities == 100
    assert decoded.max_outstanding_local_activities == 25
  end

  test "leaves concurrency fields at 0 when opts omit them" do
    bin = Hourglass.BridgeHolder.build_worker_config("default", [])

    decoded = WorkerConfig.decode(bin)
    # 0 is the wire-level "unset" sentinel; the Rust side substitutes
    # its DEFAULT_OUTSTANDING constant. Don't conflate "Elixir didn't
    # provide" with "Elixir explicitly chose 0."
    assert decoded.max_outstanding_workflow_tasks == 0
    assert decoded.max_outstanding_activities == 0
    assert decoded.max_outstanding_local_activities == 0
  end
end
