defmodule Hourglass.WorkerRegisterOptsTest do
  @moduledoc """
  Pins the contract that `Worker.init/1`'s register_opts whitelist
  preserves every key `BridgeHolder.build_worker_config/2` knows how to
  consume — including the three `:max_outstanding_*` keys threaded by
  `WorkerLauncher` from `:hourglass, Hourglass.Worker` config.

  The whitelist lives at `Hourglass.Worker.__register_opts_keys__/0`,
  so production and this test share a single source of truth. A future
  drift (key removed from the attribute) trips the structural test in
  this file before any runtime regression.
  """

  use ExUnit.Case, async: true

  alias Hourglass.BridgeHolder
  alias Hourglass.Proto.WorkerConfig
  alias Hourglass.Worker

  @task_queue "default"

  test "register_opts whitelist contains all six expected keys" do
    keys = Worker.__register_opts_keys__()

    expected = [
      :namespace,
      :max_cached_workflows,
      :target_url,
      :max_outstanding_workflow_tasks,
      :max_outstanding_activities,
      :max_outstanding_local_activities
    ]

    Enum.each(expected, fn key ->
      assert key in keys,
             "register_opts whitelist must include #{inspect(key)}; production strips otherwise."
    end)
  end

  test "concurrency opts round-trip from Worker whitelist through proto wire" do
    # Use the SAME whitelist Worker.init/1 uses — no re-derivation.
    keys = Worker.__register_opts_keys__()

    opts =
      Keyword.merge([],
        max_outstanding_workflow_tasks: 77,
        max_outstanding_activities: 88,
        max_outstanding_local_activities: 99
      )

    register_opts = Keyword.take(opts, keys)
    bin = BridgeHolder.build_worker_config(@task_queue, register_opts)
    decoded = WorkerConfig.decode(bin)

    assert decoded.max_outstanding_workflow_tasks == 77
    assert decoded.max_outstanding_activities == 88
    assert decoded.max_outstanding_local_activities == 99
  end
end
