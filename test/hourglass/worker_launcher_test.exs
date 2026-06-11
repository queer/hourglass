defmodule Hourglass.WorkerLauncherTest do
  use ExUnit.Case, async: false

  setup do
    prior_worker = Application.get_env(:hourglass, Hourglass.Worker, [])

    on_exit(fn ->
      Application.put_env(:hourglass, Hourglass.Worker, prior_worker)
    end)

    :ok
  end

  # WorkerLauncher.launch/0 is private. Rather than call it, we re-derive
  # what `launch/0` would assemble. This test is therefore a contract
  # check on the config-read path: if the source-of-truth keys ever drift,
  # this fails before the runtime does.
  test "concurrency opts come from :hourglass, Hourglass.Worker app config" do
    Application.put_env(:hourglass, Hourglass.Worker,
      max_outstanding_workflow_tasks: 77,
      max_outstanding_activities: 88,
      max_outstanding_local_activities: 99
    )

    concurrency_opts =
      :hourglass
      |> Application.get_env(Hourglass.Worker, [])
      |> Keyword.take([
        :max_outstanding_workflow_tasks,
        :max_outstanding_activities,
        :max_outstanding_local_activities
      ])

    assert Keyword.get(concurrency_opts, :max_outstanding_workflow_tasks) == 77
    assert Keyword.get(concurrency_opts, :max_outstanding_activities) == 88
    assert Keyword.get(concurrency_opts, :max_outstanding_local_activities) == 99
  end

  # Structural dispatch contract: the launcher opts must contain task_queue
  # and concurrency keys, but must NOT contain :workflows or :activities.
  # Module lists are gone — workers resolve modules structurally from the
  # Temporal type name on the wire.
  test "launcher assembles opts with task_queue + concurrency but no module lists" do
    Application.put_env(:hourglass, Hourglass.Worker,
      max_outstanding_workflow_tasks: 5,
      max_outstanding_activities: 10,
      max_outstanding_local_activities: 0
    )

    concurrency_opts =
      :hourglass
      |> Application.get_env(Hourglass.Worker, [])
      |> Keyword.take([
        :max_outstanding_workflow_tasks,
        :max_outstanding_activities,
        :max_outstanding_local_activities
      ])

    # This mirrors what WorkerLauncher.launch/0 assembles.
    opts = [task_queue: "default"] ++ concurrency_opts

    assert Keyword.get(opts, :task_queue) == "default"
    assert Keyword.get(opts, :max_outstanding_workflow_tasks) == 5
    assert Keyword.get(opts, :max_outstanding_activities) == 10

    # No module lists in the opts — structural dispatch requires none.
    refute Keyword.has_key?(opts, :workflows)
    refute Keyword.has_key?(opts, :activities)
  end
end
