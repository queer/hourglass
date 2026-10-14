defmodule Hourglass.WorkerClosedWorkflowBoundTest do
  @moduledoc """
  Pins how a Worker resolves its closed-workflow bound
  (`Hourglass.Worker`'s "Activities of a closed workflow").

  `async: false`: one test sets `config :hourglass, Hourglass.Worker`.
  """

  use ExUnit.Case, async: false

  alias Hourglass.Worker

  setup do
    previous = Application.get_env(:hourglass, Worker)
    on_exit(fn -> restore_config(previous) end)
  end

  test "the option wins over config, and config over the default of five seconds" do
    assert Worker.closed_workflow_bound([]) == 5_000

    Application.put_env(
      :hourglass,
      Worker,
      Keyword.put(Application.get_env(:hourglass, Worker, []), :closed_workflow_bound, 2_000)
    )

    assert Worker.closed_workflow_bound([]) == 2_000
    assert Worker.closed_workflow_bound(closed_workflow_bound: 100) == 100
  end

  test "anything but an integer of milliseconds from 100 to 2 x (2^32 - 1) is refused" do
    assert Worker.closed_workflow_bound(closed_workflow_bound: 8_589_934_590) == 8_589_934_590

    for bad <- [99, 0, -1, 1.5, nil, :infinity, "5000", 8_589_934_591] do
      assert_raise ArgumentError, ~r/closed_workflow_bound/, fn ->
        Worker.closed_workflow_bound(closed_workflow_bound: bad)
      end
    end
  end

  defp restore_config(nil), do: Application.delete_env(:hourglass, Worker)
  defp restore_config(previous), do: Application.put_env(:hourglass, Worker, previous)
end
