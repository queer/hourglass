defmodule Hourglass.ResultMockTest do
  @moduledoc """
  Mock-backed default-suite coverage for `Hourglass.result/2`: verifies
  the polling-and-terminal-state semantics without a real cluster. Status
  responses are fed by `Hourglass.Client.Mock` and `result/2` polls them
  through to a terminal classification.
  """

  use Hourglass.Test.HourglassCase, async: true

  alias Hourglass.Error

  test "result/2 returns {:ok, result} when the workflow completes" do
    handle = handle()
    stub_completed(handle, %{"ok" => true, "value" => 42})

    assert {:ok, %{"ok" => true, "value" => 42}} =
             Hourglass.result(handle, timeout: 1_000, poll_interval: 1)
  end

  test "result/2 returns {:error, :failed, status} when the workflow fails" do
    handle = handle()
    stub_failed(handle)

    assert {:error, :failed, %WorkflowStatus{state: :failed}} =
             Hourglass.result(handle, timeout: 1_000, poll_interval: 1)
  end

  test "result/2 returns {:error, :timeout, status} when the budget expires" do
    handle = handle()
    stub_running(handle)

    assert {:error, :timeout, %WorkflowStatus{state: :running}} =
             Hourglass.result(handle, timeout: 50, poll_interval: 5)
  end

  test "result/2 propagates {:error, %Error{reason: :not_found}} from describe" do
    handle = handle()

    Mox.stub(TemporalMock, :describe_workflow_execution, fn _workflow ->
      {:error, Error.new(:not_found, "no such workflow")}
    end)

    assert {:error, %Error{reason: :not_found}} =
             Hourglass.result(handle, timeout: 1_000)
  end
end
