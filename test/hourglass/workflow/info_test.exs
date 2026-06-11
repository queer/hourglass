defmodule Hourglass.Workflow.InfoTest do
  @moduledoc """
  Tests `Hourglass.Workflow.info/0`: the workflow-side primitive
  that surfaces per-activation context (run id + task queue) to a
  workflow body. Mirrors `Hourglass.Activity.InfoTest`.

  Unlike the activity-side, where context is populated by
  `ActivityRunner.run/4` decoding the inbound proto, workflow-side
  context comes from the evaluator's `State` stamped on the calling
  process via `CommandAccumulator.mark_evaluator_active/1`. The
  evaluator-state-set tests drive that path directly so we exercise
  `info/0` in isolation from `Evaluator.evaluate/3`.

  NOTE: `Hourglass.Workflow.info/0` is defined in the Workflow macro
  module (T14). These tests were previously gated `@tag :workflow_macro`
  and are now enabled.
  """

  use ExUnit.Case, async: true

  alias Hourglass.Workflow
  alias Hourglass.Workflow.CommandAccumulator
  alias Hourglass.Workflow.Info
  alias Hourglass.Workflow.State

  describe "calling outside an active evaluator" do
    test "info/0 raises with informative message" do
      # Sanity: no evaluator state on this process.
      assert CommandAccumulator.evaluator_state() == nil

      assert_raise RuntimeError,
                   ~r/Hourglass\.Workflow\.info\/0 called outside a workflow evaluator/,
                   fn ->
                     Workflow.info()
                   end
    end
  end

  describe "synthetic evaluator state set" do
    test "info/0 returns %Info{} populated from State.run_id + State.task_queue" do
      state = State.new("run-synthetic", "tq-synthetic")
      CommandAccumulator.mark_evaluator_active(state)

      try do
        assert Workflow.info() == %Info{
                 run_id: "run-synthetic",
                 task_queue: "tq-synthetic"
               }
      after
        CommandAccumulator.unmark_evaluator()
      end
    end

    test "task_queue empty-string fallback when State.new/2 was passed nil" do
      # State.new/2 normalises a nil task_queue to "" (the test-fresh-state
      # helper used in workflow_test.exs depends on this path). info/0 must
      # surface the empty string verbatim, not nil.
      state = State.new("run-nil-tq", nil)
      assert state.task_queue == ""

      CommandAccumulator.mark_evaluator_active(state)

      try do
        info = Workflow.info()
        assert info.run_id == "run-nil-tq"
        assert info.task_queue == ""
      after
        CommandAccumulator.unmark_evaluator()
      end
    end
  end
end
