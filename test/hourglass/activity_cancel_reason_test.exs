defmodule Hourglass.ActivityCancelReasonTest do
  @moduledoc """
  An activity ended because its run closed is told so, whatever Temporal
  Core sends it before or after: `heartbeat!/0` raises with reason
  `:workflow_closed`.

  Both orders are forced, not waited for. Core's cancel is a Cancel activity
  task handed to the runner as Core would hand it (`ActivityRunner.run/3`),
  not one Core sends after a throttled heartbeat; the run closing is either
  the closed-workflow watch's mark, or a run Temporal does not know, which it
  answers as closed.
  """

  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.Activity
  alias Hourglass.Activity.CancelRegistry
  alias Hourglass.ActivityRunner
  alias Hourglass.Client
  alias Hourglass.TestSupport.Patience
  alias Hourglass.TestSupport.UnservedRuns
  alias Hourglass.TestSupport.EndsOnCancel
  alias Hourglass.TestSupport.RaceGate
  alias Hourglass.TestSupport.WorkerProbes
  alias Hourglass.Worker.ClosedWorkflowWatch
  alias Hourglass.Worker.WorkflowPollLoop

  @moduletag capture_log: true
  @moduletag timeout: Patience.test_timeout(2)

  # Long enough that the watch's own check never comes during a test: only
  # the runner asks Temporal.
  @bound 600_000

  setup do
    on_exit(fn -> Process.delete({Activity, :info}) end)
    %{queue: "cancel-reason-#{System.unique_integer([:positive])}"}
  end

  describe "once its run is known closed" do
    test "every later heartbeat reports :workflow_closed, whatever cancel Core sends", %{
      queue: queue
    } do
      for core_reason <- [:NOT_FOUND, :CANCELLED, :TIMED_OUT, :WORKER_SHUTDOWN, :PAUSED, :RESET] do
        token = token()
        CancelRegistry.mark(token, :workflow_closed)

        :ok = core_cancels(token, queue, core_reason)

        assert CancelRegistry.cancelled?(token) == :workflow_closed
        assert heartbeat_reason(token) == :workflow_closed, "after #{core_reason}"
      end
    end

    test "a cancel Core sends as the watch marks the run closed never replaces the mark" do
      token = token()

      racers =
        for n <- 1..50 do
          reason = if rem(n, 2) == 0, do: :workflow_closed, else: :NOT_FOUND
          Task.async(fn -> CancelRegistry.mark(token, reason) end)
        end

      Task.await_many(racers)
      assert CancelRegistry.cancelled?(token) == :workflow_closed
    end
  end

  describe "a cancel Core sends first" do
    @describetag :temporal

    test "is reported as :workflow_closed for a run that turns out closed", %{queue: queue} do
      start_watch!(queue)
      # Temporal no longer knows the run: it has closed.
      {token, _activity} = activity!(queue, {"gone-#{UUIDv7.generate()}", UUIDv7.generate()})

      :ok = core_cancels(token, queue, :TIMED_OUT)

      assert CancelRegistry.cancelled?(token) == :workflow_closed
      assert heartbeat_reason(token) == :workflow_closed
    end

    test "keeps Core's reason for a run still running", %{queue: queue} do
      start_watch!(queue)
      {token, _activity} = activity!(queue, running_run!())

      :ok = core_cancels(token, queue, :TIMED_OUT)

      assert CancelRegistry.cancelled?(token) == :TIMED_OUT
      assert heartbeat_reason(token) == :cancel_requested
    end
  end

  describe "an activity ended for its workflow's cancel request" do
    @describetag :temporal

    test "is told :workflow_closed once its run closes", %{queue: queue} do
      # A run that a cancel closes, with its workflow tasks held so that the
      # run goes on, cancel requested, until the test lets it close.
      workflows = "cancel-reason-wf-#{System.unique_integer([:positive])}"
      Patience.start_worker!(task_queue: workflows)
      id = "cancel-reason-#{UUIDv7.generate()}"
      :ok = RaceGate.arm({id, :activity})
      on_exit(fn -> RaceGate.disarm({id, :activity}) end)

      {:ok, handle} =
        Hourglass.start(EndsOnCancel, %{"gate" => id, "ends_on_cancel" => true},
          id: id,
          task_queue: workflows
        )

      _held = RaceGate.await_held({id, :activity})

      # An activity of that run that ends on a cancel request, under a watch
      # whose bound leaves it running for four seconds once it is cancelled.
      start_watch!(queue, 8_000)
      {token, _activity} = activity!(queue, {handle.id, handle.run_id}, true)

      loop = workflow_poll_loop(workflows)
      true = :erlang.suspend_process(loop)

      on_exit(fn ->
        try do
          :erlang.resume_process(loop)
        rescue
          ArgumentError -> :ok
        end
      end)

      :ok = Hourglass.cancel(handle, "test")

      WorkerProbes.poll_until("the cancel request to end the activity", fn ->
        CancelRegistry.cancelled?(token) == :workflow_cancel_requested
      end)

      true = :erlang.resume_process(loop)
      assert {:ok, %{"cancelled" => true}} = Hourglass.result(handle, timeout: Patience.ms())

      WorkerProbes.poll_until(
        "the run's close to reach the activity",
        fn -> CancelRegistry.cancelled?(token) == :workflow_closed end,
        3_500
      )
    end
  end

  # Core cancels the activity `token` names, through the runner, as it would.
  defp core_cancels(token, queue, reason) do
    task = %Coresdk.ActivityTask.ActivityTask{
      task_token: token,
      variant: {:cancel, %Coresdk.ActivityTask.Cancel{reason: reason}}
    }

    ActivityRunner.run(task, queue, fn _queue, _completion -> :ok end)
  end

  # The reason `heartbeat!/0` raises with in the activity `token` names.
  defp heartbeat_reason(token) do
    Process.put({Activity, :info}, %Activity.Info{
      workflow_id: "w",
      run_id: "r",
      activity_id: "a",
      attempt: 1,
      task_token: token,
      task_queue: "q"
    })

    %Activity.Cancelled{reason: reason} = catch_error(Activity.heartbeat!())
    reason
  end

  defp workflow_poll_loop(queue) do
    queue
    |> Hourglass.Worker.inner_supervisor()
    |> Supervisor.which_children()
    |> Enum.find_value(fn {id, pid, _type, _modules} -> id == WorkflowPollLoop && pid end)
  end

  defp token, do: "cancel-reason-#{UUIDv7.generate()}"

  defp start_watch!(queue, bound \\ @bound) do
    opts = [namespace: Client.default_namespace(), target_url: Client.default_target_url()]

    start_supervised!(%{
      id: queue,
      start: {ClosedWorkflowWatch, :start_link, [queue, bound, opts]}
    })
  end

  # A process registered as the executor running an activity of `run` on
  # `queue`, as an executor registers it.
  defp activity!(queue, run, opted_in \\ false) do
    token = token()
    test = self()

    activity =
      spawn(fn ->
        {:ok, _owner} = Registry.register(Hourglass.ActivityExecutor.Registry, queue, token)

        {:ok, _owner} =
          Registry.register(
            Hourglass.ActivityExecutor.Registry,
            {:activity, token},
            {run, opted_in}
          )

        send(test, {:registered, token})
        Process.sleep(:infinity)
      end)

    assert_receive {:registered, ^token}
    on_exit(fn -> Process.exit(activity, :kill) end)
    {token, activity}
  end

  # A run Temporal knows and that goes on running (`UnservedRuns`).
  defp running_run!, do: UnservedRuns.start!("cancel-reason-running")
end
