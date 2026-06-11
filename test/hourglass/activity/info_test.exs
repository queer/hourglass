defmodule Hourglass.Activity.InfoTest do
  @moduledoc """
  Tests `Hourglass.Activity.info/0` + delegated `attempt/0`: the
  activity-side primitives that surface per-dispatch context (workflow
  id, run id, activity id, attempt) read from `Coresdk.ActivityTask.Start`.

  End-to-end tests drive through `ActivityRunner.run/3` so the
  process-dict key is set on the same process the activity body runs on.
  """

  use ExUnit.Case, async: true

  alias Hourglass.Activity
  alias Hourglass.Activity.Info
  alias Hourglass.ActivityRunner

  # RaisingReporter intentionally raises to verify process-dict cleanup;
  # the activity failure log is expected — capture it.
  @moduletag capture_log: true

  defmodule InfoReporter do
    use Hourglass.Activity

    @impl Hourglass.Activity.Behaviour
    def execute(_args) do
      info = Activity.info()

      %{
        workflow_id: info.workflow_id,
        run_id: info.run_id,
        activity_id: info.activity_id,
        attempt: info.attempt,
        attempt_via_delegate: Activity.attempt()
      }
    end
  end

  defmodule RaisingReporter do
    use Hourglass.Activity

    @impl Hourglass.Activity.Behaviour
    def execute(_args), do: raise(ArgumentError, "kaboom")
  end

  defp e2e_task(opts) do
    %{
      task_token: <<1, 2, 3>>,
      variant:
        {:start,
         %Coresdk.ActivityTask.Start{
           activity_type: opts[:activity_type] || Atom.to_string(InfoReporter),
           activity_id: opts[:activity_id] || "act-1",
           attempt: opts[:attempt] || 1,
           workflow_execution: %Temporal.Api.Common.V1.WorkflowExecution{
             workflow_id: opts[:workflow_id] || "wf-1",
             run_id: opts[:run_id] || "run-1"
           },
           input: [
             %Temporal.Api.Common.V1.Payload{
               metadata: %{"encoding" => "json/plain"},
               data: Jason.encode!(%{})
             }
           ]
         }}
    }
  end

  defp run_and_decode(task) do
    parent = self()

    capture = fn _worker, bytes ->
      send(parent, {:captured, bytes})
      :ok
    end

    ActivityRunner.run(task, "fake-task-queue", capture)
    assert_receive {:captured, bytes}
    completion = Coresdk.ActivityTaskCompletion.decode(bytes)
    {:completed, %{result: payload}} = completion.result.status
    Jason.decode!(payload.data)
  end

  describe "calling outside an active dispatch" do
    test "info/0 raises with informative message" do
      assert_raise RuntimeError,
                   ~r/Hourglass\.Activity\.info\/0.*outside an activity dispatch/,
                   fn ->
                     Activity.info()
                   end
    end

    test "attempt/0 raises (preserves prior behaviour by delegating to info/0)" do
      # attempt/0 now delegates to info/0, so the raise comes from info/0.
      # Either message is fine — the important contract is "raises outside dispatch".
      assert_raise RuntimeError, ~r/outside an activity dispatch/, fn ->
        Activity.attempt()
      end
    end
  end

  describe "synthetic process-dict set" do
    test "info/0 returns the struct, attempt/0 returns its :attempt field" do
      info = %Info{
        workflow_id: "wf-synthetic",
        run_id: "run-synthetic",
        activity_id: "act-synthetic",
        attempt: 3
      }

      Process.put({Hourglass.Activity, :info}, info)

      try do
        assert Activity.info() == info
        assert Activity.attempt() == 3
      after
        Process.delete({Hourglass.Activity, :info})
      end
    end
  end

  describe "try_info/0" do
    test "returns nil outside an active dispatch" do
      # Sanity: no key set on this process.
      assert Process.get({Hourglass.Activity, :info}) == nil
      assert Activity.try_info() == nil
    end

    test "returns the struct when the process-dict key is set" do
      info = %Info{
        workflow_id: "wf-try",
        run_id: "run-try",
        activity_id: "act-try",
        attempt: 2
      }

      Process.put({Hourglass.Activity, :info}, info)

      try do
        assert Activity.try_info() == info
      after
        Process.delete({Hourglass.Activity, :info})
      end
    end
  end

  # ---------------------------------------------------------------------------
  # End-to-end tests via ActivityRunner.run/3.
  # ---------------------------------------------------------------------------

  describe "end-to-end via ActivityRunner.run/3" do
    test "all four fields are populated from the inbound proto" do
      result =
        run_and_decode(
          e2e_task(
            workflow_id: "wf-abc",
            run_id: "run-xyz",
            activity_id: "act-42",
            attempt: 7
          )
        )

      assert result == %{
               "workflow_id" => "wf-abc",
               "run_id" => "run-xyz",
               "activity_id" => "act-42",
               "attempt" => 7,
               "attempt_via_delegate" => 7
             }
    end

    test "missing/zero attempt clamps to 1" do
      # Coresdk.ActivityTask.Start.attempt is :uint32; an unset field decodes
      # as 0 over the wire. Normalise to 1 rather than expose a sentinel.
      result = run_and_decode(e2e_task(attempt: 0))
      assert result["attempt"] == 1
      assert result["attempt_via_delegate"] == 1
    end

    test "process-dict cleanup: :info key is nil after run/3 returns" do
      []
      |> e2e_task()
      |> ActivityRunner.run("fake-task-queue", fn _w, _b -> :ok end)

      assert Process.get({Hourglass.Activity, :info}) == nil
    end

    test "process-dict cleanup also fires when the activity raises" do
      raising_task = %{
        task_token: <<9>>,
        variant:
          {:start,
           %Coresdk.ActivityTask.Start{
             activity_type: Atom.to_string(RaisingReporter),
             activity_id: "act-r",
             attempt: 1,
             workflow_execution: %Temporal.Api.Common.V1.WorkflowExecution{
               workflow_id: "wf-r",
               run_id: "run-r"
             },
             input: [
               %Temporal.Api.Common.V1.Payload{
                 metadata: %{"encoding" => "json/plain"},
                 data: Jason.encode!(%{})
               }
             ]
           }}
      }

      ActivityRunner.run(raising_task, "fake-task-queue", fn _w, _b -> :ok end)

      assert Process.get({Hourglass.Activity, :info}) == nil
    end
  end
end
