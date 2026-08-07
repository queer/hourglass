defmodule Hourglass.ActivityRunnerCancelTest do
  use ExUnit.Case, async: false
  alias Hourglass.Activity.CancelRegistry
  alias Hourglass.ActivityRunner

  defp tok, do: "tok-#{System.unique_integer([:positive])}"

  test "a Cancel activity task marks the registry and emits cancel_received telemetry" do
    t = tok()
    cancel_task = %{variant: {:cancel, %{reason: :cancelled}}, task_token: t}

    handler = "cancel-recv-#{System.unique_integer([:positive])}"
    me = self()

    :telemetry.attach(
      handler,
      [:hourglass, :activity, :cancel_received],
      fn _e, _m, meta, _config -> send(me, {:cancel_received, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    # complete_fn is injected so we don't need a live bridge.
    assert :ok = ActivityRunner.run(cancel_task, "q", fn _q, _bytes -> :ok end)

    assert CancelRegistry.cancelled?(t) == :cancelled
    assert_received {:cancel_received, %{reason: :cancelled}}
  end

  defmodule CancelRaisingActivity do
    use Hourglass.Activity, input: :map, output: :map
    @impl Hourglass.Activity.Behaviour
    def execute(_input), do: raise(Hourglass.Activity.Cancelled, reason: :cancelled)
  end

  defmodule TrivialActivity do
    use Hourglass.Activity, input: :map, output: :map
    @impl Hourglass.Activity.Behaviour
    def execute(_input), do: %{"ok" => true}
  end

  defp start_task(token, activity_module) do
    %{
      variant:
        {:start,
         %Coresdk.ActivityTask.Start{
           activity_type: Atom.to_string(activity_module),
           workflow_execution: %Temporal.Api.Common.V1.WorkflowExecution{
             workflow_id: "w",
             run_id: "r"
           },
           activity_id: "a",
           attempt: 1,
           input: [%{data: "{}", metadata: %{"encoding" => "json/plain"}}]
         }},
      task_token: token
    }
  end

  test "an activity that raises Cancelled produces a Cancellation completion" do
    t = tok()
    me = self()

    capture = fn _q, bytes ->
      send(me, {:bytes, bytes})
      :ok
    end

    task = start_task(t, CancelRaisingActivity)
    assert :ok = ActivityRunner.run(task, "q", capture)

    assert_received {:bytes, bytes}
    completion = Coresdk.ActivityTaskCompletion.decode(bytes)
    assert {:cancelled, _details} = completion.result.status
  end

  test "Start completion clears the token from the registry" do
    t = tok()
    CancelRegistry.mark(t, :cancelled)

    assert :ok =
             t
             |> start_task(TrivialActivity)
             |> ActivityRunner.run("q", fn _q, _bytes -> :ok end)

    assert CancelRegistry.cancelled?(t) == nil
  end
end
