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
    :telemetry.attach(handler, [:hourglass, :activity, :cancel_received],
      fn _e, _m, meta, _ -> send(me, {:cancel_received, meta}) end, nil)
    on_exit(fn -> :telemetry.detach(handler) end)

    # complete_fn is injected so we don't need a live bridge.
    assert :ok = ActivityRunner.run(cancel_task, "q", fn _q, _bytes -> :ok end)

    assert CancelRegistry.cancelled?(t) == :cancelled
    assert_received {:cancel_received, %{reason: :cancelled}}
  end
end
