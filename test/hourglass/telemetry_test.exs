defmodule Hourglass.TelemetryTest do
  use ExUnit.Case, async: true

  alias Hourglass.Telemetry

  test "events/0 returns a non-empty list" do
    assert not Enum.empty?(Telemetry.events())
  end

  test "events/0 returns exactly 15 canonical events" do
    assert length(Hourglass.Telemetry.events()) == 15
  end

  test "every event is a list of atoms beginning with :hourglass" do
    for event <- Telemetry.events() do
      assert is_list(event), "expected list, got: #{inspect(event)}"
      assert Enum.all?(event, &is_atom/1), "not all atoms in #{inspect(event)}"
      assert hd(event) == :hourglass, "expected :hourglass prefix in #{inspect(event)}"
    end
  end
end
