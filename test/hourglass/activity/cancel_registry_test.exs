defmodule Hourglass.Activity.CancelRegistryTest do
  use ExUnit.Case, async: false
  alias Hourglass.Activity.CancelRegistry

  # The registry is an app-scope singleton (started by Hourglass.Application).
  # Use unique tokens so tests don't contaminate each other.
  defp tok, do: "tok-#{System.unique_integer([:positive])}"

  test "unmarked token reads nil" do
    assert CancelRegistry.cancelled?(tok()) == nil
  end

  test "mark then cancelled? returns the reason; clear removes it" do
    t = tok()
    assert :ok = CancelRegistry.mark(t, :timed_out)
    assert CancelRegistry.cancelled?(t) == :timed_out
    assert :ok = CancelRegistry.clear(t)
    assert CancelRegistry.cancelled?(t) == nil
  end

  test "mark overwrites an existing entry (last writer wins)" do
    t = tok()
    CancelRegistry.mark(t, :first)
    CancelRegistry.mark(t, :second)
    assert CancelRegistry.cancelled?(t) == :second
  end

  test "sweep removes entries older than the TTL" do
    t = tok()
    # Direct insert (the table is :public by design) to forge an entry with an old stamp.
    :ets.insert(CancelRegistry, {t, :timed_out, System.monotonic_time(:millisecond) - 10_000_000})
    assert CancelRegistry.cancelled?(t) == :timed_out
    send(Process.whereis(CancelRegistry), :sweep)
    # Give the GenServer a beat to process the sweep message.
    _ = :sys.get_state(CancelRegistry)
    assert CancelRegistry.cancelled?(t) == nil
  end
end
