defmodule Hourglass.Activity.CancelledTest do
  use ExUnit.Case, async: true

  test "carries a reason and is a RuntimeError-style exception" do
    e = %Hourglass.Activity.Cancelled{reason: :timed_out}
    assert e.reason == :timed_out
    assert Exception.message(e) =~ "cancelled"
  end

  test "raisable with a reason" do
    assert_raise Hourglass.Activity.Cancelled, ~r/cancelled/, fn ->
      raise Hourglass.Activity.Cancelled, reason: :cancel_requested
    end
  end
end
