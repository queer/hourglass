defmodule Hourglass.BridgeTest do
  use ExUnit.Case, async: true
  alias Hourglass.Bridge

  test "ping/0 returns pong from the NIF" do
    assert Bridge.ping() == "pong"
  end

  test "fail/1 returns a Bridge.Error tagged :test" do
    assert {:error, %Hourglass.Bridge.Error{kind: :test}} = Bridge.fail("boom")
  end
end
