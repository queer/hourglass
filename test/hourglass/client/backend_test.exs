defmodule Hourglass.Client.BackendTest do
  use ExUnit.Case, async: true
  alias Hourglass.Client.Backend

  test "impl/0 resolves the configured backend (Mock in test env)" do
    assert Backend.impl() == Hourglass.Client.Mock
  end

  test "set_impl/1 overrides the backend for the current process" do
    on_exit(fn -> Process.delete({Backend, :impl}) end)
    Backend.set_impl(Hourglass.Client.Real)
    assert Backend.impl() == Hourglass.Client.Real
  end

  test "set_impl/1 returns the previous value" do
    on_exit(fn -> Process.delete({Backend, :impl}) end)
    previous = Backend.set_impl(Hourglass.Client.Real)
    # No prior process-dict entry means the previous value is nil
    assert previous == nil
  end

  test "set_impl/1 override is process-local and does not affect other processes" do
    parent = self()

    spawn(fn ->
      Backend.set_impl(Hourglass.Client.Real)
      send(parent, {:child_impl, Backend.impl()})
    end)

    assert_receive {:child_impl, Hourglass.Client.Real}
    # The current process is not affected
    assert Backend.impl() == Hourglass.Client.Mock
  end

  test "clearing the process-dict override restores app config resolution" do
    on_exit(fn -> Process.delete({Backend, :impl}) end)
    Backend.set_impl(Hourglass.Client.Real)
    assert Backend.impl() == Hourglass.Client.Real

    Process.delete({Backend, :impl})
    assert Backend.impl() == Hourglass.Client.Mock
  end
end
