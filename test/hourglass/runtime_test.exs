defmodule Hourglass.RuntimeTest do
  # async: true — Runtime is started globally in test_helper.exs (it's a
  # name-registered singleton holding a CoreRuntime resource shared across
  # all workers). The test asserts handle/0 returns a usable resource from
  # the running singleton; no per-test start/stop lifecycle exercised here.
  use ExUnit.Case, async: true
  alias Hourglass.Runtime

  test "handle/0 returns a usable runtime resource from the running singleton" do
    handle = Runtime.handle()
    refute is_nil(handle)
  end

  test "start_link returns {:already_started, pid} when the singleton is up" do
    # The singleton is started in test_helper.exs; a second start_link must
    # return :already_started rather than spinning up a second CoreRuntime
    # (which would be wasteful — see @moduledoc on Runtime).
    assert {:error, {:already_started, pid}} = Runtime.start_link([])
    assert is_pid(pid)
  end
end
