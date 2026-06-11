defmodule Hourglass.ReplayerBridgeTest do
  # async: true — Runtime lives globally in test_helper.exs; replayer
  # resources are per-test and don't share state across modules.
  use ExUnit.Case, async: true

  alias Hourglass.Bridge
  alias Hourglass.Runtime

  @moduletag :temporal
  # Bridge-NIF contract test — exercises real replayer_new / push_history.
  # Cannot be mocked: proves the NIF transport. The replayer NIF is purely
  # local (no cluster connection), so this test only needs the NIF loaded —
  # no live Temporal cluster required. Gated :temporal for consistency with
  # other bridge tests (default suite excludes :temporal).

  setup do
    runtime = Runtime.handle()

    config_bin =
      Protobuf.encode(%Hourglass.Proto.ReplayerConfig{
        namespace: "default",
        task_queue: "replay-bridge-test"
      })

    {:ok, replayer} = Bridge.replayer_new(runtime, config_bin)
    [replayer: replayer]
  end

  test "replayer_new returns a usable resource", %{replayer: replayer} do
    refute is_nil(replayer)
  end

  # NOTE: there used to be a "push_history with empty History accepts the
  # input" test here. It's gone because the synchronous push returns :ok
  # regardless of input validity (history is buffered for background
  # processing); Core's `ReplayWorkerInput::into_core_worker` then panics
  # on the tokio-rt-worker thread when it pulls the buffered history and
  # finds no `WorkflowExecutionStarted` first event. The test passed but
  # corrupted test-runner output with the async panic. End-to-end replay
  # is covered by `Hourglass.ReplayerTest` against real fixtures.
end
