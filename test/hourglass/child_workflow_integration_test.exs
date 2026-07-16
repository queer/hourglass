defmodule Hourglass.ChildWorkflowIntegrationTest do
  @moduledoc """
  End-to-end tests for child workflow support (`execute_child/2,3`,
  `execute_child!/2,3`, `start_child/3`) against a real Temporal cluster.

  Requires a live Temporal cluster (see `compose.yaml` / README) and is
  excluded from the default `mix test` run via the `:temporal` and
  `:integration` tags. Run with:

      mix test.integration

  ## What is tested

    * A parent awaiting a real child workflow's result end to end.
    * Fan-out: N children started via `async/1` + `await_all/1`.
    * `:parent_close_policy` — `:abandon` (child outlives the parent) vs
      `:terminate` (child dies with the parent). This is the runtime proof
      that the policy is plumbed and actually takes effect — Temporal's
      server-side default is TERMINATE, so a naive fire-and-forget child
      would otherwise be silently killed.
    * A child that continues-as-new still resolves to the parent — the
      concrete case a naive `Hourglass.result/2` poll gets wrong (treating
      `:continued_as_new` as a terminal error instead of following the
      chain to its final generation).
    * A parent that itself continues-as-new gets a non-colliding child id
      in each generation (guards the run_id-derived child id design).
    * Replay: a child-using parent replays cleanly against itself and
      mismatches a tampered twin.
  """

  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  alias Hourglass.Client
  alias Hourglass.Replay.Mismatch
  alias Hourglass.Replayer
  alias Hourglass.WorkflowHandle

  @moduletag :temporal
  @moduletag :integration
  @moduletag timeout: 120_000

  test "a parent awaits a real child workflow's result" do
    queue = "child-await-#{System.unique_integer([:positive])}"
    {:ok, _worker} = start_supervised({Hourglass.Worker, task_queue: queue})

    nonce = UUIDv7.generate()

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(Hourglass.TestSupport.ParentAwaitsChild, %{"nonce" => nonce},
        id: "child-await-#{nonce}",
        task_queue: queue
      )

    assert {:ok, result} = Hourglass.result(handle, timeout: 60_000)
    assert result == %{"child" => %{"echoed" => %{"nonce" => nonce}}}
  end

  test "fan-out: N children via async/await_all all complete" do
    queue = "child-fanout-#{System.unique_integer([:positive])}"
    {:ok, _worker} = start_supervised({Hourglass.Worker, task_queue: queue})

    nonces = for _n <- 1..3, do: UUIDv7.generate()

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(Hourglass.TestSupport.ParentFansOut, %{"nonces" => nonces},
        id: "child-fanout-#{UUIDv7.generate()}",
        task_queue: queue
      )

    assert {:ok, %{"results" => results}} = Hourglass.result(handle, timeout: 60_000)
    assert length(results) == 3

    # await_all/1 preserves scope order, so results line up with the input nonces.
    assert Enum.map(results, & &1["echoed"]["nonce"]) == nonces
  end

  test "parent_close_policy :abandon — the child outlives the parent" do
    queue = "child-abandon-#{System.unique_integer([:positive])}"
    {:ok, _worker} = start_supervised({Hourglass.Worker, task_queue: queue})

    child_id = "abandoned-child-#{UUIDv7.generate()}"

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(
        Hourglass.TestSupport.ParentDetaches,
        %{"child_id" => child_id, "policy" => "abandon"},
        id: "child-abandon-#{UUIDv7.generate()}",
        task_queue: queue
      )

    # The parent returns as soon as the child STARTS — it does not await it.
    assert {:ok, %{"child_id" => ^child_id}} = Hourglass.result(handle, timeout: 60_000)

    # Parent is closed; the abandoned child must still be running.
    assert_eventually(fn ->
      assert {:ok, %{state: :running}} = Hourglass.status(child_id)
    end)
  end

  test "parent_close_policy :terminate — the child dies with the parent" do
    queue = "child-terminate-#{System.unique_integer([:positive])}"
    {:ok, _worker} = start_supervised({Hourglass.Worker, task_queue: queue})

    child_id = "terminated-child-#{UUIDv7.generate()}"

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(
        Hourglass.TestSupport.ParentDetaches,
        %{"child_id" => child_id, "policy" => "terminate"},
        id: "child-terminate-#{UUIDv7.generate()}",
        task_queue: queue
      )

    assert {:ok, %{"child_id" => ^child_id}} = Hourglass.result(handle, timeout: 60_000)

    # Parent closed ⇒ the server terminates the still-sleeping child.
    assert_eventually(fn ->
      assert {:ok, %{state: state}} = Hourglass.status(child_id)
      assert state == :terminated
    end)
  end

  test "a child that continues-as-new still resolves to the parent" do
    # The concrete case Hourglass.result/2's poll gets wrong: it treats
    # :continued_as_new as a TERMINAL error. A real child workflow resolves
    # through its continue-as-new chain, so the parent sees the FINAL
    # generation's output.
    queue = "child-can-#{System.unique_integer([:positive])}"
    {:ok, _worker} = start_supervised({Hourglass.Worker, task_queue: queue})

    nonce = UUIDv7.generate()

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(Hourglass.TestSupport.ParentAwaitsContinuingChild, %{"nonce" => nonce},
        id: "child-can-#{nonce}",
        task_queue: queue
      )

    assert {:ok, result} = Hourglass.result(handle, timeout: 60_000)
    assert result == %{"child" => %{"finished_gen" => 2, "nonce" => nonce}}
  end

  test "a parent that continues-as-new gets a non-colliding child id in each generation" do
    # Both generations issue their child at command_id {[], 1} under
    # :reject_duplicate. Deriving the child id from run_id (fresh per
    # generation) is what keeps gen 2's start from colliding with gen 1's.
    queue = "parent-can-#{System.unique_integer([:positive])}"
    {:ok, _worker} = start_supervised({Hourglass.Worker, task_queue: queue})

    nonce = UUIDv7.generate()
    workflow_id = "parent-can-#{nonce}"

    {:ok, %WorkflowHandle{}} =
      Hourglass.start(
        Hourglass.TestSupport.ParentContinuesAsNew,
        %{"gen" => 1, "nonce" => nonce},
        id: workflow_id,
        task_queue: queue
      )

    # Poll by the bare workflow_id, NOT the handle `Hourglass.start/3` returned.
    # That handle pins gen 1's run_id, and Hourglass.result/2 by design treats
    # :continued_as_new as a terminal error for the SPECIFIC run it's given
    # (see its moduledoc) — it does not chase continue-as-new for its own
    # target the way Core transparently follows a *child's* chain. A bare id
    # normalizes to run_id: "", which the server resolves to the CURRENT run
    # for that workflow_id, so polling naturally lands on gen 2.
    #
    # Reaching gen 2's completion at all proves gen 2's child started, i.e. its
    # id did not collide with gen 1's.
    assert {:ok, %{"second" => %{"echoed" => %{"nonce" => ^nonce}}}} =
             Hourglass.result(workflow_id, timeout: 60_000)

    # And the surviving generation's history replays cleanly.
    assert {:ok, history_bytes} = fetch_history_bytes(workflow_id)

    assert :ok =
             Replayer.replay_history(history_bytes, Hourglass.TestSupport.ParentContinuesAsNew)
  end

  test "replay: a child-using parent replays cleanly against itself and mismatches a tampered twin" do
    queue = "child-replay-#{System.unique_integer([:positive])}"
    {:ok, _worker} = start_supervised({Hourglass.Worker, task_queue: queue})

    nonce = UUIDv7.generate()
    workflow_id = "child-replay-#{nonce}"

    {:ok, %WorkflowHandle{} = handle} =
      Hourglass.start(Hourglass.TestSupport.ParentAwaitsChild, %{"nonce" => nonce},
        id: workflow_id,
        task_queue: queue
      )

    assert {:ok, _result} = Hourglass.result(handle, timeout: 60_000)
    assert {:ok, history_bytes} = fetch_history_bytes(workflow_id)

    assert :ok = Replayer.replay_history(history_bytes, Hourglass.TestSupport.ParentAwaitsChild)

    assert {:error, %Mismatch{}} =
             Replayer.replay_history(history_bytes, Hourglass.TestSupport.TamperedParent)
  end

  defp fetch_history_bytes(workflow_id) do
    with {:ok, client} <- Client.connect([]) do
      Client.fetch_history(client, workflow_id)
    end
  end

  defp assert_eventually(fun, retries \\ 50)
  defp assert_eventually(fun, 0), do: fun.()

  defp assert_eventually(fun, retries) do
    fun.()
  rescue
    _error ->
      Process.sleep(100)
      assert_eventually(fun, retries - 1)
  end
end
