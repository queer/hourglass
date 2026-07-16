defmodule Hourglass.TestSupport.ChildEcho do
  @moduledoc false
  use Hourglass.Workflow, input: :map, output: :map

  @impl Hourglass.Workflow.Behaviour
  def run(args) do
    %{
      "echoed" =>
        execute_activity!(Hourglass.TestSupport.EchoActivity, args,
          start_to_close_timeout: 30_000
        )
    }
  end
end

defmodule Hourglass.TestSupport.ParentAwaitsChild do
  @moduledoc false
  use Hourglass.Workflow, input: :map, output: :map

  @impl Hourglass.Workflow.Behaviour
  def run(args) do
    %{"child" => execute_child!(Hourglass.TestSupport.ChildEcho, args)}
  end
end

defmodule Hourglass.TestSupport.ParentFansOut do
  @moduledoc false
  use Hourglass.Workflow, input: :map, output: :map

  @impl Hourglass.Workflow.Behaviour
  def run(%{"nonces" => nonces}) do
    results =
      nonces
      |> Enum.map(fn n ->
        async(fn -> execute_child!(Hourglass.TestSupport.ChildEcho, %{"nonce" => n}) end)
      end)
      |> await_all()

    %{"results" => results}
  end
end

defmodule Hourglass.TestSupport.ParentDetaches do
  @moduledoc false
  use Hourglass.Workflow, input: :map, output: :map

  @impl Hourglass.Workflow.Behaviour
  def run(%{"child_id" => child_id, "policy" => policy} = args) do
    {:ok, handle} =
      start_child(Hourglass.TestSupport.SlowChild, args,
        id: child_id,
        parent_close_policy: String.to_existing_atom(policy)
      )

    # Return immediately: the parent closes while the child is still sleeping,
    # which is exactly what the parent_close_policy governs.
    %{"child_id" => handle.id, "child_run_id" => handle.run_id}
  end
end

defmodule Hourglass.TestSupport.SlowChild do
  @moduledoc false
  use Hourglass.Workflow, input: :map, output: :map

  @impl Hourglass.Workflow.Behaviour
  def run(_args) do
    # Must still be sleeping when the :abandon/:terminate assertions run, so the
    # margin is what makes those tests non-flaky: `assert_eventually` spends up
    # to ~5s of retries plus round-trips, and the parent closes immediately
    # (it never awaits this child). 60s gives ~12x headroom under a loaded CI
    # box while still fitting the 120s @moduletag timeout. An abandoned child
    # outliving its test by design is harmless — the namespace is test-only.
    sleep({:sec, 60})
    %{"finished" => true}
  end
end

defmodule Hourglass.TestSupport.TamperedParent do
  @moduledoc false
  # Deliberately diverges from ParentAwaitsChild's command sequence (two children
  # instead of one) so replaying ParentAwaitsChild's history against it is a
  # non-determinism mismatch. Mirrors TamperedEchoWorkflow in replayer_test.exs.
  use Hourglass.Workflow, input: :map, output: :map

  @impl Hourglass.Workflow.Behaviour
  def run(args) do
    _first = execute_child!(Hourglass.TestSupport.ChildEcho, args)
    %{"child" => execute_child!(Hourglass.TestSupport.ChildEcho, args)}
  end
end

defmodule Hourglass.TestSupport.ChildContinuesAsNew do
  @moduledoc false
  use Hourglass.Workflow, input: :map, output: :map

  @impl Hourglass.Workflow.Behaviour
  def run(%{"gen" => 1} = args), do: continue_as_new(Map.put(args, "gen", 2))
  def run(%{"gen" => 2, "nonce" => nonce}), do: %{"finished_gen" => 2, "nonce" => nonce}
end

defmodule Hourglass.TestSupport.ParentAwaitsContinuingChild do
  @moduledoc false
  use Hourglass.Workflow, input: :map, output: :map

  @impl Hourglass.Workflow.Behaviour
  def run(args) do
    %{
      "child" =>
        execute_child!(Hourglass.TestSupport.ChildContinuesAsNew, Map.put(args, "gen", 1))
    }
  end
end

defmodule Hourglass.TestSupport.ParentContinuesAsNew do
  @moduledoc false
  # Both generations start a child at the SAME command_id ({[], 1}), and
  # :reject_duplicate makes an id collision a hard start failure. A child id
  # derived from parent_workflow_id + seq WOULD collide here — seq restarts at 1
  # each run while the parent's workflow id does not — so this fixture is the
  # runtime guard on deriving from run_id instead.
  use Hourglass.Workflow, input: :map, output: :map

  @impl Hourglass.Workflow.Behaviour
  def run(%{"gen" => 1, "nonce" => nonce} = args) do
    _first =
      execute_child!(Hourglass.TestSupport.ChildEcho, %{"nonce" => nonce},
        workflow_id_reuse_policy: :reject_duplicate
      )

    continue_as_new(Map.put(args, "gen", 2))
  end

  def run(%{"gen" => 2, "nonce" => nonce}) do
    %{
      "second" =>
        execute_child!(Hourglass.TestSupport.ChildEcho, %{"nonce" => nonce},
          workflow_id_reuse_policy: :reject_duplicate
        )
    }
  end
end
