defmodule Hourglass.WorkflowDeterminismCompileTest do
  use ExUnit.Case, async: true

  test "a workflow calling a banned non-deterministic primitive fails to compile" do
    src = """
    defmodule BadWf#{System.unique_integer([:positive])} do
      use Hourglass.Workflow
      @impl true
      def run(_), do: DateTime.utc_now()
    end
    """

    assert_raise CompileError, ~r/non-deterministic|DateTime/, fn -> Code.compile_string(src) end
  end

  test "a banned Erlang primitive in a private helper also fails" do
    src = """
    defmodule BadWf2#{System.unique_integer([:positive])} do
      use Hourglass.Workflow
      @impl true
      def run(_), do: pick()
      defp pick, do: :rand.uniform(10)
    end
    """

    assert_raise CompileError, ~r/non-deterministic|rand/, fn -> Code.compile_string(src) end
  end

  test "a clean workflow compiles fine" do
    src = """
    defmodule GoodWf#{System.unique_integer([:positive])} do
      use Hourglass.Workflow
      @impl true
      def run(args), do: Map.put(args, "ok", true)
    end
    """

    assert [{_mod, _bin} | _rest] = Code.compile_string(src)
  end

  test "a workflow using Plan-4 primitives (sleep, await_signal, cancelled?, continue_as_new) compiles fine" do
    src = """
    defmodule GoodWf4Primitives#{System.unique_integer([:positive])} do
      use Hourglass.Workflow
      @impl true
      def run(args) do
        sleep({:sec, 1})
        _sig = await_signal("x")
        if cancelled?(), do: continue_as_new(%{})
        args
      end
    end
    """

    assert [{_mod, _bin} | _rest] = Code.compile_string(src)
  end

  test "a workflow calling Process.sleep still fails to compile (regression)" do
    src = """
    defmodule BadWfProcessSleep#{System.unique_integer([:positive])} do
      use Hourglass.Workflow
      @impl true
      def run(_), do: Process.sleep(100)
    end
    """

    assert_raise CompileError, ~r/non-deterministic|Process/, fn -> Code.compile_string(src) end
  end
end
