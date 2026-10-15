defmodule Hourglass.NamespaceGuardTest do
  # The guard that keeps every namespace the suite creates on the path its leak
  # check covers, run against a stand-in for the NIF that registers one.
  use ExUnit.Case, async: true

  alias Hourglass.TestSupport.NamespaceGuard

  @doc false
  # The stand-in: answers what the test says, as the NIF would.
  def register(answer, _name), do: answer

  setup %{test: name} do
    test = self()

    {:ok, guard} =
      NamespaceGuard.start(
        name: name,
        registers: {__MODULE__, :register, 2},
        delete: fn namespace -> send(test, {:deleted, namespace}) end
      )

    on_exit(fn -> Process.exit(guard, :kill) end)
    %{guard: name}
  end

  test "a name scratch! handed out is registered undisturbed", %{guard: guard} do
    :ok = NamespaceGuard.allow(guard, "allowed")
    {caller, monitor} = registering(:ok, "allowed")

    send(caller, :done)
    assert_receive {:DOWN, ^monitor, :process, ^caller, :normal}
    assert NamespaceGuard.violations(guard) == []
  end

  test "any other name fails its caller and the run, and a namespace it created is deleted",
       %{guard: guard} do
    {caller, monitor} = registering(:ok, "rogue")

    assert_receive {:DOWN, ^monitor, :process, ^caller, {:namespace_outside_scratch, "rogue"}}
    assert_receive {:deleted, "rogue"}
    assert NamespaceGuard.violations(guard) == [{"rogue", :ok}]
  end

  test "a registration that created nothing deletes nothing, and still fails the run",
       %{guard: guard} do
    {caller, monitor} = registering({:error, :already_exists}, "default")

    assert_receive {:DOWN, ^monitor, :process, ^caller, {:namespace_outside_scratch, "default"}}
    assert NamespaceGuard.violations(guard) == [{"default", {:error, :already_exists}}]
    refute_received {:deleted, _namespace}
  end

  # A process that registers `name`, getting `answer`, then waits to be told
  # it is done.
  defp registering(answer, name) do
    spawn_monitor(fn ->
      __MODULE__.register(answer, name)
      receive do: (:done -> :ok)
    end)
  end
end
