defmodule Hourglass.TestSupport.NamespaceGuard do
  @moduledoc """
  Keeps every namespace the suite creates on the path whose leak check
  covers it: `Hourglass.TestSupport.Namespaces.scratch!/1`, whose
  namespaces are deleted, and checked gone, when their test ends.

  Every way hourglass registers a namespace ends in one NIF,
  `Hourglass.Bridge.client_register_namespace/2`. The guard traces it in
  every process, in a trace session of its own, so other tracing in the
  suite is undisturbed. Registering a name `scratch!/1` did not hand out is
  a violation: once the call returns, its caller is killed, so the test
  that made it fails naming the namespace; a namespace it did create is
  deleted; and `verify/1`, run after the suite, fails the run.
  """

  use GenServer

  alias Hourglass.Bridge
  alias Hourglass.Client
  alias Hourglass.TestSupport.Namespaces
  alias Hourglass.TestSupport.Patience

  @registers {Bridge, :client_register_namespace, 3}

  @doc """
  Start a guard, unlinked: it lives for the whole run. Options, for testing
  the guard itself: `:name` (default `#{inspect(__MODULE__)}`), `:registers`
  (the `{module, function, arity}` that registers the namespace named by its
  second argument) and `:delete` (deletes a namespace a violation created).
  """
  @spec start(keyword()) :: GenServer.on_start()
  def start(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start(__MODULE__, Keyword.put(opts, :name, name), name: name)
  end

  @doc "Allow `name`, handed out by `scratch!/1`, to be registered."
  @spec allow(atom(), String.t()) :: :ok
  def allow(guard \\ __MODULE__, name) do
    true = :ets.insert(guard, {name})
    :ok
  end

  @doc """
  The registrations of names not allowed, as `{name, result}`, once every
  namespace they created is deleted.
  """
  @spec violations(atom()) :: [{String.t(), term()}]
  def violations(guard \\ __MODULE__), do: GenServer.call(guard, :violations, :infinity)

  @doc """
  After the suite: if anything registered a namespace `scratch!/1` did not
  hand out, say which, and make the run exit non-zero.
  """
  @spec verify(atom()) :: :ok
  def verify(guard \\ __MODULE__) do
    case violations(guard) do
      [] ->
        :ok

      violations ->
        listed =
          Enum.map_join(violations, "\n", fn {name, result} ->
            "  * #{name}: #{inspect(result)}"
          end)

        Mix.shell().error("""

        [hourglass] FAILED: the suite registered namespaces other than through \
        Hourglass.TestSupport.Namespaces.scratch!/1, whose leak check covers every \
        namespace a test creates. Each one created has been deleted.

        #{listed}
        """)

        System.at_exit(fn _status -> exit({:shutdown, 1}) end)
        :ok
    end
  end

  @impl GenServer
  def init(opts) do
    name = Keyword.fetch!(opts, :name)
    registers = Keyword.get(opts, :registers, @registers)
    :ets.new(name, [:named_table, :public, :set])

    # Loaded first (and its NIF with it): a pattern set on a module not yet
    # loaded matches nothing, and would trace nothing.
    {module, _function, _arity} = registers
    Code.ensure_loaded!(module)
    session = :trace.session_create(name, self(), [])
    _processes = :trace.process(session, :all, true, [:call])
    1 = :trace.function(session, registers, [{:_, [], [{:return_trace}]}], [:global])

    {:ok,
     %{
       name: name,
       session: session,
       registers: registers,
       delete: Keyword.get(opts, :delete, &delete!/1),
       # The name each caller is registering: pid => name.
       calls: %{},
       violations: [],
       # Deletions in flight: monitor ref => name.
       deleting: %{},
       waiting: []
     }}
  end

  @impl GenServer
  def handle_call(:violations, _from, %{deleting: deleting} = state) when deleting == %{},
    do: {:reply, Enum.reverse(state.violations), state}

  def handle_call(:violations, from, state),
    do: {:noreply, %{state | waiting: [from | state.waiting]}}

  @impl GenServer
  def handle_info(
        {:trace, caller, :call, {m, f, [_client, name | _rest]}},
        %{registers: {m, f, _arity}} = state
      ),
      do: {:noreply, %{state | calls: Map.put(state.calls, caller, name)}}

  def handle_info(
        {:trace, caller, :return_from, {m, f, arity}, result},
        %{registers: {m, f, arity}} = state
      ) do
    {name, calls} = Map.pop(state.calls, caller)
    state = %{state | calls: calls}

    if name == nil or :ets.member(state.name, name),
      do: {:noreply, state},
      else: {:noreply, violated(state, caller, name, result)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state)
      when is_map_key(state.deleting, ref) do
    {name, deleting} = Map.pop(state.deleting, ref)

    violations =
      if reason == :normal,
        do: state.violations,
        else: [{name, {:not_deleted, reason}} | state.violations]

    {:noreply, answer_waiting(%{state | deleting: deleting, violations: violations})}
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp violated(state, caller, name, result) do
    Process.exit(caller, {:namespace_outside_scratch, name})
    state = %{state | violations: [{name, result} | state.violations]}

    if result == :ok do
      {_pid, ref} = spawn_monitor(fn -> state.delete.(name) end)
      %{state | deleting: Map.put(state.deleting, ref, name)}
    else
      state
    end
  end

  defp answer_waiting(%{deleting: deleting} = state) when deleting == %{} do
    for from <- state.waiting, do: GenServer.reply(from, Enum.reverse(state.violations))
    %{state | waiting: []}
  end

  defp answer_waiting(state), do: state

  defp delete!(name) do
    {:ok, client} = Patience.retry(fn -> Client.connect() end)
    :ok = Namespaces.gone!(client, [name, Namespaces.delete!(client, name)])
  end
end
