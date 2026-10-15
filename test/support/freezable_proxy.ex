defmodule Hourglass.TestSupport.FreezableProxy do
  @moduledoc false
  # A TCP proxy to the local Temporal that a test can freeze: frozen, it
  # forwards nothing in either direction, as a network that has gone quiet
  # would — connections stay open, and nothing answers. Thawed, it forwards
  # again on the same connections.
  #
  # `start_link/1` returns the proxy; `url/1` is the target URL to give a
  # client in place of Temporal's. `connections/1` counts the connections it
  # has accepted, and `cut/1` closes those open, as a network that drops
  # them would.

  use GenServer

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts)

  @spec url(pid()) :: String.t()
  def url(proxy), do: "http://127.0.0.1:#{GenServer.call(proxy, :port)}"

  @spec freeze(pid()) :: :ok
  def freeze(proxy), do: GenServer.call(proxy, {:frozen, true})

  @spec thaw(pid()) :: :ok
  def thaw(proxy), do: GenServer.call(proxy, {:frozen, false})

  @spec connections(pid()) :: non_neg_integer()
  def connections(proxy), do: GenServer.call(proxy, :connections)

  @spec cut(pid()) :: :ok
  def cut(proxy), do: GenServer.call(proxy, :cut)

  @impl GenServer
  def init(opts) do
    upstream = Keyword.get(opts, :upstream_port, temporal_port())
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    frozen = :atomics.new(1, [])
    proxy = self()
    _acceptor = spawn_link(fn -> accept(listen, upstream, frozen, proxy) end)
    {:ok, %{port: port, frozen: frozen, accepted: 0, open: []}}
  end

  @impl GenServer
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  def handle_call({:frozen, frozen?}, _from, state) do
    :atomics.put(state.frozen, 1, if(frozen?, do: 1, else: 0))
    {:reply, :ok, state}
  end

  def handle_call(:connections, _from, state), do: {:reply, state.accepted, state}

  def handle_call(:cut, _from, state) do
    for socket <- state.open, do: :gen_tcp.close(socket)
    {:reply, :ok, %{state | open: []}}
  end

  @impl GenServer
  def handle_info({:accepted, sockets}, state),
    do: {:noreply, %{state | accepted: state.accepted + 1, open: sockets ++ state.open}}

  defp temporal_port do
    :hourglass
    |> Application.get_env(Hourglass.Client, [])
    |> Keyword.get(:target_url, "http://localhost:7233")
    |> URI.parse()
    |> Map.fetch!(:port)
  end

  defp accept(listen, upstream, frozen, proxy) do
    {:ok, client} = :gen_tcp.accept(listen)
    {:ok, server} = :gen_tcp.connect(~c"127.0.0.1", upstream, [:binary, active: false])
    send(proxy, {:accepted, [client, server]})
    up = spawn_link(fn -> pipe(client, server, frozen) end)
    down = spawn_link(fn -> pipe(server, client, frozen) end)
    :ok = :gen_tcp.controlling_process(client, up)
    :ok = :gen_tcp.controlling_process(server, down)
    accept(listen, upstream, frozen, proxy)
  end

  # Forward what arrives, holding it back while frozen.
  defp pipe(from, to, frozen) do
    case :gen_tcp.recv(from, 0) do
      {:ok, data} ->
        await_thaw(frozen)
        _sent = :gen_tcp.send(to, data)
        pipe(from, to, frozen)

      {:error, _closed} ->
        :ok
    end
  end

  defp await_thaw(frozen) do
    if :atomics.get(frozen, 1) == 1 do
      Process.sleep(20)
      await_thaw(frozen)
    end
  end
end
