defmodule Hourglass.Runtime do
  @moduledoc """
  One-per-application GenServer holding the CoreRuntime resource.
  Workers share the resource via `handle/0`. Constructing more than
  one CoreRuntime is wasteful (each spins up its own Tokio runtime).
  """

  use GenServer

  alias Hourglass.Bridge

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @spec handle() :: term()
  def handle, do: GenServer.call(__MODULE__, :handle)

  @impl GenServer
  def init(_opts) do
    case Bridge.runtime_new() do
      {:ok, runtime} -> {:ok, %{runtime: runtime}}
      {:error, %Bridge.Error{} = err} -> {:stop, err}
    end
  end

  @impl GenServer
  def handle_call(:handle, _from, %{runtime: runtime} = state), do: {:reply, runtime, state}
end
