defmodule Hourglass.Worker.Supervisor do
  @moduledoc """
  DynamicSupervisor of per-task-queue `Hourglass.Worker` GenServers.

  Bridge-handle lifecycle is managed inside `Hourglass.Worker`
  itself: `init/1` calls `BridgeHolder.register_worker/2`,
  `terminate/2` calls `BridgeHolder.unregister_worker/1`. This module
  is a thin wrapper that supports adding and removing Workers
  dynamically (e.g. one per Temporal task queue the application
  serves).
  """

  use DynamicSupervisor

  alias Hourglass.BridgeHolder
  alias Hourglass.WorkerRegistry

  @spec start_link(term()) :: Supervisor.on_start()
  def start_link(_init_arg), do: DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)

  @impl DynamicSupervisor
  def init(_init_arg), do: DynamicSupervisor.init(strategy: :one_for_one)

  @doc """
  Start a Worker child for `opts[:task_queue]`. The Worker's `init/1`
  registers a bridge handle via `BridgeHolder` and starts the inner
  poll-loop Supervisor.
  """
  @spec start_worker(keyword()) :: DynamicSupervisor.on_start_child()
  def start_worker(opts) do
    DynamicSupervisor.start_child(__MODULE__, {Hourglass.Worker, opts})
  end

  @doc """
  Stop the Worker for `task_queue`. Looks the Worker pid up via
  `WorkerRegistry` and terminates its child entry under this DynSup,
  which runs the Worker's graceful stop (`Hourglass.Worker`'s "Graceful
  stop"): it returns once the work the Worker held has finished and
  reported, or the grace has run out, and the handle is released. That
  can take up to the Worker's `:graceful_shutdown_period` plus fifteen
  seconds, and this supervisor starts or stops no other Worker
  meanwhile. If the Worker isn't registered, defensively unregister the
  bridge handle anyway (idempotent) and return `{:error, :not_found}` —
  as a second stop of a Worker already stopping does, once the first
  has finished.
  """
  @spec stop_worker(String.t()) :: :ok | {:error, :not_found}
  def stop_worker(task_queue) when is_binary(task_queue) do
    whereis =
      task_queue
      |> WorkerRegistry.via()
      |> GenServer.whereis()

    case whereis do
      nil ->
        _result = BridgeHolder.unregister_worker(task_queue)
        {:error, :not_found}

      pid when is_pid(pid) ->
        DynamicSupervisor.terminate_child(__MODULE__, pid)
    end
  end
end
