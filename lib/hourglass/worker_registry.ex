defmodule Hourglass.WorkerRegistry do
  @moduledoc "Registry mapping task-queue names to their Worker GenServer PIDs."

  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_init_arg), do: Registry.child_spec(keys: :unique, name: __MODULE__)

  @spec via(String.t()) :: {:via, Registry, {__MODULE__, String.t()}}
  def via(task_queue), do: {:via, Registry, {__MODULE__, task_queue}}
end
