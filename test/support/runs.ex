defmodule Hourglass.TestSupport.Runs do
  @moduledoc """
  Ends a workflow a test may leave running, when the test ends: the suite
  leaves no workflow running on the Temporal it shares
  (`Hourglass.TestSupport.WorkflowGuard`).
  """

  alias Hourglass.Error
  alias Hourglass.TestSupport.PatientBackend
  alias Hourglass.WorkflowHandle

  @doc """
  Terminate the latest run of the workflow `id` (or of a handle's) when the
  calling test ends, unless it has closed by then.
  """
  @spec end_on_exit(String.t() | WorkflowHandle.t()) :: :ok
  def end_on_exit(%WorkflowHandle{id: id}), do: end_on_exit(id)
  def end_on_exit(id) when is_binary(id), do: ExUnit.Callbacks.on_exit(fn -> end_run(id) end)

  defp end_run(id) do
    case PatientBackend.terminate_workflow(
           %WorkflowHandle{id: id, run_id: ""},
           "the test is over"
         ) do
      :ok -> :ok
      {:error, %Error{reason: :not_found}} -> :ok
    end
  end
end
