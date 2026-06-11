# credo:disable-for-this-file Credo.Check.Refactor.ModuleDependencies
defmodule Hourglass.Replayer do
  @moduledoc """
  Replays a captured workflow history against the current workflow code,
  detecting nondeterminism (the workflow producing different commands
  than what the original execution produced).

  Drives each activation directly through `Hourglass.Workflow.Evaluator`
  — the same pure-function evaluator that production uses — and threads
  the evaluator state per `run_id` across activations.

  Used by the determinism CI gate (`mix hourglass.workflow.replay`).
  """

  alias Coresdk.WorkflowActivation.WorkflowActivation
  alias Hourglass.Bridge
  alias Hourglass.Replay.Mismatch
  alias Hourglass.Runtime
  alias Hourglass.Workflow.Evaluator
  alias Hourglass.Workflow.State

  require Logger

  @spec replay_history(history_response_bytes :: binary(), workflow_module :: module()) ::
          :ok | {:error, Mismatch.t()}
  def replay_history(history_response_bytes, workflow_module) do
    runtime = Runtime.handle()
    config_bin = build_replay_config()

    with {:ok, history_bytes} <- extract_history_bytes(history_response_bytes),
         {:ok, replayer} <- start_replayer(runtime, config_bin),
         :ok <- Bridge.replayer_push_history(replayer, "replay-target", history_bytes),
         :ok <- Bridge.replayer_close_feeder(replayer) do
      drive_replay(replayer, workflow_module)
    else
      {:error, reason} -> {:error, replay_error_to_mismatch(reason)}
    end
  end

  defp build_replay_config do
    Protobuf.encode(%Hourglass.Proto.ReplayerConfig{
      namespace: "default",
      task_queue: "replay-#{System.unique_integer([:positive])}"
    })
  end

  # Wrap `Bridge.replayer_new/2` so a post-hot-reload `ArgumentError` (stale
  # runtime ref decoded against a freshly loaded NIF resource registry)
  # cascades a Subsystem restart instead of crashing the caller opaquely.
  # See `Bridge.with_nif_reload_rescue/2` moduledoc.
  defp start_replayer(runtime, config_bin) do
    Bridge.with_nif_reload_rescue("replayer_new/replay_history", fn ->
      Bridge.replayer_new(runtime, config_bin)
    end)
  end

  defp replay_error_to_mismatch(:nif_reloaded),
    do: %Mismatch{detail: "NIF reload invalidated replayer runtime ref"}

  defp replay_error_to_mismatch(%Bridge.Error{kind: :nondeterminism, detail: d}),
    do: %Mismatch{detail: d}

  defp replay_error_to_mismatch(%Bridge.Error{} = err),
    do: %Mismatch{detail: inspect(err)}

  defp replay_error_to_mismatch(reason),
    do: %Mismatch{detail: inspect(reason)}

  # `Client.fetch_history` returns proto-encoded `GetWorkflowExecutionHistoryResponse`.
  # The replayer NIF expects proto-encoded `temporal.api.history.v1.History`.
  defp extract_history_bytes(response_bytes) do
    alias Temporal.Api.History.V1.History
    alias Temporal.Api.Workflowservice.V1.GetWorkflowExecutionHistoryResponse

    case GetWorkflowExecutionHistoryResponse.decode(response_bytes) do
      %GetWorkflowExecutionHistoryResponse{history: %History{} = history} ->
        {:ok, Protobuf.encode(history)}

      %GetWorkflowExecutionHistoryResponse{history: nil} ->
        {:error, :empty_history}

      _other ->
        {:error, :decode_failed}
    end
  rescue
    e -> {:error, Exception.message(e)}
  end

  defp drive_replay(replayer, workflow_module) do
    drive_loop(replayer, workflow_module, %{})
  end

  defp drive_loop(replayer, workflow_module, states) do
    case Bridge.replayer_poll_workflow_activation(replayer) do
      {:ok, activation_bytes} ->
        activation = WorkflowActivation.decode(activation_bytes)

        # Check for nondeterminism eviction before dispatching to the evaluator.
        # When Core detects a command-sequence mismatch, the *next* poll returns an
        # activation whose jobs list contains a remove_from_cache with reason NONDETERMINISM.
        # We complete the eviction with an empty ack, then let the next poll return shutdown.
        case nondeterminism_eviction?(activation) do
          {:nondeterminism, detail} ->
            # Ack the eviction so Core can clean up, then return the mismatch.
            ack_eviction(replayer, activation.run_id)
            {:error, %Mismatch{detail: detail}}

          :normal ->
            evaluate_and_complete(replayer, workflow_module, activation, states)
        end

      {:error, %Bridge.Error{kind: :shutdown}} ->
        # History exhausted — replay successful.
        :ok

      {:error, err} ->
        {:error, %Mismatch{detail: inspect(err)}}
    end
  end

  defp evaluate_and_complete(replayer, workflow_module, activation, states) do
    state = Map.get_lazy(states, activation.run_id, fn -> State.new(activation.run_id, "") end)

    case Evaluator.evaluate(workflow_module, activation, state) do
      {:ok, completion, new_state} ->
        completion_bytes = Protobuf.encode(completion)

        case Bridge.replayer_complete_workflow_activation(replayer, completion_bytes) do
          :ok ->
            drive_loop(
              replayer,
              workflow_module,
              Map.put(states, activation.run_id, new_state)
            )

          {:error, err} ->
            Logger.error("replayer_complete failed: #{inspect(err)}")
            {:error, %Mismatch{detail: inspect(err)}}
        end
    end
  end

  # Returns `{:nondeterminism, detail}` if the activation is a nondeterminism eviction,
  # or `:normal` otherwise.
  defp nondeterminism_eviction?(%Coresdk.WorkflowActivation.WorkflowActivation{jobs: jobs}) do
    Enum.find_value(jobs, :normal, fn job ->
      case job.variant do
        {:remove_from_cache,
         %Coresdk.WorkflowActivation.RemoveFromCache{reason: :NONDETERMINISM, message: msg}} ->
          {:nondeterminism, msg || "nondeterminism detected"}

        _variant ->
          nil
      end
    end)
  end

  # Sends an empty completion to acknowledge a remove_from_cache eviction.
  defp ack_eviction(replayer, run_id) do
    completion =
      Protobuf.encode(%Coresdk.WorkflowCompletion.WorkflowActivationCompletion{
        run_id: run_id,
        status: {:successful, %Coresdk.WorkflowCompletion.Success{}}
      })

    _result = Bridge.replayer_complete_workflow_activation(replayer, completion)
    :ok
  end
end
