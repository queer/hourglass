defmodule Hourglass.Worker.ClosedRuns do
  @moduledoc false
  # Which workflow runs have closed, asked of Temporal
  # (`DescribeWorkflowExecution`) through a client of the Worker's own
  # namespace and target, for `Hourglass.Worker.ClosedWorkflowWatch`.
  #
  # Neither call waits on the network: `open/2` and `describe/3` start the
  # work on the Core runtime and return; the answer comes to the caller as a
  # message, which `answer/1` reads. Each describe is given up on after its
  # timeout, so a Temporal that is slow or has gone quiet holds no scheduler
  # thread, and delays nothing past the timeout.
  #
  # A run that has closed, in any way, or that Temporal no longer knows, has
  # closed; a run Temporal cannot be asked about, or does not answer for in
  # time, has not.

  alias Hourglass.Bridge
  alias Hourglass.Proto.ClientConfig
  alias Hourglass.Runtime
  alias Temporal.Api.Workflow.V1.WorkflowExecutionInfo
  alias Temporal.Api.Workflowservice.V1.DescribeWorkflowExecutionResponse

  @typedoc "A workflow run: its workflow id and run id."
  @type run :: {String.t(), String.t()}

  # The statuses of a run that has closed.
  @closed [
    :WORKFLOW_EXECUTION_STATUS_COMPLETED,
    :WORKFLOW_EXECUTION_STATUS_FAILED,
    :WORKFLOW_EXECUTION_STATUS_CANCELED,
    :WORKFLOW_EXECUTION_STATUS_TERMINATED,
    :WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
    :WORKFLOW_EXECUTION_STATUS_TIMED_OUT
  ]

  # How many describes one check has in flight at once
  # (`CONCURRENCY` in native/hourglass/src/closed_runs.rs).
  @concurrency 16

  # The config of a client of `namespace` at `target_url`.
  @spec client_config(String.t(), String.t()) :: binary()
  def client_config(namespace, target_url) do
    Protobuf.encode(%ClientConfig{target_url: target_url, namespace: namespace})
  end

  # Start opening a client from `config`, given up on after `timeout`.
  # Returns the tag its answer will carry, or `:error` if it could not start.
  @spec open(binary(), pos_integer()) :: {:ok, pos_integer()} | :error
  def open(config, timeout) do
    tag = System.unique_integer([:positive])

    case call("closed_runs_open_client", fn ->
           Bridge.closed_runs_open_client(Runtime.handle(), config, timeout, tag)
         end) do
      :ok -> {:ok, tag}
      _failed -> :error
    end
  catch
    :exit, _runtime_gone -> :error
  end

  # Start describing `runs` through `client`, each given up on after
  # `timeout`. Returns the tag the answer will carry, or `:error` if it could
  # not start.
  @spec describe(reference(), [run()], pos_integer()) :: {:ok, pos_integer()} | :error
  def describe(client, runs, timeout) do
    tag = System.unique_integer([:positive])

    case call("closed_runs_describe", fn ->
           Bridge.closed_runs_describe(client, runs, timeout, tag)
         end) do
      :ok -> {:ok, tag}
      _failed -> :error
    end
  end

  # How long the answer to describing `count` runs, each given `timeout`, can
  # take at most: they go `@concurrency` at a time.
  @spec answer_within(non_neg_integer(), pos_integer()) :: pos_integer()
  def answer_within(count, timeout), do: (div(count, @concurrency) + 1) * timeout

  # Read a message the caller was sent: a client opened (or not), or the runs
  # among those described that have closed.
  @spec answer(term()) ::
          {:opened, pos_integer(), {:ok, reference()} | :error}
          | {:described, pos_integer(), [run()]}
          | :unknown
  def answer({:hourglass_client_opened, tag, {:ok, client}}), do: {:opened, tag, {:ok, client}}
  def answer({:hourglass_client_opened, tag, {:error, _unreachable}}), do: {:opened, tag, :error}

  def answer({:hourglass_runs_described, tag, answers}) do
    closed =
      for {workflow_id, run_id, answer} <- answers, closed?(answer), do: {workflow_id, run_id}

    {:described, tag, closed}
  end

  def answer(_other), do: :unknown

  defp call(call_site, fun), do: Bridge.with_nif_reload_rescue("#{call_site}/closed_runs", fun)

  defp closed?({:ok, bytes}) do
    case DescribeWorkflowExecutionResponse.decode(bytes) do
      %{workflow_execution_info: %WorkflowExecutionInfo{status: status}} -> status in @closed
      _unknown -> false
    end
  end

  defp closed?({:error, %Bridge.Error{kind: :tonic_error, detail: detail}}), do: gone?(detail)
  defp closed?({:error, _other}), do: false

  # Temporal no longer knows the run (NotFound), as opposed to not knowing
  # the namespace, which says nothing about the run; Temporal's message for
  # that is `Namespace <name> is not found.`.
  defp gone?(detail) when is_binary(detail) do
    String.contains?(detail, "code: 'Some requested entity was not found'") and
      not String.contains?(detail, ~s(message: "Namespace ))
  end

  defp gone?(_detail), do: false
end
