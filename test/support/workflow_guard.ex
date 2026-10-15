defmodule Hourglass.TestSupport.WorkflowGuard do
  @moduledoc """
  Fails the run if it leaves any workflow running on the Temporal it ran
  against: a run nothing serves or ends adds, for good, to the load of a
  server every suite shares.

  The guard learns of every workflow run the suite starts, in a trace session
  of its own, in every process: each start a client makes
  (`Hourglass.Bridge.client_start_workflow/2`, whatever calls it), and each
  run a worker of the suite initializes (its first activation, from
  `Hourglass.Bridge.worker_poll_workflow_activation/1`), which covers
  children and continued runs. After the suite, `verify/0` asks Temporal
  about each: any still running fails the run, naming it, and is terminated,
  so nothing is left behind either way. A test that leaves a run going ends
  it itself (`Hourglass.terminate/2`).
  """

  use GenServer

  alias Coresdk.WorkflowActivation.WorkflowActivation
  alias Hourglass.Bridge
  alias Hourglass.Client
  alias Hourglass.Client.Real
  alias Hourglass.TestSupport.Patience
  alias Hourglass.WorkflowHandle
  alias Temporal.Api.Workflowservice.V1.DescribeWorkflowExecutionResponse
  alias Temporal.Api.Workflowservice.V1.StartWorkflowExecutionRequest
  alias Temporal.Api.Workflowservice.V1.StartWorkflowExecutionResponse

  @starts {Bridge, :client_start_workflow, 2}
  @activations {Bridge, :worker_poll_workflow_activation, 1}

  @doc "Start the guard, unlinked: it lives for the whole run."
  @spec start() :: GenServer.on_start()
  def start, do: GenServer.start(__MODULE__, [], name: __MODULE__)

  @doc """
  The runs the suite started that are still running, as
  `{namespace, workflow_id, run_id}`, each terminated.
  """
  @spec running() :: [{String.t(), String.t(), String.t()}]
  def running do
    __MODULE__
    |> GenServer.call(:runs, :infinity)
    |> Enum.filter(&running?/1)
    |> Enum.map(fn run ->
      _terminated = terminate(run)
      run
    end)
  end

  @doc "After the suite: if a run is still running, say which, and make the run exit non-zero."
  @spec verify() :: :ok
  def verify do
    case running() do
      [] ->
        :ok

      running ->
        listed =
          Enum.map_join(running, "\n", fn {namespace, workflow_id, run_id} ->
            "  * #{workflow_id} (run #{run_id}, namespace #{namespace})"
          end)

        Mix.shell().error("""

        [hourglass] FAILED: the suite left workflows running on Temporal. Each has now \
        been terminated; the test that started it should end it itself.

        #{listed}
        """)

        System.at_exit(fn _status -> exit({:shutdown, 1}) end)
        :ok
    end
  end

  @impl GenServer
  def init(_opts) do
    Code.ensure_loaded!(Bridge)
    session = :trace.session_create(__MODULE__, self(), [])
    _processes = :trace.process(session, :all, true, [:call])
    returning = [{:_, [], [{:return_trace}]}]
    1 = :trace.function(session, @starts, returning, [:global])
    1 = :trace.function(session, @activations, returning, [:global])
    {:ok, %{session: session, starting: %{}, runs: MapSet.new()}}
  end

  @impl GenServer
  def handle_call(:runs, _from, state), do: {:reply, MapSet.to_list(state.runs), state}

  @impl GenServer
  def handle_info(
        {:trace, caller, :call, {Bridge, :client_start_workflow, [_client, bytes]}},
        state
      ) do
    %StartWorkflowExecutionRequest{namespace: namespace, workflow_id: workflow_id} =
      StartWorkflowExecutionRequest.decode(bytes)

    {:noreply, %{state | starting: Map.put(state.starting, caller, {namespace, workflow_id})}}
  end

  def handle_info(
        {:trace, caller, :return_from, {Bridge, :client_start_workflow, 2}, result},
        state
      ) do
    {start, starting} = Map.pop(state.starting, caller)
    state = %{state | starting: starting}

    case {start, result} do
      {{namespace, workflow_id}, {:ok, bytes}} ->
        run_id = StartWorkflowExecutionResponse.decode(bytes).run_id
        {:noreply, %{state | runs: MapSet.put(state.runs, {namespace, workflow_id, run_id})}}

      _not_started ->
        {:noreply, state}
    end
  end

  def handle_info(
        {:trace, _caller, :return_from, {Bridge, :worker_poll_workflow_activation, 1},
         {:ok, bytes}},
        state
      ) do
    activation = WorkflowActivation.decode(bytes)

    runs =
      for %{variant: {:initialize_workflow, init}} <- activation.jobs,
          into: state.runs,
          do: {Client.default_namespace(), init.workflow_id, activation.run_id}

    {:noreply, %{state | runs: runs}}
  end

  def handle_info(_trace, state), do: {:noreply, state}

  defp running?({namespace, workflow_id, run_id}) do
    case Patience.retry(fn -> Client.connect(namespace: namespace) end) do
      {:ok, client} -> running_answer?(describe(client, workflow_id, run_id))
      {:error, _unreachable} -> true
    end
  end

  defp describe(client, workflow_id, run_id) do
    Patience.retry(fn ->
      Bridge.client_describe_workflow_execution(client, workflow_id, run_id)
    end)
  end

  defp running_answer?({:ok, bytes}) do
    DescribeWorkflowExecutionResponse.decode(bytes).workflow_execution_info.status ==
      :WORKFLOW_EXECUTION_STATUS_RUNNING
  end

  # Unknown to Temporal, or its namespace gone: not running. Temporal not
  # answering: the run counts as left running.
  defp running_answer?({:error, %Bridge.Error{kind: :namespace_not_found}}), do: false

  defp running_answer?({:error, %Bridge.Error{detail: detail}}),
    do: not String.contains?(detail, "not found")

  defp terminate({namespace, workflow_id, run_id}) do
    with {:ok, client} <- Patience.retry(fn -> Client.connect(namespace: namespace) end) do
      terminate(client, namespace, workflow_id, run_id)
    end
  end

  defp terminate(client, namespace, workflow_id, run_id) do
    request =
      Real.build_terminate_request(
        namespace,
        %WorkflowHandle{id: workflow_id, run_id: run_id},
        "left running by the suite"
      )

    Patience.retry(fn -> Bridge.client_terminate_workflow(client, Protobuf.encode(request)) end)
  end
end
