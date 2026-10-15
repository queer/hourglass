defmodule Hourglass.WorkerBootIntegrationTest do
  @moduledoc """
  A host whose Temporal is down or silent when it boots still starts: its
  `Hourglass.NamespaceEnsurer` and `Hourglass.Worker` keep trying, say so on
  each attempt, and the worker serves once Temporal answers. A worker given
  a configuration Temporal refuses (a namespace it does not know) fails to
  start instead, as before.

  `async: false`: the host's namespace ensurer reads its target from
  application config, which the boot test points at a Temporal that is not
  there.
  """

  use ExUnit.Case, async: false
  use Hourglass.Test.UseRealTemporalBackend

  import ExUnit.CaptureLog

  alias Hourglass.NamespaceEnsurer
  alias Hourglass.TestSupport.EchoWorkflow
  alias Hourglass.TestSupport.FreezableProxy
  alias Hourglass.TestSupport.Patience
  alias Hourglass.TestSupport.WorkerProbes

  @moduletag :temporal
  @moduletag :integration
  @moduletag timeout: Patience.test_timeout(3)

  # Nothing listens here: a connection to it is refused at once.
  @nowhere "http://127.0.0.1:1"

  setup do
    # The namespace ensurer runs in its supervisor's process, so the backend
    # it uses is the configured one.
    backend = Application.fetch_env!(:hourglass, :client_backend)
    Application.put_env(:hourglass, :client_backend, Hourglass.Client.Real)
    on_exit(fn -> Application.put_env(:hourglass, :client_backend, backend) end)

    queue = "boot-#{System.unique_integer([:positive])}"
    attach_registration_failures(queue)
    %{queue: queue}
  end

  test "a host whose Temporal does not answer at boot starts, says so, and serves once it answers",
       %{queue: queue} do
    proxy = start_supervised!(FreezableProxy)
    :ok = FreezableProxy.freeze(proxy)
    point_client_config_at(@nowhere)

    # A host's tree, as apathy's: its namespace made to exist, then its worker.
    host = [
      NamespaceEnsurer,
      {Hourglass.Worker, task_queue: queue, target_url: FreezableProxy.url(proxy)}
    ]

    {started, log} =
      with_log(fn ->
        start_supervised(%{
          id: :host,
          start: {Supervisor, :start_link, [host, [strategy: :one_for_one]]},
          type: :supervisor
        })
      end)

    assert {:ok, _host} = started
    assert log =~ "could not reach Temporal to ensure namespace"
    assert log =~ "worker for task_queue=#{queue} could not register with Temporal (attempt 1"
    assert_received {:registration_failed, %{attempt: 1}, %{failure_class: :unreachable}}
    assert Hourglass.Worker.inner_supervisor(queue) == nil
    assert NamespaceEnsurer.ensuring?(Hourglass.Client.default_namespace())

    # Temporal answers again.
    restore_client_config()
    :ok = FreezableProxy.thaw(proxy)

    WorkerProbes.poll_until("the worker to register and poll", fn ->
      Hourglass.Worker.inner_supervisor(queue) != nil
    end)

    WorkerProbes.poll_until("the namespace ensurer to finish", fn ->
      not NamespaceEnsurer.ensuring?(Hourglass.Client.default_namespace())
    end)

    id = "boot-#{UUIDv7.generate()}"

    {:ok, handle} =
      Patience.start(
        fn -> Hourglass.start(EchoWorkflow, %{"nonce" => id}, id: id, task_queue: queue) end,
        id
      )

    assert {:ok, %{"echoed" => %{"nonce" => ^id}}} =
             Hourglass.result(handle, timeout: Patience.ms())
  end

  test "a worker that cannot reach Temporal reports each attempt, and keeps trying", %{
    queue: queue
  } do
    {worker, log} =
      with_log(fn ->
        pid = start_supervised!({Hourglass.Worker, task_queue: queue, target_url: @nowhere})

        for attempt <- 1..3 do
          assert_receive {:registration_failed, %{attempt: ^attempt, retry_in: retry_in},
                          %{failure_class: :unreachable, task_queue: ^queue}},
                         10_000

          assert retry_in == 1_000 * 2 ** (attempt - 1)
        end

        pid
      end)

    for attempt <- 1..3, do: assert(log =~ "(attempt #{attempt}, unreachable)")
    assert Hourglass.Worker.inner_supervisor(queue) == nil
    monitor = Process.monitor(worker)
    assert :ok = stop_supervised(Hourglass.Worker)
    assert_received {:DOWN, ^monitor, :process, ^worker, _reason}
  end

  test "a worker given a namespace Temporal does not know fails to start", %{queue: queue} do
    namespace = "no-such-namespace-#{System.unique_integer([:positive])}"

    assert {:error, reason} =
             Patience.start_worker(task_queue: queue, namespace: namespace)

    assert inspect(reason) =~ "namespace_not_found"
    refute_received {:registration_failed, _measurements, _metadata}
    refute Hourglass.BridgeHolder.registered?(queue)
  end

  test "a namespace ensurer that cannot reach Temporal does not fail the boot, and finishes once it can" do
    point_client_config_at(@nowhere)

    {started, log} = with_log(fn -> start_supervised(NamespaceEnsurer) end)

    assert {:ok, ensurer} = started
    assert log =~ "could not reach Temporal to ensure namespace"
    assert NamespaceEnsurer.ensuring?(Hourglass.Client.default_namespace())
    refute NamespaceEnsurer.ensuring?("another-namespace")

    monitor = Process.monitor(ensurer)
    restore_client_config()
    assert_receive {:DOWN, ^monitor, :process, ^ensurer, :normal}, Patience.ms()
  end

  # Point `Hourglass.Client`'s configured target at `url` until the test ends,
  # or `restore_client_config/0`.
  defp point_client_config_at(url) do
    original = Application.get_env(:hourglass, Hourglass.Client, [])
    Process.put({__MODULE__, :client_config}, original)
    Application.put_env(:hourglass, Hourglass.Client, Keyword.put(original, :target_url, url))
    on_exit(fn -> Application.put_env(:hourglass, Hourglass.Client, original) end)
  end

  defp restore_client_config do
    Application.put_env(:hourglass, Hourglass.Client, Process.get({__MODULE__, :client_config}))
  end

  defp attach_registration_failures(queue) do
    handler = {__MODULE__, queue}

    :telemetry.attach(
      handler,
      [:hourglass, :worker, :registration_failed],
      &__MODULE__.forward_failure/4,
      %{test: self(), queue: queue}
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  @doc false
  def forward_failure(_event, measurements, %{task_queue: queue} = metadata, %{
        test: test,
        queue: queue
      }),
      do: send(test, {:registration_failed, measurements, metadata})

  def forward_failure(_event, _measurements, _metadata, _config), do: :ok
end
