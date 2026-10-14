defmodule Hourglass.WorkerGracefulShutdownPeriodTest do
  @moduledoc """
  Pins how a Worker resolves its graceful shutdown period, that the same
  value reaches Core on the proto wire, and that the Worker's child spec
  gives its supervisor time to wait for the whole graceful stop.

  `async: false`: one test sets `config :hourglass, Hourglass.Worker`.
  """

  use ExUnit.Case, async: false

  alias Hourglass.BridgeHolder
  alias Hourglass.Proto.WorkerConfig
  alias Hourglass.Worker

  setup do
    previous = Application.get_env(:hourglass, Worker)
    on_exit(fn -> restore_config(previous) end)
  end

  test "the option wins over config, and config over the default" do
    assert Worker.graceful_shutdown_period([]) == BridgeHolder.default_graceful_shutdown_period()

    Application.put_env(
      :hourglass,
      Worker,
      Keyword.put(Application.get_env(:hourglass, Worker, []), :graceful_shutdown_period, 7_000)
    )

    assert Worker.graceful_shutdown_period([]) == 7_000
    assert Worker.graceful_shutdown_period(graceful_shutdown_period: 0) == 0
  end

  test "anything but a non-negative 32-bit integer of milliseconds is refused" do
    for bad <- [-1, 1.5, :infinity, "20000", 4_294_967_296] do
      assert_raise ArgumentError, ~r/graceful_shutdown_period/, fn ->
        Worker.graceful_shutdown_period(graceful_shutdown_period: bad)
      end
    end
  end

  test "the child spec's shutdown covers the grace and the drain's margins" do
    spec = Worker.child_spec(task_queue: "q", graceful_shutdown_period: 30_000)
    assert spec.shutdown == 45_000

    default = Worker.child_spec(task_queue: "q")
    assert default.shutdown == BridgeHolder.default_graceful_shutdown_period() + 15_000
  end

  test "the grace reaches Core as graceful_shutdown_period_ms, defaulted when unset" do
    explicit = wire_config(graceful_shutdown_period: 0)
    assert explicit.graceful_shutdown_period_ms == 0

    defaulted = wire_config([])

    assert defaulted.graceful_shutdown_period_ms ==
             BridgeHolder.default_graceful_shutdown_period()
  end

  defp wire_config(opts) do
    "q"
    |> BridgeHolder.build_worker_config(opts)
    |> WorkerConfig.decode()
  end

  defp restore_config(nil), do: Application.delete_env(:hourglass, Worker)
  defp restore_config(previous), do: Application.put_env(:hourglass, Worker, previous)
end
