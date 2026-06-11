defmodule Hourglass.Telemetry.LoggerHandlerTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  alias Hourglass.Telemetry.LoggerHandler

  setup do
    :ok = LoggerHandler.attach()
    on_exit(&LoggerHandler.detach/0)
  end

  test "logs an emitted Hourglass event" do
    log =
      capture_log(fn ->
        :telemetry.execute([:hourglass, :connection, :failed], %{count: 1}, %{
          failure_class: :nif_reload,
          call_site: "x",
          detail: "y"
        })
      end)

    assert log =~ "hourglass.connection.failed"
    assert log =~ "nif_reload"
  end
end
