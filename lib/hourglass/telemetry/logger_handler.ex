defmodule Hourglass.Telemetry.LoggerHandler do
  @moduledoc """
  Opt-in default handler that logs every `Hourglass.Telemetry` event. Off by
  default; call `attach/0` (e.g. from your application start) to enable.
  """
  require Logger

  @handler_id "hourglass-logger-handler"

  @spec attach() :: :ok | {:error, :already_exists}
  def attach do
    :telemetry.attach_many(@handler_id, Hourglass.Telemetry.events(), &__MODULE__.handle/4, nil)
  end

  @spec detach() :: :ok | {:error, :not_found}
  def detach, do: :telemetry.detach(@handler_id)

  @doc false
  @spec handle([atom()], map(), map(), term()) :: :ok
  def handle(event, measurements, metadata, _config) do
    Logger.info("#{Enum.join(event, ".")} #{inspect(measurements)} #{inspect(metadata)}")
  end
end
