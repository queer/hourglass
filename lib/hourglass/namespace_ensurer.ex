defmodule Hourglass.NamespaceEnsurer do
  @moduledoc """
  Boot-time supervisor child that ensures the configured Temporal
  namespace exists. Returns `:ignore` from `start_link/0` so no process
  is kept around after the check; the supervisor's child-startup
  ordering guarantees later children (notably `Hourglass.Subsystem`,
  which hosts Workers) don't start until this succeeds.

  Crashes the boot supervisor on connect failure or namespace-register
  failure. Operators see the real error instead of silently-broken
  workers spinning on `:worker_not_registered`.

  Reads `:target_url` and `:namespace` from
  `Application.get_env(:hourglass, Hourglass.Client)`, falling back to
  the same defaults as `Hourglass.Client.connect/1`.
  """

  alias Hourglass.Client
  alias Hourglass.Client.Backend

  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, []},
      type: :worker,
      restart: :transient
    }
  end

  @spec start_link() :: :ignore | {:error, term()}
  def start_link do
    namespace = Client.default_namespace()

    with :ok <- Backend.impl().ensure_namespace(namespace) do
      :ignore
    end
  end
end
