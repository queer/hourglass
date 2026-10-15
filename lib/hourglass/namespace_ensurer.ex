defmodule Hourglass.NamespaceEnsurer do
  @moduledoc """
  Boot-time supervisor child that ensures the configured Temporal
  namespace exists.

  `start_link/0` makes the namespace exist and returns `:ignore`, so no
  process is kept around after the check, and children started after it
  find the namespace there. It waits on that first attempt, so a silent
  Temporal holds the boot for as long as Core waits on a call (30
  seconds) before the attempt fails.

  If Temporal cannot be reached, it does not fail the boot: Temporal
  being down while the host starts (a deploy, a reboot) is no reason for
  the host to stay down. Its process, registered under this module's
  name, stays, and tries again, logging each failed attempt, after a
  second, then twice as long each time up to thirty seconds; it exits
  once the namespace exists. For as long as the process runs (its first
  attempt included), `ensuring?/1` is true for the namespace, and a
  `Hourglass.Worker` of that namespace that finds Temporal does not know
  it yet keeps trying to register, rather than failing.

  Any other failure — a namespace Temporal refuses to register, say —
  fails `start_link/0`, so the boot fails and operators see the real
  error.

  Reads `:target_url` and `:namespace` from
  `Application.get_env(:hourglass, Hourglass.Client)`, falling back to
  the same defaults as `Hourglass.Client.connect/1`.
  """

  use GenServer

  alias Hourglass.Client
  alias Hourglass.Client.Backend
  alias Hourglass.Error

  require Logger

  @first_retry 1_000
  @max_retry 30_000

  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, []},
      type: :worker,
      restart: :transient
    }
  end

  @spec start_link() :: :ignore | GenServer.on_start()
  def start_link do
    GenServer.start_link(__MODULE__, Client.default_namespace(), name: __MODULE__)
  end

  @doc """
  Whether `namespace`, the configured one, is still being made to exist:
  the ensurer's first attempt is under way, or Temporal could not be
  reached for it.
  """
  @spec ensuring?(String.t()) :: boolean()
  def ensuring?(namespace),
    do: Process.whereis(__MODULE__) != nil and namespace == Client.default_namespace()

  @impl GenServer
  def init(namespace) do
    case Backend.impl().ensure_namespace(namespace) do
      :ok ->
        :ignore

      {:error, %Error{reason: :unreachable} = error} ->
        {:ok, try_again(%{namespace: namespace, failed_attempts: 0}, error)}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_info(:ensure, state) do
    case Backend.impl().ensure_namespace(state.namespace) do
      :ok ->
        Logger.info(
          "namespace #{state.namespace} ensured after #{state.failed_attempts} failed attempts"
        )

        {:stop, :normal, state}

      {:error, %Error{reason: :unreachable} = error} ->
        {:noreply, try_again(state, error)}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  defp try_again(state, error) do
    attempt = state.failed_attempts + 1
    retry_in = min(@first_retry * 2 ** (attempt - 1), @max_retry)

    Logger.warning(
      "could not reach Temporal to ensure namespace #{state.namespace} (attempt #{attempt}): " <>
        "#{inspect(error.detail)}; trying again in #{retry_in} ms"
    )

    Process.send_after(self(), :ensure, retry_in)
    %{state | failed_attempts: attempt}
  end
end
