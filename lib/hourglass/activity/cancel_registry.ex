defmodule Hourglass.Activity.CancelRegistry do
  @moduledoc """
  Per-`task_token` cancellation flags bridging the Cancel activity task (received by
  `Hourglass.ActivityRunner`) to the running activity's `Hourglass.Activity.heartbeat/0`.

  Owns a single public, `read_concurrency` ETS table named after this module. `cancelled?/1`
  is a lock-free direct read on the heartbeat hot path; `mark/2` and `clear/1` are direct ETS
  writes (no GenServer round-trip). The GenServer exists to own the table's lifetime and to
  periodically sweep leaked entries — a Cancel that arrives AFTER its Start task already
  completed (and `clear`ed) would otherwise leave an entry nobody reads.

  Started app-scope (always-on) by `Hourglass.Application` so `heartbeat/0` can read it in every
  test lane, not only the `:temporal` suite.
  """
  use GenServer

  @table __MODULE__
  @sweep_interval_ms 60_000
  # Generous — larger than any reasonable start_to_close. A real in-flight activity reads its
  # flag and stops within one heartbeat interval, so only post-completion leaks reach the sweep.
  @entry_ttl_ms 600_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @spec mark(binary(), term()) :: :ok
  def mark(task_token, reason) when is_binary(task_token) do
    :ets.insert(@table, {task_token, reason, now_ms()})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @spec cancelled?(binary()) :: term() | nil
  def cancelled?(task_token) when is_binary(task_token) do
    case :ets.lookup(@table, task_token) do
      [{^task_token, reason, _ts}] -> reason
      _no_entry -> nil
    end
  rescue
    ArgumentError -> nil
  end

  @spec clear(binary()) :: :ok
  def clear(task_token) when is_binary(task_token) do
    :ets.delete(@table, task_token)
    :ok
  rescue
    ArgumentError -> :ok
  end

  @impl GenServer
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    schedule_sweep()
    {:ok, %{}}
  end

  @impl GenServer
  def handle_info(:sweep, state) do
    cutoff = now_ms() - @entry_ttl_ms
    :ets.select_delete(@table, [{{:_, :_, :"$1"}, [{:<, :"$1", cutoff}], [true]}])
    schedule_sweep()
    {:noreply, state}
  end

  defp schedule_sweep, do: Process.send_after(self(), :sweep, @sweep_interval_ms)
  defp now_ms, do: System.monotonic_time(:millisecond)
end
