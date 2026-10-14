defmodule Hourglass.ContinueAsNewIntegrationTest do
  @moduledoc """
  A workflow that continues as new causes no sticky-cache miss — against a
  real Temporal cluster.

  Core evicts every run it sees end, a continued-as-new one included. The
  worker has already dropped that run's state when the run continued, so the
  eviction finds nothing cached. It is acked as an eviction, not taken for an
  incremental activation the worker cannot resolve, logged as a cache miss and
  failed.
  """

  use ExUnit.Case, async: true
  use Hourglass.Test.UseRealTemporalBackend

  import ExUnit.CaptureLog

  alias Hourglass.WorkflowHandle

  require Logger

  @moduletag :temporal
  @moduletag :integration
  @moduletag timeout: 60_000

  defmodule Generations do
    @moduledoc false
    use Hourglass.Workflow, input: :map, output: :map

    @impl Hourglass.Workflow.Behaviour
    def run(%{"gen" => gen, "last" => last}) when gen < last do
      # A timer gives each run more than one activation before it continues.
      sleep(10)
      continue_as_new(%{"gen" => gen + 1, "last" => last})
    end

    def run(%{"gen" => gen}), do: %{"finished_gen" => gen}
  end

  test "runs that continue as new are evicted without a sticky-cache miss" do
    queue = "can-evict-#{System.unique_integer([:positive])}"
    id = "can-evict-#{UUIDv7.generate()}"

    log =
      capture_log([level: :debug], fn ->
        {:ok, _worker} = start_supervised({Hourglass.Worker, task_queue: queue})

        {:ok, handle} =
          Hourglass.start(Generations, %{"gen" => 1, "last" => 3}, id: id, task_queue: queue)

        assert {:ok, %{"finished_gen" => 3}} =
                 Hourglass.result(%WorkflowHandle{id: handle.id}, timeout: 30_000)

        # A graceful stop hands out, and waits for the completion of, every
        # activation Core still holds for the queue, the runs' evictions
        # included, so every line they would log is in by the time it returns.
        :ok = stop_supervised(Hourglass.Worker)

        # A miss is logged at info: prove such a line is captured here, so
        # the absence below cannot pass for want of capturing it.
        Logger.info("info probe for #{queue}")
      end)

    assert log =~ "info probe for #{queue}"

    misses =
      log
      |> String.split("\n")
      |> Enum.filter(&(&1 =~ "sticky-cache" and &1 =~ queue))

    assert misses == []
  end
end
