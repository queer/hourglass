defmodule Hourglass.Test.UseRealTemporalBackend do
  @moduledoc """
  Opts a test module into the production `Hourglass.Client.Real`
  backend, through `Hourglass.TestSupport.PatientBackend`, which makes a
  call again while the shared Temporal times it out. The default test
  config sets the backend to a Mox mock; tests that need to round-trip a
  real Temporal cluster (`:temporal`-tagged) swap per-process here.

  ## Use

      use ExUnit.Case, async: true
      use Hourglass.Test.UseRealTemporalBackend

      @moduletag :temporal

      test "..." do
        {:ok, _handle} = Hourglass.start(...)
      end

  The swap is per-process via `Hourglass.Client.Backend.set_impl/1`, so it
  does not race concurrent tests that use the Mock.
  """

  defmacro __using__(_opts) do
    quote do
      setup do
        Hourglass.Client.Backend.set_impl(Hourglass.TestSupport.PatientBackend)
        :ok
      end
    end
  end
end
