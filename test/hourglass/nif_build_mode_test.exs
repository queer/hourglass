defmodule Hourglass.NifBuildModeTest do
  # The NIF is built in release mode in every Mix environment, so the NIF the
  # suite tests is the one that ships (README, "NIF build mode"). Nothing may
  # ask for a debug build, including settings Rustler would ignore.
  use ExUnit.Case, async: true

  test "mix.exs carries no crate build settings of its own" do
    # Rustler 0.38 ignores `rustler_crates`; a mode set there only misleads.
    refute Keyword.has_key?(Mix.Project.config(), :rustler_crates)
  end

  test "no application config asks Rustler for a debug build" do
    config = Application.get_env(:hourglass, Hourglass.Bridge, [])
    assert Keyword.get(config, :mode, :release) == :release
  end

  test "the NIF module asks for release mode" do
    source = File.read!("lib/hourglass/bridge.ex")
    assert source =~ ~r/use Rustler,.*mode: :release/s
  end
end
