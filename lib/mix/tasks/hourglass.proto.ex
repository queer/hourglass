defmodule Mix.Tasks.Hourglass.Proto do
  @shortdoc "Regenerate committed protobuf modules"
  @moduledoc """
  Regenerate committed protobuf modules from `proto/`.

  Outputs land in `lib/proto/` and ARE committed, so normal builds need no
  protoc. Run this only after editing a `.proto`.

  Requires protoc on PATH and the Elixir plugin:
      mix escript.install hex protobuf
      export PATH="$HOME/.mix/escripts:$PATH"
  """
  use Mix.Task

  @out "lib/proto"

  @impl Mix.Task
  def run(_args) do
    root = File.cwd!()
    out = Path.join(root, @out)
    File.mkdir_p!(out)

    ensure_tool!("protoc")
    ensure_tool!("protoc-gen-elixir")

    # Pass 1: hourglass config protos (package hourglass.proto) -> Hourglass.Proto.*
    hourglass_protos =
      root
      |> Path.join("proto/hourglass/*.proto")
      |> Path.wildcard()

    cmd!("protoc", [
      "--elixir_out=#{out}",
      "-I=#{Path.join(root, "proto")}"
      | hourglass_protos
    ])

    # Pass 2: temporal sdk-core protos -> Coresdk.* (api tree is include-only)
    core_dir = Path.join(root, "proto/temporal/temporal/sdk/core")

    core_protos =
      core_dir
      |> Path.join("**/*.proto")
      |> Path.wildcard()

    cmd!("protoc", [
      "--elixir_out=#{out}",
      "-I=#{Path.join(root, "proto/temporal")}"
      | core_protos
    ])

    Mix.shell().info("Generated protobuf modules under #{@out}")
  end

  defp ensure_tool!(tool) do
    if System.find_executable(tool) == nil do
      Mix.raise("#{tool} not found on PATH. See @moduledoc for install steps.")
    end
  end

  defp cmd!(bin, args) do
    # Dev-only protoc codegen task; there is no sensitive env to scrub here.
    # credo:disable-for-next-line Credo.Check.Warning.LeakyEnvironment
    {out, status} = System.cmd(bin, args, stderr_to_stdout: true)
    if status != 0, do: Mix.raise("#{bin} failed (#{status}):\n#{out}")
  end
end
