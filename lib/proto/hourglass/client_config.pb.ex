defmodule Hourglass.Proto.ClientConfig do
  @moduledoc false

  use Protobuf,
    full_name: "hourglass.proto.ClientConfig",
    protoc_gen_elixir_version: "0.16.0",
    syntax: :proto3

  field :target_url, 1, type: :string, json_name: "targetUrl"
  field :namespace, 2, type: :string
end
