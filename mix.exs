defmodule Hourglass.MixProject do
  use Mix.Project

  def project do
    [
      app: :hourglass,
      version: "0.1.1",
      elixir: "~> 1.15",
      name: "hourglass",
      description: description(),
      source_url: "https://github.com/queer/hourglass",
      package: package(),
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps(),
      # `:mix` (Mix.Tasks.Hourglass.Proto) and `:credo` (the packaged
      # Hourglass.Check.WorkflowDeterminism check) are dev/build-time-only
      # apps not in the runtime PLT; add them so dialyzer sees their modules.
      dialyzer: [plt_add_apps: [:mix, :credo]],
      rustler_crates: [
        hourglass: [
          path: "native/hourglass",
          mode: if(Mix.env() == :prod, do: :release, else: :debug)
        ]
      ]
    ]
  end

  def application do
    [
      mod: {Hourglass.Application, []},
      extra_applications: [:logger]
    ]
  end

  def cli do
    # `mix test.integration` is a test command, not a dev one.
    [preferred_envs: ["test.integration": :test]]
  end

  defp description do
    "A standalone Elixir SDK for Temporal: workflow and activity definitions, " <>
      "a worker that polls a Temporal cluster, a client, a deterministic " <>
      "replayer, and a Rust NIF bridge over temporalio-sdk-core."
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/queer/hourglass"},
      files:
        ~w(lib priv mix.exs .formatter.exs README.md LICENSE) ++
          ~w(native/hourglass/src native/hourglass/Cargo.toml
             native/hourglass/Cargo.lock native/hourglass/.cargo)
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:rustler, "~> 0.36", runtime: false},
      {:temporalio, "~> 1.62"},
      {:protobuf, "~> 0.14"},
      {:jason, "~> 1.2"},
      {:ecto, "~> 3.13"},
      {:polymorphic_embed, "~> 5.0"},
      {:uuidv7, "~> 1.0"},
      {:telemetry, "~> 1.0"},
      {:mox, "~> 1.1", only: :test},
      {:stream_data, "~> 1.1", only: :test},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      proto: ["hourglass.proto"],
      # Cluster integration tests (tagged :temporal + :integration, excluded
      # from the default lane). Bring the cluster up first (see compose.yaml):
      #   podman compose up -d  &&  mix test.integration
      "test.integration": [
        "test --include temporal --include integration"
      ]
    ]
  end
end
