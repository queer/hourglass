defmodule Hourglass.MixProject do
  use Mix.Project

  def project do
    [
      app: :hourglass,
      version: "0.6.0",
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
      #
      # Concurrency is capped at run time by test/test_helper.exs, deliberately
      # NOT by a --max-cases flag here: each :temporal test starts a Worker
      # holding two blocking dirty-IO long-polls, so the safe width depends on
      # the VM's dirty-IO pool and any number hardcoded here would be wrong on a
      # box with a different pool. For full width, give the VM a bigger pool:
      #   ERL_FLAGS="+SDio 128" mix test.integration
      "test.integration": [
        "test --include temporal --include integration"
      ],
      # Publish through this, never `mix hex.publish` directly. A version
      # already on Hex is immutable after an hour — so republishing one is
      # either rejected late, after docs have been built and a tarball
      # uploaded, or, inside that window, SILENTLY REPLACES a release someone
      # may already have resolved and locked. Forgetting the version bump is
      # the ordinary way to arrive there, and it is not a mistake this repo
      # should rely on catching by eye.
      publish: [&refuse_republish/1, "hex.publish"]
    ]
  end

  defp refuse_republish(_args) do
    version = Mix.Project.config()[:version]

    case published_versions() do
      {:ok, versions} ->
        if version in versions do
          Mix.raise("""
          Refusing to publish: hourglass #{version} is already on Hex.

          Published: #{Enum.join(versions, ", ")}

          Bump `version:` in mix.exs (and the install snippet in README.md) \
          before publishing.\
          """)
        end

        Mix.shell().info("[hourglass] #{version} is not on Hex — publishing.")

      {:error, reason} ->
        # Fail closed. A publish that cannot establish what is already
        # published is exactly the one that must not proceed unattended.
        Mix.raise("""
        Refusing to publish: could not read hourglass's published versions \
        from Hex (#{reason}).

        Check the version by hand at https://hex.pm/packages/hourglass and \
        publish with `mix hex.publish` if #{version} is genuinely new.\
        """)
    end
  end

  defp published_versions do
    {:ok, _} = Application.ensure_all_started([:inets, :ssl])
    url = ~c"https://hex.pm/api/packages/hourglass"
    headers = [{~c"user-agent", ~c"hourglass-publish-preflight"}]

    case :httpc.request(:get, {url, headers}, [{:timeout, 15_000}], body_format: :binary) do
      {:ok, {{_, 200, _}, _, body}} ->
        # OTP's own JSON decoder, not Jason: an alias function runs before deps
        # are loaded, so `Jason` is genuinely unavailable here.
        {:ok, body |> :json.decode() |> Map.fetch!("releases") |> Enum.map(& &1["version"])}

      {:ok, {{_, 404, _}, _, _}} ->
        # Never published at all: nothing to collide with.
        {:ok, []}

      {:ok, {{_, status, _}, _, _}} ->
        {:error, "HTTP #{status}"}

      {:error, reason} ->
        {:error, inspect(reason)}
    end
  end
end
