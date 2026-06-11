[
  import_deps: [:ecto, :polymorphic_embed],
  inputs:
    (Path.wildcard("{mix,.formatter}.exs") ++
       Path.wildcard("config/**/*.{ex,exs}") ++
       Path.wildcard("lib/**/*.{ex,exs}") ++
       Path.wildcard("test/**/*.{ex,exs}")) --
      Path.wildcard("lib/proto/**/*.{ex,exs}")
]
