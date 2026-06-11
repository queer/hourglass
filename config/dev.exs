import Config
# Enable the runtime and default worker for interactive development.
config :hourglass, :start_runtime, true
config :hourglass, :start_default_worker, true
config :hourglass, Hourglass.Client, namespace: "hourglass-dev"
