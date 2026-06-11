import Config

# Tests use the `hourglass-test` namespace so runs don't pollute dev history.
config :hourglass, Hourglass.Client, namespace: "hourglass-test"

# Default suite uses the Mox mock; cluster tests opt back into Real per-process.
config :hourglass, :client_backend, Hourglass.Client.Mock

# Runtime off by default in test; temporal-tagged tests start what they need.
config :hourglass, :start_runtime, false
config :hourglass, :start_default_worker, false

# Surface missing classifier clauses at the source under test.
config :hourglass, :strict_unclassified_activity_failures, true
