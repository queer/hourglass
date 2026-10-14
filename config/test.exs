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

# The activity calls BridgeHolder makes through the bridge (completions and
# heartbeats) go through a stand-in a test can stall, as a call stuck on the
# network would (test/support/stallable_bridge.ex). It passes every call
# straight through to Hourglass.Bridge unless a test stalls it.
config :hourglass, :activity_bridge, Hourglass.TestSupport.StallableBridge
