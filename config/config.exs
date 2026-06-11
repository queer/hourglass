import Config

# Cluster-facing client backend. Production = Real (Bridge NIFs); tests
# override to a Mox mock in config/test.exs. See Hourglass.Client.Backend.
config :hourglass, :client_backend, Hourglass.Client.Real

# Default worker concurrency. 0 means "use the Rust sdk-core default".
config :hourglass, Hourglass.Worker,
  max_outstanding_workflow_tasks: 100,
  max_outstanding_activities: 100,
  max_outstanding_local_activities: 100

# Workflow/activity modules the default worker hosts. Hosts append theirs.
config :hourglass, :workflows, []
config :hourglass, :activities, []

# Retry classifier behaviour module (Hourglass.Activity.RetryClassifier).
config :hourglass, :retry_classifier, Hourglass.Activity.RetryClassifier.Default

# Start the Temporal runtime subsystem under Hourglass.Application at boot.
# Hosts that embed Hourglass set this true; default false so the library is
# inert until asked. Tests opt in per-test.
config :hourglass, :start_runtime, false

# Auto-start the "default" task-queue worker once the runtime is up.
config :hourglass, :start_default_worker, false

import_config "#{config_env()}.exs"
