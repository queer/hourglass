import Config
# Host-overridable runtime config. Reads TEMPORAL_TARGET_URL / TEMPORAL_NAMESPACE
# when present.
if url = System.get_env("TEMPORAL_TARGET_URL") do
  config :hourglass, Hourglass.Client, target_url: url
end

if namespace = System.get_env("TEMPORAL_NAMESPACE") do
  config :hourglass, Hourglass.Client, namespace: namespace
end
