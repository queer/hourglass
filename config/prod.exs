import Config

# Hourglass is a library: production config (target URL, namespace, runtime
# enablement) is supplied by the host application. This file exists so the
# `import_config "#{config_env()}.exs"` in config.exs resolves under :prod.
