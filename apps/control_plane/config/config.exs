import Config

config :telemetry_fabric_control,
  ecto_repos: [TelemetryFabricControl.Repo]

config :telemetry_fabric_control, TelemetryFabricControl.Repo,
  pool_size: String.to_integer(System.get_env("TELEMETRY_FABRIC_CONTROL_DB_POOL_SIZE") || "10"),
  stacktrace: config_env() == :dev,
  show_sensitive_data_on_connection_error: config_env() == :dev
