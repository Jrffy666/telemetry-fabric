defmodule TelemetryFabricControl.Application do
  @moduledoc """
  OTP entry point for the Telemetry Fabric control plane.
  """

  use Application

  @impl true
  def start(_type, _args) do
    storage_dir = System.get_env("TELEMETRY_FABRIC_CONTROL_DATA_DIR")
    ensure_postgres_primary_config!()

    children =
      [
        {Registry, keys: :unique, name: TelemetryFabricControl.AgentProcessRegistry},
        TelemetryFabricControl.HttpMetrics,
        {TelemetryFabricControl.AuditLog,
         storage_path: TelemetryFabricControl.StateFile.path(storage_dir, "audit_log.term")},
        {TelemetryFabricControl.AgentRegistry,
         storage_path: TelemetryFabricControl.StateFile.path(storage_dir, "agent_registry.term")},
        {TelemetryFabricControl.PipelineStore,
         storage_path: TelemetryFabricControl.StateFile.path(storage_dir, "pipeline_store.term")},
        {TelemetryFabricControl.Modules.Store,
         storage_path: TelemetryFabricControl.StateFile.path(storage_dir, "module_store.term")},
        {TelemetryFabricControl.Modules.Blockchain.Store,
         storage_path: TelemetryFabricControl.StateFile.path(storage_dir, "blockchain_store.term")},
        {TelemetryFabricControl.CommandQueue,
         storage_path: TelemetryFabricControl.StateFile.path(storage_dir, "command_queue.term")}
      ]
      |> maybe_add_repo()
      |> maybe_add_postgres_sync()
      |> maybe_add_http_server()

    Supervisor.start_link(children,
      strategy: :one_for_one,
      name: TelemetryFabricControl.Supervisor
    )
  end

  defp maybe_add_repo(children) do
    if postgres_configured?() or postgres_primary?() do
      [TelemetryFabricControl.Repo | children]
    else
      children
    end
  end

  defp maybe_add_postgres_sync(children) do
    if postgres_configured?() and postgres_sync_enabled?() and not postgres_primary?() do
      children ++
        [
          {TelemetryFabricControl.PostgresSync, interval_ms: postgres_sync_interval_ms()}
        ]
    else
      children
    end
  end

  defp maybe_add_http_server(children) do
    case System.get_env("TELEMETRY_FABRIC_CONTROL_HTTP_LISTEN") do
      nil ->
        children

      listen ->
        {host, port} = parse_listen!(listen)

        children ++
          [
            {TelemetryFabricControl.HttpControlServer,
             [
               host: host,
               port: port,
               agent_token: System.get_env("TELEMETRY_FABRIC_CONTROL_AGENT_TOKEN"),
               operator_token: System.get_env("TELEMETRY_FABRIC_CONTROL_OPERATOR_TOKEN"),
               max_body_bytes: control_http_max_body_bytes(),
               tls_enabled: truthy_env?("TELEMETRY_FABRIC_CONTROL_TLS_ENABLED"),
               tls_cert_file: System.get_env("TELEMETRY_FABRIC_CONTROL_TLS_CERT_FILE"),
               tls_key_file: System.get_env("TELEMETRY_FABRIC_CONTROL_TLS_KEY_FILE"),
               tls_ca_file: System.get_env("TELEMETRY_FABRIC_CONTROL_TLS_CA_FILE"),
               tls_require_client_auth:
                 truthy_env?("TELEMETRY_FABRIC_CONTROL_TLS_REQUIRE_CLIENT_AUTH")
             ]}
          ]
    end
  end

  defp parse_listen!(listen) do
    case String.split(listen, ":", parts: 2) do
      [host, port] -> {host, String.to_integer(port)}
      _ -> raise ArgumentError, "invalid TELEMETRY_FABRIC_CONTROL_HTTP_LISTEN=#{inspect(listen)}"
    end
  end

  defp postgres_configured? do
    case System.get_env("TELEMETRY_FABRIC_CONTROL_DATABASE_URL") do
      nil -> false
      "" -> false
      _url -> true
    end
  end

  defp ensure_postgres_primary_config! do
    if postgres_primary?() and not postgres_configured?() do
      raise "set TELEMETRY_FABRIC_CONTROL_DATABASE_URL when TELEMETRY_FABRIC_CONTROL_STORAGE=postgres"
    end
  end

  defp postgres_primary? do
    case System.get_env("TELEMETRY_FABRIC_CONTROL_STORAGE") do
      "postgres" -> true
      _ -> truthy_env?("TELEMETRY_FABRIC_CONTROL_POSTGRES_PRIMARY")
    end
  end

  defp control_http_max_body_bytes do
    case System.get_env("TELEMETRY_FABRIC_CONTROL_HTTP_MAX_BODY_BYTES") do
      nil -> 1_048_576
      value -> String.to_integer(value)
    end
  end

  defp truthy_env?(name) do
    case System.get_env(name) do
      nil -> false
      value -> String.downcase(value) in ["1", "true", "on", "yes"]
    end
  end

  defp postgres_sync_enabled? do
    case System.get_env("TELEMETRY_FABRIC_CONTROL_POSTGRES_SYNC") do
      nil -> true
      value -> String.downcase(value) not in ["0", "false", "off", "no"]
    end
  end

  defp postgres_sync_interval_ms do
    case System.get_env("TELEMETRY_FABRIC_CONTROL_POSTGRES_SYNC_INTERVAL_MS") do
      nil -> 5_000
      value -> String.to_integer(value)
    end
  end
end
