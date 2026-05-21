defmodule TelemetryFabricControl.PostgresCodec do
  @moduledoc """
  Converts control-plane structs into PostgreSQL row maps.

  The row maps match `priv/postgres/001_control_plane_schema.sql` and are kept
  dependency-free so they can be tested before the Ecto repository lands.
  """

  alias TelemetryFabricControl.AgentRegistry
  alias TelemetryFabricControl.AuditLog
  alias TelemetryFabricControl.ControlCommand
  alias TelemetryFabricControl.ControlStateSnapshot
  alias TelemetryFabricControl.PipelineConfig

  def tenant_rows(%ControlStateSnapshot{} = snapshot) do
    snapshot
    |> tenant_ids()
    |> Enum.map(&%{tenant_id: &1})
  end

  def agent_row(%AgentRegistry{} = agent) do
    %{
      agent_id: agent.agent_id,
      tenant_id: agent.tenant_id,
      hostname: agent.hostname,
      version: agent.version,
      config_version: agent.config_version,
      queue_depth_bytes: agent.queue_depth_bytes,
      ingest_bytes_per_second: agent.ingest_bytes_per_second,
      labels: atomize_json_map(agent.labels),
      last_seen_at: timestamp(agent.last_seen_at)
    }
  end

  def pipeline_version_row(%PipelineConfig{} = pipeline, updated_by \\ "unknown") do
    payload = PipelineConfig.to_agent_yaml(pipeline)

    %{
      tenant_id: pipeline.tenant_id,
      pipeline_name: pipeline.name,
      version: pipeline.version,
      config: pipeline_config_map(pipeline),
      agent_yaml: payload,
      checksum: sha256(payload),
      updated_by: updated_by
    }
  end

  def command_row(%ControlCommand{} = command) do
    %{
      command_id: command.command_id,
      agent_id: command.agent_id,
      tenant_id: command.tenant_id,
      kind: Atom.to_string(command.kind),
      reason: command.reason,
      status: command |> ControlCommand.status() |> Atom.to_string(),
      inserted_at: timestamp(command.inserted_at),
      delivered_at: timestamp(ControlCommand.delivered_at(command))
    }
  end

  def audit_event_row(%AuditLog{} = event) do
    %{
      event_id: event.id,
      actor: event.actor,
      action: event.action,
      resource: event.resource,
      metadata: atomize_json_map(event.metadata),
      inserted_at: timestamp(event.inserted_at)
    }
  end

  def snapshot_rows(%ControlStateSnapshot{} = snapshot) do
    %{
      tenants: tenant_rows(snapshot),
      agents: Enum.map(snapshot.agents, &agent_row/1),
      pipeline_versions: Enum.map(snapshot.pipeline_versions, &pipeline_version_row/1),
      agent_commands: Enum.map(snapshot.agent_commands, &command_row/1),
      audit_events: Enum.map(snapshot.audit_events, &audit_event_row/1)
    }
  end

  defp tenant_ids(%ControlStateSnapshot{} = snapshot) do
    [
      Enum.map(snapshot.agents, & &1.tenant_id),
      Enum.map(snapshot.pipeline_versions, & &1.tenant_id),
      Enum.map(snapshot.agent_commands, & &1.tenant_id)
    ]
    |> List.flatten()
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp pipeline_config_map(%PipelineConfig{} = pipeline) do
    %{
      tenant_id: pipeline.tenant_id,
      name: pipeline.name,
      version: pipeline.version,
      receivers: normalize_json(pipeline.receivers),
      processors: normalize_json(pipeline.processors),
      exporters: normalize_json(pipeline.exporters),
      routes: normalize_json(pipeline.routes)
    }
  end

  defp atomize_json_map(map) when is_map(map), do: normalize_json(map)

  defp normalize_json(%DateTime{} = value), do: timestamp(value)

  defp normalize_json(value) when is_map(value) do
    value
    |> Enum.map(fn {key, item} -> {to_string(key), normalize_json(item)} end)
    |> Map.new()
  end

  defp normalize_json(value) when is_list(value), do: Enum.map(value, &normalize_json/1)
  defp normalize_json(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_json(value), do: value

  defp timestamp(nil), do: nil
  defp timestamp(%DateTime{} = value), do: DateTime.truncate(value, :microsecond)

  defp sha256(payload) do
    :crypto.hash(:sha256, payload)
    |> Base.encode16(case: :lower)
  end
end
