defmodule TelemetryFabricControl.PostgresWriter do
  @moduledoc """
  Builds Ecto write plans for syncing an OTP state snapshot into PostgreSQL.
  """

  alias Ecto.Multi
  alias TelemetryFabricControl.ControlStateSnapshot
  alias TelemetryFabricControl.PostgresCodec
  alias TelemetryFabricControl.Schema.Agent
  alias TelemetryFabricControl.Schema.AgentCommand
  alias TelemetryFabricControl.Schema.AuditEvent
  alias TelemetryFabricControl.Schema.PipelineVersion
  alias TelemetryFabricControl.Schema.Tenant

  def to_multi(%ControlStateSnapshot{} = snapshot) do
    rows = PostgresCodec.snapshot_rows(snapshot)

    Multi.new()
    |> Multi.insert_all(:tenants, Tenant, rows.tenants,
      on_conflict: :nothing,
      conflict_target: [:tenant_id]
    )
    |> Multi.insert_all(:agents, Agent, rows.agents,
      on_conflict:
        {:replace,
         [
           :tenant_id,
           :hostname,
           :version,
           :config_version,
           :queue_depth_bytes,
           :ingest_bytes_per_second,
           :labels,
           :last_seen_at
         ]},
      conflict_target: [:agent_id]
    )
    |> Multi.insert_all(:pipeline_versions, PipelineVersion, rows.pipeline_versions,
      on_conflict: :nothing,
      conflict_target: [:tenant_id, :pipeline_name, :version]
    )
    |> Multi.insert_all(:agent_commands, AgentCommand, rows.agent_commands,
      on_conflict:
        {:replace,
         [:kind, :reason, :status, :inserted_at, :delivered_at, :acknowledged_at, :last_error]},
      conflict_target: [:command_id]
    )
    |> Multi.insert_all(:audit_events, AuditEvent, rows.audit_events,
      on_conflict: :nothing,
      conflict_target: [:event_id]
    )
  end
end
