defmodule TelemetryFabricControl.PostgresPersistenceTest do
  use ExUnit.Case

  alias TelemetryFabricControl.AgentRegistry
  alias TelemetryFabricControl.ControlService
  alias TelemetryFabricControl.ControlStateSnapshot
  alias TelemetryFabricControl.PipelineStore
  alias TelemetryFabricControl.PostgresCodec
  alias TelemetryFabricControl.PostgresMigrator
  alias TelemetryFabricControl.PostgresSchema
  alias TelemetryFabricControl.PostgresSync
  alias TelemetryFabricControl.PostgresWriter
  alias TelemetryFabricControl.SamplePipeline
  alias TelemetryFabricControl.Schema.Agent
  alias TelemetryFabricControl.Schema.AgentCommand
  alias TelemetryFabricControl.Schema.AuditEvent
  alias TelemetryFabricControl.Schema.PipelineVersion
  alias TelemetryFabricControl.Schema.Tenant

  defmodule FakeRepo do
    def transaction(%Ecto.Multi{} = multi), do: {:ok, Ecto.Multi.to_list(multi)}
  end

  setup do
    TelemetryFabricControl.AuditLog.clear()
    TelemetryFabricControl.CommandQueue.clear()
    AgentRegistry.clear()
    PipelineStore.clear()
    :ok
  end

  test "postgres schema defines core control-plane tables and indexes" do
    sql = PostgresSchema.migration_sql()

    assert sql =~ "CREATE TABLE IF NOT EXISTS tenants"
    assert sql =~ "CREATE TABLE IF NOT EXISTS agents"
    assert sql =~ "CREATE TABLE IF NOT EXISTS pipeline_versions"
    assert sql =~ "CREATE TABLE IF NOT EXISTS agent_commands"
    assert sql =~ "CREATE TABLE IF NOT EXISTS audit_events"
    assert sql =~ "audit_events_event_id_idx"
    assert sql =~ "agent_commands_pending_idx"
    assert sql =~ "pipeline_versions_latest_idx"
  end

  test "postgres migration SQL splits into executable statements" do
    statements = PostgresMigrator.statements(PostgresSchema.migration_sql())

    assert Enum.any?(statements, &String.starts_with?(&1, "CREATE TABLE IF NOT EXISTS tenants"))
    assert Enum.any?(statements, &String.starts_with?(&1, "CREATE INDEX IF NOT EXISTS"))
    refute Enum.any?(statements, &String.ends_with?(&1, ";"))
  end

  test "control state snapshot converts to postgres row maps" do
    SamplePipeline.build("payments-prod")
    |> PipelineStore.put_pipeline("operator")

    assert {:ok, _agent} =
             AgentRegistry.register(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               hostname: "node-a",
               version: "0.1.0",
               config_version: 1,
               labels: %{role: "edge"}
             })

    assert {:ok, _command} =
             ControlService.enqueue_command("agent-1", :pause_exports, "maintenance")

    snapshot = ControlStateSnapshot.collect()
    rows = PostgresCodec.snapshot_rows(snapshot)

    assert rows.tenants == [%{tenant_id: "payments-prod"}]
    assert [%{agent_id: "agent-1", labels: %{"role" => "edge"}}] = rows.agents
    assert [%{pipeline_name: "default", version: 1, checksum: checksum}] = rows.pipeline_versions
    assert byte_size(checksum) == 64

    assert [%{agent_id: "agent-1", kind: "pause_exports", status: "pending"}] =
             rows.agent_commands

    assert Enum.any?(rows.audit_events, &(&1.action == "pipeline.updated" and &1.event_id))
    assert Enum.any?(rows.audit_events, &(&1.action == "agent.registered" and &1.event_id))
  end

  test "delivered control commands convert to postgres row maps" do
    assert {:ok, _agent} =
             AgentRegistry.register(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               hostname: "node-a",
               version: "0.1.0",
               config_version: 1
             })

    assert {:ok, _command} =
             ControlService.enqueue_command("agent-1", :pause_exports, "maintenance")

    assert {:ok, [_command]} =
             ControlService.heartbeat(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               config_version: 1
             })

    snapshot = ControlStateSnapshot.collect()
    rows = PostgresCodec.snapshot_rows(snapshot)

    assert [
             %{
               agent_id: "agent-1",
               kind: "pause_exports",
               status: "delivered",
               delivered_at: %DateTime{}
             }
           ] = rows.agent_commands
  end

  test "postgres writer builds deterministic ecto multi operations" do
    SamplePipeline.build("payments-prod")
    |> PipelineStore.put_pipeline("operator")

    AgentRegistry.register(%{
      agent_id: "agent-1",
      tenant_id: "payments-prod",
      hostname: "node-a",
      version: "0.1.0",
      config_version: 1
    })

    snapshot = ControlStateSnapshot.collect()

    assert [:tenants, :agents, :pipeline_versions, :agent_commands, :audit_events] =
             snapshot
             |> PostgresWriter.to_multi()
             |> Ecto.Multi.to_list()
             |> Enum.map(fn {name, _operation} -> name end)
  end

  test "postgres sync writes a collected snapshot through a repo transaction" do
    SamplePipeline.build("payments-prod")
    |> PipelineStore.put_pipeline("operator")

    AgentRegistry.register(%{
      agent_id: "agent-1",
      tenant_id: "payments-prod",
      hostname: "node-a",
      version: "0.1.0",
      config_version: 1
    })

    snapshot = ControlStateSnapshot.collect()

    assert {:ok, operations} = PostgresSync.sync_once(repo: FakeRepo, snapshot: snapshot)

    assert [:tenants, :agents, :pipeline_versions, :agent_commands, :audit_events] =
             Enum.map(operations, fn {name, _operation} -> name end)
  end

  test "ecto schemas validate required control-plane rows" do
    now = DateTime.utc_now()

    assert Tenant.changeset(%{tenant_id: "payments-prod"}).valid?

    assert Agent.changeset(%{
             agent_id: "agent-1",
             tenant_id: "payments-prod",
             hostname: "node-a",
             version: "0.1.0",
             last_seen_at: now
           }).valid?

    assert PipelineVersion.changeset(%{
             tenant_id: "payments-prod",
             pipeline_name: "default",
             version: 1,
             config: %{"name" => "default"},
             agent_yaml: "tenant: payments-prod\n",
             checksum: String.duplicate("a", 64),
             updated_by: "operator"
           }).valid?

    assert AgentCommand.changeset(%{
             command_id: "cmd-1",
             agent_id: "agent-1",
             tenant_id: "payments-prod",
             kind: "resume_exports",
             status: "pending",
             inserted_at: now
           }).valid?

    assert AuditEvent.changeset(%{
             event_id: 1,
             actor: "operator",
             action: "pipeline.updated",
             resource: "payments-prod/default",
             inserted_at: now
           }).valid?

    refute AgentCommand.changeset(%{
             command_id: "cmd-1",
             agent_id: "agent-1",
             tenant_id: "payments-prod",
             kind: "not_real",
             status: "pending",
             inserted_at: now
           }).valid?
  end
end
