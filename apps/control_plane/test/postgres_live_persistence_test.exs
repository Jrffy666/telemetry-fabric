defmodule TelemetryFabricControl.PostgresLivePersistenceTest do
  use ExUnit.Case, async: false

  defmodule LiveRepo do
    use Ecto.Repo,
      otp_app: :telemetry_fabric_control,
      adapter: Ecto.Adapters.Postgres
  end

  @integration_enabled System.get_env("TELEMETRY_FABRIC_CONTROL_POSTGRES_INTEGRATION") == "1"
  database_url = System.get_env("TELEMETRY_FABRIC_CONTROL_TEST_DATABASE_URL")

  if @integration_enabled and is_binary(database_url) and database_url != "" do
    alias Ecto.Adapters.SQL
    alias TelemetryFabricControl.AgentRegistry
    alias TelemetryFabricControl.ControlService
    alias TelemetryFabricControl.ControlStateSnapshot
    alias TelemetryFabricControl.PipelineStore
    alias TelemetryFabricControl.PostgresMigrator
    alias TelemetryFabricControl.PostgresSync
    alias TelemetryFabricControl.SamplePipeline

    @database_url database_url

    setup do
      start_supervised!({LiveRepo, url: @database_url, pool_size: 1})

      TelemetryFabricControl.AuditLog.clear()
      TelemetryFabricControl.CommandQueue.clear()
      AgentRegistry.clear()
      PipelineStore.clear()

      reset_tables!()

      on_exit(fn -> reset_tables!() end)

      :ok
    end

    test "migration and snapshot sync persist control-plane state into PostgreSQL" do
      SamplePipeline.build("payments-prod")
      |> PipelineStore.put_pipeline("operator")

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

      snapshot = ControlStateSnapshot.collect()

      assert :ok = PostgresMigrator.migrate(LiveRepo)

      assert {:ok, _changes} =
               PostgresSync.sync_once(repo: LiveRepo, snapshot: snapshot)

      assert count!("tenants", "tenant_id = $1", ["payments-prod"]) == 1
      assert count!("agents", "agent_id = $1", ["agent-1"]) == 1
      assert count!("pipeline_versions", "tenant_id = $1", ["payments-prod"]) == 1
      assert count!("agent_commands", "agent_id = $1", ["agent-1"]) == 1

      assert scalar!("SELECT status FROM agent_commands WHERE agent_id = $1", ["agent-1"]) ==
               "pending"

      audit_count =
        count!("audit_events", "resource IN ($1, $2)", ["payments-prod/default", "agent-1"])

      assert audit_count >= 2

      assert {:ok, [_command]} =
               ControlService.heartbeat(%{
                 agent_id: "agent-1",
                 tenant_id: "payments-prod",
                 config_version: 1
               })

      delivered_snapshot = ControlStateSnapshot.collect()

      assert {:ok, _changes} =
               PostgresSync.sync_once(repo: LiveRepo, snapshot: delivered_snapshot)

      assert scalar!("SELECT status FROM agent_commands WHERE agent_id = $1", ["agent-1"]) ==
               "delivered"

      assert scalar!("SELECT delivered_at IS NOT NULL FROM agent_commands WHERE agent_id = $1", [
               "agent-1"
             ]) == true

      assert count!("audit_events", "action = $1", ["command.enqueued"]) == 1
      assert count!("audit_events", "action = $1", ["command.delivered"]) == 1

      assert {:ok, _changes} =
               PostgresSync.sync_once(repo: LiveRepo, snapshot: delivered_snapshot)

      assert count!("audit_events", "resource IN ($1, $2)", ["payments-prod/default", "agent-1"]) ==
               audit_count
    end

    defp count!(table, where_clause, params) do
      %{rows: [[count]]} =
        SQL.query!(LiveRepo, "SELECT count(*) FROM #{table} WHERE #{where_clause}", params)

      count
    end

    defp scalar!(statement, params) do
      %{rows: [[value]]} = SQL.query!(LiveRepo, statement, params)
      value
    end

    defp reset_tables! do
      SQL.query!(
        LiveRepo,
        """
        DROP TABLE IF EXISTS
          audit_events,
          agent_commands,
          pipeline_versions,
          agents,
          tenants
        CASCADE
        """,
        []
      )
    end
  else
    @tag skip:
           "set TELEMETRY_FABRIC_CONTROL_POSTGRES_INTEGRATION=1 and TELEMETRY_FABRIC_CONTROL_TEST_DATABASE_URL to run live PostgreSQL persistence tests"
    test "live PostgreSQL persistence integration is opt-in" do
      :ok
    end
  end
end
