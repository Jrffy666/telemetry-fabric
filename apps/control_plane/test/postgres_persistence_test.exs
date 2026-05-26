defmodule TelemetryFabricControl.PostgresPersistenceTest do
  use ExUnit.Case

  alias TelemetryFabricControl.AgentRegistry
  alias TelemetryFabricControl.ControlCommand
  alias TelemetryFabricControl.ControlService
  alias TelemetryFabricControl.ControlStateSnapshot
  alias TelemetryFabricControl.PipelineStore
  alias TelemetryFabricControl.PostgresCodec
  alias TelemetryFabricControl.PostgresControlStore
  alias TelemetryFabricControl.PostgresMigrator
  alias TelemetryFabricControl.PostgresSchema
  alias TelemetryFabricControl.PostgresSync
  alias TelemetryFabricControl.PostgresWriter
  alias TelemetryFabricControl.SamplePipeline
  alias TelemetryFabricControl.Schema.Agent
  alias TelemetryFabricControl.Schema.AgentCommand
  alias TelemetryFabricControl.Schema.AuditEvent
  alias TelemetryFabricControl.Schema.BlockchainAddressWatch
  alias TelemetryFabricControl.Schema.BlockchainChain
  alias TelemetryFabricControl.Schema.BlockchainCheckpoint
  alias TelemetryFabricControl.Schema.BlockchainContractWatch
  alias TelemetryFabricControl.Schema.BlockchainCrawlAssignment
  alias TelemetryFabricControl.Schema.BlockchainFilterRule
  alias TelemetryFabricControl.Schema.BlockchainRpcEndpoint
  alias TelemetryFabricControl.Schema.BlockchainTokenWatch
  alias TelemetryFabricControl.Schema.ModuleConfigVersion
  alias TelemetryFabricControl.Schema.ModuleRegistration
  alias TelemetryFabricControl.Schema.PipelineVersion
  alias TelemetryFabricControl.Schema.Tenant

  defmodule FakeRepo do
    def transaction(%Ecto.Multi{} = multi), do: {:ok, Ecto.Multi.to_list(multi)}
  end

  defmodule CommandRepo do
    @rows_key {__MODULE__, :rows}
    @one_key {__MODULE__, :one}
    @updates_key {__MODULE__, :updates}
    @audits_key {__MODULE__, :audits}
    @all_fragments_key {__MODULE__, :all_fragments}
    @one_fragments_key {__MODULE__, :one_fragments}

    def reset! do
      Enum.each(
        [@rows_key, @one_key, @updates_key, @audits_key, @all_fragments_key, @one_fragments_key],
        &Process.delete/1
      )
    end

    def put_all(rows, fragments \\ []) do
      Process.put(@rows_key, rows)
      Process.put(@all_fragments_key, fragments)
    end

    def put_one(row, fragments \\ []) do
      Process.put(@one_key, row)
      Process.put(@one_fragments_key, fragments)
    end

    def updates do
      @updates_key
      |> Process.get([])
      |> Enum.reverse()
    end

    def audits do
      @audits_key
      |> Process.get([])
      |> Enum.reverse()
    end

    def transaction(fun) when is_function(fun, 0) do
      try do
        {:ok, fun.()}
      catch
        {:rollback, reason} -> {:error, reason}
      end
    end

    def all(query) do
      assert_query_fragments!(query, Process.get(@all_fragments_key, []))
      Process.get(@rows_key, [])
    end

    def one(query) do
      assert_query_fragments!(query, Process.get(@one_fragments_key, []))
      Process.get(@one_key)
    end

    def update_all(_query, opts) do
      Process.put(@updates_key, [opts | Process.get(@updates_key, [])])
      {1, nil}
    end

    def insert_all(TelemetryFabricControl.Schema.AuditEvent, [row], _opts) do
      Process.put(@audits_key, [row | Process.get(@audits_key, [])])
      {1, nil}
    end

    def rollback(reason), do: throw({:rollback, reason})

    defp assert_query_fragments!(query, fragments) do
      query_text = inspect(query)

      Enum.each(fragments, fn fragment ->
        unless query_text =~ fragment do
          raise "expected query to contain #{inspect(fragment)}"
        end
      end)
    end
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
    assert sql =~ "CREATE TABLE IF NOT EXISTS module_registry"
    assert sql =~ "CREATE TABLE IF NOT EXISTS module_config_versions"
    assert sql =~ "CREATE TABLE IF NOT EXISTS blockchain_chains"
    assert sql =~ "CREATE TABLE IF NOT EXISTS blockchain_rpc_endpoints"
    assert sql =~ "CREATE TABLE IF NOT EXISTS blockchain_address_watchlist"
    assert sql =~ "CREATE TABLE IF NOT EXISTS blockchain_contract_watchlist"
    assert sql =~ "CREATE TABLE IF NOT EXISTS blockchain_token_watchlist"
    assert sql =~ "CREATE TABLE IF NOT EXISTS blockchain_filter_rules"
    assert sql =~ "CREATE TABLE IF NOT EXISTS blockchain_crawl_assignments"
    assert sql =~ "CREATE TABLE IF NOT EXISTS blockchain_checkpoints"
    assert sql =~ "audit_events_event_id_idx"
    assert sql =~ "agent_commands_pending_idx"
    assert sql =~ "agent_commands_delivered_lease_idx"
    assert sql =~ "pipeline_versions_latest_idx"
    assert sql =~ "module_config_versions_latest_idx"
    assert sql =~ "blockchain_rpc_endpoints_chain_idx"
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

    assert Enum.any?(rows.audit_events, &(&1.action == "command.enqueued"))
    assert Enum.any?(rows.audit_events, &(&1.action == "command.delivered"))
  end

  test "acknowledged control commands convert to postgres row maps" do
    assert {:ok, _agent} =
             AgentRegistry.register(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               hostname: "node-a",
               version: "0.1.0",
               config_version: 1
             })

    assert {:ok, command} =
             ControlService.enqueue_command("agent-1", :pause_exports, "maintenance")

    assert {:ok, [_command]} =
             ControlService.heartbeat(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               config_version: 1
             })

    assert {:ok, _acknowledged} =
             ControlService.ack_command(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               command_id: command.command_id,
               success: false,
               error: "processor reload failed"
             })

    snapshot = ControlStateSnapshot.collect()
    rows = PostgresCodec.snapshot_rows(snapshot)

    assert [
             %{
               agent_id: "agent-1",
               kind: "pause_exports",
               status: "failed",
               delivered_at: %DateTime{},
               acknowledged_at: %DateTime{},
               last_error: "processor reload failed"
             }
           ] = rows.agent_commands

    assert Enum.any?(rows.audit_events, &(&1.action == "command.failed"))
  end

  test "postgres control store redelivers commands whose delivered lease expired" do
    CommandRepo.reset!()

    delivered_at = DateTime.add(DateTime.utc_now(), -120, :second)

    row = %AgentCommand{
      command_id: "cmd-lease",
      agent_id: "agent-1",
      tenant_id: "payments-prod",
      kind: "pause_exports",
      reason: "maintenance",
      status: "delivered",
      inserted_at: delivered_at,
      delivered_at: delivered_at
    }

    CommandRepo.put_all([row], ["pending", "delivered", "delivered_at"])

    assert {:ok, [%ControlCommand{command_id: "cmd-lease", status: :delivered} = redelivered]} =
             PostgresControlStore.drain_commands("agent-1", CommandRepo)

    assert DateTime.compare(redelivered.delivered_at, delivered_at) == :gt

    assert [[set: [status: "delivered", delivered_at: %DateTime{}]]] = CommandRepo.updates()
    assert [%{action: "command.delivered", resource: "cmd-lease"}] = CommandRepo.audits()
  end

  test "postgres control store treats repeated ACKs as idempotent" do
    CommandRepo.reset!()

    delivered_at = DateTime.add(DateTime.utc_now(), -120, :second)
    acknowledged_at = DateTime.add(DateTime.utc_now(), -60, :second)

    row = %AgentCommand{
      command_id: "cmd-ack",
      agent_id: "agent-1",
      tenant_id: "payments-prod",
      kind: "pause_exports",
      reason: "maintenance",
      status: "succeeded",
      inserted_at: delivered_at,
      delivered_at: delivered_at,
      acknowledged_at: acknowledged_at
    }

    CommandRepo.put_one(row, ["agent_id", "command_id"])

    assert {:ok,
            %ControlCommand{
              command_id: "cmd-ack",
              status: :succeeded,
              acknowledged_at: ^acknowledged_at
            }} =
             PostgresControlStore.ack_command(
               "agent-1",
               "cmd-ack",
               false,
               "changed on retry",
               CommandRepo
             )

    assert CommandRepo.updates() == []
    assert CommandRepo.audits() == []
  end

  test "rolled back pipeline versions convert to postgres row maps" do
    first_config = SamplePipeline.build("payments-prod")
    second_config = %{first_config | processors: [%{name: "redact", enabled: true}]}

    assert {:ok, first} = PipelineStore.put_pipeline(first_config, "operator")
    assert {:ok, _second} = PipelineStore.put_pipeline(second_config, "operator")

    assert {:ok, rollback} =
             PipelineStore.rollback_pipeline(
               "payments-prod",
               "default",
               first.version,
               "operator"
             )

    snapshot = ControlStateSnapshot.collect()
    rows = PostgresCodec.snapshot_rows(snapshot)

    assert Enum.map(rows.pipeline_versions, & &1.version) == [1, 2, 3]

    assert [%{version: 3, checksum: checksum}] =
             Enum.filter(rows.pipeline_versions, &(&1.version == rollback.version))

    assert byte_size(checksum) == 64
    assert Enum.any?(rows.audit_events, &(&1.action == "pipeline.rolled_back"))
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
             status: "succeeded",
             inserted_at: now
           }).valid?

    assert AuditEvent.changeset(%{
             event_id: 1,
             actor: "operator",
             action: "pipeline.updated",
             resource: "payments-prod/default",
             inserted_at: now
           }).valid?

    assert ModuleRegistration.changeset(%{
             module_name: "blockchain",
             display_name: "Blockchain",
             owner: "data-platform",
             enabled: true
           }).valid?

    assert ModuleConfigVersion.changeset(%{
             tenant_id: "payments-prod",
             module_name: "blockchain",
             version: 1,
             config: %{"chains" => []},
             checksum: String.duplicate("b", 64),
             updated_by: "operator"
           }).valid?

    assert BlockchainChain.changeset(%{
             tenant_id: "payments-prod",
             chain_key: "ethereum-mainnet",
             display_name: "Ethereum Mainnet",
             network: "mainnet",
             enabled: true
           }).valid?

    assert BlockchainRpcEndpoint.changeset(%{
             tenant_id: "payments-prod",
             endpoint_id: "eth-mainnet-primary",
             chain_key: "ethereum-mainnet",
             url: "https://rpc.example.invalid",
             priority: 10,
             enabled: true
           }).valid?

    assert BlockchainAddressWatch.changeset(%{
             tenant_id: "payments-prod",
             entry_id: "treasury",
             chain_key: "ethereum-mainnet",
             address: "0x0000000000000000000000000000000000000000",
             enabled: true
           }).valid?

    assert BlockchainContractWatch.changeset(%{
             tenant_id: "payments-prod",
             contract_id: "usdc",
             chain_key: "ethereum-mainnet",
             address: "0x0000000000000000000000000000000000000000",
             enabled: true
           }).valid?

    assert BlockchainTokenWatch.changeset(%{
             tenant_id: "payments-prod",
             token_id: "usdc",
             chain_key: "ethereum-mainnet",
             contract_address: "0x0000000000000000000000000000000000000000",
             decimals: 6,
             enabled: true
           }).valid?

    assert BlockchainFilterRule.changeset(%{
             tenant_id: "payments-prod",
             rule_id: "large-transfers",
             name: "Large transfers",
             expression: %{"field" => "amount", "op" => "gte", "value" => "1000000"},
             action: "keep",
             enabled: true
           }).valid?

    assert BlockchainCrawlAssignment.changeset(%{
             tenant_id: "payments-prod",
             assignment_id: "crawler-a-eth",
             chain_key: "ethereum-mainnet",
             crawler_id: "crawler-a",
             enabled: true
           }).valid?

    assert BlockchainCheckpoint.changeset(%{
             tenant_id: "payments-prod",
             assignment_id: "crawler-a-eth",
             chain_key: "ethereum-mainnet",
             cursor: %{"block" => 100},
             updated_by: "crawler-a"
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
