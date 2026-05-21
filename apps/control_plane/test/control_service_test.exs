defmodule TelemetryFabricControl.ControlServiceTest do
  use ExUnit.Case

  alias TelemetryFabricControl.AuditLog
  alias TelemetryFabricControl.AgentRegistry
  alias TelemetryFabricControl.CommandQueue
  alias TelemetryFabricControl.ControlCommand
  alias TelemetryFabricControl.ControlService
  alias TelemetryFabricControl.ControlService.AgentStatusResponse
  alias TelemetryFabricControl.ControlService.ConfigUpdate
  alias TelemetryFabricControl.ControlService.RegisterAgentResponse
  alias TelemetryFabricControl.PipelineStore
  alias TelemetryFabricControl.SamplePipeline

  setup do
    AuditLog.clear()
    CommandQueue.clear()
    AgentRegistry.clear()
    PipelineStore.clear()
    :ok
  end

  test "registers agents and returns the latest config update" do
    assert {:ok, pipeline} =
             "payments-prod"
             |> SamplePipeline.build()
             |> PipelineStore.put_pipeline("test")

    assert pipeline.version == 1

    assert {:ok, %RegisterAgentResponse{accepted: true, config_version: 1}} =
             ControlService.register_agent(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               hostname: "node-a",
               version: "0.1.0"
             })

    assert {:ok, %ConfigUpdate{version: 1, pipeline_config: payload, checksum: checksum}} =
             ControlService.config_update(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               current_version: 0
             })

    assert payload =~ ~s(tenant: "payments-prod")
    assert payload =~ ~s(pipeline: "default")
    assert payload =~ ~s(otlp-grpc:)
    assert payload =~ "retry:"
    assert payload =~ "max_attempts: 3"
    assert {:ok, decoded_checksum} = Base.decode16(checksum, case: :mixed)
    assert byte_size(decoded_checksum) == 32

    assert {:ok, :up_to_date} =
             ControlService.config_update(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               current_version: 1
             })
  end

  test "heartbeat returns reload commands when the agent config is stale" do
    "payments-prod"
    |> SamplePipeline.build()
    |> PipelineStore.put_pipeline("test")

    ControlService.register_agent(%{
      agent_id: "agent-1",
      tenant_id: "payments-prod",
      hostname: "node-a",
      version: "0.1.0"
    })

    assert {:ok, [%ControlCommand{kind: :reload_config, reason: reason}]} =
             ControlService.heartbeat(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               config_version: 0,
               queue_depth_bytes: 128,
               ingest_bytes_per_second: 1024
             })

    assert reason =~ "version 1"

    assert {:ok, %AgentStatusResponse{healthy: false, warnings: warnings}} =
             ControlService.report_status(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod"
             })

    assert "config_outdated" in warnings
    assert "queue_not_empty" in warnings

    assert {:ok, []} =
             ControlService.heartbeat(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               config_version: 1,
               queue_depth_bytes: 0
             })

    assert {:ok, %AgentStatusResponse{healthy: true, warnings: []}} =
             ControlService.report_status(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod"
             })
  end

  test "rolls back pipeline configs and exposes the rollback as a new update" do
    first_config = SamplePipeline.build("payments-prod")
    second_config = %{first_config | processors: [%{name: "redact", enabled: true}]}

    assert {:ok, first} = PipelineStore.put_pipeline(first_config, "test")
    assert {:ok, _second} = PipelineStore.put_pipeline(second_config, "test")

    assert {:ok, %RegisterAgentResponse{config_version: 2}} =
             ControlService.register_agent(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               hostname: "node-a",
               version: "0.1.0"
             })

    assert {:ok, rollback} =
             ControlService.rollback_pipeline(%{
               tenant_id: "payments-prod",
               pipeline: "default",
               target_version: first.version,
               actor: "operator"
             })

    assert rollback.version == 3
    assert rollback.processors == first.processors

    assert {:ok, %ConfigUpdate{version: 3, pipeline_config: payload}} =
             ControlService.config_update(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               current_version: 2
             })

    assert payload =~ "memory-limiter"

    assert {:ok, [%ControlCommand{kind: :reload_config, reason: reason}]} =
             ControlService.heartbeat(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               config_version: 2
             })

    assert reason =~ "version 3"
  end

  test "publishes pipeline configs and exposes the release as a config update" do
    attrs = %{
      tenant_id: "payments-prod",
      pipeline: "default",
      actor: "operator",
      receivers: [
        %{"name" => "tf-line", "protocol" => "tf_line", "endpoint" => "127.0.0.1:4319"}
      ],
      processors: [
        %{"name" => "memory-limiter", "enabled" => true},
        %{"name" => "tenant-rate-limit", "enabled" => true}
      ],
      exporters: [
        %{"name" => "stdout", "protocol" => "stdout", "endpoint" => "stdout://local"}
      ],
      routes: [
        %{"signal" => "trace", "exporters" => ["stdout"]}
      ]
    }

    assert {:ok, pipeline} = ControlService.put_pipeline(attrs)
    assert pipeline.version == 1

    assert {:ok, %RegisterAgentResponse{config_version: 1}} =
             ControlService.register_agent(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               hostname: "node-a",
               version: "0.1.0"
             })

    assert {:ok, %ConfigUpdate{version: 1, pipeline_config: payload}} =
             ControlService.config_update(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               current_version: 0
             })

    assert payload =~ "tenant-rate-limit"

    assert {:ok, updated} =
             ControlService.put_pipeline(%{
               attrs
               | processors: [%{"name" => "redact", "enabled" => true}]
             })

    assert updated.version == 2

    assert {:ok, [%ControlCommand{kind: :reload_config, reason: reason}]} =
             ControlService.heartbeat(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               config_version: 1
             })

    assert reason =~ "version 2"

    assert Enum.any?(AuditLog.list(:all), &(&1.action == "pipeline.updated"))
  end

  test "heartbeat drains queued operator commands" do
    "payments-prod"
    |> SamplePipeline.build()
    |> PipelineStore.put_pipeline("test")

    ControlService.register_agent(%{
      agent_id: "agent-1",
      tenant_id: "payments-prod",
      hostname: "node-a",
      version: "0.1.0"
    })

    assert {:ok, %ControlCommand{kind: :pause_exports}} =
             ControlService.enqueue_command("agent-1", :pause_exports, "maintenance")

    assert {:ok, [%ControlCommand{kind: :pause_exports, reason: "maintenance"}]} =
             ControlService.heartbeat(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               config_version: 1
             })

    assert {:ok, %ControlCommand{kind: :resume_exports}} =
             ControlService.enqueue_command("agent-1", :resume_exports, "maintenance complete")

    assert {:ok, [%ControlCommand{kind: :resume_exports, reason: "maintenance complete"}]} =
             ControlService.heartbeat(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               config_version: 1
             })

    assert {:ok, []} =
             ControlService.heartbeat(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               config_version: 1
             })

    assert [] = CommandQueue.list("agent-1")

    assert [
             %ControlCommand{kind: :pause_exports, status: :delivered, delivered_at: %DateTime{}},
             %ControlCommand{kind: :resume_exports, status: :delivered, delivered_at: %DateTime{}}
           ] = CommandQueue.list_all()

    audit_actions =
      AuditLog.list(:all)
      |> Enum.map(& &1.action)

    assert Enum.count(audit_actions, &(&1 == "command.enqueued")) == 2
    assert Enum.count(audit_actions, &(&1 == "command.delivered")) == 2
  end

  test "agents acknowledge delivered operator commands" do
    ControlService.register_agent(%{
      agent_id: "agent-1",
      tenant_id: "payments-prod",
      hostname: "node-a",
      version: "0.1.0"
    })

    assert {:ok, queued} =
             ControlService.enqueue_command("agent-1", :pause_exports, "maintenance")

    assert {:ok, [%ControlCommand{status: :delivered, command_id: command_id}]} =
             ControlService.heartbeat(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               config_version: 1
             })

    assert command_id == queued.command_id

    assert {:ok, %ControlCommand{status: :succeeded, acknowledged_at: %DateTime{}}} =
             ControlService.ack_command(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               command_id: command_id,
               success: true
             })

    assert [
             %ControlCommand{
               command_id: ^command_id,
               status: :succeeded,
               delivered_at: %DateTime{},
               acknowledged_at: %DateTime{},
               last_error: nil
             }
           ] = CommandQueue.list_all()

    assert Enum.any?(AuditLog.list(:all), &(&1.action == "command.succeeded"))
  end

  test "command queue persists pending commands across process restarts" do
    dir = tmp_dir("command-queue")
    path = Path.join(dir, "commands.term")
    name = unique_name("command_queue")
    restarted_name = unique_name("command_queue")

    command =
      ControlCommand.new(%{
        agent_id: "agent-1",
        tenant_id: "payments-prod",
        kind: :drain_and_restart,
        reason: "upgrade"
      })

    {:ok, pid} = CommandQueue.start_link(name: name, storage_path: path)
    assert {:ok, _command} = CommandQueue.enqueue(name, command)

    GenServer.stop(pid)
    {:ok, _pid} = CommandQueue.start_link(name: restarted_name, storage_path: path)

    assert [%ControlCommand{kind: :drain_and_restart, reason: "upgrade"}] =
             CommandQueue.drain(restarted_name, "agent-1")

    assert [] = CommandQueue.list(restarted_name, "agent-1")

    assert [
             %ControlCommand{
               kind: :drain_and_restart,
               reason: "upgrade",
               status: :delivered,
               delivered_at: %DateTime{}
             }
           ] = CommandQueue.list_all(restarted_name)

    GenServer.stop(Process.whereis(restarted_name))
    delivered_name = unique_name("command_queue")
    {:ok, _pid} = CommandQueue.start_link(name: delivered_name, storage_path: path)

    assert [
             %ControlCommand{
               kind: :drain_and_restart,
               status: :delivered,
               delivered_at: %DateTime{}
             }
           ] = CommandQueue.list_all(delivered_name)
  end

  defp unique_name(prefix) do
    String.to_atom("#{prefix}_#{System.unique_integer([:positive, :monotonic])}")
  end

  defp tmp_dir(name) do
    root = Path.join([".tmp", "control_plane_tests"])
    path = Path.join(root, "#{name}-#{System.unique_integer([:positive, :monotonic])}")
    File.rm_rf!(path)
    File.mkdir_p!(path)
    on_exit(fn -> File.rm_rf!(path) end)
    path
  end
end
