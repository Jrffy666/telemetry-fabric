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

    assert {:ok, []} =
             ControlService.heartbeat(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               config_version: 1
             })
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
  end

  defp unique_name(prefix) do
    String.to_atom("#{prefix}_#{System.unique_integer([:positive, :monotonic])}")
  end

  defp tmp_dir(name) do
    root = Path.expand("../../../.tmp/control_plane_tests", __DIR__)
    path = Path.join(root, "#{name}-#{System.unique_integer([:positive, :monotonic])}")
    File.rm_rf!(path)
    File.mkdir_p!(path)
    on_exit(fn -> File.rm_rf!(path) end)
    path
  end
end
