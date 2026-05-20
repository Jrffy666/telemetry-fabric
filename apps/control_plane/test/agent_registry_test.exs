defmodule TelemetryFabricControl.AgentRegistryTest do
  use ExUnit.Case

  alias TelemetryFabricControl.AgentRegistry

  setup do
    TelemetryFabricControl.AuditLog.clear()
    TelemetryFabricControl.CommandQueue.clear()
    AgentRegistry.clear()
    :ok
  end

  test "registers and heartbeats agents" do
    assert {:ok, agent} =
             AgentRegistry.register(%{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               hostname: "node-a",
               version: "0.1.0",
               config_version: 1
             })

    assert agent.agent_id == "agent-1"

    assert {:ok, updated} = AgentRegistry.heartbeat("agent-1", 2)
    assert updated.config_version == 2
  end

  test "persists registered agents across process restarts" do
    dir = tmp_dir("agent-registry")
    path = Path.join(dir, "agents.term")
    name = unique_name("agent_registry")
    restarted_name = unique_name("agent_registry")

    {:ok, pid} = AgentRegistry.start_link(name: name, storage_path: path)

    assert {:ok, _agent} =
             AgentRegistry.register(name, %{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               hostname: "node-a",
               version: "0.1.0",
               config_version: 1
             })

    GenServer.stop(pid)
    {:ok, _pid} = AgentRegistry.start_link(name: restarted_name, storage_path: path)

    assert {:ok, agent} = AgentRegistry.get_agent(restarted_name, "agent-1")
    assert agent.hostname == "node-a"
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
