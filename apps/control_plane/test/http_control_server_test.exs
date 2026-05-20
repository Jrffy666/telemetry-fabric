defmodule TelemetryFabricControl.HttpControlServerTest do
  use ExUnit.Case

  alias TelemetryFabricControl.AgentRegistry
  alias TelemetryFabricControl.AuditLog
  alias TelemetryFabricControl.CommandQueue
  alias TelemetryFabricControl.HttpControlServer
  alias TelemetryFabricControl.Json
  alias TelemetryFabricControl.PipelineStore
  alias TelemetryFabricControl.SamplePipeline

  setup do
    AuditLog.clear()
    CommandQueue.clear()
    AgentRegistry.clear()
    PipelineStore.clear()

    server = start_supervised!({HttpControlServer, host: "127.0.0.1", port: 0})

    %{port: HttpControlServer.port(server)}
  end

  test "serves the control-plane registration and config workflow", %{port: port} do
    "payments-prod"
    |> SamplePipeline.build()
    |> PipelineStore.put_pipeline("test")

    assert {200, register_body} =
             post_json(port, "/v1/agents/register", %{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               hostname: "node-a",
               version: "0.1.0"
             })

    assert register_body["agent"]["accepted"] == true
    assert register_body["agent"]["config_version"] == 1

    assert {200, config_body} =
             post_json(port, "/v1/agents/config", %{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               current_version: 0
             })

    assert config_body["update"]["version"] == 1
    assert config_body["update"]["pipeline_config"] =~ ~s(tenant: "payments-prod")
    assert byte_size(config_body["update"]["checksum"]) == 64

    assert {200, current_config_body} =
             post_json(port, "/v1/agents/config", %{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               current_version: 1
             })

    assert current_config_body["update"] == nil
  end

  test "serves heartbeat-derived and operator-queued commands", %{port: port} do
    "payments-prod"
    |> SamplePipeline.build()
    |> PipelineStore.put_pipeline("test")

    post_json(port, "/v1/agents/register", %{
      agent_id: "agent-1",
      tenant_id: "payments-prod",
      hostname: "node-a",
      version: "0.1.0"
    })

    assert {200, stale_heartbeat} =
             post_json(port, "/v1/agents/heartbeat", %{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               config_version: 0,
               queue_depth_bytes: 128
             })

    assert [%{"kind" => "reload_config"}] = stale_heartbeat["commands"]

    assert {200, queued_command} =
             post_json(port, "/v1/agents/commands", %{
               agent_id: "agent-1",
               kind: "pause_exports",
               reason: "maintenance"
             })

    assert queued_command["command"]["kind"] == "pause_exports"

    assert {200, heartbeat} =
             post_json(port, "/v1/agents/heartbeat", %{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               config_version: 1,
               queue_depth_bytes: 0
             })

    assert [%{"kind" => "pause_exports", "reason" => "maintenance"}] = heartbeat["commands"]

    assert {200, status} =
             post_json(port, "/v1/agents/status", %{
               agent_id: "agent-1",
               tenant_id: "payments-prod"
             })

    assert status["status"]["healthy"] == true
    assert status["status"]["warnings"] == []
  end

  test "returns HTTP errors for bad requests", %{port: port} do
    assert {404, body} =
             post_json(port, "/v1/agents/status", %{
               agent_id: "missing",
               tenant_id: "payments-prod"
             })

    assert body["error"] == "not_found"

    assert {400, body} =
             raw_request(port, "POST /v1/agents/register HTTP/1.1\r\nContent-Length: 1\r\n\r\n{")

    assert body["error"] =~ "invalid JSON request body"
  end

  test "serves health checks", %{port: port} do
    assert {200, %{"status" => "ok"}} = raw_request(port, "GET /healthz HTTP/1.1\r\n\r\n")
  end

  defp post_json(port, path, body) do
    payload = Json.encode!(body)

    raw_request(
      port,
      [
        "POST #{path} HTTP/1.1\r\n",
        "Host: 127.0.0.1\r\n",
        "Content-Type: application/json\r\n",
        "Content-Length: #{byte_size(payload)}\r\n",
        "\r\n",
        payload
      ]
    )
  end

  defp raw_request(port, request) do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])
    :ok = :gen_tcp.send(socket, request)
    response = recv_all(socket, "")
    parse_response(response)
  end

  defp recv_all(socket, acc) do
    case :gen_tcp.recv(socket, 0, 1_000) do
      {:ok, chunk} -> recv_all(socket, acc <> chunk)
      {:error, :closed} -> acc
    end
  end

  defp parse_response(response) do
    [head, body] = String.split(response, "\r\n\r\n", parts: 2)
    ["HTTP/1.1 " <> status | _headers] = String.split(head, "\r\n")
    [code | _] = String.split(status, " ", parts: 2)
    {String.to_integer(code), Json.decode!(body)}
  end
end
