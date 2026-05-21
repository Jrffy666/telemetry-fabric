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

  test "serves pipeline publication workflow", %{port: port} do
    assert {200, publish_body} =
             post_json(port, "/v1/pipelines", pipeline_payload("payments-prod"))

    assert publish_body["pipeline"]["tenant_id"] == "payments-prod"
    assert publish_body["pipeline"]["name"] == "default"
    assert publish_body["pipeline"]["version"] == 1

    assert {200, register_body} =
             post_json(port, "/v1/agents/register", %{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               hostname: "node-a",
               version: "0.1.0"
             })

    assert register_body["agent"]["config_version"] == 1

    assert {200, config_body} =
             post_json(port, "/v1/agents/config", %{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               current_version: 0
             })

    assert config_body["update"]["version"] == 1
    assert config_body["update"]["pipeline_config"] =~ "tenant-rate-limit"
  end

  test "serves pipeline rollback workflow", %{port: port} do
    first_config = SamplePipeline.build("payments-prod")
    second_config = %{first_config | processors: [%{name: "redact", enabled: true}]}

    assert {:ok, first} = PipelineStore.put_pipeline(first_config, "test")
    assert {:ok, _second} = PipelineStore.put_pipeline(second_config, "test")

    assert {200, rollback_body} =
             post_json(port, "/v1/pipelines/rollback", %{
               tenant_id: "payments-prod",
               pipeline: "default",
               target_version: first.version,
               actor: "operator"
             })

    assert rollback_body["pipeline"]["tenant_id"] == "payments-prod"
    assert rollback_body["pipeline"]["name"] == "default"
    assert rollback_body["pipeline"]["version"] == 3

    assert {:ok, latest} = PipelineStore.get_pipeline("payments-prod", "default")
    assert latest.version == 3
    assert latest.processors == first.processors
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
    [pause_command] = heartbeat["commands"]

    assert {200, queued_resume} =
             post_json(port, "/v1/agents/commands", %{
               agent_id: "agent-1",
               kind: "resume_exports",
               reason: "maintenance complete"
             })

    assert queued_resume["command"]["kind"] == "resume_exports"

    assert {200, resumed_heartbeat} =
             post_json(port, "/v1/agents/heartbeat", %{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               config_version: 1,
               queue_depth_bytes: 0
             })

    assert [%{"kind" => "resume_exports", "reason" => "maintenance complete"}] =
             resumed_heartbeat["commands"]

    assert {200, status} =
             post_json(port, "/v1/agents/status", %{
               agent_id: "agent-1",
               tenant_id: "payments-prod"
             })

    assert status["status"]["healthy"] == true
    assert status["status"]["warnings"] == []

    assert {200, acked} =
             post_json(port, "/v1/agents/commands/ack", %{
               agent_id: "agent-1",
               tenant_id: "payments-prod",
               command_id: pause_command["command_id"],
               success: true
             })

    assert acked["command"]["status"] == "succeeded"
    assert is_binary(acked["command"]["acknowledged_at"])
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

    assert {400, body} =
             post_json(port, "/v1/pipelines/rollback", %{
               tenant_id: "payments-prod",
               pipeline: "default",
               target_version: 0
             })

    assert body["error"] == "invalid_integer"

    assert {400, body} =
             post_json(port, "/v1/pipelines", %{
               tenant_id: "payments-prod",
               pipeline: "default",
               receivers: [],
               exporters: [],
               routes: []
             })

    assert body["error"] == "empty_receivers"
  end

  test "serves health checks", %{port: port} do
    assert {200, %{"status" => "ok"}} = raw_request(port, "GET /healthz HTTP/1.1\r\n\r\n")
  end

  test "enforces agent and operator bearer tokens" do
    spec =
      Supervisor.child_spec(
        {HttpControlServer,
         [
           name: :auth_http_control_server,
           host: "127.0.0.1",
           port: 0,
           agent_token: "agent-token",
           operator_token: "operator-token"
         ]},
        id: :auth_http_control_server
      )

    server = start_supervised!(spec)
    port = HttpControlServer.port(server)

    assert {401, %{"error" => "missing_bearer_token"}} =
             post_json(port, "/v1/agents/register", %{
               agent_id: "agent-1",
               tenant_id: "payments-prod"
             })

    assert {200, _body} =
             post_json(
               port,
               "/v1/agents/register",
               %{
                 agent_id: "agent-1",
                 tenant_id: "payments-prod"
               },
               authorization: "Bearer agent-token"
             )

    assert {403, %{"error" => "invalid_bearer_token"}} =
             post_json(port, "/v1/pipelines", pipeline_payload("payments-prod"),
               authorization: "Bearer agent-token"
             )

    assert {200, _body} =
             post_json(port, "/v1/pipelines", pipeline_payload("payments-prod"),
               authorization: "Bearer operator-token"
             )
  end

  defp post_json(port, path, body, opts \\ []) do
    payload = Json.encode!(body)
    authorization = Keyword.get(opts, :authorization)
    auth_header = if authorization, do: "Authorization: #{authorization}\r\n", else: ""

    raw_request(
      port,
      [
        "POST #{path} HTTP/1.1\r\n",
        "Host: 127.0.0.1\r\n",
        auth_header,
        "Content-Type: application/json\r\n",
        "Content-Length: #{byte_size(payload)}\r\n",
        "\r\n",
        payload
      ]
    )
  end

  defp pipeline_payload(tenant_id) do
    %{
      tenant_id: tenant_id,
      pipeline: "default",
      actor: "operator",
      receivers: [
        %{name: "tf-line", protocol: "tf_line", endpoint: "127.0.0.1:4319"}
      ],
      processors: [
        %{name: "memory-limiter", enabled: true},
        %{name: "tenant-rate-limit", enabled: true}
      ],
      exporters: [
        %{name: "stdout", protocol: "stdout", endpoint: "stdout://local"}
      ],
      routes: [
        %{signal: "trace", exporters: ["stdout"]}
      ]
    }
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
