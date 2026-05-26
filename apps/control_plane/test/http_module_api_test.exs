defmodule TelemetryFabricControl.HttpModuleApiTest do
  use ExUnit.Case

  alias TelemetryFabricControl.AuditLog
  alias TelemetryFabricControl.HttpControlServer
  alias TelemetryFabricControl.Json
  alias TelemetryFabricControl.Modules.Blockchain.Control, as: BlockchainControl
  alias TelemetryFabricControl.Modules.Blockchain.Store, as: BlockchainStore
  alias TelemetryFabricControl.Modules.Store, as: ModuleStore

  setup do
    AuditLog.clear()
    ModuleStore.clear()
    BlockchainStore.clear()
    TelemetryFabricControl.HttpMetrics.clear()

    server = start_supervised!({HttpControlServer, host: "127.0.0.1", port: 0})

    %{port: HttpControlServer.port(server)}
  end

  test "serves module registry, rollout, fetch, and rollback workflow", %{port: port} do
    assert {200, module_body} =
             post_json(port, "/v1/modules", %{
               module: "blockchain",
               display_name: "Blockchain",
               owner: "data-platform",
               actor: "operator"
             })

    assert module_body["module"]["module"] == "blockchain"

    assert {200, rollout_body} =
             post_json(port, "/v1/modules/configs/rollout", %{
               tenant_id: "payments-prod",
               module: "blockchain",
               actor: "operator",
               config: %{
                 chains: [%{chain_key: "ethereum-mainnet"}],
                 assignments: []
               }
             })

    assert rollout_body["config"]["version"] == 1
    assert byte_size(rollout_body["config"]["checksum"]) == 64

    assert {200, fetch_body} =
             post_json(port, "/v1/modules/configs/fetch", %{
               tenant_id: "payments-prod",
               module: "blockchain",
               current_version: 0
             })

    assert fetch_body["update"]["version"] == 1
    assert fetch_body["update"]["config"]["chains"] == [%{"chain_key" => "ethereum-mainnet"}]

    assert {200, current_body} =
             post_json(port, "/v1/modules/configs/fetch", %{
               tenant_id: "payments-prod",
               module: "blockchain",
               current_version: 1
             })

    assert current_body["update"] == nil

    assert {200, rollback_body} =
             post_json(port, "/v1/modules/configs/rollback", %{
               tenant_id: "payments-prod",
               module: "blockchain",
               target_version: 1,
               actor: "operator"
             })

    assert rollback_body["config"]["version"] == 2
  end

  test "serves hardened module config lifecycle endpoints", %{port: port} do
    assert {200, _module_body} =
             post_json(port, "/v1/modules", %{
               module: "blockchain",
               display_name: "Blockchain",
               owner: "data-platform"
             })

    assert {200, validation_body} =
             post_json(port, "/v1/modules/configs/validate", %{
               tenant_id: "payments-prod",
               module: "blockchain",
               config: %{rpc_endpoints: %{}}
             })

    refute validation_body["validation"]["valid"]
    assert "rpc_endpoints_must_be_list" in validation_body["validation"]["validation_errors"]

    assert {200, dry_run_body} =
             post_json(port, "/v1/modules/configs/dry-run", %{
               tenant_id: "payments-prod",
               module: "blockchain",
               config: %{chains: [%{chain_key: "ethereum-mainnet"}]}
             })

    assert dry_run_body["dry_run"]["valid"]
    assert dry_run_body["dry_run"]["dry_run"]
    assert dry_run_body["dry_run"]["next_version"] == 1
    assert dry_run_body["dry_run"]["diff"]["added"] == ["chains"]

    assert {403, approval_body} =
             post_json(port, "/v1/modules/configs/publish", %{
               tenant_id: "payments-prod",
               module: "blockchain",
               require_approval: true,
               config: %{chains: [%{chain_key: "ethereum-mainnet"}]}
             })

    assert approval_body["error"] == "approval_required"

    assert {200, publish_body} =
             post_json(port, "/v1/modules/configs/publish", %{
               tenant_id: "payments-prod",
               module: "blockchain",
               require_approval: true,
               approval_id: "approval-1",
               config: %{chains: [%{chain_key: "ethereum-mainnet"}]}
             })

    assert publish_body["config"]["version"] == 1

    assert {200, diff_body} =
             post_json(port, "/v1/modules/configs/diff", %{
               tenant_id: "payments-prod",
               module: "blockchain",
               config: %{
                 chains: [%{chain_key: "ethereum-mainnet"}, %{chain_key: "base-mainnet"}],
                 rpc_endpoints: []
               }
             })

    assert diff_body["diff"]["added"] == ["rpc_endpoints"]
    assert diff_body["diff"]["changed"] == ["chains"]

    assert {200, idempotent_body} =
             post_json(port, "/v1/modules/configs/publish", %{
               tenant_id: "payments-prod",
               module: "blockchain",
               config: %{chains: [%{chain_key: "ethereum-mainnet"}]}
             })

    assert idempotent_body["config"]["version"] == 1
  end

  test "serves blockchain CRUD endpoints and checkpoint reads", %{port: port} do
    resources = [
      {"/v1/modules/blockchain/chains", "chain_key", "ethereum-mainnet", "chain", "chains",
       %{
         tenant_id: "payments-prod",
         chain_key: "ethereum-mainnet",
         display_name: "Ethereum Mainnet",
         network: "mainnet"
       }},
      {"/v1/modules/blockchain/rpc-endpoints", "endpoint_id", "eth-primary", "rpc_endpoint",
       "rpc_endpoints",
       %{
         tenant_id: "payments-prod",
         endpoint_id: "eth-primary",
         chain_key: "ethereum-mainnet",
         url: "https://rpc.example.invalid",
         priority: 10
       }},
      {"/v1/modules/blockchain/address-watchlist", "entry_id", "treasury", "address_watch",
       "address_watchlist",
       %{
         tenant_id: "payments-prod",
         entry_id: "treasury",
         chain_key: "ethereum-mainnet",
         address: "0x0000000000000000000000000000000000000000"
       }},
      {"/v1/modules/blockchain/contract-watchlist", "contract_id", "usdc-contract",
       "contract_watch", "contract_watchlist",
       %{
         tenant_id: "payments-prod",
         contract_id: "usdc-contract",
         chain_key: "ethereum-mainnet",
         address: "0x0000000000000000000000000000000000000000"
       }},
      {"/v1/modules/blockchain/token-watchlist", "token_id", "usdc", "token_watch",
       "token_watchlist",
       %{
         tenant_id: "payments-prod",
         token_id: "usdc",
         chain_key: "ethereum-mainnet",
         contract_address: "0x0000000000000000000000000000000000000000",
         symbol: "USDC",
         decimals: 6
       }},
      {"/v1/modules/blockchain/filter-rules", "rule_id", "large-transfers", "filter_rule",
       "filter_rules",
       %{
         tenant_id: "payments-prod",
         rule_id: "large-transfers",
         name: "Large transfers",
         expression: %{"field" => "amount", "op" => "gte", "value" => "1000000"}
       }},
      {"/v1/modules/blockchain/crawl-assignments", "assignment_id", "crawler-a-eth",
       "crawl_assignment", "crawl_assignments",
       %{
         tenant_id: "payments-prod",
         assignment_id: "crawler-a-eth",
         chain_key: "ethereum-mainnet",
         crawler_id: "crawler-a",
         start_cursor: %{"block" => 1}
       }}
    ]

    Enum.each(resources, fn {path, id_field, id, item_key, list_key, payload} ->
      assert {200, create_body} = post_json(port, path, payload)
      assert create_body[item_key][id_field] == id

      assert {200, list_body} =
               raw_request(port, "GET #{path}?tenant_id=payments-prod HTTP/1.1\r\n\r\n")

      assert [%{^id_field => ^id}] = list_body[list_key]

      assert {200, get_body} =
               raw_request(port, "GET #{path}/#{id}?tenant_id=payments-prod HTTP/1.1\r\n\r\n")

      assert get_body[item_key][id_field] == id

      assert {200, %{"deleted" => true}} =
               raw_request(port, "DELETE #{path}/#{id}?tenant_id=payments-prod HTTP/1.1\r\n\r\n")
    end)

    assert {:ok, _checkpoint} =
             BlockchainControl.put_checkpoint(%{
               tenant_id: "payments-prod",
               assignment_id: "crawler-a-eth",
               chain_key: "ethereum-mainnet",
               cursor: %{"block" => 123},
               updated_by: "crawler-a"
             })

    assert {200, checkpoint_body} =
             raw_request(
               port,
               "GET /v1/modules/blockchain/checkpoints/crawler-a-eth?tenant_id=payments-prod HTTP/1.1\r\n\r\n"
             )

    assert checkpoint_body["checkpoint"]["cursor"]["block"] == 123
  end

  test "hardens blockchain HTTP resources with redaction, pagination, and id checks", %{
    port: port
  } do
    assert {200, rpc_body} =
             post_json(port, "/v1/modules/blockchain/rpc-endpoints", %{
               tenant_id: "payments-prod",
               endpoint_id: "eth-primary",
               chain_key: "ethereum-mainnet",
               url: "https://user:secret@rpc.example.invalid/path?api_key=super-secret&region=us",
               priority: 10
             })

    refute String.contains?(rpc_body["rpc_endpoint"]["url"], "super-secret")
    refute String.contains?(rpc_body["rpc_endpoint"]["url"], "user:secret")
    assert String.contains?(rpc_body["rpc_endpoint"]["url"], "REDACTED")

    chains = [
      {"base-mainnet", "Base Mainnet"},
      {"ethereum-mainnet", "Ethereum Mainnet"},
      {"polygon-mainnet", "Polygon Mainnet"}
    ]

    Enum.each(chains, fn {chain_key, display_name} ->
      assert {200, _body} =
               post_json(port, "/v1/modules/blockchain/chains", %{
                 tenant_id: "payments-prod",
                 chain_key: chain_key,
                 display_name: display_name,
                 network: "mainnet"
               })
    end)

    assert {200, _body} =
             post_json(port, "/v1/modules/blockchain/chains", %{
               tenant_id: "risk-prod",
               chain_key: "ethereum-mainnet",
               display_name: "Ethereum Mainnet",
               network: "mainnet"
             })

    assert {200, page_body} =
             raw_request(
               port,
               "GET /v1/modules/blockchain/chains?tenant_id=payments-prod&limit=2&offset=1 HTTP/1.1\r\n\r\n"
             )

    assert Enum.map(page_body["chains"], & &1["chain_key"]) == [
             "ethereum-mainnet",
             "polygon-mainnet"
           ]

    assert page_body["pagination"] == %{"limit" => 2, "offset" => 1, "returned" => 2}

    assert {404, %{"error" => "not_found"}} =
             raw_request(
               port,
               "GET /v1/modules/blockchain/chains/base-mainnet?tenant_id=risk-prod HTTP/1.1\r\n\r\n"
             )

    assert {400, mismatch_body} =
             put_json(port, "/v1/modules/blockchain/chains/base-mainnet", %{
               tenant_id: "payments-prod",
               chain_key: "ethereum-mainnet",
               display_name: "Ethereum Mainnet",
               network: "mainnet"
             })

    assert mismatch_body["error"] == "path_id_mismatch"
    assert mismatch_body["field"] == "chain_key"
  end

  test "reuses existing bearer-token auth roles for module APIs" do
    spec =
      Supervisor.child_spec(
        {HttpControlServer,
         [
           name: :module_auth_http_control_server,
           host: "127.0.0.1",
           port: 0,
           agent_token: "agent-token",
           operator_token: "operator-token"
         ]},
        id: :module_auth_http_control_server
      )

    server = start_supervised!(spec)
    port = HttpControlServer.port(server)

    assert {401, %{"error" => "missing_bearer_token"}} =
             post_json(port, "/v1/modules", %{module: "blockchain"})

    assert {403, %{"error" => "invalid_bearer_token"}} =
             post_json(port, "/v1/modules", %{module: "blockchain"},
               authorization: "Bearer agent-token"
             )

    assert {200, _body} =
             post_json(port, "/v1/modules", %{module: "blockchain"},
               authorization: "Bearer operator-token"
             )

    assert {200, _body} =
             post_json(
               port,
               "/v1/modules/configs/rollout",
               %{
                 tenant_id: "payments-prod",
                 module: "blockchain",
                 config: %{chains: []}
               },
               authorization: "Bearer operator-token"
             )

    assert {200, fetch_body} =
             post_json(
               port,
               "/v1/modules/configs/fetch",
               %{
                 tenant_id: "payments-prod",
                 module: "blockchain",
                 current_version: 0
               },
               authorization: "Bearer agent-token"
             )

    assert fetch_body["update"]["version"] == 1
  end

  test "rate limits write-heavy module API paths" do
    spec =
      Supervisor.child_spec(
        {HttpControlServer,
         [
           name: :module_rate_limit_http_control_server,
           host: "127.0.0.1",
           port: 0,
           rate_limit_per_second: 1
         ]},
        id: :module_rate_limit_http_control_server
      )

    server = start_supervised!(spec)
    port = HttpControlServer.port(server)

    statuses =
      Enum.map(1..3, fn index ->
        {status, _body} =
          post_json(port, "/v1/modules", %{
            module: "blockchain-#{index}",
            display_name: "Blockchain #{index}"
          })

        status
      end)

    assert 429 in statuses
  end

  defp post_json(port, path, body, opts \\ []) do
    json_request(port, "POST", path, body, opts)
  end

  defp put_json(port, path, body, opts \\ []) do
    json_request(port, "PUT", path, body, opts)
  end

  defp json_request(port, method, path, body, opts) do
    payload = Json.encode!(body)
    authorization = Keyword.get(opts, :authorization)
    auth_header = if authorization, do: "Authorization: #{authorization}\r\n", else: ""

    raw_request(
      port,
      [
        "#{method} #{path} HTTP/1.1\r\n",
        "Host: 127.0.0.1\r\n",
        auth_header,
        "Content-Type: application/json\r\n",
        "Content-Length: #{byte_size(payload)}\r\n",
        "\r\n",
        payload
      ]
    )
  end

  defp raw_request(port, request) do
    {code, _headers, body} = raw_request_with_headers(port, request)
    {code, body}
  end

  defp raw_request_with_headers(port, request) do
    {code, headers, body} = raw_text_request(port, request)
    {code, headers, Json.decode!(body)}
  end

  defp raw_text_request(port, request) do
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])
    :ok = :gen_tcp.send(socket, request)
    response = recv_all(socket, "")
    parse_text_response(response)
  end

  defp recv_all(socket, acc) do
    case :gen_tcp.recv(socket, 0, 1_000) do
      {:ok, chunk} -> recv_all(socket, acc <> chunk)
      {:error, :closed} -> acc
    end
  end

  defp parse_text_response(response) do
    [head, body] = String.split(response, "\r\n\r\n", parts: 2)
    ["HTTP/1.1 " <> status | header_lines] = String.split(head, "\r\n")
    [code | _] = String.split(status, " ", parts: 2)
    headers = parse_headers(header_lines)
    {String.to_integer(code), headers, body}
  end

  defp parse_headers(header_lines) do
    Enum.reduce(header_lines, %{}, fn line, headers ->
      case String.split(line, ":", parts: 2) do
        [name, value] -> Map.put(headers, String.downcase(String.trim(name)), String.trim(value))
        _ -> headers
      end
    end)
  end
end
