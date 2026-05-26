defmodule TelemetryFabricControl.Modules.Blockchain.ControlTest do
  use ExUnit.Case

  alias TelemetryFabricControl.AuditLog
  alias TelemetryFabricControl.Modules.Blockchain.Control
  alias TelemetryFabricControl.Modules.Blockchain.Store

  setup do
    AuditLog.clear()
    Store.clear()
    :ok
  end

  test "manages blockchain module resources without touching platform core fields" do
    resources = [
      {:chain, &Control.upsert_chain/2, &Control.list_chains/1, &Control.get_chain/2,
       &Control.delete_chain/3, "ethereum-mainnet",
       %{
         tenant_id: "payments-prod",
         chain_key: "ethereum-mainnet",
         display_name: "Ethereum Mainnet",
         network: "mainnet"
       }},
      {:rpc_endpoint, &Control.upsert_rpc_endpoint/2, &Control.list_rpc_endpoints/1,
       &Control.get_rpc_endpoint/2, &Control.delete_rpc_endpoint/3, "eth-primary",
       %{
         tenant_id: "payments-prod",
         endpoint_id: "eth-primary",
         chain_key: "ethereum-mainnet",
         url: "https://rpc.example.invalid",
         priority: 10
       }},
      {:address_watch, &Control.upsert_address_watch/2, &Control.list_address_watchlist/1,
       &Control.get_address_watch/2, &Control.delete_address_watch/3, "treasury",
       %{
         tenant_id: "payments-prod",
         entry_id: "treasury",
         chain_key: "ethereum-mainnet",
         address: "0x0000000000000000000000000000000000000000"
       }},
      {:contract_watch, &Control.upsert_contract_watch/2, &Control.list_contract_watchlist/1,
       &Control.get_contract_watch/2, &Control.delete_contract_watch/3, "usdc-contract",
       %{
         tenant_id: "payments-prod",
         contract_id: "usdc-contract",
         chain_key: "ethereum-mainnet",
         address: "0x0000000000000000000000000000000000000000",
         abi_ref: "s3://abis/usdc.json"
       }},
      {:token_watch, &Control.upsert_token_watch/2, &Control.list_token_watchlist/1,
       &Control.get_token_watch/2, &Control.delete_token_watch/3, "usdc",
       %{
         tenant_id: "payments-prod",
         token_id: "usdc",
         chain_key: "ethereum-mainnet",
         contract_address: "0x0000000000000000000000000000000000000000",
         symbol: "USDC",
         decimals: 6
       }},
      {:filter_rule, &Control.upsert_filter_rule/2, &Control.list_filter_rules/1,
       &Control.get_filter_rule/2, &Control.delete_filter_rule/3, "large-transfers",
       %{
         tenant_id: "payments-prod",
         rule_id: "large-transfers",
         name: "Large transfers",
         expression: %{"field" => "amount", "op" => "gte", "value" => "1000000"}
       }},
      {:crawl_assignment, &Control.upsert_crawl_assignment/2, &Control.list_crawl_assignments/1,
       &Control.get_crawl_assignment/2, &Control.delete_crawl_assignment/3, "crawler-a-eth",
       %{
         tenant_id: "payments-prod",
         assignment_id: "crawler-a-eth",
         chain_key: "ethereum-mainnet",
         crawler_id: "crawler-a",
         start_cursor: %{"block" => 1}
       }}
    ]

    Enum.each(resources, fn {_name, upsert, list, get, delete, id, payload} ->
      assert {:ok, record} = upsert.(payload, "operator")
      assert record["tenant_id"] == "payments-prod"
      assert {:ok, [^record]} = list.("payments-prod")
      assert {:ok, ^record} = get.("payments-prod", id)
      assert :ok = delete.("payments-prod", id, "operator")
      assert {:error, :not_found} = get.("payments-prod", id)
    end)

    audit_actions = AuditLog.list(:all) |> Enum.map(& &1.action)
    assert "blockchain.chain.upserted" in audit_actions
    assert "blockchain.crawl_assignment.deleted" in audit_actions
  end

  test "exposes checkpoint read state for crawlers and operators" do
    assert {:ok, checkpoint} =
             Control.put_checkpoint(%{
               tenant_id: "payments-prod",
               assignment_id: "crawler-a-eth",
               chain_key: "ethereum-mainnet",
               cursor: %{"block" => 123, "hash" => "0xabc"},
               finalized_cursor: %{"block" => 120},
               updated_by: "crawler-a"
             })

    assert checkpoint["cursor"]["block"] == 123

    assert {:ok, ^checkpoint} = Control.get_checkpoint("payments-prod", "crawler-a-eth")
    assert {:ok, [^checkpoint]} = Control.list_checkpoints("payments-prod")
  end

  test "enforces validation, tenant isolation, pagination, redaction, and idempotency" do
    assert {:error, {:validation_failed, errors}} =
             Control.upsert_address_watch(
               %{
                 tenant_id: "payments-prod",
                 entry_id: "bad-address",
                 chain_key: "ethereum-mainnet",
                 address: "not-an-address"
               },
               "operator"
             )

    assert "address_invalid" in errors

    assert {:ok, first_chain} =
             Control.upsert_chain(%{
               tenant_id: "payments-prod",
               chain_key: "base-mainnet",
               display_name: "Base Mainnet",
               network: "mainnet"
             })

    audit_count = AuditLog.list(:all) |> length()

    assert {:ok, replayed_chain} =
             Control.upsert_chain(%{
               tenant_id: "payments-prod",
               chain_key: "base-mainnet",
               display_name: "Base Mainnet",
               network: "mainnet"
             })

    assert replayed_chain["inserted_at"] == first_chain["inserted_at"]
    assert AuditLog.list(:all) |> length() == audit_count

    assert {:ok, _} =
             Control.upsert_chain(%{
               tenant_id: "payments-prod",
               chain_key: "ethereum-mainnet",
               display_name: "Ethereum Mainnet",
               network: "mainnet"
             })

    assert {:ok, _} =
             Control.upsert_chain(%{
               tenant_id: "payments-prod",
               chain_key: "polygon-mainnet",
               display_name: "Polygon Mainnet",
               network: "mainnet"
             })

    assert {:ok, _} =
             Control.upsert_chain(%{
               tenant_id: "risk-prod",
               chain_key: "ethereum-mainnet",
               display_name: "Ethereum Mainnet",
               network: "mainnet"
             })

    assert {:ok, page} = Control.list_chains("payments-prod", limit: 2, offset: 1)
    assert Enum.map(page, & &1["chain_key"]) == ["ethereum-mainnet", "polygon-mainnet"]

    assert {:ok, risk_page} = Control.list_chains("risk-prod")
    assert Enum.map(risk_page, & &1["tenant_id"]) == ["risk-prod"]
    assert {:error, :not_found} = Control.get_chain("risk-prod", "base-mainnet")

    assert {:ok, rpc_endpoint} =
             Control.upsert_rpc_endpoint(%{
               tenant_id: "payments-prod",
               endpoint_id: "eth-primary",
               chain_key: "ethereum-mainnet",
               url: "https://user:secret@rpc.example.invalid/path?api_key=super-secret&region=us",
               priority: 10
             })

    refute String.contains?(rpc_endpoint["url"], "super-secret")
    refute String.contains?(rpc_endpoint["url"], "user:secret")
    assert String.contains?(rpc_endpoint["url"], "REDACTED")
  end
end
