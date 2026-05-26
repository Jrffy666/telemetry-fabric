defmodule TelemetryFabricControl.Modules.ControlTest do
  use ExUnit.Case

  alias TelemetryFabricControl.AuditLog
  alias TelemetryFabricControl.Modules.ConfigPlan
  alias TelemetryFabricControl.Modules.Control
  alias TelemetryFabricControl.Modules.ModuleConfigVersion
  alias TelemetryFabricControl.Modules.ModuleRegistration
  alias TelemetryFabricControl.Modules.Store

  setup do
    AuditLog.clear()
    Store.clear()
    :ok
  end

  test "registers modules and manages versioned module configs" do
    assert {:ok, %ModuleRegistration{} = registration} =
             Control.register_module(%{
               module: "blockchain",
               display_name: "Blockchain",
               owner: "data-platform",
               description: "multi-chain collection config",
               actor: "operator"
             })

    assert registration.module == "blockchain"
    assert registration.enabled

    assert [%ModuleRegistration{module: "blockchain"}] = Control.list_modules()

    assert {:ok, %ModuleConfigVersion{} = first} =
             Control.rollout_config(%{
               tenant_id: "payments-prod",
               module: "blockchain",
               actor: "operator",
               config: %{
                 chains: [%{chain_key: "ethereum-mainnet"}],
                 assignments: []
               }
             })

    assert first.version == 1
    assert byte_size(first.checksum) == 64

    assert {:ok, %ModuleConfigVersion{version: 1}} =
             Control.fetch_config(%{
               tenant_id: "payments-prod",
               module: "blockchain",
               current_version: 0
             })

    assert {:ok, :up_to_date} =
             Control.fetch_config(%{
               tenant_id: "payments-prod",
               module: "blockchain",
               current_version: 1
             })

    assert {:ok, second} =
             Control.rollout_config(%{
               tenant_id: "payments-prod",
               module: "blockchain",
               actor: "operator",
               config: %{
                 chains: [%{chain_key: "ethereum-mainnet"}, %{chain_key: "base-mainnet"}]
               }
             })

    assert second.version == 2

    assert {:ok, rollback} =
             Control.rollback_config(%{
               tenant_id: "payments-prod",
               module: "blockchain",
               target_version: first.version,
               actor: "operator"
             })

    assert rollback.version == 3
    assert rollback.config == first.config

    assert Enum.map(Control.list_versions("payments-prod", "blockchain"), & &1.version) ==
             [1, 2, 3]

    audit_actions = AuditLog.list(:all) |> Enum.map(& &1.action)
    assert "module.registered" in audit_actions
    assert "module_config.published" in audit_actions
    assert "module_config.rolled_out" in audit_actions
    assert "module_config.rolled_back" in audit_actions
  end

  test "validates, dry-runs, diffs, publishes, and idempotently replays module configs" do
    assert {:ok, _registration} =
             Control.register_module(%{module: "blockchain", display_name: "Blockchain"})

    assert {:ok, %ConfigPlan{} = invalid} =
             Control.validate_config(%{
               tenant_id: "payments-prod",
               module: "blockchain",
               config: %{rpc_endpoints: %{}}
             })

    refute invalid.valid
    assert "rpc_endpoints_must_be_list" in invalid.validation_errors

    assert {:error, {:validation_failed, errors}} =
             Control.publish_config(%{
               tenant_id: "payments-prod",
               module: "blockchain",
               config: %{rpc_endpoints: %{}}
             })

    assert "rpc_endpoints_must_be_list" in errors

    assert {:ok, %ConfigPlan{} = dry_run} =
             Control.dry_run_config(%{
               tenant_id: "payments-prod",
               module: "blockchain",
               config: %{chains: [%{chain_key: "ethereum-mainnet"}]}
             })

    assert dry_run.valid
    assert dry_run.dry_run
    assert dry_run.next_version == 1
    assert dry_run.diff.added == ["chains"]
    assert dry_run.approval.required == false

    assert {:error, {:approval_required, approval}} =
             Control.publish_config(%{
               tenant_id: "payments-prod",
               module: "blockchain",
               require_approval: true,
               config: %{chains: [%{chain_key: "ethereum-mainnet"}]}
             })

    assert approval.required
    refute approval.approved

    assert {:ok, first} =
             Control.publish_config(%{
               tenant_id: "payments-prod",
               module: "blockchain",
               require_approval: true,
               approval_id: "approval-1",
               config: %{chains: [%{chain_key: "ethereum-mainnet"}]}
             })

    assert first.version == 1

    assert {:ok, diff} =
             Control.diff_config(%{
               tenant_id: "payments-prod",
               module: "blockchain",
               config: %{
                 chains: [%{chain_key: "ethereum-mainnet"}, %{chain_key: "base-mainnet"}],
                 rpc_endpoints: []
               }
             })

    assert diff.added == ["rpc_endpoints"]
    assert diff.changed == ["chains"]

    assert {:ok, same} =
             Control.publish_config(%{
               tenant_id: "payments-prod",
               module: "blockchain",
               config: %{chains: [%{chain_key: "ethereum-mainnet"}]}
             })

    assert same.version == first.version

    audit_actions = AuditLog.list(:all) |> Enum.map(& &1.action)
    assert "module_config.published" in audit_actions
    assert "module_config.publish_idempotent" in audit_actions
  end

  test "rejects module config rollout before registration" do
    assert {:error, {:module_not_registered, "blockchain"}} =
             Control.rollout_config(%{
               tenant_id: "payments-prod",
               module: "blockchain",
               config: %{}
             })
  end

  test "persists module registry and config versions across store restarts" do
    dir = tmp_dir("module-store")
    path = Path.join(dir, "modules.term")
    name = unique_name("module_store")
    restarted_name = unique_name("module_store")

    {:ok, pid} = Store.start_link(name: name, storage_path: path)

    assert {:ok, _registration} =
             Store.register_module(name, %{module: "blockchain", display_name: "Blockchain"})

    assert {:ok, version} =
             Store.rollout_config(name, %{
               tenant_id: "payments-prod",
               module: "blockchain",
               config: %{chains: []}
             })

    assert version.version == 1

    GenServer.stop(pid)
    {:ok, _pid} = Store.start_link(name: restarted_name, storage_path: path)

    assert {:ok, %ModuleRegistration{module: "blockchain"}} =
             Store.get_module(restarted_name, "blockchain")

    assert {:ok, %ModuleConfigVersion{version: 1}} =
             Store.get_latest_config(restarted_name, "payments-prod", "blockchain")
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
