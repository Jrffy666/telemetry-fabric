defmodule TelemetryFabricControl.Modules.Control do
  @moduledoc """
  Generic business-module control API.

  This module is deliberately domain-neutral. It manages module registration and
  opaque versioned module configs; domain-specific APIs live in namespaces such
  as `TelemetryFabricControl.Modules.Blockchain`.
  """

  alias TelemetryFabricControl.Modules.PostgresStore
  alias TelemetryFabricControl.Modules.Store

  def register_module(attrs), do: store().register_module(attrs)
  def list_modules, do: store().list_modules()
  def get_module(module_name), do: store().get_module(module_name)
  def validate_config(attrs), do: store().validate_config(attrs)
  def dry_run_config(attrs), do: store().dry_run_config(attrs)
  def diff_config(attrs), do: store().diff_config(attrs)
  def publish_config(attrs), do: store().publish_config(attrs)
  def rollout_config(attrs), do: store().rollout_config(attrs)
  def fetch_config(attrs), do: store().fetch_config(attrs)
  def rollback_config(attrs), do: store().rollback_config(attrs)
  def list_versions(tenant_id, module_name), do: store().list_versions(tenant_id, module_name)

  defp store do
    if postgres_primary?(), do: PostgresStore, else: Store
  end

  defp postgres_primary? do
    System.get_env("TELEMETRY_FABRIC_CONTROL_STORAGE") == "postgres" or
      truthy_env?("TELEMETRY_FABRIC_CONTROL_POSTGRES_PRIMARY")
  end

  defp truthy_env?(name) do
    case System.get_env(name) do
      nil -> false
      value -> String.downcase(value) in ["1", "true", "on", "yes"]
    end
  end
end
