defmodule TelemetryFabricControl.PostgresSchema do
  @moduledoc """
  PostgreSQL schema artifact for the control plane.

  The runtime still uses OTP stores, but this module gives deployment and tests
  one stable place to load the first PostgreSQL migration.
  """

  @migration_file Path.join("postgres", "001_control_plane_schema.sql")
  @source_migration_path Path.expand("../../priv/postgres/001_control_plane_schema.sql", __DIR__)

  def migration_path do
    case :code.priv_dir(:telemetry_fabric_control) do
      priv_dir when is_list(priv_dir) -> Path.join(List.to_string(priv_dir), @migration_file)
      {:error, _reason} -> @source_migration_path
    end
  end

  def migration_sql do
    File.read!(migration_path())
  end
end
