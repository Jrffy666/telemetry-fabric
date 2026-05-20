defmodule TelemetryFabricControl.PostgresSchema do
  @moduledoc """
  PostgreSQL schema artifact for the control plane.

  The runtime still uses OTP stores, but this module gives deployment and tests
  one stable place to load the first PostgreSQL migration.
  """

  @migration_path Path.expand("../../priv/postgres/001_control_plane_schema.sql", __DIR__)

  def migration_path, do: @migration_path

  def migration_sql do
    File.read!(@migration_path)
  end
end
