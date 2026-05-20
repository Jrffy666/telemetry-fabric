defmodule TelemetryFabricControl.PostgresMigrator do
  @moduledoc """
  Minimal PostgreSQL migration runner for the first control-plane schema.
  """

  alias TelemetryFabricControl.PostgresSchema
  alias TelemetryFabricControl.Repo

  def migrate(repo \\ Repo) do
    PostgresSchema.migration_sql()
    |> statements()
    |> Enum.each(fn statement ->
      Ecto.Adapters.SQL.query!(repo, statement, [])
    end)

    :ok
  end

  def statements(sql) when is_binary(sql) do
    sql
    |> String.split("\n")
    |> Enum.reject(&(String.trim(&1) == "" or String.starts_with?(String.trim(&1), "--")))
    |> Enum.join("\n")
    |> String.split(";")
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
  end
end
