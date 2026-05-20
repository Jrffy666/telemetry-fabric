defmodule Mix.Tasks.TelemetryFabric.Postgres.Migrate do
  @moduledoc """
  Runs the control-plane PostgreSQL schema migration.

      mix telemetry_fabric.postgres.migrate --database-url postgres://user:pass@localhost/db

  Use `--dry-run` to print the SQL statements without opening a database
  connection.
  """

  use Mix.Task

  alias TelemetryFabricControl.PostgresMigrator

  @shortdoc "Runs Telemetry Fabric control-plane PostgreSQL migrations"

  @impl true
  def run(args) do
    {opts, _rest, invalid} =
      OptionParser.parse(args, strict: [database_url: :string, dry_run: :boolean])

    if invalid != [] do
      Mix.raise("invalid options: #{inspect(invalid)}")
    end

    if database_url = opts[:database_url] do
      System.put_env("TELEMETRY_FABRIC_CONTROL_DATABASE_URL", database_url)
    end

    if opts[:dry_run] do
      PostgresMigrator.statements(TelemetryFabricControl.PostgresSchema.migration_sql())
      |> Enum.each(fn statement -> Mix.shell().info(statement) end)
    else
      ensure_database_url!()
      System.put_env("TELEMETRY_FABRIC_CONTROL_POSTGRES_SYNC", "false")
      Mix.Task.run("app.start")
      :ok = PostgresMigrator.migrate()
      Mix.shell().info("Telemetry Fabric control-plane PostgreSQL schema is up to date")
    end
  end

  defp ensure_database_url! do
    unless System.get_env("TELEMETRY_FABRIC_CONTROL_DATABASE_URL") do
      Mix.raise(
        "set TELEMETRY_FABRIC_CONTROL_DATABASE_URL or pass --database-url before running migrations"
      )
    end
  end
end
