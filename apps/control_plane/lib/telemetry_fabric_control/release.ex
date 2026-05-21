defmodule TelemetryFabricControl.Release do
  @moduledoc """
  Release-time helpers for container and orchestration entrypoints.

  These functions are intentionally small wrappers around runtime modules so
  deployments can run one-off operational tasks without depending on Mix.
  """

  @app :telemetry_fabric_control

  def migrate(repo \\ TelemetryFabricControl.Repo) do
    load_app()
    ensure_database_url!()
    {:ok, _apps} = Application.ensure_all_started(:ecto_sql)
    {pid, owned?} = start_repo(repo)

    try do
      TelemetryFabricControl.PostgresMigrator.migrate(repo)
    after
      if owned? and Process.alive?(pid) do
        Supervisor.stop(pid)
      end
    end
  end

  defp load_app do
    case Application.load(@app) do
      :ok -> :ok
      {:error, {:already_loaded, _app}} -> :ok
    end
  end

  defp ensure_database_url! do
    case System.get_env("TELEMETRY_FABRIC_CONTROL_DATABASE_URL") do
      nil -> raise "set TELEMETRY_FABRIC_CONTROL_DATABASE_URL before running migrations"
      "" -> raise "set TELEMETRY_FABRIC_CONTROL_DATABASE_URL before running migrations"
      _url -> :ok
    end
  end

  defp start_repo(repo) do
    case repo.start_link() do
      {:ok, pid} -> {pid, true}
      {:error, {:already_started, pid}} -> {pid, false}
    end
  end
end
