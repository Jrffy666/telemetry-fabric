defmodule TelemetryFabricControl.Repo do
  @moduledoc """
  PostgreSQL repository for the production control-plane persistence adapter.

  The repository is only started by the application when
  `TELEMETRY_FABRIC_CONTROL_DATABASE_URL` is present, so the MVP can still run
  without an external database.
  """

  use Ecto.Repo,
    otp_app: :telemetry_fabric_control,
    adapter: Ecto.Adapters.Postgres

  @impl true
  def init(_type, config) do
    case System.get_env("TELEMETRY_FABRIC_CONTROL_DATABASE_URL") do
      nil -> {:ok, config}
      url -> {:ok, Keyword.put(config, :url, url)}
    end
  end
end
