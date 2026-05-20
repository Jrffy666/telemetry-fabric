defmodule TelemetryFabricControl.MixProject do
  use Mix.Project

  def project do
    [
      app: :telemetry_fabric_control,
      version: "0.1.0",
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {TelemetryFabricControl.Application, []}
    ]
  end

  defp deps do
    [
      {:ecto_sql, "~> 3.12"},
      {:postgrex, ">= 0.0.0"}
    ]
  end
end
