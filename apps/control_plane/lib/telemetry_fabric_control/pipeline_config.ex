defmodule TelemetryFabricControl.PipelineConfig do
  @moduledoc """
  Control-plane representation of a tenant pipeline.
  """

  defstruct [
    :tenant_id,
    :name,
    receivers: [],
    processors: [],
    exporters: [],
    routes: [],
    version: 0
  ]

  @type t :: %__MODULE__{
          tenant_id: String.t(),
          name: String.t(),
          receivers: list(map()),
          processors: list(map()),
          exporters: list(map()),
          routes: list(map()),
          version: non_neg_integer()
        }

  def validate(%__MODULE__{} = config) do
    with :ok <- require_text(:tenant_id, config.tenant_id),
         :ok <- require_text(:name, config.name),
         :ok <- require_non_empty(:receivers, config.receivers),
         :ok <- require_non_empty(:exporters, config.exporters),
         :ok <- require_non_empty(:routes, config.routes),
         :ok <- validate_route_exporters(config.routes, config.exporters) do
      :ok
    end
  end

  def validate(_), do: {:error, :invalid_pipeline_config}

  def to_agent_yaml(%__MODULE__{} = config) do
    :ok = validate(config)

    [
      "tenant: #{yaml_scalar(config.tenant_id)}",
      "pipeline: #{yaml_scalar(config.name)}",
      "",
      named_section("receivers", config.receivers, [:protocol, :endpoint]),
      named_section("processors", config.processors, [:enabled]),
      named_section("exporters", config.exporters, [:protocol, :endpoint, :tls]),
      route_section(config.routes)
    ]
    |> List.flatten()
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  defp require_text(field, value) when is_binary(value) do
    if String.trim(value) == "" do
      {:error, {:empty, field}}
    else
      :ok
    end
  end

  defp require_text(field, _), do: {:error, {:empty, field}}

  defp require_non_empty(_field, values) when is_list(values) and values != [], do: :ok
  defp require_non_empty(field, _), do: {:error, {:empty, field}}

  defp validate_route_exporters(routes, exporters) do
    exporter_names =
      exporters
      |> Enum.map(&Map.get(&1, :name))
      |> MapSet.new()

    routes
    |> Enum.flat_map(&Map.get(&1, :exporters, []))
    |> Enum.find(fn exporter -> not MapSet.member?(exporter_names, exporter) end)
    |> case do
      nil -> :ok
      exporter -> {:error, {:unknown_exporter, exporter}}
    end
  end

  defp named_section(title, entries, keys) do
    [
      "#{title}:",
      entries
      |> Enum.sort_by(&map_get(&1, :name))
      |> Enum.flat_map(fn entry ->
        name = map_get(entry, :name)

        [
          "  #{name}:",
          keys
          |> Enum.filter(&map_has_key?(entry, &1))
          |> Enum.map(fn key ->
            "    #{key}: #{yaml_scalar(map_get(entry, key))}"
          end)
        ]
      end),
      ""
    ]
  end

  defp route_section(routes) do
    [
      "routes:",
      routes
      |> Enum.sort_by(&map_get(&1, :signal))
      |> Enum.flat_map(fn route ->
        exporters =
          route
          |> map_get(:exporters)
          |> Enum.map(&yaml_scalar/1)
          |> Enum.join(", ")

        [
          "  #{map_get(route, :signal)}:",
          "    exporters: [#{exporters}]"
        ]
      end),
      ""
    ]
  end

  defp map_get(map, key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp map_has_key?(map, key) do
    Map.has_key?(map, key) || Map.has_key?(map, Atom.to_string(key))
  end

  defp yaml_scalar(value) when is_boolean(value), do: to_string(value)
  defp yaml_scalar(value) when is_integer(value), do: to_string(value)

  defp yaml_scalar(value) when is_binary(value) do
    escaped =
      value
      |> String.replace("\\", "\\\\")
      |> String.replace("\"", "\\\"")
      |> String.replace("\n", "\\n")

    "\"#{escaped}\""
  end
end
