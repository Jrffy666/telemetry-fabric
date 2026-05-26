defmodule TelemetryFabricControl.Modules.ModuleConfigVersion do
  @moduledoc """
  Versioned business-module configuration.

  The config body is intentionally an opaque map. Domain-specific fields live
  in module namespaces and contracts, not in the platform core.
  """

  alias TelemetryFabricControl.Json
  alias TelemetryFabricControl.Modules.ModuleRegistration

  defstruct [
    :tenant_id,
    :module,
    :version,
    :config,
    :checksum,
    :updated_by,
    :inserted_at
  ]

  def new(attrs) when is_map(attrs) do
    with :ok <- ModuleRegistration.require_text(:tenant_id, attr(attrs, :tenant_id)),
         :ok <- ModuleRegistration.require_text(:module, attr(attrs, :module)),
         {:ok, version} <- require_positive_integer(:version, attr(attrs, :version)),
         {:ok, config} <- ModuleRegistration.optional_map(:config, attr(attrs, :config, %{})) do
      config = normalize_json(config)

      {:ok,
       %__MODULE__{
         tenant_id: String.trim(attr(attrs, :tenant_id)),
         module: String.trim(attr(attrs, :module)),
         version: version,
         config: config,
         checksum: attr(attrs, :checksum, checksum(config)),
         updated_by: ModuleRegistration.text_or_default(attr(attrs, :updated_by), "system"),
         inserted_at: attr(attrs, :inserted_at, ModuleRegistration.timestamp())
       }}
    end
  end

  def attr(attrs, key, default \\ nil), do: ModuleRegistration.attr(attrs, key, default)

  def checksum(config) when is_map(config) do
    config
    |> Json.encode!()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  def normalize_json(value) when is_map(value) do
    value
    |> Enum.map(fn {key, item} -> {to_string(key), normalize_json(item)} end)
    |> Map.new()
  end

  def normalize_json(value) when is_list(value), do: Enum.map(value, &normalize_json/1)
  def normalize_json(value) when is_atom(value), do: Atom.to_string(value)
  def normalize_json(value), do: value

  defp require_positive_integer(_field, value) when is_integer(value) and value > 0,
    do: {:ok, value}

  defp require_positive_integer(field, value), do: {:error, {:invalid_integer, field, value}}
end
