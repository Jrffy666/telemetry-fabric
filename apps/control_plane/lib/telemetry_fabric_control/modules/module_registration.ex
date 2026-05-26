defmodule TelemetryFabricControl.Modules.ModuleRegistration do
  @moduledoc """
  Business-module registration record.

  Module registrations describe optional domain modules without making the
  control-plane core business-specific.
  """

  defstruct [
    :module,
    :display_name,
    :owner,
    :description,
    :enabled,
    :metadata,
    :inserted_at,
    :updated_at
  ]

  def new(attrs) when is_map(attrs) do
    with :ok <- require_text(:module, attr(attrs, :module)),
         {:ok, enabled} <- optional_boolean(:enabled, attr(attrs, :enabled, true)),
         {:ok, metadata} <- optional_map(:metadata, attr(attrs, :metadata, %{})) do
      now = timestamp()
      module_name = String.trim(attr(attrs, :module))

      {:ok,
       %__MODULE__{
         module: module_name,
         display_name: text_or_default(attr(attrs, :display_name), module_name),
         owner: text_or_default(attr(attrs, :owner), "platform"),
         description: text_or_default(attr(attrs, :description), ""),
         enabled: enabled,
         metadata: metadata,
         inserted_at: attr(attrs, :inserted_at, now),
         updated_at: attr(attrs, :updated_at, now)
       }}
    end
  end

  def attr(attrs, key, default \\ nil) do
    Map.get(attrs, key, Map.get(attrs, Atom.to_string(key), default))
  end

  def require_text(field, value) when is_binary(value) do
    if String.trim(value) == "" do
      {:error, {:empty, field}}
    else
      :ok
    end
  end

  def require_text(field, _value), do: {:error, {:empty, field}}

  def optional_boolean(_field, value) when is_boolean(value), do: {:ok, value}
  def optional_boolean(field, value), do: {:error, {:invalid_boolean, field, value}}

  def optional_map(_field, value) when is_map(value), do: {:ok, value}
  def optional_map(field, value), do: {:error, {:invalid_map, field, value}}

  def text_or_default(value, default) when is_binary(value) do
    value = String.trim(value)
    if value == "", do: default, else: value
  end

  def text_or_default(_value, default), do: default

  def timestamp, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)
end
