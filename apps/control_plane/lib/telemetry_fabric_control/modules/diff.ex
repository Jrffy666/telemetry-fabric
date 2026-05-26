defmodule TelemetryFabricControl.Modules.Diff do
  @moduledoc """
  Small deterministic config diff used by module config dry-runs.

  The diff is deliberately JSON-like so HTTP adapters can return it directly.
  """

  alias TelemetryFabricControl.Modules.ModuleConfigVersion

  def compare(old_config, new_config) when is_map(old_config) and is_map(new_config) do
    old_config = ModuleConfigVersion.normalize_json(old_config)
    new_config = ModuleConfigVersion.normalize_json(new_config)

    %{
      added: added(old_config, new_config),
      removed: removed(old_config, new_config),
      changed: changed(old_config, new_config)
    }
  end

  defp added(old_config, new_config) do
    new_config
    |> Map.keys()
    |> Enum.reject(&Map.has_key?(old_config, &1))
    |> Enum.sort()
  end

  defp removed(old_config, new_config) do
    old_config
    |> Map.keys()
    |> Enum.reject(&Map.has_key?(new_config, &1))
    |> Enum.sort()
  end

  defp changed(old_config, new_config) do
    old_config
    |> Map.keys()
    |> Enum.filter(
      &(Map.has_key?(new_config, &1) and Map.fetch!(old_config, &1) != Map.fetch!(new_config, &1))
    )
    |> Enum.sort()
  end
end
