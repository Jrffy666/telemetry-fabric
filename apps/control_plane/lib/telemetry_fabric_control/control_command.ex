defmodule TelemetryFabricControl.ControlCommand do
  @moduledoc """
  Control-plane command targeted at one registered data-plane agent.
  """

  @kinds [:reload_config, :drain_and_restart, :pause_exports]

  defstruct [
    :command_id,
    :agent_id,
    :tenant_id,
    :kind,
    :reason,
    :inserted_at
  ]

  @type kind :: :reload_config | :drain_and_restart | :pause_exports

  @type t :: %__MODULE__{
          command_id: String.t(),
          agent_id: String.t(),
          tenant_id: String.t(),
          kind: kind(),
          reason: String.t(),
          inserted_at: DateTime.t()
        }

  def new(attrs) when is_map(attrs) do
    kind = Map.fetch!(attrs, :kind)

    if kind not in @kinds do
      raise ArgumentError, "unknown control command kind: #{inspect(kind)}"
    end

    %__MODULE__{
      command_id: Map.get(attrs, :command_id, new_id()),
      agent_id: Map.fetch!(attrs, :agent_id),
      tenant_id: Map.fetch!(attrs, :tenant_id),
      kind: kind,
      reason: Map.get(attrs, :reason, ""),
      inserted_at: Map.get(attrs, :inserted_at, DateTime.utc_now())
    }
  end

  def kinds, do: @kinds

  defp new_id do
    "cmd-" <> (System.unique_integer([:positive, :monotonic]) |> Integer.to_string(36))
  end
end
