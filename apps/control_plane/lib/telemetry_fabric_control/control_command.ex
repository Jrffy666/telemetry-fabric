defmodule TelemetryFabricControl.ControlCommand do
  @moduledoc """
  Control-plane command targeted at one registered data-plane agent.
  """

  @kinds [:reload_config, :drain_and_restart, :pause_exports, :resume_exports]
  @statuses [:pending, :delivered]

  defstruct [
    :command_id,
    :agent_id,
    :tenant_id,
    :kind,
    :reason,
    :inserted_at,
    status: :pending,
    delivered_at: nil
  ]

  @type kind :: :reload_config | :drain_and_restart | :pause_exports | :resume_exports
  @type status :: :pending | :delivered

  @type t :: %__MODULE__{
          command_id: String.t(),
          agent_id: String.t(),
          tenant_id: String.t(),
          kind: kind(),
          reason: String.t(),
          inserted_at: DateTime.t(),
          status: status(),
          delivered_at: DateTime.t() | nil
        }

  def new(attrs) when is_map(attrs) do
    kind = Map.fetch!(attrs, :kind)

    if kind not in @kinds do
      raise ArgumentError, "unknown control command kind: #{inspect(kind)}"
    end

    status = Map.get(attrs, :status, :pending)

    if status not in @statuses do
      raise ArgumentError, "unknown control command status: #{inspect(status)}"
    end

    %__MODULE__{
      command_id: Map.get(attrs, :command_id, new_id()),
      agent_id: Map.fetch!(attrs, :agent_id),
      tenant_id: Map.fetch!(attrs, :tenant_id),
      kind: kind,
      reason: Map.get(attrs, :reason, ""),
      inserted_at: Map.get(attrs, :inserted_at, DateTime.utc_now()),
      status: status,
      delivered_at: Map.get(attrs, :delivered_at)
    }
  end

  def kinds, do: @kinds

  def pending?(%__MODULE__{} = command), do: Map.get(command, :status, :pending) == :pending

  def status(%__MODULE__{} = command), do: Map.get(command, :status, :pending)

  def delivered_at(%__MODULE__{} = command), do: Map.get(command, :delivered_at)

  def mark_delivered(%__MODULE__{} = command, delivered_at \\ DateTime.utc_now()) do
    command
    |> Map.from_struct()
    |> Map.put(:status, :delivered)
    |> Map.put(:delivered_at, delivered_at)
    |> then(&struct(__MODULE__, &1))
  end

  defp new_id do
    "cmd-" <> (System.unique_integer([:positive, :monotonic]) |> Integer.to_string(36))
  end
end
