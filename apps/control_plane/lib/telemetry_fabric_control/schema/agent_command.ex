defmodule TelemetryFabricControl.Schema.AgentCommand do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @valid_kinds ["reload_config", "drain_and_restart", "pause_exports", "resume_exports"]
  @valid_statuses ["pending", "delivered"]

  @primary_key {:command_id, :string, autogenerate: false}
  schema "agent_commands" do
    field(:agent_id, :string)
    field(:tenant_id, :string)
    field(:kind, :string)
    field(:reason, :string, default: "")
    field(:status, :string, default: "pending")
    field(:inserted_at, :utc_datetime_usec)
    field(:delivered_at, :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :command_id,
      :agent_id,
      :tenant_id,
      :kind,
      :reason,
      :status,
      :inserted_at,
      :delivered_at
    ])
    |> validate_required([:command_id, :agent_id, :tenant_id, :kind, :status, :inserted_at])
    |> validate_inclusion(:kind, @valid_kinds)
    |> validate_inclusion(:status, @valid_statuses)
  end
end
