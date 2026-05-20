defmodule TelemetryFabricControl.Schema.AuditEvent do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  schema "audit_events" do
    field(:event_id, :integer)
    field(:actor, :string)
    field(:action, :string)
    field(:resource, :string)
    field(:metadata, :map, default: %{})
    field(:inserted_at, :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [:event_id, :actor, :action, :resource, :metadata, :inserted_at])
    |> validate_required([:event_id, :actor, :action, :resource, :inserted_at])
  end
end
