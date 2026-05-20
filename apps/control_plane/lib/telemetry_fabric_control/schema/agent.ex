defmodule TelemetryFabricControl.Schema.Agent do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:agent_id, :string, autogenerate: false}
  schema "agents" do
    field(:tenant_id, :string)
    field(:hostname, :string)
    field(:version, :string)
    field(:config_version, :integer, default: 0)
    field(:queue_depth_bytes, :integer, default: 0)
    field(:ingest_bytes_per_second, :integer, default: 0)
    field(:labels, :map, default: %{})
    field(:last_seen_at, :utc_datetime_usec)

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :agent_id,
      :tenant_id,
      :hostname,
      :version,
      :config_version,
      :queue_depth_bytes,
      :ingest_bytes_per_second,
      :labels,
      :last_seen_at
    ])
    |> validate_required([:agent_id, :tenant_id, :hostname, :version, :last_seen_at])
    |> validate_number(:config_version, greater_than_or_equal_to: 0)
    |> validate_number(:queue_depth_bytes, greater_than_or_equal_to: 0)
    |> validate_number(:ingest_bytes_per_second, greater_than_or_equal_to: 0)
  end
end
