defmodule TelemetryFabricControl.Schema.BlockchainCheckpoint do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  schema "blockchain_checkpoints" do
    field(:tenant_id, :string, primary_key: true)
    field(:assignment_id, :string, primary_key: true)
    field(:chain_key, :string)
    field(:cursor, :map, default: %{})
    field(:finalized_cursor, :map, default: %{})
    field(:updated_by, :string)

    timestamps(inserted_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :tenant_id,
      :assignment_id,
      :chain_key,
      :cursor,
      :finalized_cursor,
      :updated_by
    ])
    |> validate_required([:tenant_id, :assignment_id, :chain_key, :cursor, :updated_by])
  end
end
