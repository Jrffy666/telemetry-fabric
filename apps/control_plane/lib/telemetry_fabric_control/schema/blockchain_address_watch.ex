defmodule TelemetryFabricControl.Schema.BlockchainAddressWatch do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  schema "blockchain_address_watchlist" do
    field(:tenant_id, :string, primary_key: true)
    field(:entry_id, :string, primary_key: true)
    field(:chain_key, :string)
    field(:address, :string)
    field(:label, :string)
    field(:enabled, :boolean, default: true)
    field(:metadata, :map, default: %{})

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [:tenant_id, :entry_id, :chain_key, :address, :label, :enabled, :metadata])
    |> validate_required([:tenant_id, :entry_id, :chain_key, :address, :enabled])
  end
end
