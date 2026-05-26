defmodule TelemetryFabricControl.Schema.BlockchainChain do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  schema "blockchain_chains" do
    field(:tenant_id, :string, primary_key: true)
    field(:chain_key, :string, primary_key: true)
    field(:display_name, :string)
    field(:network, :string)
    field(:enabled, :boolean, default: true)
    field(:metadata, :map, default: %{})

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [:tenant_id, :chain_key, :display_name, :network, :enabled, :metadata])
    |> validate_required([:tenant_id, :chain_key, :display_name, :network, :enabled])
  end
end
