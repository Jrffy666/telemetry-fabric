defmodule TelemetryFabricControl.Schema.BlockchainTokenWatch do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  schema "blockchain_token_watchlist" do
    field(:tenant_id, :string, primary_key: true)
    field(:token_id, :string, primary_key: true)
    field(:chain_key, :string)
    field(:contract_address, :string)
    field(:symbol, :string)
    field(:decimals, :integer)
    field(:enabled, :boolean, default: true)
    field(:metadata, :map, default: %{})

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :tenant_id,
      :token_id,
      :chain_key,
      :contract_address,
      :symbol,
      :decimals,
      :enabled,
      :metadata
    ])
    |> validate_required([:tenant_id, :token_id, :chain_key, :contract_address, :enabled])
    |> validate_number(:decimals, greater_than_or_equal_to: 0)
  end
end
