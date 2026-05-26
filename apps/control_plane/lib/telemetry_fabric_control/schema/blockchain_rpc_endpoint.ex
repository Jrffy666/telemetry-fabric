defmodule TelemetryFabricControl.Schema.BlockchainRpcEndpoint do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  schema "blockchain_rpc_endpoints" do
    field(:tenant_id, :string, primary_key: true)
    field(:endpoint_id, :string, primary_key: true)
    field(:chain_key, :string)
    field(:url, :string)
    field(:priority, :integer, default: 100)
    field(:enabled, :boolean, default: true)
    field(:metadata, :map, default: %{})

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :tenant_id,
      :endpoint_id,
      :chain_key,
      :url,
      :priority,
      :enabled,
      :metadata
    ])
    |> validate_required([:tenant_id, :endpoint_id, :chain_key, :url, :priority, :enabled])
    |> validate_number(:priority, greater_than_or_equal_to: 0)
  end
end
