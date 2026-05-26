defmodule TelemetryFabricControl.Schema.BlockchainFilterRule do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  schema "blockchain_filter_rules" do
    field(:tenant_id, :string, primary_key: true)
    field(:rule_id, :string, primary_key: true)
    field(:chain_key, :string)
    field(:name, :string)
    field(:expression, :map)
    field(:action, :string, default: "keep")
    field(:enabled, :boolean, default: true)
    field(:metadata, :map, default: %{})

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :tenant_id,
      :rule_id,
      :chain_key,
      :name,
      :expression,
      :action,
      :enabled,
      :metadata
    ])
    |> validate_required([:tenant_id, :rule_id, :name, :expression, :action, :enabled])
    |> validate_inclusion(:action, ["keep", "drop"])
  end
end
