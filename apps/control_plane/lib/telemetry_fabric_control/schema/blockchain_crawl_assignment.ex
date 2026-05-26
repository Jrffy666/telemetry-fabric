defmodule TelemetryFabricControl.Schema.BlockchainCrawlAssignment do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  schema "blockchain_crawl_assignments" do
    field(:tenant_id, :string, primary_key: true)
    field(:assignment_id, :string, primary_key: true)
    field(:chain_key, :string)
    field(:crawler_id, :string)
    field(:start_cursor, :map, default: %{})
    field(:end_cursor, :map)
    field(:config, :map, default: %{})
    field(:enabled, :boolean, default: true)
    field(:metadata, :map, default: %{})

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :tenant_id,
      :assignment_id,
      :chain_key,
      :crawler_id,
      :start_cursor,
      :end_cursor,
      :config,
      :enabled,
      :metadata
    ])
    |> validate_required([:tenant_id, :assignment_id, :chain_key, :crawler_id, :enabled])
  end
end
