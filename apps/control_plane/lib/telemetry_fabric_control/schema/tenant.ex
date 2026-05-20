defmodule TelemetryFabricControl.Schema.Tenant do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key {:tenant_id, :string, autogenerate: false}
  schema "tenants" do
    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [:tenant_id])
    |> validate_required([:tenant_id])
  end
end
