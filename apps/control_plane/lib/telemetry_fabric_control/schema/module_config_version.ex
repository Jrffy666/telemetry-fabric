defmodule TelemetryFabricControl.Schema.ModuleConfigVersion do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  schema "module_config_versions" do
    field(:tenant_id, :string, primary_key: true)
    field(:module_name, :string, primary_key: true)
    field(:version, :integer, primary_key: true)
    field(:config, :map)
    field(:checksum, :string)
    field(:updated_by, :string)

    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [:tenant_id, :module_name, :version, :config, :checksum, :updated_by])
    |> validate_required([:tenant_id, :module_name, :version, :config, :checksum, :updated_by])
    |> validate_number(:version, greater_than: 0)
    |> validate_length(:checksum, is: 64)
  end
end
