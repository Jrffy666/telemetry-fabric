defmodule TelemetryFabricControl.Schema.PipelineVersion do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  schema "pipeline_versions" do
    field(:tenant_id, :string, primary_key: true)
    field(:pipeline_name, :string, primary_key: true)
    field(:version, :integer, primary_key: true)
    field(:config, :map)
    field(:agent_yaml, :string)
    field(:checksum, :string)
    field(:updated_by, :string)

    timestamps(updated_at: false, type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :tenant_id,
      :pipeline_name,
      :version,
      :config,
      :agent_yaml,
      :checksum,
      :updated_by
    ])
    |> validate_required([
      :tenant_id,
      :pipeline_name,
      :version,
      :config,
      :agent_yaml,
      :checksum,
      :updated_by
    ])
    |> validate_number(:version, greater_than: 0)
    |> validate_length(:checksum, is: 64)
  end
end
