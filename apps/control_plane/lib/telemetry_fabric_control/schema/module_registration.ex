defmodule TelemetryFabricControl.Schema.ModuleRegistration do
  @moduledoc false

  use Ecto.Schema

  import Ecto.Changeset

  @primary_key false
  schema "module_registry" do
    field(:module_name, :string, primary_key: true)
    field(:display_name, :string)
    field(:owner, :string)
    field(:description, :string)
    field(:enabled, :boolean, default: true)
    field(:metadata, :map, default: %{})

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(struct \\ %__MODULE__{}, attrs) do
    struct
    |> cast(attrs, [
      :module_name,
      :display_name,
      :owner,
      :description,
      :enabled,
      :metadata
    ])
    |> validate_required([:module_name, :display_name, :owner, :enabled])
  end
end
