defmodule TelemetryFabricControl.Modules.ConfigPlan do
  @moduledoc """
  Dry-run result for a module config publication.
  """

  defstruct [
    :tenant_id,
    :module,
    :next_version,
    :checksum,
    :valid,
    :validation_errors,
    :diff,
    :approval,
    :dry_run,
    :config
  ]
end
