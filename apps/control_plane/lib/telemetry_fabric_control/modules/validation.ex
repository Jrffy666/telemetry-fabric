defmodule TelemetryFabricControl.Modules.Validation do
  @moduledoc """
  Generic module config validation entrypoint.

  Core validation stays business-neutral. Domain-specific validation is routed
  into explicit module namespaces.
  """

  alias TelemetryFabricControl.Modules.Blockchain.Validator, as: BlockchainValidator
  alias TelemetryFabricControl.Modules.ModuleRegistration

  def validate_config(attrs) when is_map(attrs) do
    with :ok <-
           ModuleRegistration.require_text(:tenant_id, ModuleRegistration.attr(attrs, :tenant_id)),
         :ok <- ModuleRegistration.require_text(:module, ModuleRegistration.attr(attrs, :module)),
         {:ok, config} <-
           ModuleRegistration.optional_map(:config, ModuleRegistration.attr(attrs, :config, %{})) do
      module_name = ModuleRegistration.attr(attrs, :module)
      validate_module_config(module_name, config)
    end
  end

  def validate_module_config("blockchain", config),
    do: BlockchainValidator.validate_config(config)

  def validate_module_config(_module_name, _config), do: :ok
end
