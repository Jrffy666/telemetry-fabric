defmodule TelemetryFabricControl.Modules.ApprovalHook do
  @moduledoc """
  Approval hook skeleton for module config publication.

  Production deployments can replace this module with a workflow-backed
  implementation. The current hook is deterministic and local: approval is
  required only when a request explicitly sets `require_approval: true`.
  """

  alias TelemetryFabricControl.Modules.ModuleRegistration

  def evaluate(attrs, diff) when is_map(attrs) do
    required = truthy?(ModuleRegistration.attr(attrs, :require_approval, false))
    approval_id = ModuleRegistration.attr(attrs, :approval_id)

    %{
      required: required,
      approved: not required or present?(approval_id),
      approval_id: approval_id,
      provider: "local-skeleton",
      reason: approval_reason(required, approval_id, diff)
    }
  end

  def authorize(attrs, diff) do
    approval = evaluate(attrs, diff)

    if approval.required and not approval.approved do
      {:error, {:approval_required, approval}}
    else
      {:ok, approval}
    end
  end

  defp approval_reason(false, _approval_id, _diff), do: "approval_not_required"

  defp approval_reason(true, value, _diff) when is_binary(value) and value != "",
    do: "approval_present"

  defp approval_reason(true, _value, _diff), do: "approval_required"

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(_value), do: false

  defp truthy?(value) when is_boolean(value), do: value

  defp truthy?(value) when is_binary(value),
    do: String.downcase(value) in ["1", "true", "on", "yes"]

  defp truthy?(_value), do: false
end
