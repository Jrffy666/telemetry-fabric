defmodule TelemetryFabricControl.ControlService do
  @moduledoc """
  Protocol-neutral implementation of the AgentControl domain workflow.

  This module mirrors the protobuf operations without binding the MVP to a
  transport yet. A gRPC or HTTP adapter should delegate to these functions.
  """

  alias TelemetryFabricControl.AgentRegistry
  alias TelemetryFabricControl.CommandQueue
  alias TelemetryFabricControl.ControlCommand
  alias TelemetryFabricControl.PipelineConfig
  alias TelemetryFabricControl.PipelineStore

  defmodule RegisterAgentResponse do
    @moduledoc false
    defstruct [:accepted, :config_version, :message]
  end

  defmodule ConfigUpdate do
    @moduledoc false
    defstruct [:version, :pipeline_config, :checksum]
  end

  defmodule AgentStatusResponse do
    @moduledoc false
    defstruct [:agent_id, :healthy, :config_version, warnings: []]
  end

  def register_agent(attrs) when is_map(attrs) do
    with :ok <- require_text(:agent_id, Map.get(attrs, :agent_id)),
         :ok <- require_text(:tenant_id, Map.get(attrs, :tenant_id)) do
      tenant_id = Map.fetch!(attrs, :tenant_id)
      {:ok, config_version} = latest_config_version(tenant_id)
      attrs = Map.put_new(attrs, :config_version, 0)

      case AgentRegistry.register(attrs) do
        {:ok, _agent} ->
          {:ok,
           %RegisterAgentResponse{
             accepted: true,
             config_version: config_version,
             message: register_message(config_version)
           }}

        error ->
          error
      end
    end
  end

  def config_update(attrs) when is_map(attrs) do
    with :ok <- require_text(:agent_id, Map.get(attrs, :agent_id)),
         :ok <- require_text(:tenant_id, Map.get(attrs, :tenant_id)),
         {:ok, agent} <- AgentRegistry.get_agent(Map.fetch!(attrs, :agent_id)),
         :ok <- require_same_tenant(agent, Map.fetch!(attrs, :tenant_id)),
         {:ok, pipeline} <- PipelineStore.get_pipeline(agent.tenant_id, pipeline_name(attrs)) do
      current_version = Map.get(attrs, :current_version, 0)

      if pipeline.version > current_version do
        {:ok, build_config_update(pipeline)}
      else
        {:ok, :up_to_date}
      end
    end
  end

  def heartbeat(attrs) when is_map(attrs) do
    with :ok <- require_text(:agent_id, Map.get(attrs, :agent_id)),
         :ok <- require_text(:tenant_id, Map.get(attrs, :tenant_id)),
         {:ok, agent} <- AgentRegistry.heartbeat(attrs),
         queued_commands <- CommandQueue.drain(agent.agent_id),
         {:ok, latest_version} <- latest_config_version(agent.tenant_id) do
      derived_commands =
        if latest_version > agent.config_version do
          [
            ControlCommand.new(%{
              agent_id: agent.agent_id,
              tenant_id: agent.tenant_id,
              kind: :reload_config,
              reason: "pipeline config version #{latest_version} is available"
            })
          ]
        else
          []
        end

      {:ok, queued_commands ++ derived_commands}
    end
  end

  def enqueue_command(agent_id, kind, reason \\ "") do
    if kind in ControlCommand.kinds() do
      with {:ok, agent} <- AgentRegistry.get_agent(agent_id) do
        CommandQueue.enqueue(
          ControlCommand.new(%{
            agent_id: agent.agent_id,
            tenant_id: agent.tenant_id,
            kind: kind,
            reason: reason
          })
        )
      end
    else
      {:error, {:unknown_command_kind, kind}}
    end
  end

  def rollback_pipeline(attrs) when is_map(attrs) do
    with :ok <- require_text(:tenant_id, Map.get(attrs, :tenant_id)),
         :ok <- require_text(:pipeline, pipeline_name(attrs)),
         {:ok, target_version} <-
           require_positive_integer(:target_version, Map.get(attrs, :target_version)) do
      PipelineStore.rollback_pipeline(
        Map.fetch!(attrs, :tenant_id),
        pipeline_name(attrs),
        target_version,
        Map.get(attrs, :actor, "operator")
      )
    end
  end

  def report_status(attrs) when is_map(attrs) do
    with :ok <- require_text(:agent_id, Map.get(attrs, :agent_id)),
         :ok <- require_text(:tenant_id, Map.get(attrs, :tenant_id)),
         {:ok, agent} <- AgentRegistry.get_agent(Map.fetch!(attrs, :agent_id)),
         :ok <- require_same_tenant(agent, Map.fetch!(attrs, :tenant_id)),
         {:ok, latest_version} <- latest_config_version(agent.tenant_id) do
      warnings =
        []
        |> maybe_warn(agent.config_version < latest_version, "config_outdated")
        |> maybe_warn(Map.get(agent, :queue_depth_bytes, 0) > 0, "queue_not_empty")

      {:ok,
       %AgentStatusResponse{
         agent_id: agent.agent_id,
         healthy: warnings == [],
         config_version: agent.config_version,
         warnings: Enum.reverse(warnings)
       }}
    end
  end

  defp build_config_update(%PipelineConfig{} = pipeline) do
    payload = PipelineConfig.to_agent_yaml(pipeline)

    %ConfigUpdate{
      version: pipeline.version,
      pipeline_config: payload,
      checksum: sha256(payload)
    }
  end

  defp latest_config_version(tenant_id) do
    case PipelineStore.get_pipeline(tenant_id, "default") do
      {:ok, pipeline} -> {:ok, pipeline.version}
      {:error, :not_found} -> {:ok, 0}
      error -> error
    end
  end

  defp pipeline_name(attrs), do: Map.get(attrs, :pipeline, "default")

  defp require_same_tenant(agent, tenant_id) do
    if agent.tenant_id == tenant_id do
      :ok
    else
      {:error, :tenant_mismatch}
    end
  end

  defp require_text(field, value) when is_binary(value) do
    if String.trim(value) == "" do
      {:error, {:empty, field}}
    else
      :ok
    end
  end

  defp require_text(field, _value), do: {:error, {:empty, field}}

  defp require_positive_integer(_field, value) when is_integer(value) and value > 0,
    do: {:ok, value}

  defp require_positive_integer(field, value), do: {:error, {:invalid_integer, field, value}}

  defp register_message(0), do: "agent registered; no pipeline config is available"
  defp register_message(_version), do: "agent registered"

  defp maybe_warn(warnings, true, warning), do: [warning | warnings]
  defp maybe_warn(warnings, false, _warning), do: warnings

  defp sha256(payload) do
    :crypto.hash(:sha256, payload)
    |> Base.encode16(case: :lower)
  end
end
