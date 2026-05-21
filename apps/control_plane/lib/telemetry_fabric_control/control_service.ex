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
  alias TelemetryFabricControl.PostgresControlStore

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

      case register_agent_record(attrs) do
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
         {:ok, agent} <- get_agent(Map.fetch!(attrs, :agent_id)),
         :ok <- require_same_tenant(agent, Map.fetch!(attrs, :tenant_id)),
         {:ok, pipeline} <- get_pipeline(agent.tenant_id, pipeline_name(attrs)) do
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
         {:ok, agent} <- heartbeat_agent(attrs),
         {:ok, queued_commands} <- drain_commands(agent.agent_id),
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
      if postgres_primary?() do
        PostgresControlStore.enqueue_command(agent_id, kind, reason)
      else
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
      end
    else
      {:error, {:unknown_command_kind, kind}}
    end
  end

  def ack_command(attrs) when is_map(attrs) do
    with :ok <- require_text(:agent_id, Map.get(attrs, :agent_id)),
         :ok <- require_text(:tenant_id, Map.get(attrs, :tenant_id)),
         :ok <- require_text(:command_id, Map.get(attrs, :command_id)),
         {:ok, success} <- require_boolean(:success, Map.get(attrs, :success)),
         {:ok, agent} <- get_agent(Map.fetch!(attrs, :agent_id)),
         :ok <- require_same_tenant(agent, Map.fetch!(attrs, :tenant_id)) do
      ack_command_record(
        agent.agent_id,
        Map.fetch!(attrs, :command_id),
        success,
        Map.get(attrs, :error)
      )
    end
  end

  def put_pipeline(attrs) when is_map(attrs) do
    with :ok <- require_text(:tenant_id, Map.get(attrs, :tenant_id)),
         :ok <- require_text(:pipeline, pipeline_name(attrs)),
         {:ok, config} <- pipeline_from_attrs(attrs) do
      put_pipeline_config(config, Map.get(attrs, :actor, "operator"))
    end
  end

  def rollback_pipeline(attrs) when is_map(attrs) do
    with :ok <- require_text(:tenant_id, Map.get(attrs, :tenant_id)),
         :ok <- require_text(:pipeline, pipeline_name(attrs)),
         {:ok, target_version} <-
           require_positive_integer(:target_version, Map.get(attrs, :target_version)) do
      rollback_pipeline_config(
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
         {:ok, agent} <- get_agent(Map.fetch!(attrs, :agent_id)),
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
    case get_pipeline(tenant_id, "default") do
      {:ok, pipeline} -> {:ok, pipeline.version}
      {:error, :not_found} -> {:ok, 0}
      error -> error
    end
  end

  defp register_agent_record(attrs) do
    if postgres_primary?() do
      PostgresControlStore.register_agent(attrs)
    else
      AgentRegistry.register(attrs)
    end
  end

  defp heartbeat_agent(attrs) do
    if postgres_primary?() do
      PostgresControlStore.heartbeat(attrs)
    else
      AgentRegistry.heartbeat(attrs)
    end
  end

  defp get_agent(agent_id) do
    if postgres_primary?() do
      PostgresControlStore.get_agent(agent_id)
    else
      AgentRegistry.get_agent(agent_id)
    end
  end

  defp drain_commands(agent_id) do
    if postgres_primary?() do
      PostgresControlStore.drain_commands(agent_id)
    else
      {:ok, CommandQueue.drain(agent_id)}
    end
  end

  defp ack_command_record(agent_id, command_id, success, message) do
    if postgres_primary?() do
      PostgresControlStore.ack_command(agent_id, command_id, success, message)
    else
      CommandQueue.ack(agent_id, command_id, success, message)
    end
  end

  defp get_pipeline(tenant_id, name) do
    if postgres_primary?() do
      PostgresControlStore.get_pipeline(tenant_id, name)
    else
      PipelineStore.get_pipeline(tenant_id, name)
    end
  end

  defp put_pipeline_config(config, actor) do
    if postgres_primary?() do
      PostgresControlStore.put_pipeline(config, actor)
    else
      PipelineStore.put_pipeline(config, actor)
    end
  end

  defp rollback_pipeline_config(tenant_id, name, target_version, actor) do
    if postgres_primary?() do
      PostgresControlStore.rollback_pipeline(tenant_id, name, target_version, actor)
    else
      PipelineStore.rollback_pipeline(tenant_id, name, target_version, actor)
    end
  end

  defp postgres_primary? do
    System.get_env("TELEMETRY_FABRIC_CONTROL_STORAGE") == "postgres" or
      truthy_env?("TELEMETRY_FABRIC_CONTROL_POSTGRES_PRIMARY")
  end

  defp truthy_env?(name) do
    case System.get_env(name) do
      nil -> false
      value -> String.downcase(value) in ["1", "true", "on", "yes"]
    end
  end

  defp pipeline_name(attrs), do: Map.get(attrs, :pipeline, "default")

  defp pipeline_from_attrs(attrs) do
    config = %PipelineConfig{
      tenant_id: Map.fetch!(attrs, :tenant_id),
      name: pipeline_name(attrs),
      receivers: Map.get(attrs, :receivers, []),
      processors: Map.get(attrs, :processors, []),
      exporters: Map.get(attrs, :exporters, []),
      routes: Map.get(attrs, :routes, [])
    }

    case PipelineConfig.validate(config) do
      :ok -> {:ok, config}
      error -> error
    end
  end

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

  defp require_boolean(_field, value) when is_boolean(value), do: {:ok, value}
  defp require_boolean(field, value), do: {:error, {:invalid_boolean, field, value}}

  defp register_message(0), do: "agent registered; no pipeline config is available"
  defp register_message(_version), do: "agent registered"

  defp maybe_warn(warnings, true, warning), do: [warning | warnings]
  defp maybe_warn(warnings, false, _warning), do: warnings

  defp sha256(payload) do
    :crypto.hash(:sha256, payload)
    |> Base.encode16(case: :lower)
  end
end
