defmodule TelemetryFabricControl.PostgresControlStore do
  @moduledoc """
  PostgreSQL-backed control-plane store.

  This adapter is used when `TELEMETRY_FABRIC_CONTROL_STORAGE=postgres`.
  The legacy OTP stores remain available for local MVP operation, but in this
  mode PostgreSQL is the source of truth for agents, pipeline versions,
  commands, and audit events.
  """

  import Ecto.Query

  alias TelemetryFabricControl.AgentRegistry
  alias TelemetryFabricControl.ControlCommand
  alias TelemetryFabricControl.PipelineConfig
  alias TelemetryFabricControl.PostgresCodec
  alias TelemetryFabricControl.Repo
  alias TelemetryFabricControl.Schema.Agent
  alias TelemetryFabricControl.Schema.AgentCommand
  alias TelemetryFabricControl.Schema.AuditEvent
  alias TelemetryFabricControl.Schema.PipelineVersion
  alias TelemetryFabricControl.Schema.Tenant

  def register_agent(attrs, repo \\ Repo) when is_map(attrs) do
    now = timestamp()

    agent = %AgentRegistry{
      agent_id: Map.fetch!(attrs, :agent_id),
      tenant_id: Map.fetch!(attrs, :tenant_id),
      hostname: Map.get(attrs, :hostname, "unknown"),
      version: Map.get(attrs, :version, "unknown"),
      config_version: Map.get(attrs, :config_version, 0),
      queue_depth_bytes: Map.get(attrs, :queue_depth_bytes, 0),
      ingest_bytes_per_second: Map.get(attrs, :ingest_bytes_per_second, 0),
      labels: Map.get(attrs, :labels, %{}),
      last_seen_at: now
    }

    result =
      repo.transaction(fn ->
        ensure_tenant!(repo, agent.tenant_id, now)
        upsert_agent!(repo, agent, now)

        insert_audit!(repo, "system", "agent.registered", agent.agent_id, %{
          tenant_id: agent.tenant_id
        })

        agent
      end)

    unwrap_transaction(result)
  end

  def heartbeat(attrs, repo \\ Repo) when is_map(attrs) do
    agent_id = Map.fetch!(attrs, :agent_id)
    config_version = Map.get(attrs, :config_version, 0)

    with {:ok, agent} <- get_agent(agent_id, repo),
         :ok <- require_same_tenant(agent, Map.get(attrs, :tenant_id)) do
      updated = %{
        agent
        | config_version: config_version,
          queue_depth_bytes: Map.get(attrs, :queue_depth_bytes, agent.queue_depth_bytes),
          ingest_bytes_per_second:
            Map.get(attrs, :ingest_bytes_per_second, agent.ingest_bytes_per_second),
          last_seen_at: timestamp()
      }

      now = timestamp(updated.last_seen_at)

      result =
        repo.transaction(fn ->
          upsert_agent!(repo, updated, now)
          updated
        end)

      unwrap_transaction(result)
    end
  end

  def get_agent(agent_id, repo \\ Repo) do
    case repo.get(Agent, agent_id) do
      nil -> {:error, :not_found}
      row -> {:ok, agent_from_row(row)}
    end
  end

  def latest_config_version(tenant_id, pipeline_name \\ "default", repo \\ Repo) do
    version =
      latest_pipeline_query(tenant_id, pipeline_name)
      |> select([pipeline], pipeline.version)
      |> repo.one()

    {:ok, version || 0}
  end

  def get_pipeline(tenant_id, pipeline_name, repo \\ Repo) do
    case repo.one(latest_pipeline_query(tenant_id, pipeline_name)) do
      nil -> {:error, :not_found}
      row -> {:ok, pipeline_from_row(row)}
    end
  end

  def put_pipeline(%PipelineConfig{} = config, actor \\ "system", repo \\ Repo) do
    with :ok <- PipelineConfig.validate(config) do
      now = timestamp()

      result =
        repo.transaction(fn ->
          ensure_tenant!(repo, config.tenant_id, now)
          version = next_pipeline_version(repo, config.tenant_id, config.name)
          updated = %{config | version: version}
          insert_pipeline_version!(repo, updated, actor)

          insert_audit!(repo, actor, "pipeline.updated", "#{config.tenant_id}/#{config.name}", %{
            version: version
          })

          updated
        end)

      unwrap_transaction(result)
    end
  end

  def rollback_pipeline(tenant_id, pipeline_name, target_version, actor \\ "system", repo \\ Repo) do
    result =
      repo.transaction(fn ->
        target =
          repo.one(
            from(pipeline in PipelineVersion,
              where:
                pipeline.tenant_id == ^tenant_id and pipeline.pipeline_name == ^pipeline_name and
                  pipeline.version == ^target_version
            )
          )

        if target == nil do
          repo.rollback(:not_found)
        end

        version = next_pipeline_version(repo, tenant_id, pipeline_name)
        rolled_back = %{pipeline_from_row(target) | version: version}
        insert_pipeline_version!(repo, rolled_back, actor)

        insert_audit!(repo, actor, "pipeline.rolled_back", "#{tenant_id}/#{pipeline_name}", %{
          version: version,
          target_version: target_version
        })

        rolled_back
      end)

    unwrap_transaction(result)
  end

  def enqueue_command(agent_id, kind, reason \\ "", repo \\ Repo) do
    with {:ok, agent} <- get_agent(agent_id, repo) do
      command =
        ControlCommand.new(%{
          agent_id: agent.agent_id,
          tenant_id: agent.tenant_id,
          kind: kind,
          reason: reason
        })

      result =
        repo.transaction(fn ->
          insert_command!(repo, command)
          audit_command!(repo, command, "command.enqueued", "operator")
          command
        end)

      unwrap_transaction(result)
    end
  end

  def drain_commands(agent_id, repo \\ Repo) do
    result =
      repo.transaction(fn ->
        pending =
          repo.all(
            from(command in AgentCommand,
              where: command.agent_id == ^agent_id and command.status == "pending",
              order_by: [asc: command.inserted_at, asc: command.command_id],
              lock: "FOR UPDATE"
            )
          )

        delivered_at = timestamp()
        ids = Enum.map(pending, & &1.command_id)

        if ids != [] do
          repo.update_all(
            from(command in AgentCommand, where: command.command_id in ^ids),
            set: [status: "delivered", delivered_at: delivered_at]
          )
        end

        commands =
          Enum.map(pending, fn row ->
            row
            |> command_from_row()
            |> ControlCommand.mark_delivered(delivered_at)
          end)

        Enum.each(commands, &audit_command!(repo, &1, "command.delivered", agent_id))
        commands
      end)

    unwrap_transaction(result)
  end

  def ack_command(agent_id, command_id, success, message \\ nil, repo \\ Repo)
      when is_boolean(success) do
    result =
      repo.transaction(fn ->
        command =
          repo.one(
            from(command in AgentCommand,
              where: command.agent_id == ^agent_id and command.command_id == ^command_id,
              lock: "FOR UPDATE"
            )
          )

        if command == nil do
          repo.rollback(:not_found)
        end

        acknowledged_at = timestamp()
        status = if success, do: "succeeded", else: "failed"
        last_error = normalize_ack_error(success, message)

        repo.update_all(
          from(row in AgentCommand, where: row.command_id == ^command_id),
          set: [status: status, acknowledged_at: acknowledged_at, last_error: last_error]
        )

        acknowledged =
          command
          |> command_from_row()
          |> ControlCommand.mark_acknowledged(success, message, acknowledged_at)

        audit_command!(
          repo,
          acknowledged,
          if(success, do: "command.succeeded", else: "command.failed"),
          agent_id
        )

        acknowledged
      end)

    unwrap_transaction(result)
  end

  defp latest_pipeline_query(tenant_id, pipeline_name) do
    from(pipeline in PipelineVersion,
      where: pipeline.tenant_id == ^tenant_id and pipeline.pipeline_name == ^pipeline_name,
      order_by: [desc: pipeline.version],
      limit: 1
    )
  end

  defp next_pipeline_version(repo, tenant_id, pipeline_name) do
    case latest_config_version(tenant_id, pipeline_name, repo) do
      {:ok, version} -> version + 1
    end
  end

  defp ensure_tenant!(repo, tenant_id, now) do
    repo.insert_all(
      Tenant,
      [%{tenant_id: tenant_id, inserted_at: now, updated_at: now}],
      on_conflict: {:replace, [:updated_at]},
      conflict_target: [:tenant_id]
    )
  end

  defp upsert_agent!(repo, %AgentRegistry{} = agent, now) do
    row =
      agent
      |> PostgresCodec.agent_row()
      |> Map.put(:inserted_at, now)
      |> Map.put(:updated_at, now)

    repo.insert_all(Agent, [row],
      on_conflict:
        {:replace,
         [
           :tenant_id,
           :hostname,
           :version,
           :config_version,
           :queue_depth_bytes,
           :ingest_bytes_per_second,
           :labels,
           :last_seen_at,
           :updated_at
         ]},
      conflict_target: [:agent_id]
    )
  end

  defp insert_pipeline_version!(repo, %PipelineConfig{} = pipeline, actor) do
    pipeline
    |> PostgresCodec.pipeline_version_row(actor)
    |> then(&repo.insert_all(PipelineVersion, [&1]))
  end

  defp insert_command!(repo, %ControlCommand{} = command) do
    command
    |> PostgresCodec.command_row()
    |> then(&repo.insert_all(AgentCommand, [&1]))
  end

  defp audit_command!(repo, %ControlCommand{} = command, action, actor) do
    insert_audit!(repo, actor, action, command.command_id, %{
      agent_id: command.agent_id,
      tenant_id: command.tenant_id,
      kind: command.kind,
      reason: command.reason
    })
  end

  defp insert_audit!(repo, actor, action, resource, metadata) do
    row = %{
      event_id: new_event_id(),
      actor: actor,
      action: action,
      resource: resource,
      metadata: normalize_json(metadata),
      inserted_at: timestamp()
    }

    repo.insert_all(AuditEvent, [row],
      on_conflict: :nothing,
      conflict_target: [:event_id]
    )
  end

  defp agent_from_row(%Agent{} = row) do
    %AgentRegistry{
      agent_id: row.agent_id,
      tenant_id: row.tenant_id,
      hostname: row.hostname,
      version: row.version,
      config_version: row.config_version,
      queue_depth_bytes: row.queue_depth_bytes,
      ingest_bytes_per_second: row.ingest_bytes_per_second,
      labels: row.labels || %{},
      last_seen_at: row.last_seen_at
    }
  end

  defp pipeline_from_row(%PipelineVersion{} = row) do
    config = row.config || %{}

    %PipelineConfig{
      tenant_id: row.tenant_id,
      name: row.pipeline_name,
      version: row.version,
      receivers: Map.get(config, "receivers", []),
      processors: Map.get(config, "processors", []),
      exporters: Map.get(config, "exporters", []),
      routes: Map.get(config, "routes", [])
    }
  end

  defp command_from_row(%AgentCommand{} = row) do
    ControlCommand.new(%{
      command_id: row.command_id,
      agent_id: row.agent_id,
      tenant_id: row.tenant_id,
      kind: String.to_existing_atom(row.kind),
      reason: row.reason || "",
      status: String.to_existing_atom(row.status),
      inserted_at: row.inserted_at,
      delivered_at: row.delivered_at,
      acknowledged_at: Map.get(row, :acknowledged_at),
      last_error: Map.get(row, :last_error)
    })
  end

  defp normalize_ack_error(true, _message), do: nil

  defp normalize_ack_error(false, message) when is_binary(message), do: String.trim(message)
  defp normalize_ack_error(false, _message), do: nil

  defp require_same_tenant(_agent, nil), do: :ok

  defp require_same_tenant(agent, tenant_id) do
    if agent.tenant_id == tenant_id do
      :ok
    else
      {:error, :tenant_mismatch}
    end
  end

  defp unwrap_transaction({:ok, value}), do: {:ok, value}
  defp unwrap_transaction({:error, reason}), do: {:error, reason}

  defp timestamp, do: timestamp(DateTime.utc_now())
  defp timestamp(nil), do: nil
  defp timestamp(%DateTime{} = value), do: DateTime.truncate(value, :microsecond)

  defp new_event_id do
    System.system_time(:microsecond) * 1_000 + rem(System.unique_integer([:positive]), 1_000)
  end

  defp normalize_json(value) when is_map(value) do
    value
    |> Enum.map(fn {key, item} -> {to_string(key), normalize_json(item)} end)
    |> Map.new()
  end

  defp normalize_json(value) when is_list(value), do: Enum.map(value, &normalize_json/1)
  defp normalize_json(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_json(value), do: value
end
