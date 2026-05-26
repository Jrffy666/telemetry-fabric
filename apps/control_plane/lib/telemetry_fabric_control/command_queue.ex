defmodule TelemetryFabricControl.CommandQueue do
  @moduledoc """
  Durable MVP queue for operator-initiated agent control commands.

  Heartbeat-derived commands such as `reload_config` can be returned directly by
  `ControlService`; this queue is for commands that must survive process
  restarts until an agent heartbeat drains them. Drained commands are retained
  as delivered history and audit events are emitted for enqueue/delivery so
  PostgreSQL snapshots can record the full lifecycle.
  """

  use GenServer

  alias TelemetryFabricControl.ControlCommand

  @default_command_lease_ms 30_000

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def enqueue(%ControlCommand{} = command) do
    enqueue(__MODULE__, command)
  end

  def enqueue(server, %ControlCommand{} = command) do
    GenServer.call(server, {:enqueue, command})
  end

  def drain(agent_id) do
    drain(__MODULE__, agent_id)
  end

  def drain(server, agent_id) do
    GenServer.call(server, {:drain, agent_id})
  end

  def ack(agent_id, command_id, success, message \\ nil) do
    ack(__MODULE__, agent_id, command_id, success, message)
  end

  def ack(server, agent_id, command_id, success, message) when is_boolean(success) do
    GenServer.call(server, {:ack, agent_id, command_id, success, message})
  end

  def list(agent_id) do
    list(__MODULE__, agent_id)
  end

  def list(server, agent_id) do
    GenServer.call(server, {:list, agent_id})
  end

  def list_all do
    list_all(__MODULE__)
  end

  def list_all(server) do
    GenServer.call(server, :list_all)
  end

  def clear do
    clear(__MODULE__)
  end

  def clear(server) do
    GenServer.call(server, :clear)
  end

  @impl true
  def init(opts) do
    storage_path = Keyword.get(opts, :storage_path)
    audit_log = Keyword.get(opts, :audit_log, TelemetryFabricControl.AuditLog)
    lease_ms = Keyword.get(opts, :lease_ms, command_lease_ms())
    commands = TelemetryFabricControl.StateFile.load(storage_path, %{})

    {:ok,
     %{commands: commands, storage_path: storage_path, audit_log: audit_log, lease_ms: lease_ms}}
  end

  @impl true
  def handle_call({:enqueue, command}, _from, state) do
    commands =
      Map.update(state.commands, command.agent_id, [command], fn existing ->
        existing ++ [command]
      end)

    persist!(state, commands)
    audit_command(state, command, "command.enqueued", "operator")
    {:reply, {:ok, command}, %{state | commands: commands}}
  end

  def handle_call({:drain, agent_id}, _from, state) do
    existing = Map.get(state.commands, agent_id, [])
    delivered_at = DateTime.utc_now()
    deliverable = Enum.filter(existing, &deliverable?(&1, delivered_at, state.lease_ms))
    deliverable_ids = MapSet.new(Enum.map(deliverable, & &1.command_id))

    updated =
      Enum.map(existing, fn command ->
        if MapSet.member?(deliverable_ids, command.command_id) do
          ControlCommand.mark_delivered(command, delivered_at)
        else
          command
        end
      end)

    delivered =
      Enum.filter(updated, &MapSet.member?(deliverable_ids, &1.command_id))

    commands =
      if updated == [] do
        Map.delete(state.commands, agent_id)
      else
        Map.put(state.commands, agent_id, updated)
      end

    persist!(state, commands)
    Enum.each(delivered, &audit_command(state, &1, "command.delivered", agent_id))
    {:reply, delivered, %{state | commands: commands}}
  end

  def handle_call({:ack, agent_id, command_id, success, message}, _from, state) do
    existing = Map.get(state.commands, agent_id, [])

    case Enum.split_with(existing, &(&1.command_id == command_id)) do
      {[command], rest} ->
        if terminal?(command) do
          {:reply, {:ok, command}, state}
        else
          acknowledged = ControlCommand.mark_acknowledged(command, success, message)

          commands =
            Map.put(state.commands, agent_id, Enum.sort_by([acknowledged | rest], &sort_key/1))

          persist!(state, commands)

          audit_command(
            state,
            acknowledged,
            if(success, do: "command.succeeded", else: "command.failed"),
            agent_id
          )

          {:reply, {:ok, acknowledged}, %{state | commands: commands}}
        end

      {[], _rest} ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call({:list, agent_id}, _from, state) do
    pending =
      state.commands
      |> Map.get(agent_id, [])
      |> Enum.filter(&ControlCommand.pending?/1)

    {:reply, pending, state}
  end

  def handle_call(:list_all, _from, state) do
    commands =
      state.commands
      |> Map.values()
      |> List.flatten()
      |> Enum.sort_by(
        &{&1.agent_id, DateTime.to_unix(&1.inserted_at, :microsecond), &1.command_id}
      )

    {:reply, commands, state}
  end

  def handle_call(:clear, _from, state) do
    persist!(state, %{})
    {:reply, :ok, %{state | commands: %{}}}
  end

  defp persist!(state, commands) do
    TelemetryFabricControl.StateFile.persist(state.storage_path, commands)
  end

  defp sort_key(%ControlCommand{} = command) do
    {DateTime.to_unix(command.inserted_at, :microsecond), command.command_id}
  end

  defp deliverable?(%ControlCommand{} = command, now, lease_ms) do
    ControlCommand.pending?(command) or lease_expired?(command, now, lease_ms)
  end

  defp lease_expired?(%ControlCommand{} = command, now, lease_ms) do
    case {ControlCommand.status(command), ControlCommand.delivered_at(command)} do
      {:delivered, nil} ->
        true

      {:delivered, %DateTime{} = delivered_at} ->
        delivered_at
        |> DateTime.add(lease_ms * 1_000, :microsecond)
        |> DateTime.compare(now)
        |> Kernel.!=(:gt)

      _other ->
        false
    end
  end

  defp terminal?(%ControlCommand{} = command) do
    ControlCommand.status(command) in [:succeeded, :failed]
  end

  defp command_lease_ms do
    case System.get_env("TELEMETRY_FABRIC_CONTROL_COMMAND_LEASE_MS") do
      nil ->
        @default_command_lease_ms

      value ->
        case Integer.parse(value) do
          {lease_ms, ""} when lease_ms > 0 -> lease_ms
          _invalid -> @default_command_lease_ms
        end
    end
  end

  defp audit_command(state, %ControlCommand{} = command, action, actor) do
    TelemetryFabricControl.AuditLog.append(state.audit_log, %{
      actor: actor,
      action: action,
      resource: command.command_id,
      metadata: %{
        agent_id: command.agent_id,
        tenant_id: command.tenant_id,
        kind: command.kind,
        reason: command.reason
      }
    })
  end
end
