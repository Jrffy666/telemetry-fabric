defmodule TelemetryFabricControl.CommandQueue do
  @moduledoc """
  Durable MVP queue for operator-initiated agent control commands.

  Heartbeat-derived commands such as `reload_config` can be returned directly by
  `ControlService`; this queue is for commands that must survive process
  restarts until an agent heartbeat drains them.
  """

  use GenServer

  alias TelemetryFabricControl.ControlCommand

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
    commands = TelemetryFabricControl.StateFile.load(storage_path, %{})
    {:ok, %{commands: commands, storage_path: storage_path}}
  end

  @impl true
  def handle_call({:enqueue, command}, _from, state) do
    commands =
      Map.update(state.commands, command.agent_id, [command], fn existing ->
        existing ++ [command]
      end)

    persist!(state, commands)
    {:reply, {:ok, command}, %{state | commands: commands}}
  end

  def handle_call({:drain, agent_id}, _from, state) do
    pending = Map.get(state.commands, agent_id, [])
    commands = Map.delete(state.commands, agent_id)
    persist!(state, commands)
    {:reply, pending, %{state | commands: commands}}
  end

  def handle_call({:list, agent_id}, _from, state) do
    {:reply, Map.get(state.commands, agent_id, []), state}
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
end
