defmodule TelemetryFabricControl.AgentRegistry do
  @moduledoc """
  Tracks registered data-plane agents and their latest heartbeat.
  """

  use GenServer

  defstruct [
    :agent_id,
    :tenant_id,
    :hostname,
    :version,
    :config_version,
    :queue_depth_bytes,
    :ingest_bytes_per_second,
    :last_seen_at,
    labels: %{}
  ]

  @type t :: %__MODULE__{
          agent_id: String.t(),
          tenant_id: String.t(),
          hostname: String.t(),
          version: String.t(),
          config_version: non_neg_integer(),
          queue_depth_bytes: non_neg_integer(),
          ingest_bytes_per_second: non_neg_integer(),
          last_seen_at: DateTime.t(),
          labels: map()
        }

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def register(agent_attrs) when is_map(agent_attrs) do
    register(__MODULE__, agent_attrs)
  end

  def register(server, agent_attrs) when is_map(agent_attrs) do
    GenServer.call(server, {:register, agent_attrs})
  end

  def heartbeat(agent_attrs) when is_map(agent_attrs) do
    heartbeat(__MODULE__, agent_attrs)
  end

  def heartbeat(agent_id, config_version)
      when is_binary(agent_id) and is_integer(config_version) do
    heartbeat(__MODULE__, agent_id, config_version)
  end

  def heartbeat(server, agent_attrs) when is_map(agent_attrs) do
    GenServer.call(server, {:heartbeat, agent_attrs})
  end

  def heartbeat(server, agent_id, config_version) do
    heartbeat(server, %{agent_id: agent_id, config_version: config_version})
  end

  def list_agents do
    list_agents(__MODULE__)
  end

  def list_agents(server) do
    GenServer.call(server, :list_agents)
  end

  def get_agent(agent_id) do
    get_agent(__MODULE__, agent_id)
  end

  def get_agent(server, agent_id) do
    GenServer.call(server, {:get_agent, agent_id})
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
    agents = TelemetryFabricControl.StateFile.load(storage_path, %{})
    {:ok, %{agents: agents, storage_path: storage_path}}
  end

  @impl true
  def handle_call({:register, attrs}, _from, state) do
    now = DateTime.utc_now()

    agent = %__MODULE__{
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

    TelemetryFabricControl.AuditLog.append(%{
      actor: "system",
      action: "agent.registered",
      resource: agent.agent_id,
      metadata: %{tenant_id: agent.tenant_id}
    })

    agents = Map.put(state.agents, agent.agent_id, agent)
    persist!(state, agents)
    {:reply, {:ok, agent}, %{state | agents: agents}}
  end

  def handle_call({:heartbeat, attrs}, _from, state) do
    agent_id = Map.fetch!(attrs, :agent_id)
    config_version = Map.get(attrs, :config_version, 0)

    case Map.fetch(state.agents, agent_id) do
      {:ok, agent} ->
        case require_same_tenant(agent, Map.get(attrs, :tenant_id)) do
          :ok ->
            updated = %{
              agent
              | config_version: config_version,
                queue_depth_bytes:
                  Map.get(attrs, :queue_depth_bytes, Map.get(agent, :queue_depth_bytes, 0)),
                ingest_bytes_per_second:
                  Map.get(
                    attrs,
                    :ingest_bytes_per_second,
                    Map.get(agent, :ingest_bytes_per_second, 0)
                  ),
                last_seen_at: DateTime.utc_now()
            }

            agents = Map.put(state.agents, agent_id, updated)
            persist!(state, agents)
            {:reply, {:ok, updated}, %{state | agents: agents}}

          error ->
            {:reply, error, state}
        end

      :error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:list_agents, _from, state) do
    {:reply, Map.values(state.agents), state}
  end

  def handle_call({:get_agent, agent_id}, _from, state) do
    result =
      case Map.fetch(state.agents, agent_id) do
        {:ok, agent} -> {:ok, agent}
        :error -> {:error, :not_found}
      end

    {:reply, result, state}
  end

  def handle_call(:clear, _from, state) do
    persist!(state, %{})
    {:reply, :ok, %{state | agents: %{}}}
  end

  defp persist!(state, agents) do
    TelemetryFabricControl.StateFile.persist(state.storage_path, agents)
  end

  defp require_same_tenant(_agent, nil), do: :ok

  defp require_same_tenant(agent, tenant_id) do
    if agent.tenant_id == tenant_id do
      :ok
    else
      {:error, :tenant_mismatch}
    end
  end
end
