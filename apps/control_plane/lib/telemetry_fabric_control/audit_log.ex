defmodule TelemetryFabricControl.AuditLog do
  @moduledoc """
  Append-only in-memory audit log used by the MVP control plane.

  Production storage should move this behind PostgreSQL with immutable rows.
  """

  use GenServer

  defstruct [:id, :actor, :action, :resource, :metadata, :inserted_at]

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def append(event) when is_map(event) do
    append(__MODULE__, event)
  end

  def append(server, event) when is_map(event) do
    GenServer.call(server, {:append, event})
  end

  def list do
    list(__MODULE__, 100)
  end

  def list(:all) do
    list(__MODULE__, :all)
  end

  def list(limit) when is_integer(limit) do
    list(__MODULE__, limit)
  end

  def list(server, limit) when is_integer(limit) do
    GenServer.call(server, {:list, limit})
  end

  def list(server, :all) do
    GenServer.call(server, {:list, :all})
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
    events = TelemetryFabricControl.StateFile.load(storage_path, [])
    {:ok, %{events: events, storage_path: storage_path}}
  end

  @impl true
  def handle_call({:append, event}, _from, state) do
    entry = %__MODULE__{
      id: System.unique_integer([:positive, :monotonic]),
      actor: Map.get(event, :actor, "unknown"),
      action: Map.fetch!(event, :action),
      resource: Map.get(event, :resource, "unknown"),
      metadata: Map.get(event, :metadata, %{}),
      inserted_at: DateTime.utc_now()
    }

    events = [entry | state.events]
    persist!(state, events)
    {:reply, {:ok, entry}, %{state | events: events}}
  end

  def handle_call({:list, limit}, _from, state) do
    events =
      case limit do
        :all -> state.events
        limit -> Enum.take(state.events, limit)
      end

    {:reply, events, state}
  end

  def handle_call(:clear, _from, state) do
    persist!(state, [])
    {:reply, :ok, %{state | events: []}}
  end

  defp persist!(state, events) do
    TelemetryFabricControl.StateFile.persist(state.storage_path, events)
  end
end
