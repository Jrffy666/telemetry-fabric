defmodule TelemetryFabricControl.PipelineStore do
  @moduledoc """
  Versioned in-memory pipeline store.

  This module deliberately keeps the public API close to the production shape
  that will later be backed by PostgreSQL and Ecto.
  """

  use GenServer

  alias TelemetryFabricControl.PipelineConfig

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def put_pipeline(%PipelineConfig{} = config, actor \\ "system") do
    put_pipeline(__MODULE__, config, actor)
  end

  def put_pipeline(server, %PipelineConfig{} = config, actor) do
    GenServer.call(server, {:put_pipeline, config, actor})
  end

  def get_pipeline(tenant_id, name) do
    get_pipeline(__MODULE__, tenant_id, name)
  end

  def get_pipeline(server, tenant_id, name) do
    GenServer.call(server, {:get_pipeline, tenant_id, name})
  end

  def list_pipelines(tenant_id) do
    list_pipelines(__MODULE__, tenant_id)
  end

  def list_pipelines(server, tenant_id) do
    GenServer.call(server, {:list_pipelines, tenant_id})
  end

  def list_versions do
    list_versions(__MODULE__)
  end

  def list_versions(server) do
    GenServer.call(server, :list_versions)
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
    pipelines = TelemetryFabricControl.StateFile.load(storage_path, %{})
    {:ok, %{pipelines: pipelines, storage_path: storage_path}}
  end

  @impl true
  def handle_call({:put_pipeline, config, actor}, _from, state) do
    with :ok <- PipelineConfig.validate(config) do
      key = {config.tenant_id, config.name}
      versions = Map.get(state.pipelines, key, [])
      next_version = next_version(versions)
      updated = %{config | version: next_version}

      TelemetryFabricControl.AuditLog.append(%{
        actor: actor,
        action: "pipeline.updated",
        resource: "#{config.tenant_id}/#{config.name}",
        metadata: %{version: next_version}
      })

      pipelines = Map.put(state.pipelines, key, [updated | versions])
      persist!(state, pipelines)
      {:reply, {:ok, updated}, %{state | pipelines: pipelines}}
    else
      error -> {:reply, error, state}
    end
  end

  def handle_call({:get_pipeline, tenant_id, name}, _from, state) do
    result =
      case Map.get(state.pipelines, {tenant_id, name}, []) do
        [latest | _] -> {:ok, latest}
        [] -> {:error, :not_found}
      end

    {:reply, result, state}
  end

  def handle_call({:list_pipelines, tenant_id}, _from, state) do
    pipelines =
      state.pipelines
      |> Enum.filter(fn {{pipeline_tenant_id, _name}, _versions} ->
        pipeline_tenant_id == tenant_id
      end)
      |> Enum.map(fn {_key, [latest | _versions]} -> latest end)

    {:reply, pipelines, state}
  end

  def handle_call(:list_versions, _from, state) do
    versions =
      state.pipelines
      |> Enum.flat_map(fn {_key, versions} -> versions end)
      |> Enum.sort_by(&{&1.tenant_id, &1.name, &1.version})

    {:reply, versions, state}
  end

  def handle_call(:clear, _from, state) do
    persist!(state, %{})
    {:reply, :ok, %{state | pipelines: %{}}}
  end

  defp next_version([]), do: 1
  defp next_version([latest | _]), do: latest.version + 1

  defp persist!(state, pipelines) do
    TelemetryFabricControl.StateFile.persist(state.storage_path, pipelines)
  end
end
