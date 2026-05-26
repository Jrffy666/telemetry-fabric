defmodule TelemetryFabricControl.Modules.Store do
  @moduledoc """
  OTP-backed business-module registry and config-version store.

  PostgreSQL can be used as the primary store in production, but this store
  keeps the local MVP and tests dependency-light.
  """

  use GenServer

  alias TelemetryFabricControl.AuditLog
  alias TelemetryFabricControl.Modules.ApprovalHook
  alias TelemetryFabricControl.Modules.ConfigPlan
  alias TelemetryFabricControl.Modules.Diff
  alias TelemetryFabricControl.Modules.ModuleConfigVersion
  alias TelemetryFabricControl.Modules.ModuleRegistration
  alias TelemetryFabricControl.Modules.Validation

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def register_module(attrs), do: register_module(__MODULE__, attrs)
  def register_module(server, attrs), do: GenServer.call(server, {:register_module, attrs})

  def list_modules, do: list_modules(__MODULE__)
  def list_modules(server), do: GenServer.call(server, :list_modules)

  def get_module(module_name), do: get_module(__MODULE__, module_name)
  def get_module(server, module_name), do: GenServer.call(server, {:get_module, module_name})

  def validate_config(attrs), do: validate_config(__MODULE__, attrs)
  def validate_config(server, attrs), do: GenServer.call(server, {:validate_config, attrs})

  def dry_run_config(attrs), do: dry_run_config(__MODULE__, attrs)
  def dry_run_config(server, attrs), do: GenServer.call(server, {:dry_run_config, attrs})

  def diff_config(attrs), do: diff_config(__MODULE__, attrs)
  def diff_config(server, attrs), do: GenServer.call(server, {:diff_config, attrs})

  def publish_config(attrs), do: publish_config(__MODULE__, attrs)
  def publish_config(server, attrs), do: GenServer.call(server, {:publish_config, attrs})

  def rollout_config(attrs), do: rollout_config(__MODULE__, attrs)
  def rollout_config(server, attrs), do: GenServer.call(server, {:rollout_config, attrs})

  def get_latest_config(tenant_id, module_name),
    do: get_latest_config(__MODULE__, tenant_id, module_name)

  def get_latest_config(server, tenant_id, module_name) do
    GenServer.call(server, {:get_latest_config, tenant_id, module_name})
  end

  def fetch_config(attrs), do: fetch_config(__MODULE__, attrs)
  def fetch_config(server, attrs), do: GenServer.call(server, {:fetch_config, attrs})

  def rollback_config(attrs), do: rollback_config(__MODULE__, attrs)
  def rollback_config(server, attrs), do: GenServer.call(server, {:rollback_config, attrs})

  def list_versions(tenant_id, module_name), do: list_versions(__MODULE__, tenant_id, module_name)

  def list_versions(server, tenant_id, module_name) do
    GenServer.call(server, {:list_versions, tenant_id, module_name})
  end

  def clear, do: clear(__MODULE__)
  def clear(server), do: GenServer.call(server, :clear)

  @impl true
  def init(opts) do
    storage_path = Keyword.get(opts, :storage_path)
    state = TelemetryFabricControl.StateFile.load(storage_path, default_state())
    {:ok, Map.put(state, :storage_path, storage_path)}
  end

  @impl true
  def handle_call({:register_module, attrs}, _from, state) do
    actor = ModuleRegistration.attr(attrs, :actor, "operator")

    case ModuleRegistration.new(attrs) do
      {:ok, registration} ->
        previous = Map.get(state.registrations, registration.module)

        registration =
          if previous do
            %{registration | inserted_at: previous.inserted_at}
          else
            registration
          end

        registrations = Map.put(state.registrations, registration.module, registration)
        audit!("module.registered", registration.module, actor, %{enabled: registration.enabled})
        state = persist!(state, %{state | registrations: registrations})
        {:reply, {:ok, registration}, state}

      error ->
        {:reply, error, state}
    end
  end

  def handle_call(:list_modules, _from, state) do
    modules =
      state.registrations
      |> Map.values()
      |> Enum.sort_by(& &1.module)

    {:reply, modules, state}
  end

  def handle_call({:get_module, module_name}, _from, state) do
    result =
      case Map.get(state.registrations, module_name) do
        nil -> {:error, :not_found}
        registration -> {:ok, registration}
      end

    {:reply, result, state}
  end

  def handle_call({:validate_config, attrs}, _from, state) do
    result =
      with :ok <- require_registered(state, ModuleRegistration.attr(attrs, :module)),
           {:ok, plan} <- build_plan(state, attrs) do
        {:ok, plan}
      end

    {:reply, result, state}
  end

  def handle_call({:dry_run_config, attrs}, _from, state) do
    result =
      with :ok <- require_registered(state, ModuleRegistration.attr(attrs, :module)),
           {:ok, plan} <- build_plan(state, Map.put(attrs, :dry_run, true)) do
        {:ok, plan}
      end

    {:reply, result, state}
  end

  def handle_call({:diff_config, attrs}, _from, state) do
    result =
      with :ok <- require_registered(state, ModuleRegistration.attr(attrs, :module)),
           {:ok, plan} <- build_plan(state, attrs) do
        {:ok, plan.diff}
      end

    {:reply, result, state}
  end

  def handle_call({:publish_config, attrs}, _from, state) do
    publish_config_in_state(state, attrs)
  end

  def handle_call({:rollout_config, attrs}, _from, state) do
    publish_config_in_state(state, attrs)
  end

  def handle_call({:get_latest_config, tenant_id, module_name}, _from, state) do
    result =
      case Map.get(state.configs, {tenant_id, module_name}, []) do
        [latest | _] -> {:ok, latest}
        [] -> {:error, :not_found}
      end

    {:reply, result, state}
  end

  def handle_call({:fetch_config, attrs}, _from, state) do
    with :ok <-
           ModuleRegistration.require_text(:tenant_id, ModuleRegistration.attr(attrs, :tenant_id)),
         :ok <- ModuleRegistration.require_text(:module, ModuleRegistration.attr(attrs, :module)) do
      tenant_id = String.trim(ModuleRegistration.attr(attrs, :tenant_id))
      module_name = String.trim(ModuleRegistration.attr(attrs, :module))
      current_version = ModuleRegistration.attr(attrs, :current_version, 0)

      result =
        case Map.get(state.configs, {tenant_id, module_name}, []) do
          [latest | _] when is_integer(current_version) and latest.version <= current_version ->
            {:ok, :up_to_date}

          [latest | _] ->
            {:ok, latest}

          [] ->
            {:error, :not_found}
        end

      {:reply, result, state}
    else
      error -> {:reply, error, state}
    end
  end

  def handle_call({:rollback_config, attrs}, _from, state) do
    with :ok <-
           ModuleRegistration.require_text(:tenant_id, ModuleRegistration.attr(attrs, :tenant_id)),
         :ok <- ModuleRegistration.require_text(:module, ModuleRegistration.attr(attrs, :module)),
         {:ok, target_version} <-
           positive_integer(:target_version, ModuleRegistration.attr(attrs, :target_version)) do
      tenant_id = String.trim(ModuleRegistration.attr(attrs, :tenant_id))
      module_name = String.trim(ModuleRegistration.attr(attrs, :module))
      key = {tenant_id, module_name}
      versions = Map.get(state.configs, key, [])
      actor = ModuleRegistration.attr(attrs, :actor, "operator")

      case Enum.find(versions, &(&1.version == target_version)) do
        nil ->
          {:reply, {:error, :not_found}, state}

        target ->
          next_version = next_version(versions)

          {:ok, rollback} =
            ModuleConfigVersion.new(%{
              tenant_id: tenant_id,
              module: module_name,
              version: next_version,
              config: target.config,
              updated_by: actor
            })

          configs = Map.put(state.configs, key, [rollback | versions])

          audit!("module_config.rolled_back", "#{tenant_id}/#{module_name}", actor, %{
            version: rollback.version,
            target_version: target_version
          })

          state = persist!(state, %{state | configs: configs})
          {:reply, {:ok, rollback}, state}
      end
    else
      error -> {:reply, error, state}
    end
  end

  def handle_call({:list_versions, tenant_id, module_name}, _from, state) do
    versions =
      state.configs
      |> Map.get({tenant_id, module_name}, [])
      |> Enum.sort_by(& &1.version)

    {:reply, versions, state}
  end

  def handle_call(:clear, _from, state) do
    state = persist!(state, %{state | registrations: %{}, configs: %{}})
    {:reply, :ok, state}
  end

  defp publish_config_in_state(state, attrs) do
    with :ok <- require_registered(state, ModuleRegistration.attr(attrs, :module)),
         {:ok, plan} <- build_plan(state, attrs),
         :ok <- require_valid_plan(plan),
         {:ok, _approval} <- ApprovalHook.authorize(attrs, plan.diff) do
      tenant_id = String.trim(ModuleRegistration.attr(attrs, :tenant_id))
      module_name = String.trim(ModuleRegistration.attr(attrs, :module))
      key = {tenant_id, module_name}
      versions = Map.get(state.configs, key, [])
      actor = ModuleRegistration.attr(attrs, :actor, "operator")

      case versions do
        [latest | _] when latest.checksum == plan.checksum ->
          audit!("module_config.publish_idempotent", "#{tenant_id}/#{module_name}", actor, %{
            version: latest.version,
            checksum: latest.checksum
          })

          {:reply, {:ok, latest}, state}

        _ ->
          {:ok, version} =
            ModuleConfigVersion.new(%{
              tenant_id: tenant_id,
              module: module_name,
              version: plan.next_version,
              config: plan.config,
              updated_by: actor
            })

          configs = Map.put(state.configs, key, [version | versions])

          audit!("module_config.published", "#{tenant_id}/#{module_name}", actor, %{
            version: version.version,
            checksum: version.checksum,
            changed: plan.diff.changed,
            added: plan.diff.added,
            removed: plan.diff.removed
          })

          audit!("module_config.rolled_out", "#{tenant_id}/#{module_name}", actor, %{
            version: version.version
          })

          state = persist!(state, %{state | configs: configs})
          {:reply, {:ok, version}, state}
      end
    else
      error -> {:reply, error, state}
    end
  end

  defp require_registered(state, module_name) when is_binary(module_name) do
    if Map.has_key?(state.registrations, String.trim(module_name)) do
      :ok
    else
      {:error, {:module_not_registered, module_name}}
    end
  end

  defp require_registered(_state, module_name),
    do: {:error, {:module_not_registered, module_name}}

  defp build_plan(state, attrs) do
    with :ok <- Validation.validate_config(attrs) do
      tenant_id = String.trim(ModuleRegistration.attr(attrs, :tenant_id))
      module_name = String.trim(ModuleRegistration.attr(attrs, :module))

      {:ok, config} =
        ModuleRegistration.optional_map(:config, ModuleRegistration.attr(attrs, :config, %{}))

      config = ModuleConfigVersion.normalize_json(config)
      versions = Map.get(state.configs, {tenant_id, module_name}, [])
      latest_config = latest_config(versions)
      diff = Diff.compare(latest_config, config)
      checksum = ModuleConfigVersion.checksum(config)

      {:ok,
       %ConfigPlan{
         tenant_id: tenant_id,
         module: module_name,
         next_version: next_version(versions),
         checksum: checksum,
         valid: true,
         validation_errors: [],
         diff: diff,
         approval: ApprovalHook.evaluate(attrs, diff),
         dry_run: truthy?(ModuleRegistration.attr(attrs, :dry_run, false)),
         config: config
       }}
    else
      {:error, {:validation_failed, errors}} ->
        {:ok,
         %ConfigPlan{
           tenant_id: ModuleRegistration.attr(attrs, :tenant_id),
           module: ModuleRegistration.attr(attrs, :module),
           valid: false,
           validation_errors: errors,
           diff: %{added: [], removed: [], changed: []},
           approval: ApprovalHook.evaluate(attrs, %{added: [], removed: [], changed: []}),
           dry_run: truthy?(ModuleRegistration.attr(attrs, :dry_run, false)),
           config: ModuleRegistration.attr(attrs, :config, %{})
         }}

      error ->
        error
    end
  end

  defp latest_config([latest | _]), do: latest.config || %{}
  defp latest_config([]), do: %{}

  defp require_valid_plan(%ConfigPlan{valid: true}), do: :ok

  defp require_valid_plan(%ConfigPlan{validation_errors: errors}),
    do: {:error, {:validation_failed, errors}}

  defp next_version([]), do: 1
  defp next_version([latest | _]), do: latest.version + 1

  defp positive_integer(_field, value) when is_integer(value) and value > 0, do: {:ok, value}
  defp positive_integer(field, value), do: {:error, {:invalid_integer, field, value}}

  defp truthy?(value) when is_boolean(value), do: value

  defp truthy?(value) when is_binary(value),
    do: String.downcase(value) in ["1", "true", "on", "yes"]

  defp truthy?(_value), do: false

  defp audit!(action, resource, actor, metadata) do
    AuditLog.append(%{
      actor: actor,
      action: action,
      resource: resource,
      metadata: metadata
    })
  end

  defp persist!(state, next_state) do
    persisted = Map.take(next_state, [:registrations, :configs])
    TelemetryFabricControl.StateFile.persist(state.storage_path, persisted)
    next_state
  end

  defp default_state, do: %{registrations: %{}, configs: %{}}
end
