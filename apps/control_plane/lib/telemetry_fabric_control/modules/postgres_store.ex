defmodule TelemetryFabricControl.Modules.PostgresStore do
  @moduledoc """
  PostgreSQL-backed store for generic business-module registry and config.
  """

  import Ecto.Query

  alias TelemetryFabricControl.Modules.ApprovalHook
  alias TelemetryFabricControl.Modules.ConfigPlan
  alias TelemetryFabricControl.Modules.Diff
  alias TelemetryFabricControl.Modules.ModuleConfigVersion
  alias TelemetryFabricControl.Modules.ModuleRegistration
  alias TelemetryFabricControl.Modules.Validation
  alias TelemetryFabricControl.Repo
  alias TelemetryFabricControl.Schema.AuditEvent
  alias TelemetryFabricControl.Schema.ModuleConfigVersion, as: ModuleConfigVersionRow
  alias TelemetryFabricControl.Schema.ModuleRegistration, as: ModuleRegistrationRow
  alias TelemetryFabricControl.Schema.Tenant

  def register_module(attrs, repo \\ Repo) when is_map(attrs) do
    with {:ok, registration} <- ModuleRegistration.new(attrs) do
      actor = ModuleRegistration.attr(attrs, :actor, "operator")
      now = timestamp()

      result =
        repo.transaction(fn ->
          row =
            registration
            |> registration_row()
            |> Map.put(:inserted_at, now)
            |> Map.put(:updated_at, now)

          repo.insert_all(ModuleRegistrationRow, [row],
            on_conflict:
              {:replace, [:display_name, :owner, :description, :enabled, :metadata, :updated_at]},
            conflict_target: [:module_name]
          )

          insert_audit!(repo, actor, "module.registered", registration.module, %{
            enabled: registration.enabled
          })

          %{registration | inserted_at: now, updated_at: now}
        end)

      unwrap_transaction(result)
    end
  end

  def list_modules(repo \\ Repo) do
    ModuleRegistrationRow
    |> order_by([module], asc: module.module_name)
    |> repo.all()
    |> Enum.map(&registration_from_row/1)
  end

  def get_module(module_name, repo \\ Repo) do
    case repo.get(ModuleRegistrationRow, module_name) do
      nil -> {:error, :not_found}
      row -> {:ok, registration_from_row(row)}
    end
  end

  def validate_config(attrs, repo \\ Repo) when is_map(attrs) do
    with {:ok, plan} <- build_plan(attrs, repo) do
      {:ok, plan}
    end
  end

  def dry_run_config(attrs, repo \\ Repo) when is_map(attrs) do
    with {:ok, plan} <- build_plan(Map.put(attrs, :dry_run, true), repo) do
      {:ok, plan}
    end
  end

  def diff_config(attrs, repo \\ Repo) when is_map(attrs) do
    with {:ok, plan} <- build_plan(attrs, repo) do
      {:ok, plan.diff}
    end
  end

  def publish_config(attrs, repo \\ Repo), do: rollout_config(attrs, repo)

  def rollout_config(attrs, repo \\ Repo) when is_map(attrs) do
    with {:ok, plan} <- build_plan(attrs, repo),
         :ok <- require_valid_plan(plan),
         {:ok, _approval} <- ApprovalHook.authorize(attrs, plan.diff) do
      tenant_id = String.trim(ModuleRegistration.attr(attrs, :tenant_id))
      module_name = String.trim(ModuleRegistration.attr(attrs, :module))
      actor = ModuleRegistration.attr(attrs, :actor, "operator")

      result =
        repo.transaction(fn ->
          if repo.get(ModuleRegistrationRow, module_name) == nil do
            repo.rollback({:module_not_registered, module_name})
          end

          ensure_tenant!(repo, tenant_id)

          latest = repo.one(latest_query(tenant_id, module_name))

          if latest && latest.checksum == plan.checksum do
            insert_audit!(
              repo,
              actor,
              "module_config.publish_idempotent",
              "#{tenant_id}/#{module_name}",
              %{
                version: latest.version,
                checksum: latest.checksum
              }
            )

            config_from_row(latest)
          else
            {:ok, config_version} =
              ModuleConfigVersion.new(%{
                tenant_id: tenant_id,
                module: module_name,
                version: plan.next_version,
                config: plan.config,
                updated_by: actor
              })

            repo.insert_all(ModuleConfigVersionRow, [config_version_row(config_version)])

            insert_audit!(
              repo,
              actor,
              "module_config.published",
              "#{tenant_id}/#{module_name}",
              %{
                version: config_version.version,
                checksum: config_version.checksum,
                added: plan.diff.added,
                removed: plan.diff.removed,
                changed: plan.diff.changed
              }
            )

            insert_audit!(
              repo,
              actor,
              "module_config.rolled_out",
              "#{tenant_id}/#{module_name}",
              %{
                version: config_version.version
              }
            )

            config_version
          end
        end)

      unwrap_transaction(result)
    end
  end

  def fetch_config(attrs, repo \\ Repo) when is_map(attrs) do
    with :ok <-
           ModuleRegistration.require_text(:tenant_id, ModuleRegistration.attr(attrs, :tenant_id)),
         :ok <- ModuleRegistration.require_text(:module, ModuleRegistration.attr(attrs, :module)) do
      tenant_id = String.trim(ModuleRegistration.attr(attrs, :tenant_id))
      module_name = String.trim(ModuleRegistration.attr(attrs, :module))
      current_version = ModuleRegistration.attr(attrs, :current_version, 0)

      case repo.one(latest_query(tenant_id, module_name)) do
        nil ->
          {:error, :not_found}

        row ->
          if is_integer(current_version) and row.version <= current_version do
            {:ok, :up_to_date}
          else
            {:ok, config_from_row(row)}
          end
      end
    end
  end

  def rollback_config(attrs, repo \\ Repo) when is_map(attrs) do
    with :ok <-
           ModuleRegistration.require_text(:tenant_id, ModuleRegistration.attr(attrs, :tenant_id)),
         :ok <- ModuleRegistration.require_text(:module, ModuleRegistration.attr(attrs, :module)),
         {:ok, target_version} <-
           positive_integer(:target_version, ModuleRegistration.attr(attrs, :target_version)) do
      tenant_id = String.trim(ModuleRegistration.attr(attrs, :tenant_id))
      module_name = String.trim(ModuleRegistration.attr(attrs, :module))
      actor = ModuleRegistration.attr(attrs, :actor, "operator")

      result =
        repo.transaction(fn ->
          target =
            repo.one(
              from(config in ModuleConfigVersionRow,
                where:
                  config.tenant_id == ^tenant_id and config.module_name == ^module_name and
                    config.version == ^target_version
              )
            )

          if target == nil do
            repo.rollback(:not_found)
          end

          version = next_version(repo, tenant_id, module_name)

          {:ok, rollback} =
            ModuleConfigVersion.new(%{
              tenant_id: tenant_id,
              module: module_name,
              version: version,
              config: target.config,
              updated_by: actor
            })

          repo.insert_all(ModuleConfigVersionRow, [config_version_row(rollback)])

          insert_audit!(
            repo,
            actor,
            "module_config.rolled_back",
            "#{tenant_id}/#{module_name}",
            %{
              version: version,
              target_version: target_version
            }
          )

          rollback
        end)

      unwrap_transaction(result)
    end
  end

  def list_versions(tenant_id, module_name, repo \\ Repo) do
    repo.all(
      from(config in ModuleConfigVersionRow,
        where: config.tenant_id == ^tenant_id and config.module_name == ^module_name,
        order_by: [asc: config.version]
      )
    )
    |> Enum.map(&config_from_row/1)
  end

  defp latest_query(tenant_id, module_name) do
    from(config in ModuleConfigVersionRow,
      where: config.tenant_id == ^tenant_id and config.module_name == ^module_name,
      order_by: [desc: config.version],
      limit: 1
    )
  end

  defp next_version(repo, tenant_id, module_name) do
    version =
      latest_query(tenant_id, module_name)
      |> select([config], config.version)
      |> repo.one()

    (version || 0) + 1
  end

  defp build_plan(attrs, repo) do
    with :ok <- Validation.validate_config(attrs),
         :ok <- module_registered?(repo, ModuleRegistration.attr(attrs, :module)) do
      tenant_id = String.trim(ModuleRegistration.attr(attrs, :tenant_id))
      module_name = String.trim(ModuleRegistration.attr(attrs, :module))

      {:ok, config} =
        ModuleRegistration.optional_map(:config, ModuleRegistration.attr(attrs, :config, %{}))

      config = ModuleConfigVersion.normalize_json(config)
      latest = repo.one(latest_query(tenant_id, module_name))
      latest_config = if latest, do: latest.config || %{}, else: %{}
      diff = Diff.compare(latest_config, config)

      {:ok,
       %ConfigPlan{
         tenant_id: tenant_id,
         module: module_name,
         next_version: next_version(repo, tenant_id, module_name),
         checksum: ModuleConfigVersion.checksum(config),
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

  defp module_registered?(repo, module_name) when is_binary(module_name) do
    if repo.get(ModuleRegistrationRow, String.trim(module_name)) do
      :ok
    else
      {:error, {:module_not_registered, module_name}}
    end
  end

  defp module_registered?(_repo, module_name), do: {:error, {:module_not_registered, module_name}}

  defp require_valid_plan(%ConfigPlan{valid: true}), do: :ok

  defp require_valid_plan(%ConfigPlan{validation_errors: errors}),
    do: {:error, {:validation_failed, errors}}

  defp ensure_tenant!(repo, tenant_id) do
    now = timestamp()

    repo.insert_all(
      Tenant,
      [%{tenant_id: tenant_id, inserted_at: now, updated_at: now}],
      on_conflict: {:replace, [:updated_at]},
      conflict_target: [:tenant_id]
    )
  end

  defp registration_row(%ModuleRegistration{} = registration) do
    %{
      module_name: registration.module,
      display_name: registration.display_name,
      owner: registration.owner,
      description: registration.description,
      enabled: registration.enabled,
      metadata: ModuleConfigVersion.normalize_json(registration.metadata || %{})
    }
  end

  defp config_version_row(%ModuleConfigVersion{} = config) do
    %{
      tenant_id: config.tenant_id,
      module_name: config.module,
      version: config.version,
      config: ModuleConfigVersion.normalize_json(config.config || %{}),
      checksum: config.checksum,
      updated_by: config.updated_by,
      inserted_at: config.inserted_at
    }
  end

  defp registration_from_row(%ModuleRegistrationRow{} = row) do
    %ModuleRegistration{
      module: row.module_name,
      display_name: row.display_name,
      owner: row.owner,
      description: row.description || "",
      enabled: row.enabled,
      metadata: row.metadata || %{},
      inserted_at: row.inserted_at,
      updated_at: row.updated_at
    }
  end

  defp config_from_row(%ModuleConfigVersionRow{} = row) do
    %ModuleConfigVersion{
      tenant_id: row.tenant_id,
      module: row.module_name,
      version: row.version,
      config: row.config || %{},
      checksum: row.checksum,
      updated_by: row.updated_by,
      inserted_at: row.inserted_at
    }
  end

  defp insert_audit!(repo, actor, action, resource, metadata) do
    row = %{
      event_id: new_event_id(),
      actor: actor,
      action: action,
      resource: resource,
      metadata: ModuleConfigVersion.normalize_json(metadata),
      inserted_at: timestamp()
    }

    repo.insert_all(AuditEvent, [row],
      on_conflict: :nothing,
      conflict_target: [:event_id]
    )
  end

  defp positive_integer(_field, value) when is_integer(value) and value > 0, do: {:ok, value}
  defp positive_integer(field, value), do: {:error, {:invalid_integer, field, value}}

  defp truthy?(value) when is_boolean(value), do: value

  defp truthy?(value) when is_binary(value),
    do: String.downcase(value) in ["1", "true", "on", "yes"]

  defp truthy?(_value), do: false

  defp unwrap_transaction({:ok, value}), do: {:ok, value}
  defp unwrap_transaction({:error, reason}), do: {:error, reason}

  defp timestamp, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)

  defp new_event_id do
    System.system_time(:microsecond) * 1_000 + rem(System.unique_integer([:positive]), 1_000)
  end
end
