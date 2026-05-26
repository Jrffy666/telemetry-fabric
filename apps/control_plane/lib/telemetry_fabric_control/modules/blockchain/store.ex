defmodule TelemetryFabricControl.Modules.Blockchain.Store do
  @moduledoc """
  OTP-backed blockchain module control store.

  This namespace contains blockchain-specific records so platform core does not
  accumulate chain fields.
  """

  use GenServer

  alias TelemetryFabricControl.AuditLog
  alias TelemetryFabricControl.Modules.Blockchain.Validator
  alias TelemetryFabricControl.Modules.ModuleConfigVersion
  alias TelemetryFabricControl.Modules.ModuleRegistration

  @kinds [
    :chains,
    :rpc_endpoints,
    :address_watchlist,
    :contract_watchlist,
    :token_watchlist,
    :filter_rules,
    :crawl_assignments,
    :checkpoints
  ]

  @definitions %{
    chains: %{
      id_field: :chain_key,
      required: [:tenant_id, :chain_key, :display_name, :network],
      defaults: %{enabled: true, metadata: %{}},
      action: "chain"
    },
    rpc_endpoints: %{
      id_field: :endpoint_id,
      required: [:tenant_id, :endpoint_id, :chain_key, :url],
      defaults: %{priority: 100, enabled: true, metadata: %{}},
      action: "rpc_endpoint"
    },
    address_watchlist: %{
      id_field: :entry_id,
      required: [:tenant_id, :entry_id, :chain_key, :address],
      defaults: %{label: "", enabled: true, metadata: %{}},
      action: "address_watch"
    },
    contract_watchlist: %{
      id_field: :contract_id,
      required: [:tenant_id, :contract_id, :chain_key, :address],
      defaults: %{label: "", abi_ref: "", enabled: true, metadata: %{}},
      action: "contract_watch"
    },
    token_watchlist: %{
      id_field: :token_id,
      required: [:tenant_id, :token_id, :chain_key, :contract_address],
      defaults: %{symbol: "", decimals: nil, enabled: true, metadata: %{}},
      action: "token_watch"
    },
    filter_rules: %{
      id_field: :rule_id,
      required: [:tenant_id, :rule_id, :name, :expression],
      defaults: %{chain_key: nil, action: "keep", enabled: true, metadata: %{}},
      action: "filter_rule"
    },
    crawl_assignments: %{
      id_field: :assignment_id,
      required: [:tenant_id, :assignment_id, :chain_key, :crawler_id],
      defaults: %{
        start_cursor: %{},
        end_cursor: nil,
        config: %{},
        enabled: true,
        metadata: %{}
      },
      action: "crawl_assignment"
    },
    checkpoints: %{
      id_field: :assignment_id,
      required: [:tenant_id, :assignment_id, :chain_key, :cursor],
      defaults: %{finalized_cursor: %{}, updated_by: "crawler"},
      action: "checkpoint"
    }
  }

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def list(kind, tenant_id, opts \\ []), do: list(__MODULE__, kind, tenant_id, opts)

  def list(server, kind, tenant_id, opts),
    do: GenServer.call(server, {:list, kind, tenant_id, opts})

  def get(kind, tenant_id, id), do: get(__MODULE__, kind, tenant_id, id)
  def get(server, kind, tenant_id, id), do: GenServer.call(server, {:get, kind, tenant_id, id})

  def upsert(kind, attrs, actor \\ "operator"), do: upsert(__MODULE__, kind, attrs, actor)

  def upsert(server, kind, attrs, actor) do
    GenServer.call(server, {:upsert, kind, attrs, actor})
  end

  def delete(kind, tenant_id, id, actor \\ "operator"),
    do: delete(__MODULE__, kind, tenant_id, id, actor)

  def delete(server, kind, tenant_id, id, actor) do
    GenServer.call(server, {:delete, kind, tenant_id, id, actor})
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
  def handle_call({:list, kind, tenant_id, opts}, _from, state) do
    with {:ok, kind} <- normalize_kind(kind),
         :ok <- ModuleRegistration.require_text(:tenant_id, tenant_id) do
      pagination = pagination(opts)

      records =
        state
        |> Map.fetch!(kind)
        |> Map.values()
        |> Enum.filter(&(&1["tenant_id"] == tenant_id))
        |> Enum.sort_by(&resource_sort_key(kind, &1))
        |> Enum.drop(pagination.offset)
        |> Enum.take(pagination.limit)

      {:reply, {:ok, records}, state}
    else
      error -> {:reply, error, state}
    end
  end

  def handle_call({:get, kind, tenant_id, id}, _from, state) do
    with {:ok, kind} <- normalize_kind(kind),
         :ok <- ModuleRegistration.require_text(:tenant_id, tenant_id),
         :ok <- ModuleRegistration.require_text(:id, id) do
      result =
        case Map.get(Map.fetch!(state, kind), {tenant_id, id}) do
          nil -> {:error, :not_found}
          record -> {:ok, record}
        end

      {:reply, result, state}
    else
      error -> {:reply, error, state}
    end
  end

  def handle_call({:upsert, kind, attrs, actor}, _from, state) do
    with {:ok, kind} <- normalize_kind(kind),
         {:ok, record} <- build_record(kind, attrs),
         :ok <- Validator.validate_record(kind, record) do
      id_field = definition(kind).id_field
      key = {record["tenant_id"], record[to_string(id_field)]}
      collection = Map.fetch!(state, kind)
      previous = Map.get(collection, key)
      record = preserve_inserted_at(previous, record)

      if equivalent_record?(previous, record) do
        {:reply, {:ok, public_record(kind, previous)}, state}
      else
        collection = Map.put(collection, key, record)

        audit!(
          "blockchain.#{definition(kind).action}.upserted",
          resource(kind, record),
          actor,
          %{tenant_id: record["tenant_id"]}
        )

        state = persist!(state, Map.put(state, kind, collection))
        {:reply, {:ok, public_record(kind, record)}, state}
      end
    else
      error -> {:reply, error, state}
    end
  end

  def handle_call({:delete, kind, tenant_id, id, actor}, _from, state) do
    with {:ok, kind} <- normalize_kind(kind),
         :ok <- ModuleRegistration.require_text(:tenant_id, tenant_id),
         :ok <- ModuleRegistration.require_text(:id, id) do
      key = {tenant_id, id}
      collection = Map.fetch!(state, kind)

      if Map.has_key?(collection, key) do
        record = Map.fetch!(collection, key)

        audit!(
          "blockchain.#{definition(kind).action}.deleted",
          resource(kind, record),
          actor,
          %{tenant_id: tenant_id}
        )

        state = persist!(state, Map.put(state, kind, Map.delete(collection, key)))
        {:reply, :ok, state}
      else
        {:reply, {:error, :not_found}, state}
      end
    else
      error -> {:reply, error, state}
    end
  end

  def handle_call(:clear, _from, state) do
    state = persist!(state, Map.merge(state, default_state()))
    {:reply, :ok, state}
  end

  defp pagination(opts) do
    limit =
      opts
      |> Keyword.get(:limit, 100)
      |> normalize_integer(100)
      |> min(500)
      |> max(1)

    offset =
      opts
      |> Keyword.get(:offset, 0)
      |> normalize_integer(0)
      |> max(0)

    %{limit: limit, offset: offset}
  end

  defp build_record(kind, attrs) do
    definition = definition(kind)
    attrs = normalize_attrs(attrs)

    with :ok <- require_fields(definition.required, attrs),
         {:ok, enabled} <- optional_enabled(definition, attrs),
         {:ok, maps} <- validate_map_fields(kind, attrs) do
      now = ModuleRegistration.timestamp() |> DateTime.to_iso8601()

      record =
        definition.defaults
        |> stringify_map()
        |> Map.merge(attrs)
        |> Map.merge(maps)
        |> maybe_put_enabled(definition, enabled)
        |> then(&Validator.redact_record(kind, &1))
        |> Map.put("updated_at", now)
        |> Map.put_new("inserted_at", now)
        |> ModuleConfigVersion.normalize_json()

      {:ok, record}
    end
  end

  defp require_fields(fields, attrs) do
    Enum.reduce_while(fields, :ok, fn field, :ok ->
      case Map.get(attrs, to_string(field)) do
        value when is_binary(value) ->
          if String.trim(value) == "" do
            {:halt, {:error, {:empty, field}}}
          else
            {:cont, :ok}
          end

        value when field in [:expression, :cursor] and is_map(value) ->
          {:cont, :ok}

        _value ->
          {:halt, {:error, {:empty, field}}}
      end
    end)
  end

  defp validate_map_fields(:filter_rules, attrs) do
    expression = Map.get(attrs, "expression", %{})

    if is_map(expression) do
      {:ok, %{"expression" => expression}}
    else
      {:error, {:invalid_map, :expression, expression}}
    end
  end

  defp validate_map_fields(:crawl_assignments, attrs) do
    with {:ok, start_cursor} <- optional_map(:start_cursor, Map.get(attrs, "start_cursor", %{})),
         {:ok, end_cursor} <- optional_nullable_map(:end_cursor, Map.get(attrs, "end_cursor")),
         {:ok, config} <- optional_map(:config, Map.get(attrs, "config", %{})),
         {:ok, metadata} <- optional_map(:metadata, Map.get(attrs, "metadata", %{})) do
      {:ok,
       %{
         "start_cursor" => start_cursor,
         "end_cursor" => end_cursor,
         "config" => config,
         "metadata" => metadata
       }}
    end
  end

  defp validate_map_fields(:checkpoints, attrs) do
    with {:ok, cursor} <- optional_map(:cursor, Map.get(attrs, "cursor", %{})),
         {:ok, finalized_cursor} <-
           optional_map(:finalized_cursor, Map.get(attrs, "finalized_cursor", %{})) do
      {:ok, %{"cursor" => cursor, "finalized_cursor" => finalized_cursor}}
    end
  end

  defp validate_map_fields(_kind, attrs) do
    with {:ok, metadata} <- optional_map(:metadata, Map.get(attrs, "metadata", %{})) do
      {:ok, %{"metadata" => metadata}}
    end
  end

  defp optional_map(_field, value) when is_map(value), do: {:ok, value}
  defp optional_map(field, value), do: {:error, {:invalid_map, field, value}}

  defp optional_nullable_map(_field, nil), do: {:ok, nil}
  defp optional_nullable_map(_field, value) when is_map(value), do: {:ok, value}
  defp optional_nullable_map(field, value), do: {:error, {:invalid_map, field, value}}

  defp optional_boolean(value) when is_boolean(value), do: {:ok, value}
  defp optional_boolean(value), do: {:error, {:invalid_boolean, :enabled, value}}

  defp optional_enabled(%{defaults: defaults}, attrs) do
    if Map.has_key?(defaults, :enabled) do
      optional_boolean(Map.get(attrs, "enabled", defaults[:enabled]))
    else
      {:ok, nil}
    end
  end

  defp maybe_put_enabled(record, %{defaults: defaults}, enabled) do
    if Map.has_key?(defaults, :enabled) do
      Map.put(record, "enabled", enabled)
    else
      record
    end
  end

  defp normalize_attrs(attrs) when is_map(attrs) do
    attrs
    |> Enum.map(fn {key, value} -> {to_string(key), normalize_value(value)} end)
    |> Map.new()
  end

  defp normalize_value(value) when is_map(value), do: normalize_attrs(value)
  defp normalize_value(value) when is_list(value), do: Enum.map(value, &normalize_value/1)
  defp normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_value(value), do: value

  defp stringify_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), value} end)
  end

  defp normalize_kind(kind) when kind in @kinds, do: {:ok, kind}

  defp normalize_kind(kind) when is_binary(kind) do
    kind
    |> String.to_existing_atom()
    |> normalize_kind()
  rescue
    ArgumentError -> {:error, {:unknown_resource, kind}}
  end

  defp normalize_kind(kind), do: {:error, {:unknown_resource, kind}}

  defp definition(kind), do: Map.fetch!(@definitions, kind)

  defp resource_sort_key(kind, record) do
    id_field = definition(kind).id_field
    {record["tenant_id"], record[to_string(id_field)]}
  end

  defp resource(kind, record) do
    id_field = definition(kind).id_field
    "blockchain/#{kind}/#{record["tenant_id"]}/#{record[to_string(id_field)]}"
  end

  defp public_record(kind, record), do: Validator.redact_record(kind, record)

  defp preserve_inserted_at(nil, record), do: record

  defp preserve_inserted_at(previous, record),
    do: Map.put(record, "inserted_at", previous["inserted_at"])

  defp equivalent_record?(nil, _record), do: false

  defp equivalent_record?(previous, record) do
    comparable(previous) == comparable(record)
  end

  defp comparable(record), do: Map.drop(record, ["inserted_at", "updated_at"])

  defp normalize_integer(value, _default) when is_integer(value), do: value

  defp normalize_integer(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> integer
      _ -> default
    end
  end

  defp normalize_integer(_value, default), do: default

  defp audit!(action, resource, actor, metadata) do
    AuditLog.append(%{
      actor: actor,
      action: action,
      resource: resource,
      metadata: metadata
    })
  end

  defp persist!(state, next_state) do
    persisted = Map.take(next_state, @kinds)
    TelemetryFabricControl.StateFile.persist(state.storage_path, persisted)
    next_state
  end

  defp default_state do
    Map.new(@kinds, fn kind -> {kind, %{}} end)
  end
end
