defmodule TelemetryFabricControl.HttpControlServer do
  @moduledoc """
  Minimal dependency-free HTTP adapter for the control-plane domain API.

  The adapter is intentionally small and replaceable. It exists so agents and
  operators can exercise the AgentControl workflow before Phoenix or gRPC is
  introduced.
  """

  use GenServer

  require Logger

  alias TelemetryFabricControl.ControlCommand
  alias TelemetryFabricControl.ControlService
  alias TelemetryFabricControl.ControlService.AgentStatusResponse
  alias TelemetryFabricControl.ControlService.ConfigUpdate
  alias TelemetryFabricControl.ControlService.RegisterAgentResponse
  alias TelemetryFabricControl.HttpMetrics
  alias TelemetryFabricControl.Json
  alias TelemetryFabricControl.Modules.Blockchain.Control, as: BlockchainControl
  alias TelemetryFabricControl.Modules.Control, as: ModuleControl
  alias TelemetryFabricControl.Modules.ModuleConfigVersion
  alias TelemetryFabricControl.Modules.ModuleRegistration
  alias TelemetryFabricControl.Repo

  @read_timeout 5_000
  @max_header_bytes 16 * 1024
  @default_max_body_bytes 1 * 1024 * 1024

  defmodule RequestTooLargeError do
    @moduledoc false
    defexception [:message, :method, :path, :request_id]
  end

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def port(server \\ __MODULE__) do
    GenServer.call(server, :port)
  end

  @impl true
  def init(opts) do
    host = Keyword.get(opts, :host, "127.0.0.1")
    port = Keyword.get(opts, :port, 4001)
    ip = parse_ip!(host)
    security = security_options(opts)
    max_body_bytes = Keyword.get(opts, :max_body_bytes, @default_max_body_bytes)
    rate_limit = rate_limit_options(opts)

    {:ok, transport, socket} = listen(port, ip, security)

    {:ok, actual_port} = bound_port(transport, socket)

    {:ok, acceptor} =
      Task.start_link(fn ->
        accept_loop(socket, transport, security, max_body_bytes, rate_limit)
      end)

    {:ok,
     %{
       socket: socket,
       transport: transport,
       acceptor: acceptor,
       host: host,
       port: actual_port,
       security: security,
       max_body_bytes: max_body_bytes,
       rate_limit: rate_limit
     }}
  end

  @impl true
  def handle_call(:port, _from, state) do
    {:reply, state.port, state}
  end

  @impl true
  def terminate(_reason, state) do
    close(state.transport, state.socket)
    :ok
  end

  defp accept_loop(socket, transport, security, max_body_bytes, rate_limit) do
    case accept(transport, socket) do
      {:ok, client} ->
        Task.start(fn ->
          handle_client(transport, client, security, max_body_bytes, rate_limit)
        end)

        accept_loop(socket, transport, security, max_body_bytes, rate_limit)

      {:error, :closed} ->
        :ok

      {:error, _reason} ->
        accept_loop(socket, transport, security, max_body_bytes, rate_limit)
    end
  end

  defp handle_client(transport, socket, security, max_body_bytes, rate_limit) do
    started_at = System.monotonic_time()

    try do
      response =
        socket
        |> read_request(transport, max_body_bytes)
        |> respond_to_request(security, rate_limit, started_at)

      :ok = send_data(transport, socket, response)
      close(transport, socket)
    rescue
      error in [RequestTooLargeError] ->
        request = error_request(error)

        response =
          response(413, %{error: "request_body_too_large", message: error.message})
          |> put_request_id_header(request.request_id)

        log_request(request, response, started_at)
        _ = send_data(transport, socket, response)
        close(transport, socket)

      error in [ArgumentError, KeyError] ->
        body = %{error: Exception.message(error)}
        response = response(400, body)
        log_request(nil, response, started_at)
        _ = send_data(transport, socket, response)
        close(transport, socket)

      error ->
        body = %{error: Exception.message(error)}
        response = response(500, body)
        log_request(nil, response, started_at)
        _ = send_data(transport, socket, response)
        close(transport, socket)
    end
  end

  defp respond_to_request(request, security, rate_limit, started_at) do
    response =
      route_request_safely(request, security, rate_limit)
      |> put_request_id_header(request.request_id)

    log_request(request, response, started_at)
    response
  end

  defp route_request_safely(request, security, rate_limit) do
    route_request(request, security, rate_limit)
  rescue
    error in [ArgumentError, KeyError] ->
      response(400, %{error: Exception.message(error)})

    error ->
      response(500, %{error: Exception.message(error)})
  end

  defp read_request(socket, transport, max_body_bytes) do
    {head, body} = read_until_headers(socket, transport, "")
    [request_line | header_lines] = String.split(head, "\r\n")
    [method, path, _version] = String.split(request_line, " ", parts: 3)
    headers = parse_headers(header_lines)
    content_length = headers |> Map.get("content-length", "0") |> String.to_integer()
    request_id = request_id(headers)

    if content_length > max_body_bytes do
      raise RequestTooLargeError,
        message: "request body exceeds maximum size",
        method: method,
        path: path,
        request_id: request_id
    end

    body = read_body(socket, transport, body, content_length)

    %{method: method, path: path, headers: headers, body: body, request_id: request_id}
  end

  defp read_until_headers(socket, transport, acc) do
    case String.split(acc, "\r\n\r\n", parts: 2) do
      [head, body] ->
        if byte_size(head) > @max_header_bytes, do: raise("request headers exceed maximum size")
        {head, body}

      [_] ->
        if byte_size(acc) > @max_header_bytes, do: raise("request headers exceed maximum size")

        case recv(transport, socket, 0, @read_timeout) do
          {:ok, chunk} -> read_until_headers(socket, transport, acc <> chunk)
          {:error, reason} -> raise "failed to read HTTP request: #{inspect(reason)}"
        end
    end
  end

  defp read_body(_socket, _transport, body, content_length)
       when byte_size(body) >= content_length do
    binary_part(body, 0, content_length)
  end

  defp read_body(socket, transport, body, content_length) do
    remaining = content_length - byte_size(body)

    case recv(transport, socket, remaining, @read_timeout) do
      {:ok, chunk} -> read_body(socket, transport, body <> chunk, content_length)
      {:error, reason} -> raise "failed to read HTTP body: #{inspect(reason)}"
    end
  end

  defp parse_headers(header_lines) do
    Enum.reduce(header_lines, %{}, fn line, headers ->
      case String.split(line, ":", parts: 2) do
        [name, value] -> Map.put(headers, String.downcase(String.trim(name)), String.trim(value))
        _ -> headers
      end
    end)
  end

  defp request_id(headers) do
    case Map.get(headers, "x-request-id") do
      value when is_binary(value) ->
        value = String.trim(value)

        if valid_request_id?(value) do
          value
        else
          generate_request_id()
        end

      _ ->
        generate_request_id()
    end
  end

  defp valid_request_id?(value) when is_binary(value) do
    byte_size(value) > 0 and byte_size(value) <= 128 and
      not String.contains?(value, ["\r", "\n"])
  end

  defp generate_request_id do
    "req-" <> Integer.to_string(System.unique_integer([:positive, :monotonic]), 36)
  end

  defp put_request_id_header(response, request_id) do
    [status_line, content_type, content_length, connection | tail] = response

    [
      status_line,
      content_type,
      content_length,
      "X-Request-Id: #{request_id}\r\n",
      connection | tail
    ]
  end

  defp log_request(request, response, started_at) do
    duration_us =
      System.monotonic_time()
      |> Kernel.-(started_at)
      |> System.convert_time_unit(:native, :microsecond)

    method = request_field(request, :method)
    path = request_field(request, :path)
    request_id = request_field(request, :request_id)
    status = response_status(response)
    :ok = HttpMetrics.record_request(method, path, status, duration_us)

    Logger.info(fn ->
      "http_control_request method=#{method} " <>
        "path=#{path} status=#{status} " <>
        "duration_us=#{duration_us} request_id=#{request_id}"
    end)
  end

  defp request_field(nil, _field), do: "unknown"
  defp request_field(request, field), do: Map.get(request, field, "unknown")

  defp error_request(%RequestTooLargeError{} = error) do
    %{
      method: error.method || "unknown",
      path: error.path || "unknown",
      request_id: error.request_id || "unknown"
    }
  end

  defp response_status(["HTTP/1.1 " <> status | _rest]) do
    case String.split(status, " ", parts: 2) do
      [code | _] -> String.to_integer(code)
      _ -> 0
    end
  rescue
    ArgumentError -> 0
  end

  defp route_request(request, security, rate_limit) do
    with :ok <- check_rate_limit(request, rate_limit),
         {:ok, role} <- authorize(request, security),
         :ok <- authorize_rbac(role, request) do
      route_authorized_request(request)
    else
      {:error, status, body} -> response(status, body)
    end
  end

  defp route_authorized_request(%{method: "GET", path: "/healthz"}) do
    response(200, %{status: "ok"})
  end

  defp route_authorized_request(%{method: "GET", path: "/readyz"}) do
    readiness_response()
  end

  defp route_authorized_request(%{method: "GET", path: "/metrics"}) do
    text_response(200, "text/plain; version=0.0.4", HttpMetrics.prometheus())
  end

  defp route_authorized_request(%{method: "POST", path: "/v1/agents/register", body: body}) do
    body
    |> decode_request()
    |> ControlService.register_agent()
    |> case do
      {:ok, %RegisterAgentResponse{} = result} -> response(200, %{agent: encode_response(result)})
      error -> error_response(error)
    end
  end

  defp route_authorized_request(%{method: "POST", path: "/v1/agents/heartbeat", body: body}) do
    body
    |> decode_request()
    |> ControlService.heartbeat()
    |> case do
      {:ok, commands} -> response(200, %{commands: Enum.map(commands, &encode_response/1)})
      error -> error_response(error)
    end
  end

  defp route_authorized_request(%{method: "POST", path: "/v1/agents/config", body: body}) do
    body
    |> decode_request()
    |> ControlService.config_update()
    |> case do
      {:ok, :up_to_date} -> response(200, %{update: nil})
      {:ok, %ConfigUpdate{} = update} -> response(200, %{update: encode_response(update)})
      error -> error_response(error)
    end
  end

  defp route_authorized_request(%{method: "POST", path: "/v1/agents/status", body: body}) do
    body
    |> decode_request()
    |> ControlService.report_status()
    |> case do
      {:ok, %AgentStatusResponse{} = status} -> response(200, %{status: encode_response(status)})
      error -> error_response(error)
    end
  end

  defp route_authorized_request(%{method: "POST", path: "/v1/agents/commands", body: body}) do
    attrs = decode_request(body)
    kind = attrs |> Map.get(:kind) |> parse_command_kind()

    attrs
    |> Map.fetch!(:agent_id)
    |> ControlService.enqueue_command(kind, Map.get(attrs, :reason, ""))
    |> case do
      {:ok, %ControlCommand{} = command} -> response(200, %{command: encode_response(command)})
      error -> error_response(error)
    end
  end

  defp route_authorized_request(%{method: "POST", path: "/v1/agents/commands/ack", body: body}) do
    body
    |> decode_request()
    |> ControlService.ack_command()
    |> case do
      {:ok, %ControlCommand{} = command} -> response(200, %{command: encode_response(command)})
      error -> error_response(error)
    end
  end

  defp route_authorized_request(%{method: "POST", path: "/v1/pipelines", body: body}) do
    body
    |> decode_request()
    |> ControlService.put_pipeline()
    |> case do
      {:ok, pipeline} -> response(200, %{pipeline: encode_pipeline(pipeline)})
      error -> error_response(error)
    end
  end

  defp route_authorized_request(%{method: "POST", path: "/v1/pipelines/rollback", body: body}) do
    body
    |> decode_request()
    |> ControlService.rollback_pipeline()
    |> case do
      {:ok, pipeline} -> response(200, %{pipeline: encode_pipeline(pipeline)})
      error -> error_response(error)
    end
  end

  defp route_authorized_request(request), do: route_module_request(request)

  defp authorize(%{method: "GET", path: path}, _security)
       when path in ["/healthz", "/readyz", "/metrics"],
       do: {:ok, :public}

  defp authorize(%{path: path} = request, security) do
    path = path_without_query(path)
    role = required_role(request, path)

    with :ok <- authorize_role(request, security, role) do
      {:ok, role}
    end
  end

  defp required_role(request, path) do
    cond do
      path in ["/v1/agents/commands", "/v1/pipelines", "/v1/pipelines/rollback"] ->
        :operator

      path == "/v1/modules/configs/fetch" ->
        :agent

      request.method == "GET" and
          String.starts_with?(path, "/v1/modules/blockchain/checkpoints") ->
        :agent

      String.starts_with?(path, "/v1/modules") ->
        :operator

      true ->
        :agent
    end
  end

  defp authorize_rbac(:public, _request), do: :ok
  defp authorize_rbac(:agent, _request), do: :ok
  defp authorize_rbac(:operator, _request), do: :ok

  defp authorize_role(request, security, :agent) do
    token = bearer_token(request.headers)

    cond do
      token_matches?(token, security.operator_token) -> :ok
      token_matches?(token, security.agent_token) -> :ok
      security.agent_token == nil and security.operator_token == nil -> :ok
      token == nil -> {:error, 401, %{error: "missing_bearer_token"}}
      true -> {:error, 403, %{error: "invalid_bearer_token"}}
    end
  end

  defp authorize_role(request, security, :operator) do
    token = bearer_token(request.headers)

    cond do
      token_matches?(token, security.operator_token) -> :ok
      security.operator_token == nil and security.agent_token == nil -> :ok
      security.operator_token == nil -> {:error, 403, %{error: "operator_token_not_configured"}}
      token == nil -> {:error, 401, %{error: "missing_bearer_token"}}
      true -> {:error, 403, %{error: "invalid_bearer_token"}}
    end
  end

  defp bearer_token(headers) do
    case Map.get(headers, "authorization") do
      "Bearer " <> token -> String.trim(token)
      _ -> nil
    end
  end

  defp token_matches?(nil, _expected), do: false
  defp token_matches?(_token, nil), do: false

  defp token_matches?(token, expected) do
    byte_size(token) == byte_size(expected) and constant_time_equal?(token, expected)
  end

  defp constant_time_equal?(left, right) do
    left
    |> :binary.bin_to_list()
    |> Enum.zip(:binary.bin_to_list(right))
    |> Enum.reduce(0, fn {a, b}, acc -> Bitwise.bor(acc, Bitwise.bxor(a, b)) end)
    |> Kernel.==(0)
  end

  defp decode_request(""), do: %{}

  defp decode_request(body) do
    body
    |> Json.decode!()
    |> normalize_keys()
  rescue
    error in ArgumentError -> raise ArgumentError, "invalid JSON request body: #{error.message}"
  end

  defp normalize_keys(value) when is_list(value), do: Enum.map(value, &normalize_keys/1)

  defp normalize_keys(value) when is_map(value) do
    Map.new(value, fn {key, item} ->
      {normalize_key(key), normalize_keys(item)}
    end)
  end

  defp normalize_keys(value), do: value

  defp normalize_key(key) when is_atom(key), do: key

  defp normalize_key(key) when is_binary(key) do
    case key do
      "agent_id" -> :agent_id
      "tenant_id" -> :tenant_id
      "hostname" -> :hostname
      "version" -> :version
      "labels" -> :labels
      "config_version" -> :config_version
      "current_version" -> :current_version
      "queue_depth_bytes" -> :queue_depth_bytes
      "ingest_bytes_per_second" -> :ingest_bytes_per_second
      "pipeline" -> :pipeline
      "target_version" -> :target_version
      "actor" -> :actor
      "receivers" -> :receivers
      "processors" -> :processors
      "exporters" -> :exporters
      "routes" -> :routes
      "kind" -> :kind
      "reason" -> :reason
      "command_id" -> :command_id
      "success" -> :success
      "error" -> :error
      other -> other
    end
  end

  defp parse_command_kind(kind) when is_atom(kind), do: kind

  defp parse_command_kind(kind) when is_binary(kind) do
    case kind do
      "reload_config" -> :reload_config
      "drain_and_restart" -> :drain_and_restart
      "pause_exports" -> :pause_exports
      "resume_exports" -> :resume_exports
      _ -> kind
    end
  end

  defp parse_command_kind(kind), do: kind

  defp encode_response(%RegisterAgentResponse{} = response) do
    %{
      accepted: response.accepted,
      config_version: response.config_version,
      message: response.message
    }
  end

  defp encode_response(%ConfigUpdate{} = update) do
    %{
      version: update.version,
      pipeline_config: update.pipeline_config,
      checksum: update.checksum
    }
  end

  defp encode_response(%AgentStatusResponse{} = response) do
    %{
      agent_id: response.agent_id,
      healthy: response.healthy,
      config_version: response.config_version,
      warnings: response.warnings
    }
  end

  defp encode_response(%ControlCommand{} = command) do
    %{
      command_id: command.command_id,
      agent_id: command.agent_id,
      tenant_id: command.tenant_id,
      kind: command.kind,
      reason: command.reason,
      status: ControlCommand.status(command),
      delivered_at: format_datetime(ControlCommand.delivered_at(command)),
      acknowledged_at: format_datetime(ControlCommand.acknowledged_at(command)),
      last_error: ControlCommand.last_error(command)
    }
  end

  defp format_datetime(nil), do: nil
  defp format_datetime(%DateTime{} = value), do: DateTime.to_iso8601(value)

  defp encode_pipeline(pipeline) do
    %{
      tenant_id: pipeline.tenant_id,
      name: pipeline.name,
      version: pipeline.version
    }
  end

  defp route_module_request(%{method: "GET", path: path}) do
    case path_segments(path) do
      ["v1", "modules"] ->
        response(200, %{modules: Enum.map(ModuleControl.list_modules(), &encode_module/1)})

      ["v1", "modules", "blockchain", "checkpoints"] ->
        opts = pagination_opts(path)

        with {:ok, records} <-
               BlockchainControl.list_checkpoints(query_param(path, "tenant_id"), opts) do
          response(200, %{checkpoints: records, pagination: pagination_response(records, opts)})
        else
          error -> error_response(error)
        end

      ["v1", "modules", "blockchain", "checkpoints", assignment_id] ->
        with {:ok, record} <-
               BlockchainControl.get_checkpoint(query_param(path, "tenant_id"), assignment_id) do
          response(200, %{checkpoint: record})
        else
          error -> error_response(error)
        end

      ["v1", "modules", "blockchain", resource_name] ->
        opts = pagination_opts(path)

        with {:ok, resource} <- blockchain_resource(resource_name),
             {:ok, records} <- resource.list.(query_param(path, "tenant_id"), opts) do
          response(200, %{
            resource.list_key => records,
            pagination: pagination_response(records, opts)
          })
        else
          error -> error_response(error)
        end

      ["v1", "modules", "blockchain", resource_name, id] ->
        with {:ok, resource} <- blockchain_resource(resource_name),
             {:ok, record} <- resource.get.(query_param(path, "tenant_id"), id) do
          response(200, %{resource.item_key => record})
        else
          error -> error_response(error)
        end

      _ ->
        response(404, %{error: "not_found"})
    end
  end

  defp route_module_request(%{method: "POST", path: "/v1/modules", body: body}) do
    body
    |> decode_request()
    |> ModuleControl.register_module()
    |> case do
      {:ok, registration} -> response(200, %{module: encode_module(registration)})
      error -> error_response(error)
    end
  end

  defp route_module_request(%{method: "POST", path: "/v1/modules/configs/validate", body: body}) do
    body
    |> decode_request()
    |> ModuleControl.validate_config()
    |> case do
      {:ok, plan} -> response(200, %{validation: encode_config_plan(plan)})
      error -> error_response(error)
    end
  end

  defp route_module_request(%{method: "POST", path: "/v1/modules/configs/dry-run", body: body}) do
    body
    |> decode_request()
    |> ModuleControl.dry_run_config()
    |> case do
      {:ok, plan} -> response(200, %{dry_run: encode_config_plan(plan)})
      error -> error_response(error)
    end
  end

  defp route_module_request(%{method: "POST", path: "/v1/modules/configs/diff", body: body}) do
    body
    |> decode_request()
    |> ModuleControl.diff_config()
    |> case do
      {:ok, diff} -> response(200, %{diff: diff})
      error -> error_response(error)
    end
  end

  defp route_module_request(%{method: "POST", path: "/v1/modules/configs/publish", body: body}) do
    body
    |> decode_request()
    |> ModuleControl.publish_config()
    |> case do
      {:ok, version} -> response(200, %{config: encode_module_config(version)})
      error -> error_response(error)
    end
  end

  defp route_module_request(%{method: "POST", path: "/v1/modules/configs/rollout", body: body}) do
    body
    |> decode_request()
    |> ModuleControl.rollout_config()
    |> case do
      {:ok, version} -> response(200, %{config: encode_module_config(version)})
      error -> error_response(error)
    end
  end

  defp route_module_request(%{method: "POST", path: "/v1/modules/configs/fetch", body: body}) do
    body
    |> decode_request()
    |> ModuleControl.fetch_config()
    |> case do
      {:ok, :up_to_date} -> response(200, %{update: nil})
      {:ok, version} -> response(200, %{update: encode_module_config(version)})
      error -> error_response(error)
    end
  end

  defp route_module_request(%{method: "POST", path: "/v1/modules/configs/rollback", body: body}) do
    body
    |> decode_request()
    |> ModuleControl.rollback_config()
    |> case do
      {:ok, version} -> response(200, %{config: encode_module_config(version)})
      error -> error_response(error)
    end
  end

  defp route_module_request(%{method: method, path: path, body: body})
       when method in ["POST", "PUT"] do
    case path_segments(path) do
      ["v1", "modules", "blockchain", resource_name] ->
        upsert_blockchain_resource(resource_name, nil, body)

      ["v1", "modules", "blockchain", resource_name, id] ->
        upsert_blockchain_resource(resource_name, id, body)

      _ ->
        response(404, %{error: "not_found"})
    end
  end

  defp route_module_request(%{method: "DELETE", path: path}) do
    case path_segments(path) do
      ["v1", "modules", "blockchain", resource_name, id] ->
        with {:ok, resource} <- blockchain_resource(resource_name),
             :ok <-
               resource.delete.(
                 query_param(path, "tenant_id"),
                 id,
                 query_param(path, "actor", "operator")
               ) do
          response(200, %{deleted: true})
        else
          error -> error_response(error)
        end

      _ ->
        response(404, %{error: "not_found"})
    end
  end

  defp route_module_request(_request), do: response(404, %{error: "not_found"})

  defp upsert_blockchain_resource(resource_name, id, body) do
    attrs = decode_request(body)

    with {:ok, resource} <- blockchain_resource(resource_name),
         {:ok, attrs} <- maybe_put_id(attrs, resource.id_field, id),
         {:ok, record} <-
           resource.upsert.(attrs, Map.get(attrs, :actor, Map.get(attrs, "actor", "operator"))) do
      response(200, %{resource.item_key => record})
    else
      error -> error_response(error)
    end
  end

  defp maybe_put_id(attrs, _id_field, nil), do: {:ok, attrs}

  defp maybe_put_id(attrs, id_field, id) do
    string_field = Atom.to_string(id_field)

    case Map.get(attrs, id_field) || Map.get(attrs, string_field) do
      nil -> {:ok, Map.put(attrs, id_field, id)}
      ^id -> {:ok, attrs}
      actual -> {:error, {:path_id_mismatch, id_field, id, actual}}
    end
  end

  defp blockchain_resource("chains") do
    {:ok,
     %{
       id_field: :chain_key,
       item_key: :chain,
       list_key: :chains,
       list: &BlockchainControl.list_chains/2,
       get: &BlockchainControl.get_chain/2,
       upsert: &BlockchainControl.upsert_chain/2,
       delete: &BlockchainControl.delete_chain/3
     }}
  end

  defp blockchain_resource("rpc-endpoints") do
    {:ok,
     %{
       id_field: :endpoint_id,
       item_key: :rpc_endpoint,
       list_key: :rpc_endpoints,
       list: &BlockchainControl.list_rpc_endpoints/2,
       get: &BlockchainControl.get_rpc_endpoint/2,
       upsert: &BlockchainControl.upsert_rpc_endpoint/2,
       delete: &BlockchainControl.delete_rpc_endpoint/3
     }}
  end

  defp blockchain_resource("address-watchlist") do
    {:ok,
     %{
       id_field: :entry_id,
       item_key: :address_watch,
       list_key: :address_watchlist,
       list: &BlockchainControl.list_address_watchlist/2,
       get: &BlockchainControl.get_address_watch/2,
       upsert: &BlockchainControl.upsert_address_watch/2,
       delete: &BlockchainControl.delete_address_watch/3
     }}
  end

  defp blockchain_resource("contract-watchlist") do
    {:ok,
     %{
       id_field: :contract_id,
       item_key: :contract_watch,
       list_key: :contract_watchlist,
       list: &BlockchainControl.list_contract_watchlist/2,
       get: &BlockchainControl.get_contract_watch/2,
       upsert: &BlockchainControl.upsert_contract_watch/2,
       delete: &BlockchainControl.delete_contract_watch/3
     }}
  end

  defp blockchain_resource("token-watchlist") do
    {:ok,
     %{
       id_field: :token_id,
       item_key: :token_watch,
       list_key: :token_watchlist,
       list: &BlockchainControl.list_token_watchlist/2,
       get: &BlockchainControl.get_token_watch/2,
       upsert: &BlockchainControl.upsert_token_watch/2,
       delete: &BlockchainControl.delete_token_watch/3
     }}
  end

  defp blockchain_resource("filter-rules") do
    {:ok,
     %{
       id_field: :rule_id,
       item_key: :filter_rule,
       list_key: :filter_rules,
       list: &BlockchainControl.list_filter_rules/2,
       get: &BlockchainControl.get_filter_rule/2,
       upsert: &BlockchainControl.upsert_filter_rule/2,
       delete: &BlockchainControl.delete_filter_rule/3
     }}
  end

  defp blockchain_resource("crawl-assignments") do
    {:ok,
     %{
       id_field: :assignment_id,
       item_key: :crawl_assignment,
       list_key: :crawl_assignments,
       list: &BlockchainControl.list_crawl_assignments/2,
       get: &BlockchainControl.get_crawl_assignment/2,
       upsert: &BlockchainControl.upsert_crawl_assignment/2,
       delete: &BlockchainControl.delete_crawl_assignment/3
     }}
  end

  defp blockchain_resource(resource), do: {:error, {:unknown_resource, resource}}

  defp encode_module(%ModuleRegistration{} = registration) do
    %{
      module: registration.module,
      display_name: registration.display_name,
      owner: registration.owner,
      description: registration.description,
      enabled: registration.enabled,
      metadata: registration.metadata || %{},
      inserted_at: format_datetime(registration.inserted_at),
      updated_at: format_datetime(registration.updated_at)
    }
  end

  defp encode_module_config(%ModuleConfigVersion{} = version) do
    %{
      tenant_id: version.tenant_id,
      module: version.module,
      version: version.version,
      config: version.config || %{},
      checksum: version.checksum,
      updated_by: version.updated_by,
      inserted_at: format_datetime(version.inserted_at)
    }
  end

  defp encode_config_plan(plan) do
    %{
      tenant_id: plan.tenant_id,
      module: plan.module,
      next_version: plan.next_version,
      checksum: plan.checksum,
      valid: plan.valid,
      validation_errors: plan.validation_errors || [],
      diff: plan.diff || %{added: [], removed: [], changed: []},
      approval: plan.approval || %{},
      dry_run: plan.dry_run,
      config: plan.config || %{}
    }
  end

  defp path_segments(path) do
    path
    |> path_without_query()
    |> String.trim_leading("/")
    |> String.split("/", trim: true)
  end

  defp path_without_query(path) do
    case URI.parse(path).path do
      nil -> path
      value -> value
    end
  end

  defp query_param(path, name, default \\ nil) do
    path
    |> URI.parse()
    |> Map.get(:query)
    |> case do
      nil -> default
      query -> query |> URI.decode_query() |> Map.get(name, default)
    end
  end

  defp pagination_opts(path) do
    [
      limit: query_integer(path, "limit", 100),
      offset: query_integer(path, "offset", 0)
    ]
  end

  defp query_integer(path, name, default) do
    case query_param(path, name) do
      nil ->
        default

      value ->
        case Integer.parse(value) do
          {integer, ""} -> integer
          _ -> default
        end
    end
  end

  defp pagination_response(records, opts) do
    %{
      limit: Keyword.fetch!(opts, :limit),
      offset: Keyword.fetch!(opts, :offset),
      returned: length(records)
    }
  end

  defp error_response({:error, :not_found}), do: response(404, %{error: "not_found"})
  defp error_response({:error, :tenant_mismatch}), do: response(403, %{error: "tenant_mismatch"})
  defp error_response({:error, {:empty, field}}), do: response(400, %{error: "empty_#{field}"})

  defp error_response({:error, {:validation_failed, errors}}),
    do: response(400, %{error: "validation_failed", errors: errors})

  defp error_response({:error, {:approval_required, approval}}),
    do: response(403, %{error: "approval_required", approval: approval})

  defp error_response({:error, {:path_id_mismatch, field, expected, actual}}),
    do:
      response(400, %{error: "path_id_mismatch", field: field, expected: expected, actual: actual})

  defp error_response({:error, {:invalid_map, field, value}}),
    do: response(400, %{error: "invalid_map", field: field, value: inspect(value)})

  defp error_response({:error, {:module_not_registered, module}}),
    do: response(404, %{error: "module_not_registered", module: module})

  defp error_response({:error, {:unknown_resource, resource}}),
    do: response(404, %{error: "unknown_resource", resource: resource})

  defp error_response({:error, {:invalid_integer, field, value}}) do
    response(400, %{error: "invalid_integer", field: field, value: value})
  end

  defp error_response({:error, {:invalid_boolean, field, value}}) do
    response(400, %{error: "invalid_boolean", field: field, value: value})
  end

  defp error_response({:error, {:unknown_command_kind, kind}}) do
    response(400, %{error: "unknown_command_kind", kind: kind})
  end

  defp error_response(error), do: response(500, %{error: inspect(error)})

  defp response(status, body) do
    payload = Json.encode!(body)

    text_response(status, "application/json", payload)
  end

  defp text_response(status, content_type, payload) do
    payload = IO.iodata_to_binary(payload)

    [
      "HTTP/1.1 #{status} #{reason(status)}\r\n",
      "Content-Type: #{content_type}\r\n",
      "Content-Length: #{byte_size(payload)}\r\n",
      "Connection: close\r\n",
      "\r\n",
      payload
    ]
  end

  defp reason(200), do: "OK"
  defp reason(400), do: "Bad Request"
  defp reason(401), do: "Unauthorized"
  defp reason(403), do: "Forbidden"
  defp reason(404), do: "Not Found"
  defp reason(413), do: "Payload Too Large"
  defp reason(429), do: "Too Many Requests"
  defp reason(503), do: "Service Unavailable"
  defp reason(500), do: "Internal Server Error"
  defp reason(_), do: "OK"

  defp readiness_response do
    checks = readiness_checks()

    if Enum.all?(checks, fn {_name, check} -> check.status == "ok" end) do
      response(200, %{status: "ok", checks: checks})
    else
      response(503, %{status: "unready", checks: checks})
    end
  end

  defp readiness_checks do
    %{
      storage: storage_readiness()
    }
  end

  defp storage_readiness do
    if postgres_configured?() or postgres_primary?() do
      postgres_readiness()
    else
      %{status: "ok", mode: "otp"}
    end
  end

  defp postgres_readiness do
    if Process.whereis(Repo) do
      case Ecto.Adapters.SQL.query(Repo, "SELECT 1", [], timeout: 1_000) do
        {:ok, _result} -> %{status: "ok", mode: "postgres"}
        {:error, reason} -> %{status: "error", mode: "postgres", reason: inspect(reason)}
      end
    else
      %{status: "error", mode: "postgres", reason: "repo_not_started"}
    end
  rescue
    error -> %{status: "error", mode: "postgres", reason: Exception.message(error)}
  end

  defp security_options(opts) do
    %{
      agent_token: blank_to_nil(Keyword.get(opts, :agent_token)),
      operator_token: blank_to_nil(Keyword.get(opts, :operator_token)),
      tls_enabled: Keyword.get(opts, :tls_enabled, false),
      tls_cert_file: blank_to_nil(Keyword.get(opts, :tls_cert_file)),
      tls_key_file: blank_to_nil(Keyword.get(opts, :tls_key_file)),
      tls_ca_file: blank_to_nil(Keyword.get(opts, :tls_ca_file)),
      tls_require_client_auth: Keyword.get(opts, :tls_require_client_auth, false)
    }
  end

  defp rate_limit_options(opts) do
    per_second = Keyword.get(opts, :rate_limit_per_second, 0)

    %{
      enabled: is_integer(per_second) and per_second > 0,
      per_second: per_second,
      table:
        :ets.new(:telemetry_fabric_control_http_rate_limit, [
          :set,
          :public,
          {:read_concurrency, true},
          {:write_concurrency, true}
        ])
    }
  end

  defp check_rate_limit(%{method: "GET", path: path}, _rate_limit)
       when path in ["/healthz", "/readyz", "/metrics"],
       do: :ok

  defp check_rate_limit(_request, %{enabled: false}), do: :ok

  defp check_rate_limit(request, rate_limit) do
    bucket = System.system_time(:second)
    key = {request.method, path_without_query(request.path), bucket}
    count = :ets.update_counter(rate_limit.table, key, {2, 1}, {key, 0})

    if count > rate_limit.per_second do
      {:error, 429, %{error: "rate_limited"}}
    else
      :ok
    end
  end

  defp blank_to_nil(value) when value in [nil, ""], do: nil
  defp blank_to_nil(value), do: value

  defp postgres_configured? do
    case System.get_env("TELEMETRY_FABRIC_CONTROL_DATABASE_URL") do
      nil -> false
      "" -> false
      _url -> true
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

  defp listen(port, ip, %{tls_enabled: true} = security) do
    :ok = :ssl.start()

    cert_file =
      security.tls_cert_file ||
        raise ArgumentError, "TLS control server requires tls_cert_file"

    key_file =
      security.tls_key_file ||
        raise ArgumentError, "TLS control server requires tls_key_file"

    tls_opts =
      [
        :binary,
        active: false,
        packet: :raw,
        reuseaddr: true,
        ip: ip,
        certfile: cert_file,
        keyfile: key_file
      ]
      |> maybe_require_client_cert(security)

    case :ssl.listen(port, tls_opts) do
      {:ok, socket} -> {:ok, :ssl, socket}
      error -> error
    end
  end

  defp listen(port, ip, _security) do
    opts = [
      :binary,
      active: false,
      packet: :raw,
      reuseaddr: true,
      ip: ip
    ]

    case :gen_tcp.listen(port, opts) do
      {:ok, socket} -> {:ok, :gen_tcp, socket}
      error -> error
    end
  end

  defp maybe_require_client_cert(opts, %{tls_require_client_auth: true, tls_ca_file: ca_file})
       when is_binary(ca_file) do
    opts ++ [verify: :verify_peer, fail_if_no_peer_cert: true, cacertfile: ca_file]
  end

  defp maybe_require_client_cert(_opts, %{tls_require_client_auth: true}) do
    raise ArgumentError, "mTLS control server requires tls_ca_file"
  end

  defp maybe_require_client_cert(opts, _security), do: opts

  defp accept(:gen_tcp, socket), do: :gen_tcp.accept(socket)

  defp accept(:ssl, socket) do
    with {:ok, transport_socket} <- :ssl.transport_accept(socket),
         {:ok, ssl_socket} <- :ssl.handshake(transport_socket) do
      {:ok, ssl_socket}
    end
  end

  defp recv(:gen_tcp, socket, length, timeout), do: :gen_tcp.recv(socket, length, timeout)
  defp recv(:ssl, socket, length, timeout), do: :ssl.recv(socket, length, timeout)

  defp send_data(:gen_tcp, socket, response), do: :gen_tcp.send(socket, response)
  defp send_data(:ssl, socket, response), do: :ssl.send(socket, response)

  defp close(:gen_tcp, socket), do: :gen_tcp.close(socket)
  defp close(:ssl, socket), do: :ssl.close(socket)

  defp bound_port(:gen_tcp, socket), do: :inet.port(socket)

  defp bound_port(:ssl, socket) do
    with {:ok, {_address, port}} <- :ssl.sockname(socket) do
      {:ok, port}
    end
  end

  defp parse_ip!(host) do
    host
    |> String.to_charlist()
    |> :inet.parse_address()
    |> case do
      {:ok, ip} ->
        ip

      {:error, reason} ->
        raise ArgumentError, "invalid listen host #{inspect(host)}: #{inspect(reason)}"
    end
  end
end
