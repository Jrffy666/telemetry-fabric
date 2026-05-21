defmodule TelemetryFabricControl.HttpControlServer do
  @moduledoc """
  Minimal dependency-free HTTP adapter for the control-plane domain API.

  The adapter is intentionally small and replaceable. It exists so agents and
  operators can exercise the AgentControl workflow before Phoenix or gRPC is
  introduced.
  """

  use GenServer

  alias TelemetryFabricControl.ControlCommand
  alias TelemetryFabricControl.ControlService
  alias TelemetryFabricControl.ControlService.AgentStatusResponse
  alias TelemetryFabricControl.ControlService.ConfigUpdate
  alias TelemetryFabricControl.ControlService.RegisterAgentResponse
  alias TelemetryFabricControl.Json

  @read_timeout 5_000
  @max_header_bytes 16 * 1024
  @default_max_body_bytes 1 * 1024 * 1024

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

    {:ok, transport, socket} = listen(port, ip, security)

    {:ok, actual_port} = bound_port(transport, socket)

    {:ok, acceptor} =
      Task.start_link(fn -> accept_loop(socket, transport, security, max_body_bytes) end)

    {:ok,
     %{
       socket: socket,
       transport: transport,
       acceptor: acceptor,
       host: host,
       port: actual_port,
       security: security,
       max_body_bytes: max_body_bytes
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

  defp accept_loop(socket, transport, security, max_body_bytes) do
    case accept(transport, socket) do
      {:ok, client} ->
        Task.start(fn -> handle_client(transport, client, security, max_body_bytes) end)
        accept_loop(socket, transport, security, max_body_bytes)

      {:error, :closed} ->
        :ok

      {:error, _reason} ->
        accept_loop(socket, transport, security, max_body_bytes)
    end
  end

  defp handle_client(transport, socket, security, max_body_bytes) do
    response =
      socket
      |> read_request(transport, max_body_bytes)
      |> route_request(security)

    :ok = send_data(transport, socket, response)
    close(transport, socket)
  rescue
    error in [ArgumentError, KeyError] ->
      body = %{error: Exception.message(error)}
      _ = send_data(transport, socket, response(400, body))
      close(transport, socket)

    error ->
      body = %{error: Exception.message(error)}
      _ = send_data(transport, socket, response(500, body))
      close(transport, socket)
  end

  defp read_request(socket, transport, max_body_bytes) do
    {head, body} = read_until_headers(socket, transport, "")
    [request_line | header_lines] = String.split(head, "\r\n")
    [method, path, _version] = String.split(request_line, " ", parts: 3)
    headers = parse_headers(header_lines)
    content_length = headers |> Map.get("content-length", "0") |> String.to_integer()
    if content_length > max_body_bytes, do: raise("request body exceeds maximum size")
    body = read_body(socket, transport, body, content_length)

    %{method: method, path: path, headers: headers, body: body}
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

  defp route_request(request, security) do
    case authorize(request, security) do
      :ok -> route_authorized_request(request)
      {:error, status, body} -> response(status, body)
    end
  end

  defp route_authorized_request(%{method: "GET", path: path})
       when path in ["/healthz", "/readyz"] do
    response(200, %{status: "ok"})
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

  defp route_authorized_request(_request), do: response(404, %{error: "not_found"})

  defp authorize(%{method: "GET", path: path}, _security) when path in ["/healthz", "/readyz"],
    do: :ok

  defp authorize(%{path: path} = request, security) do
    role =
      if path in ["/v1/agents/commands", "/v1/pipelines", "/v1/pipelines/rollback"] do
        :operator
      else
        :agent
      end

    authorize_role(request, security, role)
  end

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

  defp error_response({:error, :not_found}), do: response(404, %{error: "not_found"})
  defp error_response({:error, :tenant_mismatch}), do: response(403, %{error: "tenant_mismatch"})
  defp error_response({:error, {:empty, field}}), do: response(400, %{error: "empty_#{field}"})

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

    [
      "HTTP/1.1 #{status} #{reason(status)}\r\n",
      "Content-Type: application/json\r\n",
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
  defp reason(500), do: "Internal Server Error"
  defp reason(_), do: "OK"

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

  defp blank_to_nil(value) when value in [nil, ""], do: nil
  defp blank_to_nil(value), do: value

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
