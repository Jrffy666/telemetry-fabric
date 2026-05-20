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

    {:ok, socket} =
      :gen_tcp.listen(port, [
        :binary,
        active: false,
        packet: :raw,
        reuseaddr: true,
        ip: ip
      ])

    {:ok, actual_port} = :inet.port(socket)
    {:ok, acceptor} = Task.start_link(fn -> accept_loop(socket) end)

    {:ok, %{socket: socket, acceptor: acceptor, host: host, port: actual_port}}
  end

  @impl true
  def handle_call(:port, _from, state) do
    {:reply, state.port, state}
  end

  @impl true
  def terminate(_reason, state) do
    :gen_tcp.close(state.socket)
    :ok
  end

  defp accept_loop(socket) do
    case :gen_tcp.accept(socket) do
      {:ok, client} ->
        Task.start(fn -> handle_client(client) end)
        accept_loop(socket)

      {:error, :closed} ->
        :ok

      {:error, _reason} ->
        accept_loop(socket)
    end
  end

  defp handle_client(socket) do
    response =
      socket
      |> read_request()
      |> route_request()

    :ok = :gen_tcp.send(socket, response)
    :gen_tcp.close(socket)
  rescue
    error in [ArgumentError, KeyError] ->
      body = %{error: Exception.message(error)}
      _ = :gen_tcp.send(socket, response(400, body))
      :gen_tcp.close(socket)

    error ->
      body = %{error: Exception.message(error)}
      _ = :gen_tcp.send(socket, response(500, body))
      :gen_tcp.close(socket)
  end

  defp read_request(socket) do
    {head, body} = read_until_headers(socket, "")
    [request_line | header_lines] = String.split(head, "\r\n")
    [method, path, _version] = String.split(request_line, " ", parts: 3)
    headers = parse_headers(header_lines)
    content_length = headers |> Map.get("content-length", "0") |> String.to_integer()
    body = read_body(socket, body, content_length)

    %{method: method, path: path, headers: headers, body: body}
  end

  defp read_until_headers(socket, acc) do
    case String.split(acc, "\r\n\r\n", parts: 2) do
      [head, body] ->
        {head, body}

      [_] ->
        case :gen_tcp.recv(socket, 0, @read_timeout) do
          {:ok, chunk} -> read_until_headers(socket, acc <> chunk)
          {:error, reason} -> raise "failed to read HTTP request: #{inspect(reason)}"
        end
    end
  end

  defp read_body(_socket, body, content_length) when byte_size(body) >= content_length do
    binary_part(body, 0, content_length)
  end

  defp read_body(socket, body, content_length) do
    remaining = content_length - byte_size(body)

    case :gen_tcp.recv(socket, remaining, @read_timeout) do
      {:ok, chunk} -> read_body(socket, body <> chunk, content_length)
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

  defp route_request(%{method: "GET", path: path}) when path in ["/healthz", "/readyz"] do
    response(200, %{status: "ok"})
  end

  defp route_request(%{method: "POST", path: "/v1/agents/register", body: body}) do
    body
    |> decode_request()
    |> ControlService.register_agent()
    |> case do
      {:ok, %RegisterAgentResponse{} = result} -> response(200, %{agent: encode_response(result)})
      error -> error_response(error)
    end
  end

  defp route_request(%{method: "POST", path: "/v1/agents/heartbeat", body: body}) do
    body
    |> decode_request()
    |> ControlService.heartbeat()
    |> case do
      {:ok, commands} -> response(200, %{commands: Enum.map(commands, &encode_response/1)})
      error -> error_response(error)
    end
  end

  defp route_request(%{method: "POST", path: "/v1/agents/config", body: body}) do
    body
    |> decode_request()
    |> ControlService.config_update()
    |> case do
      {:ok, :up_to_date} -> response(200, %{update: nil})
      {:ok, %ConfigUpdate{} = update} -> response(200, %{update: encode_response(update)})
      error -> error_response(error)
    end
  end

  defp route_request(%{method: "POST", path: "/v1/agents/status", body: body}) do
    body
    |> decode_request()
    |> ControlService.report_status()
    |> case do
      {:ok, %AgentStatusResponse{} = status} -> response(200, %{status: encode_response(status)})
      error -> error_response(error)
    end
  end

  defp route_request(%{method: "POST", path: "/v1/agents/commands", body: body}) do
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

  defp route_request(_request), do: response(404, %{error: "not_found"})

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
      "kind" -> :kind
      "reason" -> :reason
      other -> other
    end
  end

  defp parse_command_kind(kind) when is_atom(kind), do: kind

  defp parse_command_kind(kind) when is_binary(kind) do
    case kind do
      "reload_config" -> :reload_config
      "drain_and_restart" -> :drain_and_restart
      "pause_exports" -> :pause_exports
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
      reason: command.reason
    }
  end

  defp error_response({:error, :not_found}), do: response(404, %{error: "not_found"})
  defp error_response({:error, :tenant_mismatch}), do: response(403, %{error: "tenant_mismatch"})
  defp error_response({:error, {:empty, field}}), do: response(400, %{error: "empty_#{field}"})

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
  defp reason(403), do: "Forbidden"
  defp reason(404), do: "Not Found"
  defp reason(500), do: "Internal Server Error"
  defp reason(_), do: "OK"

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
