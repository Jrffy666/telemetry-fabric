defmodule TelemetryFabricControl.HttpMetrics do
  @moduledoc """
  In-process Prometheus metrics for the dependency-free HTTP control adapter.
  """

  use GenServer

  @type route_key :: {String.t(), String.t(), non_neg_integer()}

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, %{}, name: name)
  end

  def clear(server \\ __MODULE__) do
    if Process.whereis(server) do
      GenServer.call(server, :clear)
    else
      :ok
    end
  end

  def record_request(method, path, status, duration_us, server \\ __MODULE__) do
    if Process.whereis(server) do
      GenServer.cast(server, {:record_request, method, path, status, duration_us})
    end

    :ok
  end

  def prometheus(server \\ __MODULE__) do
    if Process.whereis(server) do
      GenServer.call(server, :prometheus)
    else
      render(%{})
    end
  end

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_call(:clear, _from, _state), do: {:reply, :ok, %{}}

  def handle_call(:prometheus, _from, state), do: {:reply, render(state), state}

  @impl true
  def handle_cast({:record_request, method, path, status, duration_us}, state) do
    key = {normalize_label(method), normalize_path(path), status}

    metrics =
      Map.update(state, key, %{count: 1, duration_us: duration_us}, fn metrics ->
        %{
          count: metrics.count + 1,
          duration_us: metrics.duration_us + max(duration_us, 0)
        }
      end)

    {:noreply, metrics}
  end

  defp render(state) do
    [
      "# HELP telemetry_fabric_control_http_requests_total HTTP requests handled by the control plane.",
      "# TYPE telemetry_fabric_control_http_requests_total counter",
      render_counter_lines(state, "telemetry_fabric_control_http_requests_total", :count),
      "# HELP telemetry_fabric_control_http_request_duration_seconds_total Total HTTP request handling time in seconds.",
      "# TYPE telemetry_fabric_control_http_request_duration_seconds_total counter",
      render_duration_lines(state),
      ""
    ]
    |> Enum.reject(&(&1 == ""))
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  defp render_counter_lines(state, metric_name, field) do
    state
    |> Enum.sort_by(fn {{method, path, status}, _metrics} -> {method, path, status} end)
    |> Enum.map_join("\n", fn {key, metrics} ->
      "#{metric_name}{#{labels(key)}} #{Map.fetch!(metrics, field)}"
    end)
  end

  defp render_duration_lines(state) do
    state
    |> Enum.sort_by(fn {{method, path, status}, _metrics} -> {method, path, status} end)
    |> Enum.map_join("\n", fn {key, metrics} ->
      seconds = metrics.duration_us / 1_000_000

      "telemetry_fabric_control_http_request_duration_seconds_total{#{labels(key)}} #{format_seconds(seconds)}"
    end)
  end

  defp labels({method, path, status}) do
    [
      ~s(method="#{escape_label(method)}"),
      ~s(path="#{escape_label(path)}"),
      ~s(status="#{status}")
    ]
    |> Enum.join(",")
  end

  defp normalize_label(value) when is_binary(value), do: value
  defp normalize_label(value), do: inspect(value)

  defp normalize_path(path)
       when path in [
              "/healthz",
              "/readyz",
              "/metrics",
              "/v1/agents/register",
              "/v1/agents/heartbeat",
              "/v1/agents/config",
              "/v1/agents/status",
              "/v1/agents/commands",
              "/v1/agents/commands/ack",
              "/v1/pipelines",
              "/v1/pipelines/rollback"
            ] do
    path
  end

  defp normalize_path(_path), do: "__unknown__"

  defp escape_label(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("\n", "\\n")
    |> String.replace("\"", "\\\"")
  end

  defp format_seconds(value) do
    :io_lib.format("~.6f", [value])
    |> IO.iodata_to_binary()
  end
end
