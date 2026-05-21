defmodule TelemetryFabricControl.SamplePipeline do
  @moduledoc """
  Factory for the default MVP pipeline.
  """

  alias TelemetryFabricControl.PipelineConfig

  def build(tenant_id \\ "default") do
    %PipelineConfig{
      tenant_id: tenant_id,
      name: "default",
      receivers: [
        %{name: "otlp-grpc", protocol: "otlp_grpc", endpoint: "0.0.0.0:4317"},
        %{name: "otlp-http", protocol: "otlp_http", endpoint: "0.0.0.0:4318"},
        %{name: "tf-line", protocol: "tf_line", endpoint: "127.0.0.1:4319"}
      ],
      processors: [
        %{name: "memory-limiter", kind: "memory_limiter", enabled: true},
        %{name: "batch", kind: "batch", enabled: true}
      ],
      exporters: [
        %{
          name: "stdout",
          protocol: "stdout",
          endpoint: "stdout://local",
          tls: false,
          retry: %{max_attempts: 3, timeout_ms: 30_000, initial_backoff_ms: 100}
        }
      ],
      routes: [
        %{signal: "trace", exporters: ["stdout"]},
        %{signal: "metric", exporters: ["stdout"]},
        %{signal: "log", exporters: ["stdout"]}
      ]
    }
  end
end
