# Data Plane Runtime

The Rust data plane is organized around four hot-path stages:

```text
receiver -> durable queue -> processor chain -> routed exporters
```

## Durable Queue

`telemetry-buffer` implements a segmented append-only disk queue. Each record is
stored as:

```text
u32 payload_length
u32 checksum
bytes payload
```

The queue supports two drain modes:

- `drain`: commits each record after the sink succeeds.
- `drain_batch`: reads a batch and commits the cursor only after the full sink
  succeeds.

`drain_batch` is the preferred production path because exporter failures should
keep the whole batch available for retry.

If a segment fails checksum validation or contains a truncated record, the queue
renames it with a `.corrupt` suffix, writes a companion `.reason` file, and
continues from the next healthy segment. This prevents one bad segment from
blocking the whole agent indefinitely.

The runtime also enforces the configured tenant `max_queue_bytes` limit before
accepting new records. When the local queue is full, ingestion is rejected with a
clear backpressure error instead of growing disk usage without bound.

## Processor Chain

`telemetry-processors` owns record-level processors.

Implemented:

- `MemoryLimiterProcessor`: drops records whose estimated size exceeds the
  configured tenant record limit.
- `RedactProcessor`: masks sensitive attributes such as authorization headers,
  passwords, secrets, tokens, and database statements.

Planned:

- Tenant rate limiting.
- Tail-based sampling.
- Resource attribute enrichment.
- Log body redaction with structured parsers.

## Exporters

`telemetry-exporters` owns exporter implementations behind the `Exporter` trait.

Implemented:

- `StdoutExporter`
- `FileExporter`
- `OtlpGrpcExporter` for OTLP/gRPC traces, metrics, and logs. Metrics currently
  support gauge, sum, histogram, exponential histogram, and summary datapoints.
- `OtlpHttpExporter` for OTLP/HTTP protobuf or JSON traces, metrics, and logs
  over `http://` or `https://` endpoints. Use `protocol: otlp_http` for
  protobuf and `protocol: otlp_http_json` for JSON export. HTTPS exporters can
  use the platform WebPKI roots, a custom `tls.ca_file`, and optional client
  `tls.cert_file` / `tls.key_file` for mTLS.
- `PrometheusRemoteWriteExporter` for metrics over Snappy-compressed protobuf
  `WriteRequest` payloads. Metric and label names are sanitized to Prometheus
  identifier rules, and histogram/exponential histogram/summary records are
  expanded to Prometheus bucket, quantile, sum, and count series.

Planned:

- Kafka
- S3-compatible object storage
- ClickHouse

## Runtime Semantics

The agent runtime follows this sequence during `flush`:

1. Read up to `max_items` from the disk queue without committing.
2. Decode records.
3. Run processors.
4. Group records by configured route and exporter.
5. Export each grouped batch.
6. Commit the queue cursor only after all exports succeed.

This gives the MVP at-least-once delivery semantics for exporter failures.

In `--listen`, `--otlp-grpc`, and `--otlp-http` modes, the receivers
automatically call `flush` from a background worker. `--flush-batch-size`
controls the maximum records per flush, and `--flush-interval-ms` controls the
worker cadence. The TCP line receiver accepts clients concurrently, while queue
mutation and export remain serialized inside the runtime.

When the OTLP/HTTP receiver is configured from YAML, `tls.enabled: true`
requires `tls.cert_file` and `tls.key_file`. Set `tls.require_client_auth: true`
with `tls.ca_file` to require client certificates.

When `--otlp-export-endpoint` is set, trace, metric, and log records are routed
to an upstream OTLP/gRPC endpoint. YAML pipelines can route those same signals
to OTLP/HTTP by using an exporter with `protocol: otlp_http` or
`protocol: otlp_http_json`.
Metric routes can also target Prometheus Remote Write with
`protocol: prometheus_remote_write`; non-metric records are ignored by that
exporter.

## Health And Metrics Endpoint

`telemetry-agent` can expose a simple JSON health endpoint and Prometheus text
metrics on the same listener:

```bash
telemetry-agent --otlp-grpc 0.0.0.0:4317 --health-listen 0.0.0.0:13133
```

`/healthz` and `/readyz` respond with:

```json
{"status":"ok","queued_bytes":0,"cursor_segment_id":1,"cursor_offset":0}
```

`/metrics` responds in Prometheus text format. Implemented metric families
include:

- `telemetry_agent_queue_bytes`
- `telemetry_agent_ingested_records_total`
- `telemetry_agent_ingest_rejected_records_total`
- `telemetry_agent_flush_attempts_total`
- `telemetry_agent_flush_successes_total`
- `telemetry_agent_flush_failures_total`
- `telemetry_agent_drained_records_total`
- `telemetry_agent_exported_records_total`
- `telemetry_agent_dropped_records_total`
- `telemetry_agent_exported_bytes_total`
- `telemetry_agent_config_reloads_total`
- `telemetry_agent_config_reload_failures_total`
- `telemetry_agent_control_registrations_total`
- `telemetry_agent_control_registration_failures_total`
- `telemetry_agent_control_heartbeats_total`
- `telemetry_agent_control_heartbeat_failures_total`
- `telemetry_agent_control_config_fetches_total`
- `telemetry_agent_control_config_fetch_failures_total`

Kubernetes and Helm deployment manifests wire this endpoint into liveness and
readiness probes.

## YAML Configuration

The agent can load a pipeline from YAML:

```bash
telemetry-agent --config config/pipeline.example.yaml
```

The file controls:

- tenant and pipeline name
- receivers and endpoints
- processors and enabled flags
- exporters and endpoints
- routes by signal type
- tenant limits such as `max_queue_bytes`

Command-line flags such as `--tenant`, `--otlp-grpc`, `--otlp-http`, `--listen`, and
`--otlp-export-endpoint` can still override runtime behavior for local testing
or simple deployments.
