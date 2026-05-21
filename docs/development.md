# Development

## Prerequisites

- Rust 1.94 or newer.
- Elixir 1.16 or newer for the control plane.
- Docker for integration environments.

## Rust

Run all Rust tests:

```bash
cargo test --workspace
```

Run the agent self-test:

```bash
cargo run -p telemetry-agent -- --self-test
```

Run the agent from a YAML pipeline file:

```bash
cargo run -p telemetry-agent -- --config config/pipeline.example.yaml --health-listen 127.0.0.1:13133
```

Flush any records already in the local durable queue:

```bash
cargo run -p telemetry-agent
```

Start the MVP TCP receiver:

```bash
cargo run -p telemetry-agent -- --listen 127.0.0.1:4319 --flush-batch-size 128
```

Start the OTLP/gRPC trace, metrics, and logs receiver:

```bash
cargo run -p telemetry-agent -- --otlp-grpc 127.0.0.1:4317 --flush-batch-size 128
```

Start the OTLP/HTTP protobuf trace, metrics, and logs receiver:

```bash
cargo run -p telemetry-agent -- --otlp-http 127.0.0.1:4318 --flush-batch-size 128
```

Start the receiver with a health endpoint for local or Kubernetes probes:

```bash
cargo run -p telemetry-agent -- --otlp-grpc 127.0.0.1:4317 --health-listen 127.0.0.1:13133
```

Scrape agent self-metrics from the same listener:

```bash
curl http://127.0.0.1:13133/metrics
```

Tune background export cadence:

```bash
cargo run -p telemetry-agent -- --otlp-grpc 127.0.0.1:4317 --flush-batch-size 512 --flush-interval-ms 500
```

Forward received traces, metrics, and logs to another OTLP/gRPC endpoint:

```bash
cargo run -p telemetry-agent -- --otlp-grpc 127.0.0.1:4317 --otlp-export-endpoint http://127.0.0.1:14317
```

For OTLP/HTTP export, set an exporter with `protocol: otlp_http` for protobuf
or `protocol: otlp_http_json` for JSON in the YAML pipeline. Use `https://`
endpoints for TLS. Add `tls.ca_file` for private CAs and optional
`tls.cert_file` / `tls.key_file` for client-certificate auth.

For OTLP/HTTP receiver TLS in YAML:

```yaml
receivers:
  otlp-http:
    protocol: otlp_http
    endpoint: 0.0.0.0:4318
    tls:
      enabled: true
      cert_file: certs/server.pem
      key_file: certs/server-key.pem
      ca_file: certs/ca.pem
      require_client_auth: true
```

For Prometheus Remote Write, set a metrics route to an exporter with
`protocol: prometheus_remote_write` and an endpoint such as
`http://prometheus:9090/api/v1/write`. The exporter sends Snappy-compressed
protobuf `WriteRequest` payloads for metric records only.

Start the agent with the MVP HTTP control-plane client:

```bash
cargo run -p telemetry-agent -- --tenant payments-prod --agent-id agent-1 --otlp-grpc 127.0.0.1:4317 --control-endpoint http://127.0.0.1:4001
```

The client registers once, then sends periodic heartbeats. When the control
plane returns `reload_config`, the agent fetches `/v1/agents/config`, verifies
the SHA-256 checksum, parses the YAML pipeline, and reloads processors,
exporters, routes, and limits in the running runtime. Receiver sockets are not
rebound during this MVP hot reload. Operator commands can also pause exports,
resume exports, or ask the agent to drain queued records and exit so systemd or
Kubernetes restarts it.

The control plane still runs without external services by default. PostgreSQL
persistence now includes the plain SQL schema at
`apps/control_plane/priv/postgres/001_control_plane_schema.sql`, row codecs that
convert OTP state into the target tables, Ecto schemas, Repo wiring, a migration
task, and a periodic `Ecto.Multi` snapshot sync for PostgreSQL persistence.

Send a sample event:

```bash
printf "trace payments-prod checkout request-started\n" | nc 127.0.0.1 4319
```

## Elixir

Run the control-plane tests:

```bash
cd apps/control_plane
mix test
```

The control-plane domain API is exposed by
`TelemetryFabricControl.ControlService`. It implements the transport-neutral
shape of the future AgentControl API: `register_agent/1`, `heartbeat/1`,
`config_update/1`, `enqueue_command/3`, and `report_status/1`.

Run the control plane with dependency-free file persistence:

```powershell
$env:TELEMETRY_FABRIC_CONTROL_DATA_DIR = "../../.telemetry-fabric/control-plane"
mix.bat run --no-halt
```

Run the control plane with the MVP HTTP control API:

```powershell
$env:TELEMETRY_FABRIC_CONTROL_HTTP_LISTEN = "127.0.0.1:4001"
mix.bat run --no-halt
```

Available HTTP endpoints:

- `GET /healthz`
- `GET /readyz`
- `POST /v1/agents/register`
- `POST /v1/agents/heartbeat`
- `POST /v1/agents/config`
- `POST /v1/agents/status`
- `POST /v1/agents/commands`
- `POST /v1/pipelines/rollback`

Queue an operator command:

```json
{
  "agent_id": "agent-1",
  "kind": "pause_exports",
  "reason": "backend maintenance"
}
```

Supported command kinds are `reload_config`, `pause_exports`,
`resume_exports`, and `drain_and_restart`.

Operator commands are durable until an agent heartbeat receives them. Delivery
marks the stored command as `delivered` with a `delivered_at` timestamp, and the
PostgreSQL snapshot sync preserves both pending and delivered commands.
Command enqueue and delivery both append audit events.

Rollback a pipeline by creating a new latest version from an older version:

```json
{
  "tenant_id": "payments-prod",
  "pipeline": "default",
  "target_version": 1,
  "actor": "operator"
}
```

Agents learn about the rollback through the normal heartbeat and config update
workflow because rollback creates a higher config version.

Example registration request:

```json
{
  "agent_id": "agent-1",
  "tenant_id": "payments-prod",
  "hostname": "node-a",
  "version": "0.1.0"
}
```

The directory stores `agent_registry.term`, `pipeline_store.term`,
`command_queue.term`, and `audit_log.term`. This is an MVP durability boundary
for local development and edge prototypes.

Run a dry-run PostgreSQL migration without connecting to a database:

```powershell
mix.bat telemetry_fabric.postgres.migrate --dry-run
```

Run the migration against PostgreSQL:

```powershell
mix.bat telemetry_fabric.postgres.migrate --database-url "postgres://user:pass@localhost/telemetry_fabric"
```

Run the optional live PostgreSQL persistence test against a dedicated disposable
database. This test drops and recreates the control-plane tables in the target
database:

```powershell
$env:TELEMETRY_FABRIC_CONTROL_POSTGRES_INTEGRATION = "1"
$env:TELEMETRY_FABRIC_CONTROL_TEST_DATABASE_URL = "postgres://user:pass@localhost/telemetry_fabric_test"
mix.bat test test/postgres_live_persistence_test.exs
```

When `TELEMETRY_FABRIC_CONTROL_DATABASE_URL` is set, the OTP application starts
`TelemetryFabricControl.Repo` and `TelemetryFabricControl.PostgresSync` under
supervision. Set `TELEMETRY_FABRIC_CONTROL_POSTGRES_SYNC=false` to disable the
sync loop, or tune `TELEMETRY_FABRIC_CONTROL_POSTGRES_SYNC_INTERVAL_MS`. The
live PostgreSQL persistence test is wired into CI with a disposable PostgreSQL
service. The remaining production work is the Phoenix API and gRPC config
stream.
