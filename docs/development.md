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

Validate the resolved pipeline configuration without opening the local queue or
binding receiver ports:

```bash
cargo run -p telemetry-agent -- --check-config --config config/pipeline.example.yaml
```

Flush any records already in the local durable queue:

```bash
cargo run -p telemetry-agent
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
Exporter failure handling can be tuned per exporter with `retry.max_attempts`,
`retry.timeout_ms`, and `retry.initial_backoff_ms`; omitted values use the
runtime defaults.

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

Start the agent with the HTTP control-plane client:

```bash
cargo run -p telemetry-agent -- --tenant payments-prod --agent-id agent-1 --otlp-grpc 127.0.0.1:4317 --control-endpoint http://127.0.0.1:4001
```

For a protected control plane, pass an agent token with
`--control-auth-token` or `TELEMETRY_FABRIC_CONTROL_AUTH_TOKEN`. HTTPS control
endpoints support `--control-ca-file`, `--control-cert-file`,
`--control-key-file`, and `--control-server-name` for private CAs and mTLS.

The client registers once, then sends periodic heartbeats. When the control
plane returns `reload_config`, the agent fetches `/v1/agents/config`, verifies
the SHA-256 checksum, parses the YAML pipeline, and reloads processors,
exporters, routes, and limits in the running runtime. Receiver sockets are not
rebound during hot reload. Operator commands can also pause exports,
resume exports, or ask the agent to drain queued records and exit so systemd or
Kubernetes restarts it.

The control plane still runs without external services by default. PostgreSQL
persistence now includes the plain SQL schema at
`apps/control_plane/priv/postgres/001_control_plane_schema.sql`, row codecs that
convert OTP state into the target tables, Ecto schemas, Repo wiring, a migration
task, and a periodic `Ecto.Multi` snapshot sync for PostgreSQL persistence.

## Elixir

Run the control-plane tests:

```bash
cd apps/control_plane
mix test
```

The control-plane domain API is exposed by
`TelemetryFabricControl.ControlService`. It implements the transport-neutral
agent-control workflow: `register_agent/1`, `heartbeat/1`, `config_update/1`,
`enqueue_command/3`, and `report_status/1`.

Run the control plane with dependency-free file persistence:

```powershell
$env:TELEMETRY_FABRIC_CONTROL_DATA_DIR = "../../.telemetry-fabric/control-plane"
mix.bat run --no-halt
```

On Windows, if `mix.bat test` fails inside `Mix.Sync.PubSub` with
`File.mkdir_p!` reporting `:enotdir` for an existing absolute temp directory,
run Mix through the project-local wrapper:

```powershell
elixir.bat .\scripts\mix_windows.exs test
```

From the repository root, use the script's full path:

```powershell
elixir.bat .\apps\control_plane\scripts\mix_windows.exs test
```

The wrapper patches `File.mkdir_p/1` and Mix's Windows temp lock/pubsub roots
for the current VM process only. Test persistence paths stay relative to the
app working directory so they do not depend on Windows absolute-path mkdir
behavior.

Run the control plane with the HTTP control API:

```powershell
$env:TELEMETRY_FABRIC_CONTROL_HTTP_LISTEN = "127.0.0.1:4001"
mix.bat run --no-halt
```

Enable HTTP API authorization by setting one or both tokens:

```powershell
$env:TELEMETRY_FABRIC_CONTROL_AGENT_TOKEN = "agent-token"
$env:TELEMETRY_FABRIC_CONTROL_OPERATOR_TOKEN = "operator-token"
```

Enable TLS or mTLS for the control API:

```powershell
$env:TELEMETRY_FABRIC_CONTROL_TLS_ENABLED = "true"
$env:TELEMETRY_FABRIC_CONTROL_TLS_CERT_FILE = "certs/server.pem"
$env:TELEMETRY_FABRIC_CONTROL_TLS_KEY_FILE = "certs/server-key.pem"
$env:TELEMETRY_FABRIC_CONTROL_TLS_REQUIRE_CLIENT_AUTH = "true"
$env:TELEMETRY_FABRIC_CONTROL_TLS_CA_FILE = "certs/ca.pem"
```

Available HTTP endpoints:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`
- `POST /v1/agents/register`
- `POST /v1/agents/heartbeat`
- `POST /v1/agents/config`
- `POST /v1/agents/status`
- `POST /v1/agents/commands`
- `POST /v1/agents/commands/ack`
- `POST /v1/pipelines`
- `POST /v1/pipelines/rollback`

`/readyz` returns HTTP 200 in the default OTP-backed mode. When PostgreSQL is
configured, readiness performs a lightweight `SELECT 1` through the configured
Repo and returns HTTP 503 if the dependency is unavailable. `/metrics` exposes
Prometheus text metrics for control-plane HTTP request counts and total request
handling duration. The HTTP adapter also propagates a valid `X-Request-Id`
header or generates one for each request and includes it in access logs.

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
agent then acknowledges command execution as `succeeded` or `failed` through
`/v1/agents/commands/ack`. PostgreSQL snapshot sync preserves pending,
delivered, succeeded, and failed commands. Command enqueue, delivery, success,
and failure append audit events.

Publish a pipeline version:

```json
{
  "tenant_id": "payments-prod",
  "pipeline": "default",
  "actor": "operator",
  "receivers": [
    {"name": "otlp-grpc", "protocol": "otlp_grpc", "endpoint": "0.0.0.0:4317"}
  ],
  "processors": [
    {"name": "memory-limiter", "enabled": true},
    {"name": "tenant-rate-limit", "enabled": true}
  ],
  "exporters": [
    {"name": "stdout", "protocol": "stdout", "endpoint": "stdout://local"}
  ],
  "routes": [
    {"signal": "trace", "exporters": ["stdout"]}
  ]
}
```

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
`command_queue.term`, and `audit_log.term`. This is the durability boundary for
file-backed local operation.

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
service.

Set `TELEMETRY_FABRIC_CONTROL_STORAGE=postgres` to make PostgreSQL the
control-plane source of truth instead of the file-backed OTP stores. This mode
requires `TELEMETRY_FABRIC_CONTROL_DATABASE_URL` and disables the periodic
snapshot sync.

## Docker And Kubernetes

Run the local container stack:

```bash
docker compose -f deploy/docker/docker-compose.yml up --build
```

The stack starts PostgreSQL, runs the control-plane schema migration through
the release helper, exposes the control-plane HTTP API on
`http://127.0.0.1:4001`, and starts an agent connected to that control plane.

Build the individual images:

```bash
docker build -f deploy/docker/telemetry-agent.Dockerfile -t telemetry-fabric/agent:0.1.0 .
docker build -f deploy/docker/control-plane.Dockerfile -t telemetry-fabric/control-plane:0.1.0 .
```

Run the same Docker Compose smoke test used by CI:

```bash
bash scripts/ci_smoke_compose.sh
```

The smoke test builds the compose stack, waits for control-plane and agent
readiness, registers a test agent, queues an operator command, verifies
heartbeat delivery, scrapes control-plane and agent metrics, and tears the
stack down.

Apply raw Kubernetes manifests:

```bash
kubectl apply -f deploy/k8s/control-plane.yaml
kubectl apply -f deploy/k8s/telemetry-agent-daemonset.yaml
kubectl apply -f deploy/k8s/network-policy.yaml
```

Install the Helm chart with the control plane enabled:

```bash
helm install telemetry-fabric deploy/helm/telemetry-fabric --set controlPlane.enabled=true
```

Create the agent pipeline ConfigMap and install with the production example
values:

```bash
kubectl create configmap telemetry-fabric-agent-pipeline \
  --from-file=pipeline.yaml=config/pipeline.example.yaml

kubectl create secret generic telemetry-fabric-postgres \
  --from-literal=database-url='postgres://user:password@postgres:5432/telemetry_fabric'
kubectl create secret generic telemetry-fabric-agent-token --from-literal=token='agent-token'
kubectl create secret generic telemetry-fabric-operator-token --from-literal=token='operator-token'
kubectl create secret generic telemetry-fabric-control-plane-tls \
  --from-file=ca.crt=ca.crt \
  --from-file=tls.crt=control-plane.crt \
  --from-file=tls.key=control-plane.key

helm upgrade --install telemetry-fabric deploy/helm/telemetry-fabric \
  -f deploy/helm/telemetry-fabric/values-production.example.yaml
```

For local experiments, Helm can also inline the pipeline file instead of
referencing an existing ConfigMap:

```bash
helm upgrade --install telemetry-fabric deploy/helm/telemetry-fabric \
  --set controlPlane.enabled=true \
  --set-file agent.config.inline=config/pipeline.example.yaml
```

The production example sets `productionMode=true`, which makes Helm fail
rendering unless the control plane uses PostgreSQL, required auth/database/TLS
secrets are set, NetworkPolicy is enabled, and agent/control-plane image tags
are pinned to non-`latest` values.

The Helm chart also exposes baseline production hardening switches:
`agent.serviceAccount`, `controlPlane.serviceAccount`,
`agent.config`, `agent.extraEnv`, `agent.nodeSelector`, `agent.tolerations`,
`agent.affinity`, `controlPlane.podDisruptionBudget`, `controlPlane.extraEnv`,
`controlPlane.nodeSelector`, `controlPlane.tolerations`,
`controlPlane.affinity`, `networkPolicy.enabled`, and `serviceMonitor.enabled`.

For the control plane, Helm enforces the storage topology:

- `controlPlane.storage=otp` is file-backed and must run as a single replica.
- `controlPlane.storage=postgres` requires `controlPlane.databaseUrlSecret.name`
  and may run multiple replicas.
- In Postgres-primary mode, the pod data directory is mounted as `emptyDir`
  because PostgreSQL is the source of truth; PVC persistence is only used for
  the file-backed OTP mode.

## Release And Security Automation

The default CI workflow runs Rust formatting, clippy, unit tests, Elixir tests,
PostgreSQL integration tests, Docker image builds, Docker Compose smoke tests,
and Helm validation.

The security workflow runs weekly and on pushes/PRs. It checks Rust advisories
and scans the repository with Trivy for high and critical vulnerabilities.

Pushing a `vX.Y.Z` tag runs the release workflow, builds multi-architecture
agent and control-plane images for GHCR, emits build provenance and SBOM data,
and signs the published image digests with keyless cosign.
