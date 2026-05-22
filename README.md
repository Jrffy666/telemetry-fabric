# Telemetry Fabric

Telemetry Fabric is a distributed observability pipeline built with Rust and
Elixir. The Rust data plane is responsible for ingestion, buffering,
processing, and export. The Elixir control plane is responsible for tenancy,
configuration, rollout control, agent registration, auditability, and live
operations.

The project is designed to run across Kubernetes, Docker, systemd-managed Linux
hosts, bare metal, cloud VMs, and constrained edge environments.

## Current MVP Scope

This repository currently contains:

- A Rust workspace with data-plane core models.
- A durable segmented disk queue for local buffering.
- A batch-oriented runtime that only advances queue cursors after successful export.
- Per-exporter timeout and retry budgets with exponential backoff.
- Record processors for memory limiting, tenant rate limiting, and sensitive
  attribute redaction.
- Stdout, file, OTLP/gRPC, OTLP/HTTP protobuf/JSON, and Prometheus Remote Write
  exporters behind a stable async exporter trait, including HTTPS and optional
  client certificate support for HTTP-based exporters.
- A Rust agent CLI with a minimal line-protocol ingestion mode.
- OTLP/gRPC and OTLP/HTTP protobuf/JSON trace, metrics, and logs receivers and
  forwarders, with TLS/mTLS support for the OTLP/HTTP receiver. Metrics
  currently cover gauge, sum, histogram, exponential histogram, and summary
  datapoints.
- A JSON health endpoint and Prometheus-format `/metrics` for agent self-observability.
- Durable MVP control commands for config reload, export pause/resume, and
  drain-before-restart, including delivered/executed-state persistence and
  audit events.
- An Elixir OTP control-plane skeleton.
- PostgreSQL schema, state-to-row codec, Ecto schemas, Repo wiring, migration
  task, periodic snapshot sync, and an opt-in PostgreSQL-primary control-plane
  storage mode.
- HTTP MVP control endpoints for agent registration, heartbeat, config fetch,
  status, command queueing, pipeline publication, and rollback.
- Control-plane `/healthz`, dependency-aware `/readyz`, request ID propagation,
  structured HTTP access logs, and Prometheus-format `/metrics`.
- Bearer-token authorization and optional TLS/mTLS for the control-plane HTTP
  API, plus HTTPS/mTLS support in the agent control client.
- Versioned pipeline rollback that creates a new latest config version and
  preserves rollback audit history.
- Protobuf contracts for future agent-control and pipeline APIs.
- Deployment and operations scaffolding.
- Docker Compose, raw Kubernetes, Helm, and systemd deployment examples for the
  agent, plus Docker Compose and Kubernetes/Helm deployment examples for the
  MVP control-plane HTTP API. Kubernetes and Helm assets include service
  accounts, baseline pod security settings, optional NetworkPolicy,
  PodDisruptionBudget for the control plane, optional ServiceMonitor scraping,
  agent pipeline ConfigMap wiring, and a production values example.
- CI coverage for Rust, Elixir, Docker image builds, Helm rendering, and a
  Docker Compose smoke test, plus scheduled dependency/security scanning and a
  tag-driven container release workflow with SBOM/provenance output and image
  signing.

The Rust crates keep protocol integrations behind stable internal traits so the
runtime can evolve without changing queue, processor, and routing semantics.

## Repository Layout

```text
apps/
  control_plane/        Elixir OTP control plane
crates/
  telemetry-agent/      Rust agent binary
  telemetry-buffer/     Durable disk queue
  telemetry-core/       Shared data-plane config and telemetry types
  telemetry-exporters/  Exporter implementations
  telemetry-processors/ Processor implementations
proto/                  gRPC/protobuf contracts
deploy/                 Docker, Kubernetes, systemd deployment assets
docs/                   Architecture and operations documentation
```

## Rust Development

```bash
cargo test --workspace
cargo run -p telemetry-agent -- --self-test
cargo run -p telemetry-agent -- --check-config --config config/pipeline.example.yaml
cargo run -p telemetry-agent -- --config config/pipeline.example.yaml --health-listen 127.0.0.1:13133
cargo run -p telemetry-agent -- --listen 127.0.0.1:4319 --flush-batch-size 128
cargo run -p telemetry-agent -- --otlp-grpc 127.0.0.1:4317 --flush-batch-size 128
cargo run -p telemetry-agent -- --otlp-http 127.0.0.1:4318 --flush-batch-size 128
cargo run -p telemetry-agent -- --otlp-grpc 127.0.0.1:4317 --health-listen 127.0.0.1:13133
cargo run -p telemetry-agent -- --otlp-grpc 127.0.0.1:4317 --otlp-export-endpoint http://127.0.0.1:14317
cargo run -p telemetry-agent -- --tenant payments-prod --agent-id agent-1 --otlp-grpc 127.0.0.1:4317 --control-endpoint http://127.0.0.1:4001
```

When `--config` is provided, the agent loads receivers, processors, exporters,
routes, and tenant limits from YAML. In receiver modes, accepted telemetry is
written to the local queue first; a background flush worker exports queued
records every `--flush-interval-ms` milliseconds.

Line protocol example:

```text
trace payments-prod checkout.request request-started
metric payments-prod checkout.latency_ms 42
log payments-prod checkout.worker worker-ready
```

## Elixir Development

The Elixir control plane is structured as a standalone OTP application:

```bash
cd apps/control_plane
mix test
```

Elixir/Mix must be installed locally before these commands can run.

## Local Docker Stack

```bash
docker compose -f deploy/docker/docker-compose.yml up --build
```

The compose stack starts PostgreSQL, the MVP HTTP control plane on `:4001`, and
an agent that heartbeats to the control plane while receiving OTLP/gRPC on
`:4317`.

Run the CI smoke test locally with:

```bash
make docker-smoke
```
