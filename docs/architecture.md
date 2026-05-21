# Architecture

Telemetry Fabric is split into an Elixir control plane and a Rust data plane.

## Control Plane

The control plane owns slow-changing operational state:

- Tenants and environments.
- Pipeline configuration.
- Agent registration and heartbeat state.
- Config rollout and rollback.
- RBAC and audit logs.
- Live topology and fleet health.

The MVP still uses pure OTP processes so it can be bootstrapped without an
external database. The production target is Phoenix, PostgreSQL-backed Ecto
repositories, and a gRPC config-stream endpoint.

The current OTP control domain includes:

- File-backed MVP persistence for agent registry, pipeline versions, command
  queue, and audit events.
- PostgreSQL schema, row codecs, Ecto schemas, Repo wiring, migration task, and
  a periodic `Ecto.Multi` snapshot sync for the production persistence model,
  covering tenants, agents, pipeline versions, pending and delivered commands,
  and audit events.
- A protocol-neutral `ControlService` that mirrors the AgentControl protobuf
  workflow: agent registration, heartbeat handling, config update generation,
  status reporting, and queued operator commands.
- Audit events for agent registration, pipeline updates, command enqueue, and
  command delivery.
- YAML pipeline payload generation with a SHA-256 checksum so a future transport
  adapter can return `ConfigUpdate` messages without changing the domain API.
- A dependency-free MVP HTTP adapter for local integration and smoke testing.

The production Phoenix API and gRPC transport layer are still future adapters
and should be built on top of `ControlService` and the PostgreSQL schema/codec
boundary. CI now runs the live PostgreSQL persistence test against a disposable
database.

## Data Plane

The data plane owns hot-path telemetry work:

- Protocol receivers.
- Attribute processing.
- Batching.
- Durable buffering.
- Export retries.
- Backpressure.

The first Rust implementation includes core data models, routing validation, a
segmented disk queue, record processors for memory limiting, redaction, and
tenant rate limiting, exporter traits, a minimal TCP line
receiver, OTLP/gRPC plus OTLP/HTTP protobuf/JSON trace/metrics/logs paths, and
a Prometheus Remote Write metrics exporter. Metrics support gauge, sum,
histogram, exponential histogram, and summary datapoints in this slice. Kafka
and S3 are future protocol adapters.

## Runtime Topology

```text
Applications
  -> local Rust agent
  -> regional Rust gateway
  -> external observability backend

Elixir control plane
  -> streams signed config versions to agents and gateways
```

## Failure Model

- If an exporter is unavailable, the Rust agent keeps accepted telemetry in the
  local disk queue.
- If the control plane is unavailable, agents continue using their last valid
  config.
- If a config rollout fails validation, agents reject it and continue on the
  previous version.
- If a segment is corrupt, the data plane must quarantine it and continue with
  later healthy segments in a future hardening phase.
