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

The control plane can run with dependency-free OTP stores for local operation
or with PostgreSQL as the source of truth for multi-replica deployments.

The current OTP control domain includes:

- File-backed persistence for agent registry, pipeline versions, command
  queue, and audit events.
- PostgreSQL schema, row codecs, Ecto schemas, Repo wiring, migration task,
  periodic `Ecto.Multi` snapshot sync, and an opt-in PostgreSQL-primary store
  for tenants, agents, pipeline versions, pending and delivered commands, and
  audit events.
- A protocol-neutral `ControlService` for agent registration, heartbeat
  handling, config update generation, status reporting, pipeline publication
  and rollback, and queued operator commands.
- Audit events for agent registration, pipeline updates, pipeline rollback,
  command enqueue, and command delivery.
- YAML pipeline payload generation with a SHA-256 checksum for config update
  validation.
- A dependency-free HTTP adapter for local integration and smoke testing,
  with optional bearer-token authorization, TLS/mTLS, request ID propagation,
  structured access logs, Prometheus request metrics, and dependency-aware
  readiness checks.

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
tenant rate limiting, exporter traits, OTLP/gRPC plus OTLP/HTTP protobuf/JSON
trace/metrics/logs paths, and
a Prometheus Remote Write metrics exporter. Metrics support gauge, sum,
histogram, exponential histogram, and summary datapoints in this slice.

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
- If a segment is corrupt, the data plane quarantines it and continues with
  later healthy segments.
