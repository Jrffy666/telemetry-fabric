# Service Boundaries

Telemetry Fabric is evolving into a Modular Distributed Data Fabric without
replacing the current Rust and Elixir foundation. The existing project remains
the platform core. New business domains and specialized runtimes are added as
separate service boundaries.

## Platform Core

Platform core is the existing repository foundation:

- `crates/`: Rust data plane for ingestion, durable buffering, processing,
  routing, exporter execution, retry, backpressure, and agent runtime.
- `apps/control_plane/`: Elixir control plane for tenancy, configuration,
  agent registration, heartbeat, config fetch, status, command queueing,
  pipeline publication, rollback, and audit.
- `proto/`: existing platform protocol contracts for agent control and
  pipeline/config APIs.
- `deploy/`, `config/`, `scripts/`, `.github/`: deployment, example
  configuration, local automation, and CI scaffolding.

The platform core must stay business-neutral. It should not contain blockchain
domain fields, crawler logic, Python notebooks, model code, C++ kernels, CUDA
runtime bindings, or service-specific schemas.

## Chain Crawler Go

`services/chain-crawler-go/` is the service boundary for blockchain data
collection. It is intentionally outside the Rust agent.

It owns:

- Multi-chain RPC and node connectivity.
- Chain-specific adapters.
- Cursor, checkpoint, and replay logic.
- Block, transaction, log, trace, and reorg handling.
- Source-side rate limiting and retry policy.
- Emission of normalized blockchain events into shared contracts and storage
  pipelines.

It does not own:

- Generic telemetry queue internals.
- Platform control-plane tenancy and rollout semantics.
- Rust `telemetry-core` record definitions.
- Analytical queries or GPU acceleration.

## Stream Processor

`services/stream-processor/` is the service boundary for cross-source stream
work that is too domain-specific or stateful for the existing generic
`telemetry-processors` crate.

It may own:

- Event enrichment.
- Stateful joins.
- Domain filtering.
- Deduplication.
- Partitioning for storage sinks.
- Materialization preparation for ClickHouse, Kafka, and S3.

Generic processing that applies to all telemetry can still live in
`crates/telemetry-processors/`. Blockchain-only logic should stay out of
platform core.

## Analytics Python

`services/analytics-python/` is the Python analytics boundary.

It owns:

- Batch analytics and exploratory analysis.
- Feature extraction from ClickHouse, S3, or Kafka.
- Model training and scoring workflows.
- Research code that should not affect the Rust data-plane hot path.
- Python packaging, dependency isolation, and reproducible analysis jobs.

It does not own platform ingestion, durable queue behavior, or agent runtime
configuration.

## Compute Accelerator

`services/compute-accelerator/` is the native acceleration boundary for C++ and
CUDA work.

It owns:

- CPU/GPU kernels.
- Native library packaging.
- Benchmark harnesses.
- Stable APIs called by analytics or stream-processing workloads.

It must stay outside the Rust workspace unless a future integration RFC defines
a narrow FFI boundary. The current Rust workspace forbids unsafe code, so CUDA
and C++ runtime concerns should not be introduced into `crates/`.

## Modules

`modules/` contains business modules. A module defines domain vocabulary,
business rules, examples, and service-specific mapping guidance without
polluting platform core.

`modules/blockchain/` is the first business module. It should depend on
contracts and services, not on private Rust data-plane internals.

## Contracts

`contracts/` is the home for cross-service contracts that are not existing
platform agent-control contracts.

It may contain:

- Protobuf, Avro, JSON Schema, OpenAPI, or AsyncAPI definitions.
- Versioning policy.
- Compatibility notes.
- Generated-code ownership rules.
- Shared event envelopes for modular services.

Contracts are the boundary between services. Services should exchange data
through contracts rather than importing each other's internal packages.

## Storage

`storage/` contains storage infrastructure definitions and operational
guidance.

- `storage/clickhouse/`: analytical tables, materialized views, retention,
  partitioning, and query-shape guidance.
- `storage/kafka/`: topics, partitioning, retention, consumer-group
  conventions, and schema references.
- `storage/s3/`: object layout, lifecycle policy, compaction guidance, and
  replay/export formats.

Storage definitions should reference contracts, not private service structs.

## Observability

`observability/` contains operational telemetry for the fabric itself:

- Dashboards.
- Alerts.
- SLOs.
- Runbook links.
- Service-level metrics and log conventions.

It should cover platform core and modular services without embedding business
logic in the observability assets.

## Tests

`tests/` remains the shared integration and system-test area. Service-specific
unit tests should live with each service. Cross-service compatibility,
contract, replay, deployment, and end-to-end tests belong under `tests/`.

## Boundary Rules

- Do not put blockchain crawler logic in `crates/telemetry-agent`.
- Do not add blockchain-specific fields to `crates/telemetry-core`.
- Do not put Python analytics in the Rust workspace.
- Do not put C++ or CUDA code in the Rust workspace.
- Do not bypass `contracts/` for service-to-service data exchange.
- Do not let storage schemas depend on private service implementation types.
- Keep platform-core APIs generic and stable.
