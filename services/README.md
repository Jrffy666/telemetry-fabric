# Services

`services/` contains independently deployable services that extend the
platform core. Services may use different languages and runtimes when that is
the right fit, but they should communicate through `contracts/` and storage
interfaces rather than private implementation imports.

## Service Boundaries

### chain-crawler-go

`services/chain-crawler-go/` is responsible for multi-chain collection:

- Chain RPC clients and node connectivity.
- Chain adapter interfaces.
- Cursoring, checkpoints, replay, and reorg handling.
- Source-side rate limits and retries.
- Publishing normalized blockchain events through shared contracts.

It must not be embedded in `crates/telemetry-agent`.

### stream-processor

`services/stream-processor/` is responsible for domain-aware stream work:

- Stateful filtering.
- Enrichment.
- Deduplication.
- Joins.
- Partitioning for downstream storage.

Generic processors can still live in `crates/telemetry-processors/`; business
logic belongs here or in a module-specific service package.

### analytics-python

`services/analytics-python/` is responsible for Python analytics:

- Batch analysis.
- Feature extraction.
- Research workflows.
- Model training and scoring.
- Jobs that read from ClickHouse, Kafka, or S3.

Python dependencies should stay isolated from the Rust workspace and Elixir
control plane.

### compute-accelerator

`services/compute-accelerator/` is responsible for C++ and CUDA acceleration:

- Native kernels.
- GPU-enabled algorithms.
- Benchmarks.
- Stable APIs for analytics or stream-processing callers.

Native acceleration should remain outside the Rust workspace unless a future
design introduces a narrow, reviewed FFI layer.

## Service Rules

- Services publish and consume shared data through `contracts/`.
- Services should not import private internals from other services.
- Services should own their local unit tests.
- Cross-service tests belong under top-level `tests/`.
- Deployment integration belongs under `deploy/` only after the service
  boundary is stable.
