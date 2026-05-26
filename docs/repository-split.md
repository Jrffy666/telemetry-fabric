# Repository Split Plan

The current repository is useful while the platform is still changing quickly,
but it is now carrying several different product lifecycles in one tree:

- Rust and Elixir distributed-system core.
- Go blockchain crawler.
- Python analytics and data cleaning.
- C++ and CUDA compute accelerator.
- Shared contracts, storage schemas, deployment, and integration tests.

Splitting is feasible, but it should be done in stages. A direct directory move
would break relative paths in `Makefile`, CI, Dockerfiles, deployment assets,
Python tests, Go module imports, and shared storage/query fixtures.

## Target Projects

### `telemetry-fabric-core`

Owns the distributed-system runtime:

- `crates/`
- `apps/control_plane/`
- `proto/` for platform agent/control APIs
- core `config/`
- core deployment examples
- Rust and Elixir CI

This project should stay business-neutral. Blockchain, analytics, and native
accelerator implementation details should not move into it.

### `telemetry-fabric-contracts`

Owns cross-service compatibility:

- `contracts/`
- shared protobuf schemas
- storage table and topic contracts from `storage/`
- business module contracts from `modules/`
- canonical fixtures from `tests/fixtures/`

Other projects should consume this project as a versioned dependency, generated
SDK, package artifact, or Git submodule. Services should not import private code
from each other.

### `telemetry-chain-crawler-go`

Owns blockchain collection:

- `services/chain-crawler-go/`
- crawler-specific docs and examples
- crawler unit tests
- optional local fixture copies for developer tests

Required follow-up after extraction:

- Rename `go.mod` from the monorepo path to the new module path.
- Replace relative fixture paths with testdata-local paths or a contracts
  package checkout.
- Publish crawler output only through versioned contracts.

### `telemetry-analytics-python`

Owns data processing, cleaning, feature extraction, and analysis jobs:

- `services/analytics-python/`
- analytics package metadata and tests
- SQL query files that analytics executes directly

Required follow-up after extraction:

- Package ClickHouse query files inside the Python package, or load them from
  the contracts/storage artifact.
- Remove tests that reach back into `../../storage/...`.
- Keep notebooks and jobs out of the platform core release process.

### `telemetry-compute-accelerator`

Owns native acceleration:

- `services/compute-accelerator/`
- CMake configuration
- C++ service API
- CUDA kernels
- native tests and benchmarks

Required follow-up after extraction:

- Keep the CMake project self-contained.
- Publish a narrow gRPC, C ABI, or package-level API for callers.
- Keep CUDA and unsafe native runtime concerns out of the Rust workspace.

### `telemetry-fabric-deploy`

Owns environment composition:

- `deploy/`
- `observability/`
- smoke and system test orchestration
- Compose, Kubernetes, Helm, and runbook assets

This project should consume images and artifacts produced by the other projects.
It is the right place for end-to-end tests that need multiple services running
together.

## Shared Dependency Rules

- Contracts flow from `telemetry-fabric-contracts` to every service.
- Services communicate through protobuf, JSON schema, Kafka, storage tables,
  gRPC, or HTTP APIs, not by importing another service's internal package.
- Storage schemas reference contracts, not private implementation structs.
- Deployment consumes released images and packages, not source paths.
- Integration tests may compose projects, but unit tests stay inside each
  project.

## Migration Sequence

1. Freeze contract names, event identity fields, table names, and topic names.
2. Create the split preview with:

   ```powershell
   powershell -ExecutionPolicy Bypass -File scripts\dev\preview-repository-split.ps1 -Clean
   ```

3. Review `.tmp/split-preview` and adjust ownership of any misplaced files.
4. Update each project so local tests do not depend on monorepo-relative paths.
5. Extract real repositories with history preservation, for example:

   ```powershell
   git subtree split --prefix services/chain-crawler-go -b split/chain-crawler-go
   git subtree split --prefix services/analytics-python -b split/analytics-python
   git subtree split --prefix services/compute-accelerator -b split/compute-accelerator
   ```

6. Add the contracts project to each service as a versioned dependency or
   submodule.
7. Move deployment composition to `telemetry-fabric-deploy`.
8. Replace monorepo CI with per-project CI plus a scheduled integration matrix.

## Current Coupling To Remove

- `Makefile` and CI call service paths directly.
- Python tests and query checks read `storage/clickhouse` from the monorepo.
- Go code uses a monorepo module path.
- Docker and deployment assets assume source directories live in this repo.
- Shared fixtures under `tests/fixtures` are consumed by multiple services.

The preview script intentionally copies shared files where useful. The actual
split should replace those copies with a versioned contracts dependency before
the projects are released independently.

