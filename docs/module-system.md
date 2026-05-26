# Module System

The module system lets the fabric support business domains without making the
platform core domain-specific. Modules describe what a domain means; services
implement how domain data is collected, processed, stored, and analyzed.

## Goals

- Keep platform core business-neutral.
- Let multiple business modules coexist.
- Keep service implementations independently deployable.
- Make cross-service data exchange explicit through `contracts/`.
- Let domain teams add rules and mappings without modifying Rust telemetry
  primitives.

## Directory Roles

```text
modules/
  blockchain/          First business module: domain vocabulary and rules
contracts/             Cross-service schemas and compatibility policy
services/              Independently deployable service implementations
storage/               Storage layouts and operational schema guidance
observability/         Dashboards, alerts, SLOs, and runbooks
```

## Platform Core Relationship

Platform core owns generic data-fabric behavior:

- Ingestion.
- Durable buffering.
- Generic processors.
- Exporter abstractions.
- Agent runtime.
- Control-plane tenancy, config, rollout, rollback, heartbeat, command, and
  audit workflows.

Modules must not require platform core to know blockchain-specific fields such
as chain IDs, block numbers, transaction hashes, log indexes, wallet addresses,
token identifiers, or contract ABIs. Those concepts belong in module docs,
contracts, storage schemas, and services.

## Blockchain Module

`modules/blockchain/` owns blockchain-domain meaning:

- Domain glossary.
- Supported chain families and adapter expectations.
- Reorg and finality semantics.
- Event classification rules.
- Mapping from chain-native records to contract-defined events.
- Module-level examples and test fixtures.

It does not own the crawler implementation, analytics runtime, storage engine,
or platform agent internals.

## Contracts First

New module data that crosses a service boundary should start in `contracts/`.
A service may use private internal structs, but the data it publishes or
consumes across process boundaries must have an explicit contract.

Contract changes should include:

- A versioning note.
- Backward/forward compatibility expectations.
- Owning service or module.
- Example payloads or fixtures when practical.
- Migration guidance for storage and analytics consumers.

## Adding A New Module

1. Create `modules/<module-name>/README.md`.
2. Define the domain vocabulary and non-goals.
3. Add or extend contracts under `contracts/`.
4. Add service implementations under `services/` only when needed.
5. Add storage definitions under `storage/` only for persisted data.
6. Add observability assets under `observability/` when the module has
   operational signals.
7. Add cross-service tests under `tests/`.

## Dependency Direction

Preferred dependency direction:

```text
modules -> contracts
services -> contracts
services -> storage definitions
analytics -> contracts + storage
observability -> service metrics/log contracts
platform-core -> generic contracts only
```

Avoid:

- `crates/telemetry-core` depending on `modules/blockchain`.
- `apps/control_plane` depending on crawler internals.
- Python analytics importing Go service internals.
- C++/CUDA kernels importing Rust agent internals.
- Storage schemas generated from private service structs without a contract.

## Compatibility

Modules should evolve through additive changes first. Breaking changes require
new contract versions and migration notes. Platform-core changes should remain
generic enough to serve other future modules, not only blockchain.
