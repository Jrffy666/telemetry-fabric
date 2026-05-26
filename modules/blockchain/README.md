# Blockchain Module

`modules/blockchain/` is the first business module for the Modular Distributed
Data Fabric. It defines blockchain-domain meaning without pushing blockchain
concepts into platform core.

## Responsibilities

This module owns:

- Blockchain vocabulary and glossary.
- Supported chain families and adapter expectations.
- Reorg, finality, and replay semantics.
- Domain filtering and retention policy guidance.
- Mapping rules from chain-native data to contract-defined events.
- Example payloads and fixtures for blockchain contracts.
- Module-level documentation for crawler, stream processor, analytics, and
  storage teams.

## Non-Responsibilities

This module does not own:

- Go crawler implementation.
- Rust agent runtime.
- Elixir control-plane workflows.
- Python analytics jobs.
- C++/CUDA kernels.
- ClickHouse, Kafka, or S3 operational definitions.

## Platform-Core Boundary

Blockchain fields must not be added to `crates/telemetry-core`. Examples of
module-specific fields that should stay in contracts, module docs, or storage
schemas include:

- Chain ID.
- Block number and block hash.
- Transaction hash.
- Log index.
- Trace address.
- Contract address.
- Wallet address.
- Token ID.
- ABI or event signature details.

Platform core should only see generic records, routing, processors, exporters,
and control-plane configuration.

## Collaboration Model

The blockchain module can be developed in parallel with:

- `services/chain-crawler-go/` for adapters and collection logic.
- `contracts/` for event schemas.
- `storage/` for persisted layout.
- `services/analytics-python/` for analysis consumers.
- `observability/` for module-level operational metrics.
- `tests/` for contract and replay tests.

Contract files and shared storage schemas should be treated as serialized
coordination points.
