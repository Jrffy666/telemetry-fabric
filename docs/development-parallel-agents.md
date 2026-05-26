# Development With Parallel Codex Agents

The repository is being structured so multiple Codex agents can work in
parallel without stepping on the same files. Agents should prefer narrow
ownership, explicit contracts, and short-lived branches or patches.

## Safe Parallel Areas

These areas can usually be developed by different agents at the same time:

- `services/chain-crawler-go/`: Go crawler and chain adapters.
- `services/stream-processor/`: stream enrichment, stateful filtering, and
  materialization logic.
- `services/analytics-python/`: Python analytics, notebooks, batch jobs, and
  model code.
- `services/compute-accelerator/`: C++/CUDA kernels, native packaging, and
  benchmarks.
- `modules/blockchain/`: blockchain module vocabulary, mapping docs, examples,
  and module fixtures.
- `storage/clickhouse/`: ClickHouse schemas and query guidance.
- `storage/kafka/`: topic definitions and broker-level conventions.
- `storage/s3/`: object layout and lifecycle definitions.
- `observability/`: dashboards, alerts, SLOs, and runbooks.
- `tests/`: cross-service tests, when each agent owns a clearly named test
  area.

Agents can work in these directories concurrently if their changes do not edit
the same contract file, shared README section, CI workflow, or deployment
manifest.

## Serialized Areas

These areas should not be modified by multiple agents at the same time without
coordination:

- `crates/telemetry-core/`: shared Rust data-plane types and config models.
- `crates/telemetry-agent/`: runtime behavior and receiver/exporter wiring.
- `crates/telemetry-buffer/`: durable queue correctness.
- `crates/telemetry-exporters/`: shared exporter trait and production sinks.
- `crates/telemetry-processors/`: generic processor semantics.
- `apps/control_plane/`: Elixir control-plane state, API, persistence, and
  rollout behavior.
- `proto/`: existing platform control/config protobufs.
- `contracts/`: shared cross-service contracts.
- `deploy/`: deployment manifests that bind multiple services together.
- `.github/`, `Makefile`, and top-level build files.

If more than one agent needs one of these areas, agree on the file owner and
sequence before editing.

## Contract Workflow

Contracts are shared coordination points. Treat them as serialized unless the
files are completely independent.

Recommended sequence:

1. Draft or update the contract.
2. Add examples or fixtures.
3. Update one producing service.
4. Update one consuming service.
5. Add compatibility or integration tests.
6. Update storage and observability references.

Do not let services exchange private structs or undocumented JSON payloads to
avoid contract review.

## Platform-Core Guardrails

Agents working outside platform core should not edit platform core unless a
generic capability is missing.

Before modifying `crates/` or `apps/control_plane/`, confirm:

- The change is business-neutral.
- It is useful beyond blockchain.
- It does not add Python, C++, CUDA, or Go runtime assumptions.
- It does not add blockchain-specific fields to `telemetry-core`.
- Existing Rust and Elixir tests still run.

## Suggested Agent Ownership

- Agent A: `services/chain-crawler-go/` and blockchain adapter scaffolding.
- Agent B: `contracts/` and compatibility fixtures.
- Agent C: `storage/clickhouse/`, `storage/kafka/`, and `storage/s3/`.
- Agent D: `services/analytics-python/`.
- Agent E: `services/compute-accelerator/`.
- Agent F: `observability/` and cross-service runbooks.
- Agent G: `tests/` for integration and replay test harnesses.

Only one agent should own platform-core changes at a time.

## Merge Hygiene

- Keep changes scoped to the assigned directory.
- Do not reformat unrelated files.
- Do not delete existing directories.
- Do not rewrite history or revert another agent's uncommitted work.
- Run the smallest relevant test command before handing off.
- Include contract and storage migration notes with any schema change.
