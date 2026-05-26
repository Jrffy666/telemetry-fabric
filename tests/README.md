# Tests

`tests/` is the shared test harness for integration, fixture replay, smoke,
load, and chaos tests across Telemetry Fabric services. Service-specific unit
tests still live inside each service; this directory focuses on public APIs,
contract payloads, externally visible behavior, and reusable fixtures.

## Layout

```text
tests/
  integration/        Cross-service and replay-oriented test skeletons.
  load/               Backfill and steady-state load test plans.
  chaos/              Failure scenario playbooks.
  fixtures/
    evm/              EVM block/log fixtures and expected keys.
    reorg/            Reorg, gap, and duplicate-event fixtures.
    rpc/              Mock EVM RPC server and JSON-RPC scenarios.
  smoke/              Lightweight checks for local and CI preflight.
  README.md
```

## Quick Start

Run the smoke validation with stdlib Python:

```sh
python tests/smoke/run_smoke.py
python tests/smoke/run_mock_rpc_smoke.py
```

Replay and validate a fixture scenario:

```sh
python tests/integration/fixture_replay.py \
  --scenario tests/fixtures/rpc/scenario_canonical.json
```

Start the mock EVM JSON-RPC server:

```sh
python tests/fixtures/rpc/mock_evm_rpc.py \
  --scenario tests/fixtures/rpc/scenario_reorg_gap_duplicate.json \
  --host 127.0.0.1 \
  --port 18545
```

Example JSON-RPC calls:

```sh
curl -s http://127.0.0.1:18545 \
  -H "content-type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'

curl -s http://127.0.0.1:18545 \
  -H "content-type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"telemetry_simulateReorg","params":[]}'
```

## Acceptance Metrics

The industrial crawler test suite should document and eventually automate these
checks:

- Historical backfill block gap count is `0`.
- A worker crash resumes from durable checkpoint state.
- Checkpoints advance only after contiguous ordered completion.
- Blocks inside the reorg window remain pending until finalized.
- Reorged blocks are marked, corrected, or replayed according to policy.
- Duplicate EVM events dedupe by `chain_id + block_hash + tx_hash + log_index`.
- Duplicate transactions dedupe by `chain_id + block_hash + tx_hash`.
- Short Kafka outages do not lose critical data.
- Short ClickHouse outages do not lose critical data.
- Metrics expose crawler head height, processed height, checkpoint height,
  block gaps, reorg events, retry/error counters, and export lag.

## Scenario Coverage

- `fixtures/rpc/scenario_canonical.json` covers clean fixture replay.
- `fixtures/rpc/scenario_reorg_gap_duplicate.json` covers reorg, block gap,
  and duplicate event simulation through the mock RPC server.
- `chaos/kafka_outage_scenario.md` covers producer/broker interruption.
- `chaos/clickhouse_outage_scenario.md` covers sink unavailability.
- `chaos/worker_crash_scenario.md` covers crash recovery expectations.
- `chaos/config_rollback_scenario.md` covers rollback after bad config.
- `load/load_test_plan.md` defines the backfill and steady-state load skeleton.

## Guardrails

- Do not move Rust, Elixir, or service-local unit tests here.
- Do not import private service structs across language boundaries.
- Prefer contract payloads, fixtures, HTTP/RPC calls, and public service APIs.
- Keep long-running load and chaos tests separate from smoke tests.
- Document external dependencies such as Kafka, ClickHouse, S3-compatible
  storage, GPU runtime, or chain RPC endpoints.
