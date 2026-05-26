# Smoke Tests

Smoke tests are lightweight preflight checks that should run without Kafka,
ClickHouse, Docker, or a live chain RPC.

## Run

```sh
python tests/smoke/run_smoke.py
python tests/smoke/run_mock_rpc_smoke.py
```

The smoke runner validates:

- Required test directories exist.
- Fixture JSON files parse.
- RPC scenarios contain chain id, head, blocks, gaps, and reorg metadata.
- The mock EVM RPC module can be imported.
- The mock EVM RPC server can start, serve logs, simulate a gap, and apply a
  reorg.
- Dedupe expectation fixtures contain unique expected keys.
