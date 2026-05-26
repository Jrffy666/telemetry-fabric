# Fixtures

Fixtures are intentionally small and deterministic. They are designed to seed
integration tests, replay tests, mock RPC responses, and future end-to-end test
runs without depending on live chain RPCs.

## Directories

- `evm/`: Canonical EVM blocks, raw logs, normalized event expectations, and
  dedupe keys.
- `reorg/`: Reorg replacement, block gap, and duplicate-event scenarios.
- `rpc/`: Mock JSON-RPC server plus scenario files consumed by that server.

## Fixture Rules

- Keep hashes stable across files so replay comparisons are deterministic.
- Prefer compact ranges with explicit block numbers and parent hashes.
- Use raw EVM JSON-RPC payload shapes where possible.
- Put scenario metadata in separate JSON files rather than embedding behavior in
  the mock server.
- Add expected results next to the input fixture when the expected behavior is
  part of the acceptance contract.
