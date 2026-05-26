# Mock EVM RPC

`mock_evm_rpc.py` is a stdlib Python JSON-RPC server used as the foundation for
future crawler integration tests. It loads a scenario JSON file and serves EVM
methods needed by crawler replay tests.

## Supported Methods

- `eth_chainId`
- `eth_blockNumber`
- `eth_getBlockByNumber`
- `eth_getLogs`
- `telemetry_health`
- `telemetry_setHead`
- `telemetry_setGap`
- `telemetry_clearGaps`
- `telemetry_simulateReorg`

The `telemetry_*` methods are test-only controls. They should not be called by
production code.

## Scenario Files

- `scenario_canonical.json`: contiguous blocks and deterministic logs.
- `scenario_reorg_gap_duplicate.json`: gap at block `0x11`, duplicate log at
  block `0x12`, and a replacement block applied by `telemetry_simulateReorg`.
