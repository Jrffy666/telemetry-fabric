# Integration Tests

Integration tests use public boundaries: fixture JSON, JSON-RPC, Kafka,
ClickHouse, service HTTP endpoints, and exported contract payloads. They should
not import private service internals.

## Current Skeleton

- `fixture_replay.py` validates block continuity, gaps, duplicate keys, and
  expected replay metadata from fixture scenarios.
- `consistency_check_skeleton.py` defines the eventual source-to-sink
  consistency checks for checkpoint height, exported events, dedupe keys, and
  metrics.

## Example

```sh
python tests/integration/fixture_replay.py \
  --scenario tests/fixtures/rpc/scenario_canonical.json
```

For a failure-oriented fixture:

```sh
python tests/integration/fixture_replay.py \
  --scenario tests/fixtures/rpc/scenario_reorg_gap_duplicate.json \
  --expect-gap \
  --expect-duplicates \
  --expect-reorg
```
