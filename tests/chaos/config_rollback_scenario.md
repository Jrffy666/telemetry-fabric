# Config Rollback Scenario

## Purpose

Verify that a bad crawler or pipeline config can be rolled back without losing
critical data or corrupting checkpoint state.

## Preconditions

- Control plane can publish a config version and rollback to the previous one.
- Crawler reports active config version and checkpoint metrics.
- Fixture replay is deterministic.

## Steps

1. Start crawler with a known-good config.
2. Replay `fixtures/rpc/scenario_canonical.json`.
3. Publish a bad config that rejects or misroutes the fixture event type.
4. Confirm errors or discarded-event metrics rise.
5. Roll back to the previous config version.
6. Replay from the durable checkpoint or configured recovery point.
7. Run consistency checks against exported events and checkpoint state.

## Expected Results

- Rollback returns crawler to the previous good config version.
- Checkpoint does not skip events affected by the bad config.
- Critical events are replayed or retained according to durability policy.
- Metrics and audit state show config publication, failure, and rollback.
