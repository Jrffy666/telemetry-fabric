# ClickHouse Outage Scenario

## Purpose

Verify that a short ClickHouse outage does not lose critical data and that
events buffered upstream are delivered after the database recovers.

## Preconditions

- Crawler or stream processor exports normalized events toward ClickHouse.
- Kafka or another durable buffer remains available.
- ClickHouse health and ingestion metrics are observable.

## Steps

1. Start the fixture-backed mock RPC server.
2. Start crawler, buffering, and ClickHouse ingestion components.
3. Replay the canonical fixture until at least one event is visible.
4. Stop ClickHouse or make it reject writes for 30 to 120 seconds.
5. Continue producing fixture events.
6. Restore ClickHouse.
7. Wait for ingestion lag to drain.
8. Run the consistency check skeleton against sink counts and dedupe keys.

## Expected Results

- No critical event is lost after ClickHouse recovers.
- Checkpoint advancement remains tied to the configured durability boundary.
- Exporter and sink metrics show errors during outage and recovery afterward.
- Replayed duplicate keys do not create duplicate visible rows.
