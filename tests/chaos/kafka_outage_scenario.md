# Kafka Outage Scenario

## Purpose

Verify that a short Kafka outage does not lose critical crawler events and does
not advance checkpoints beyond data that has been durably accepted.

## Preconditions

- Crawler reads from `fixtures/rpc/scenario_canonical.json`.
- Kafka producer uses retryable delivery with durable local buffering.
- Metrics endpoint is scrapeable.

## Steps

1. Start Kafka, the crawler, and the mock EVM RPC server.
2. Replay blocks `0x10` through `0x13`.
3. Stop or firewall Kafka for 30 to 120 seconds.
4. Continue crawler polling during the outage.
5. Restore Kafka.
6. Wait for exporter lag to drain.
7. Query the sink and checkpoint store.

## Expected Results

- Critical event count after recovery equals the replay fixture expectation.
- Crawler checkpoint does not advance past undelivered ranges during outage.
- Duplicate event keys are either absent or collapse to one visible event.
- Metrics expose exporter retries, Kafka errors, lag, and eventual recovery.
