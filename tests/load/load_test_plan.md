# Load Test Plan

## Goals

- Validate historical backfill throughput with block gap count equal to `0`.
- Validate steady-state polling under realistic head movement.
- Validate export lag and retry behavior under bounded Kafka or ClickHouse
  instability.
- Validate checkpoint advancement remains ordered under parallel completion.

## Workloads

| Workload | Input | Target Signal |
| --- | --- | --- |
| Small deterministic replay | `scenario_canonical.json` | correctness and stable metrics |
| Reorg window replay | `scenario_reorg_gap_duplicate.json` | reorg and duplicate behavior |
| Historical backfill | generated or extended EVM fixtures | block gap count = 0 |
| Steady state | mock head advanced by `telemetry_setHead` | lag remains bounded |

## Metrics To Capture

- Crawler chain head height.
- Processed height.
- Checkpoint height.
- Block gap count.
- Reorg event count.
- Export retry/error count.
- Kafka producer lag or buffer depth.
- ClickHouse ingestion lag.
- End-to-end event count by dedupe key.

## Exit Criteria

- No historical backfill gaps.
- No checkpoint advancement past incomplete ranges.
- No lost critical events during short downstream outages.
- Duplicate input event keys collapse to one visible event.
- Metrics are present and explain the final test state.
