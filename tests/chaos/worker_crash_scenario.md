# Worker Crash Scenario

## Purpose

Verify crash recovery for range assignment, out-of-order completion, and
checkpoint advancement.

## Preconditions

- Crawler uses a durable checkpoint store.
- Replay uses `fixtures/rpc/scenario_canonical.json`.
- Test harness can terminate one worker process or container.

## Steps

1. Start the mock RPC server.
2. Start the crawler with parallel range workers.
3. Assign at least three block ranges.
4. Let a later range complete before an earlier range.
5. Kill the worker processing the earlier range.
6. Restart the worker.
7. Replay the failed range from the last durable checkpoint.
8. Let all ranges complete.

## Expected Results

- Worker resumes without manual checkpoint edits.
- Checkpoint does not advance past the missing earlier range.
- Once the missing range succeeds, checkpoint advances through contiguous
  completed ranges.
- Exported event keys remain idempotent after retry.
- Metrics expose worker crash, retry, lag, and checkpoint height.
