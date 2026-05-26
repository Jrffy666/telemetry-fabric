-- Compare node head with the latest persisted canonical block.
-- Parameters:
--   tenant_id String
--   chain String
--   network String
--   lookback_minutes UInt32

WITH
    node_head AS
    (
        SELECT
            max(latest_block) AS latest_node_block,
            max(finalized_block) AS latest_finalized_block,
            max(checked_at) AS node_checked_at
        FROM telemetry_fabric.chain_node_health FINAL
        WHERE tenant_id = {tenant_id:String}
          AND chain = {chain:String}
          AND network = {network:String}
          AND checked_at >= now64(3, 'UTC') - toIntervalMinute({lookback_minutes:UInt32})
    ),
    stored_head AS
    (
        SELECT
            max(block_number) AS latest_stored_block,
            argMax(block_timestamp, block_number) AS latest_stored_block_time
        FROM telemetry_fabric.chain_blocks FINAL
        WHERE tenant_id = {tenant_id:String}
          AND chain = {chain:String}
          AND network = {network:String}
          AND reorged = 0
    )
SELECT
    latest_node_block,
    latest_finalized_block,
    latest_stored_block,
    greatest(toInt64(latest_node_block) - toInt64(latest_stored_block), 0) AS crawler_lag_blocks,
    dateDiff('second', latest_stored_block_time, now64(3, 'UTC')) AS latest_block_age_seconds,
    node_checked_at,
    latest_stored_block_time
FROM node_head
CROSS JOIN stored_head;
