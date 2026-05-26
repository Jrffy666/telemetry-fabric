-- Address activity by day.
-- Parameters:
--   tenant_id String
--   chain String
--   network String
--   address String
--   from_date Date
--   to_date Date
--
-- Use FINAL when strict idempotent reads are required before background merges.

SELECT
    activity_date,
    address,
    sum(tx_count) AS tx_count,
    sum(log_count) AS log_count,
    sum(transfer_count) AS transfer_count,
    sum(success_count) AS success_count,
    sum(failed_count) AS failed_count,
    sumOrNull(amount_usd_value) AS amount_usd_sum,
    minIf(first_block_number, first_block_number > 0) AS first_block_number,
    max(last_block_number) AS last_block_number,
    min(first_seen) AS first_seen,
    max(last_seen) AS last_seen
FROM telemetry_fabric.chain_address_activity FINAL
WHERE tenant_id = {tenant_id:String}
  AND chain = {chain:String}
  AND network = {network:String}
  AND address = {address:String}
  AND activity_date BETWEEN {from_date:Date} AND {to_date:Date}
GROUP BY
    activity_date,
    address
ORDER BY activity_date ASC;
