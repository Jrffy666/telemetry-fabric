-- Find duplicate physical rows that still exist before ReplacingMergeTree
-- background merges. Do not use FINAL here; duplicates are the signal.
-- Parameters:
--   tenant_id String
--   chain String
--   network String
--   from_date Date
--   to_date Date
--   limit UInt64

SELECT *
FROM
(
    SELECT
        'transactions' AS event_table,
        concat(toString(chain_id), ':', block_hash, ':', tx_hash, ':0:0') AS identity_key,
        min(block_timestamp) AS first_seen,
        max(block_timestamp) AS last_seen,
        count() AS physical_rows
    FROM telemetry_fabric.chain_transactions
    WHERE tenant_id = {tenant_id:String}
      AND chain = {chain:String}
      AND network = {network:String}
      AND block_date BETWEEN {from_date:Date} AND {to_date:Date}
    GROUP BY identity_key
    HAVING physical_rows > 1

    UNION ALL

    SELECT
        'logs' AS event_table,
        concat(toString(chain_id), ':', block_hash, ':', tx_hash, ':', toString(log_index), ':0') AS identity_key,
        min(block_timestamp) AS first_seen,
        max(block_timestamp) AS last_seen,
        count() AS physical_rows
    FROM telemetry_fabric.chain_logs
    WHERE tenant_id = {tenant_id:String}
      AND chain = {chain:String}
      AND network = {network:String}
      AND block_date BETWEEN {from_date:Date} AND {to_date:Date}
    GROUP BY identity_key
    HAVING physical_rows > 1

    UNION ALL

    SELECT
        'token_transfers' AS event_table,
        concat(toString(chain_id), ':', block_hash, ':', tx_hash, ':', toString(log_index), ':', toString(transfer_index)) AS identity_key,
        min(block_timestamp) AS first_seen,
        max(block_timestamp) AS last_seen,
        count() AS physical_rows
    FROM telemetry_fabric.chain_token_transfers
    WHERE tenant_id = {tenant_id:String}
      AND chain = {chain:String}
      AND network = {network:String}
      AND block_date BETWEEN {from_date:Date} AND {to_date:Date}
    GROUP BY identity_key
    HAVING physical_rows > 1
)
ORDER BY physical_rows DESC, last_seen DESC
LIMIT {limit:UInt64};
