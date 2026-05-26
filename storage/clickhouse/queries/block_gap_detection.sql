-- Detect missing block numbers in a bounded range.
-- Parameters:
--   tenant_id String
--   chain String
--   network String
--   from_block UInt64
--   to_block UInt64

SELECT expected_block AS missing_block_number
FROM
(
    SELECT arrayJoin(range({from_block:UInt64}, {to_block:UInt64} + 1)) AS expected_block
)
WHERE expected_block NOT IN
(
    SELECT block_number
    FROM telemetry_fabric.chain_blocks FINAL
    WHERE tenant_id = {tenant_id:String}
      AND chain = {chain:String}
      AND network = {network:String}
      AND block_number BETWEEN {from_block:UInt64} AND {to_block:UInt64}
      AND reorged = 0
)
ORDER BY missing_block_number ASC;
