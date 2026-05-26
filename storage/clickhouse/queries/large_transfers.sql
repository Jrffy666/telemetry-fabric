-- Large token transfers over a USD threshold.
-- Parameters:
--   tenant_id String
--   chain String
--   network String
--   from_time DateTime64(3)
--   to_time DateTime64(3)
--   min_amount_usd String
--   limit UInt64

SELECT
    block_timestamp,
    block_number,
    tx_hash,
    log_index,
    transfer_index,
    token_address,
    token_symbol,
    token_standard,
    from_address,
    to_address,
    amount_raw,
    amount_decimal,
    amount_usd,
    amount_usd_value,
    finality_status,
    source_rpc
FROM telemetry_fabric.chain_token_transfers FINAL
WHERE tenant_id = {tenant_id:String}
  AND chain = {chain:String}
  AND network = {network:String}
  AND block_timestamp >= {from_time:DateTime64(3)}
  AND block_timestamp < {to_time:DateTime64(3)}
  AND amount_usd_value >= toDecimal128({min_amount_usd:String}, 18)
  AND reorged = 0
ORDER BY amount_usd_value DESC, block_timestamp DESC
LIMIT {limit:UInt64};
