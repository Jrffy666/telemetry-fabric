-- Token inflow/outflow for one address.
-- Parameters:
--   tenant_id String
--   chain String
--   network String
--   address String
--   token_address String -- pass '' to include all tokens
--   from_date Date
--   to_date Date

WITH flows AS
(
    SELECT
        block_date AS flow_date,
        token_address,
        token_symbol,
        amount_usd_value,
        amount_decimal_value,
        1 AS direction
    FROM telemetry_fabric.chain_token_transfers FINAL
    WHERE tenant_id = {tenant_id:String}
      AND chain = {chain:String}
      AND network = {network:String}
      AND to_address = {address:String}
      AND ({token_address:String} = '' OR token_address = {token_address:String})
      AND block_date BETWEEN {from_date:Date} AND {to_date:Date}
      AND reorged = 0

    UNION ALL

    SELECT
        block_date AS flow_date,
        token_address,
        token_symbol,
        amount_usd_value,
        amount_decimal_value,
        -1 AS direction
    FROM telemetry_fabric.chain_token_transfers FINAL
    WHERE tenant_id = {tenant_id:String}
      AND chain = {chain:String}
      AND network = {network:String}
      AND from_address = {address:String}
      AND ({token_address:String} = '' OR token_address = {token_address:String})
      AND block_date BETWEEN {from_date:Date} AND {to_date:Date}
      AND reorged = 0
)
SELECT
    flow_date,
    token_address,
    any(token_symbol) AS token_symbol,
    sumIf(amount_decimal_value, direction = 1) AS inbound_amount,
    sumIf(amount_decimal_value, direction = -1) AS outbound_amount,
    sumIf(amount_decimal_value, direction = 1) - sumIf(amount_decimal_value, direction = -1) AS net_amount,
    sumIf(amount_usd_value, direction = 1) AS inbound_usd,
    sumIf(amount_usd_value, direction = -1) AS outbound_usd,
    sumIf(amount_usd_value, direction = 1) - sumIf(amount_usd_value, direction = -1) AS net_usd
FROM flows
GROUP BY
    flow_date,
    token_address
ORDER BY flow_date ASC, abs(net_usd) DESC;
