-- Latest RPC/node health by endpoint.
-- Parameters:
--   tenant_id String
--   chain String
--   network String
--   lookback_minutes UInt32

SELECT
    node_id,
    rpc_endpoint_id,
    source_rpc,
    argMax(status, checked_at) AS status,
    argMax(synced, checked_at) AS synced,
    argMax(latest_block, checked_at) AS latest_block,
    argMax(finalized_block, checked_at) AS finalized_block,
    argMax(block_lag, checked_at) AS block_lag,
    argMax(peer_count, checked_at) AS peer_count,
    argMax(latency_ms, checked_at) AS latency_ms,
    argMax(error_rate, checked_at) AS error_rate,
    argMax(client_version, checked_at) AS client_version,
    argMax(last_error, checked_at) AS last_error,
    max(checked_at) AS last_checked_at
FROM telemetry_fabric.chain_node_health FINAL
WHERE tenant_id = {tenant_id:String}
  AND chain = {chain:String}
  AND network = {network:String}
  AND checked_at >= now64(3, 'UTC') - toIntervalMinute({lookback_minutes:UInt32})
GROUP BY
    node_id,
    rpc_endpoint_id,
    source_rpc
ORDER BY status DESC, block_lag DESC, latency_ms DESC;
