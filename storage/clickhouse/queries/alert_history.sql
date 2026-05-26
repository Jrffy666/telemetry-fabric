-- Alert history and current state.
-- Parameters:
--   tenant_id String
--   chain String
--   network String
--   from_time DateTime64(3)
--   to_time DateTime64(3)
--   severity String -- pass '' for all severities
--   resolved UInt8 -- pass 2 for both resolved and unresolved
--   limit UInt64

SELECT
    alert_id,
    severity,
    title,
    description,
    event_type,
    rule_id,
    rule_version,
    dedupe_key,
    first_seen,
    last_seen,
    occurrence_count,
    resolved,
    resolved_at,
    resolution_reason,
    attributes
FROM telemetry_fabric.chain_alert_events FINAL
WHERE tenant_id = {tenant_id:String}
  AND chain = {chain:String}
  AND network = {network:String}
  AND first_seen >= {from_time:DateTime64(3)}
  AND first_seen < {to_time:DateTime64(3)}
  AND ({severity:String} = '' OR severity = {severity:String})
  AND ({resolved:UInt8} = 2 OR resolved = {resolved:UInt8})
ORDER BY last_seen DESC, severity DESC
LIMIT {limit:UInt64};
