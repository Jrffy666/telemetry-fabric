# Security Model

## MVP

The MVP establishes security boundaries but does not yet enforce all production
controls.

Implemented or represented:

- Explicit tenant IDs on all telemetry records.
- Pipeline validation before runtime use.
- Audit events for agent registration, pipeline changes, pipeline rollback, and
  operator command delivery.
- Per-tenant queue quotas and processor-level ingest rate limiting.
- Bearer-token authorization for the MVP control-plane HTTP API. Agent
  endpoints accept the configured agent token or operator token; operator
  endpoints require the operator token when configured.
- No inline secret model in the protocol definitions.
- OTLP/HTTP exporter TLS for `https://` endpoints, with custom CA and optional
  client certificates.
- OTLP/HTTP receiver TLS/mTLS when configured with server cert/key and optional
  client CA.
- Control-plane HTTP TLS/mTLS when
  `TELEMETRY_FABRIC_CONTROL_TLS_ENABLED=true`; agents can call `https://`
  control endpoints with custom CA and optional client certificates.
- PostgreSQL-primary control-plane storage mode with
  `TELEMETRY_FABRIC_CONTROL_STORAGE=postgres`.

## Production Requirements

- mTLS for all agent-to-control-plane communication.
- Signed API keys and policy controls for telemetry ingestion.
- Fine-grained multi-role RBAC beyond the current agent/operator token split.
- Immutable audit log storage.
- Centralized redaction policy management.
- Envelope encryption for stored exporter credentials.
- Certificate rotation workflow for agents and gateways.
