# Security Model

## Implemented Security

The core system establishes security boundaries for the agent and control-plane
path. Remaining hardening items are listed below.

Implemented or represented:

- Explicit tenant IDs on all telemetry records.
- Pipeline validation before runtime use.
- Audit events for agent registration, pipeline changes, pipeline rollback, and
  operator command delivery.
- Per-tenant queue quotas and processor-level ingest rate limiting.
- Bearer-token authorization for the control-plane HTTP API. Agent
  endpoints accept the configured agent token or operator token; operator
  endpoints require the operator token when configured.
- Pipeline/config payloads avoid inline secrets by design.
- OTLP/HTTP exporter TLS for `https://` endpoints, with custom CA and optional
  client certificates.
- OTLP/HTTP receiver TLS/mTLS when configured with server cert/key and optional
  client CA.
- Control-plane HTTP TLS/mTLS when
  `TELEMETRY_FABRIC_CONTROL_TLS_ENABLED=true`; agents can call `https://`
  control endpoints with custom CA and optional client certificates.
- PostgreSQL-primary control-plane storage mode with
  `TELEMETRY_FABRIC_CONTROL_STORAGE=postgres`.
- Scheduled GitHub Actions dependency and vulnerability scanning for Rust
  advisories and high/critical Trivy findings.
- Tag-driven container release automation with image provenance, SBOM output,
  and keyless cosign signing for published GHCR images.

## Production Requirements

- mTLS for all agent-to-control-plane communication.
- Signed API keys and policy controls for telemetry ingestion.
- Fine-grained multi-role RBAC beyond the current agent/operator token split.
- Immutable audit log storage.
- Centralized redaction policy management.
- Envelope encryption for stored exporter credentials.
- Certificate rotation workflow for agents and gateways.
