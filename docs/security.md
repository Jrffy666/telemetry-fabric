# Security Model

## MVP

The MVP establishes security boundaries but does not yet enforce all production
controls.

Implemented or represented:

- Explicit tenant IDs on all telemetry records.
- Pipeline validation before runtime use.
- Audit events for agent registration, pipeline changes, and operator command
  delivery.
- Per-tenant queue quotas and processor-level ingest rate limiting.
- No inline secret model in the protocol definitions.
- OTLP/HTTP exporter TLS for `https://` endpoints, with custom CA and optional
  client certificates.
- OTLP/HTTP receiver TLS/mTLS when configured with server cert/key and optional
  client CA.

## Production Requirements

- mTLS for all agent-to-control-plane communication.
- Signed API keys and policy controls for telemetry ingestion.
- RBAC for all control-plane APIs.
- Immutable audit log storage.
- Centralized redaction policy management.
- Envelope encryption for stored exporter credentials.
- Certificate rotation workflow for agents and gateways.
