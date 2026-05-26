# Control Plane Hardening

This note documents the industrial hardening boundaries added for module and
blockchain control-plane APIs.

## Scope

`apps/control_plane` remains the control plane for tenancy, auth, audit,
configuration lifecycle, agent registration, heartbeat, command queueing,
publication, and rollback. Business-specific blockchain concepts stay in the
`TelemetryFabricControl.Modules.Blockchain` namespace and in
`/v1/modules/blockchain/...` HTTP paths.

The control plane does not embed crawler logic, Python analytics, or
accelerator code. Those remain separate service boundaries.

## Module Config Lifecycle

The generic module config lifecycle supports:

- module registry
- config validation
- dry-run plan generation
- deterministic top-level diff
- publish with versioning and idempotency
- rollback by creating a new latest version
- audit events for publish, idempotent publish, and rollback
- approval hook skeleton with `require_approval` and `approval_id`

`rollout` remains as a backwards-compatible alias for `publish`.

## Blockchain Control APIs

Blockchain APIs cover the first operational module surface:

- chains CRUD
- RPC endpoints CRUD with URL secret redaction
- address watchlist CRUD
- contract watchlist CRUD
- token watchlist CRUD
- filter rules CRUD
- crawl assignments CRUD
- checkpoint read API

All records require `tenant_id`; list and get paths are tenant-scoped. Path/body
ID mismatches are rejected before mutation.

## Security

The HTTP adapter reuses existing bearer tokens. Operator role is required for
module mutations and blockchain CRUD. Agent role can fetch config and read
checkpoints. A small RBAC skeleton keeps the role decision explicit for future
policy integration.

RPC endpoint secrets are redacted before records are persisted or returned. The
redactor handles URL userinfo and query keys such as `api_key`, `token`,
`secret`, `password`, and `access_token`.

## Reliability

PostgreSQL-backed module config publication uses an `Ecto.Multi` transaction.
The OTP-backed stores use single GenServer transitions. HTTP responses propagate
`X-Request-Id`, mutation paths are idempotent where practical, collection reads
support pagination, and optional per-path rate limiting is available through
HTTP server options.

## Tests

The control-plane tests cover:

- Ecto schema changesets
- module config publish/rollback
- validation, dry-run, diff, approval, and idempotent publish
- blockchain CRUD and checkpoint reads
- tenant isolation, pagination, path/body ID mismatch, validation, and redaction
- auth role reuse and HTTP rate limiting
