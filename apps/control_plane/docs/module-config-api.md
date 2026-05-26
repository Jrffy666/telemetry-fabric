# Module Config And Blockchain Control APIs

The control plane now exposes a generic module configuration surface plus the
first blockchain module APIs. Platform-core stays business-neutral: module
configs are opaque maps, and blockchain-specific records live under
`TelemetryFabricControl.Modules.Blockchain`.

## Auth

The APIs reuse the existing bearer-token mechanism:

- Operator token: module registration, config rollout, config rollback, and
  blockchain CRUD.
- Agent token: module config fetch and checkpoint reads. Operator tokens are
  also accepted for agent-authorized endpoints.

Health, readiness, and metrics keep their existing unauthenticated behavior.

## Generic Module APIs

Register or update a module:

```http
POST /v1/modules
```

```json
{
  "module": "blockchain",
  "display_name": "Blockchain",
  "owner": "data-platform",
  "description": "multi-chain collection config",
  "actor": "operator"
}
```

List modules:

```http
GET /v1/modules
```

Validate a candidate module config without writing state:

```http
POST /v1/modules/configs/validate
```

```json
{
  "tenant_id": "payments-prod",
  "module": "blockchain",
  "config": {
    "chains": []
  }
}
```

The response includes `valid`, `validation_errors`, `next_version`, `checksum`,
`diff`, and the current approval-hook decision.

Dry-run a candidate publication:

```http
POST /v1/modules/configs/dry-run
```

Dry-run uses the same validation and diff path as publish, but it never creates
a config version and returns `dry_run: true`.

Diff a candidate config against the latest published version:

```http
POST /v1/modules/configs/diff
```

The diff is top-level and deterministic:

```json
{
  "diff": {
    "added": ["rpc_endpoints"],
    "removed": [],
    "changed": ["chains"]
  }
}
```

Publish a new module config version:

```http
POST /v1/modules/configs/publish
```

`publish` validates the config, evaluates the approval hook, writes the new
version atomically in the selected store, and emits audit events. Replaying the
same config is idempotent: the latest version is returned and no new version is
created.

Roll out a new module config version:

```http
POST /v1/modules/configs/rollout
```

```json
{
  "tenant_id": "payments-prod",
  "module": "blockchain",
  "actor": "operator",
  "config": {
    "chains": [],
    "assignments": []
  }
}
```

`rollout` is kept as a backwards-compatible alias for `publish`.

Approval hook skeleton:

```json
{
  "tenant_id": "payments-prod",
  "module": "blockchain",
  "require_approval": true,
  "approval_id": "change-approval-123",
  "config": {
    "chains": []
  }
}
```

When `require_approval` is true and `approval_id` is missing, publication
returns `403 approval_required`. The current provider is `local-skeleton`; it is
intended to be replaced by a workflow-backed approval service.

Fetch module config:

```http
POST /v1/modules/configs/fetch
```

```json
{
  "tenant_id": "payments-prod",
  "module": "blockchain",
  "current_version": 0
}
```

The response returns `{"update": null}` when the caller is already current.
Otherwise it returns the latest version, config map, checksum, and update
metadata.

Rollback to an earlier config version:

```http
POST /v1/modules/configs/rollback
```

```json
{
  "tenant_id": "payments-prod",
  "module": "blockchain",
  "target_version": 1,
  "actor": "operator"
}
```

Rollback creates a new latest version. It does not mutate historical versions.

## Blockchain APIs

All blockchain resource APIs are scoped under:

```http
/v1/modules/blockchain
```

Supported CRUD resources:

- `/chains`
- `/rpc-endpoints`
- `/address-watchlist`
- `/contract-watchlist`
- `/token-watchlist`
- `/filter-rules`
- `/crawl-assignments`

Collection operations:

```http
GET /v1/modules/blockchain/chains?tenant_id=payments-prod
GET /v1/modules/blockchain/chains?tenant_id=payments-prod&limit=100&offset=0
POST /v1/modules/blockchain/chains
```

Collection reads support `limit` and `offset`. The server caps limit at 500 and
returns a `pagination` object with the requested limit, offset, and returned
count.

Item operations:

```http
GET /v1/modules/blockchain/chains/ethereum-mainnet?tenant_id=payments-prod
PUT /v1/modules/blockchain/chains/ethereum-mainnet
DELETE /v1/modules/blockchain/chains/ethereum-mainnet?tenant_id=payments-prod
```

Each resource has a stable ID field:

- chains: `chain_key`
- rpc endpoints: `endpoint_id`
- address watchlist: `entry_id`
- contract watchlist: `contract_id`
- token watchlist: `token_id`
- filter rules: `rule_id`
- crawl assignments: `assignment_id`

`PUT` item operations enforce path/body ID consistency. If the path ID and body
ID disagree, the API returns `400 path_id_mismatch` and does not mutate state.

Checkpoint read API:

```http
GET /v1/modules/blockchain/checkpoints/crawler-a-eth?tenant_id=payments-prod
```

Checkpoint writes are intentionally not exposed as an operator CRUD endpoint in
this first slice. Crawlers should use the service boundary that owns checkpoint
updates when it is introduced.

## Security And Reliability

- Auth is required when bearer tokens are configured. Operator token is required
  for module mutations and blockchain CRUD. Agent token can fetch module config
  and read checkpoints.
- RBAC is explicit in the HTTP adapter as `public`, `agent`, and `operator`
  roles. The current skeleton is intentionally small and can be extended without
  moving blockchain fields into platform-core.
- Tenant isolation is enforced by `tenant_id` on every list/get/delete path.
  Lists only return records for the requested tenant.
- RPC endpoint URLs are redacted before they are stored or returned. Query keys
  such as `api_key`, `token`, `secret`, and URL userinfo are replaced.
- Mutation paths are idempotent where possible. Replaying the same module config
  returns the existing version; replaying the same blockchain resource does not
  emit another mutation audit event.
- Optional HTTP rate limiting can be enabled with `rate_limit_per_second` in the
  HTTP server options. Health, readiness, and metrics remain exempt.
- Every HTTP response includes `X-Request-Id`; supplied request IDs are
  propagated to logs and responses when valid.
- PostgreSQL-backed module config publication runs inside an `Ecto.Multi`
  transaction; the OTP store performs single GenServer state transitions.

## Crawler Config Pull

A Go crawler should pull control-plane config in two layers:

1. Fetch the generic module config:

   ```http
   POST /v1/modules/configs/fetch
   Authorization: Bearer <agent-token>
   ```

   ```json
   {
     "tenant_id": "payments-prod",
     "module": "blockchain",
     "current_version": 12
   }
   ```

2. Read operational blockchain resources as needed:

   ```http
   GET /v1/modules/blockchain/chains?tenant_id=payments-prod
   GET /v1/modules/blockchain/rpc-endpoints?tenant_id=payments-prod
   GET /v1/modules/blockchain/crawl-assignments?tenant_id=payments-prod
   GET /v1/modules/blockchain/checkpoints/crawler-a-eth?tenant_id=payments-prod
   ```

The crawler must not import Elixir internals. It should treat these HTTP
responses and future `contracts/` schemas as the service boundary.

## Audit Events

The new APIs reuse the existing audit log with these action names:

- `module.registered`
- `module_config.published`
- `module_config.publish_idempotent`
- `module_config.rolled_out`
- `module_config.rolled_back`
- `blockchain.<resource>.upserted`
- `blockchain.<resource>.deleted`

Blockchain fields remain isolated to the blockchain namespace and PostgreSQL
tables prefixed with `blockchain_`.
