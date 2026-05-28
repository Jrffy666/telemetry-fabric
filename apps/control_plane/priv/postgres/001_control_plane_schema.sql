-- Telemetry Fabric control-plane PostgreSQL schema.
-- This SQL is intentionally plain PostgreSQL so it can be used by the Mix
-- migration task and by operators before Phoenix migrations are introduced.

CREATE TABLE IF NOT EXISTS tenants (
  tenant_id text PRIMARY KEY,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agents (
  agent_id text PRIMARY KEY,
  tenant_id text NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  hostname text NOT NULL,
  version text NOT NULL,
  config_version bigint NOT NULL DEFAULT 0,
  queue_depth_bytes bigint NOT NULL DEFAULT 0,
  ingest_bytes_per_second bigint NOT NULL DEFAULT 0,
  labels jsonb NOT NULL DEFAULT '{}'::jsonb,
  last_seen_at timestamptz NOT NULL,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS agents_tenant_last_seen_idx
  ON agents (tenant_id, last_seen_at DESC);

CREATE TABLE IF NOT EXISTS pipeline_versions (
  tenant_id text NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  pipeline_name text NOT NULL,
  version bigint NOT NULL,
  config jsonb NOT NULL,
  agent_yaml text NOT NULL,
  checksum text NOT NULL,
  updated_by text NOT NULL,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, pipeline_name, version)
);

CREATE INDEX IF NOT EXISTS pipeline_versions_latest_idx
  ON pipeline_versions (tenant_id, pipeline_name, version DESC);

CREATE TABLE IF NOT EXISTS agent_commands (
  command_id text PRIMARY KEY,
  agent_id text NOT NULL REFERENCES agents(agent_id) ON DELETE CASCADE,
  tenant_id text NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  kind text NOT NULL,
  reason text NOT NULL DEFAULT '',
  status text NOT NULL DEFAULT 'pending',
  inserted_at timestamptz NOT NULL,
  delivered_at timestamptz,
  acknowledged_at timestamptz,
  last_error text
);

CREATE INDEX IF NOT EXISTS agent_commands_pending_idx
  ON agent_commands (agent_id, inserted_at)
  WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS agent_commands_delivered_lease_idx
  ON agent_commands (agent_id, delivered_at)
  WHERE status = 'delivered';

ALTER TABLE agent_commands
  ADD COLUMN IF NOT EXISTS acknowledged_at timestamptz;

ALTER TABLE agent_commands
  ADD COLUMN IF NOT EXISTS last_error text;

CREATE TABLE IF NOT EXISTS audit_events (
  id bigserial PRIMARY KEY,
  event_id bigint NOT NULL,
  actor text NOT NULL,
  action text NOT NULL,
  resource text NOT NULL,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL
);

ALTER TABLE audit_events
  ADD COLUMN IF NOT EXISTS event_id bigint;

CREATE UNIQUE INDEX IF NOT EXISTS audit_events_event_id_idx
  ON audit_events (event_id);

CREATE INDEX IF NOT EXISTS audit_events_resource_idx
  ON audit_events (resource, inserted_at DESC);
