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

CREATE TABLE IF NOT EXISTS module_registry (
  module_name text PRIMARY KEY,
  display_name text NOT NULL,
  owner text NOT NULL,
  description text NOT NULL DEFAULT '',
  enabled boolean NOT NULL DEFAULT true,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS module_config_versions (
  tenant_id text NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  module_name text NOT NULL REFERENCES module_registry(module_name) ON DELETE CASCADE,
  version bigint NOT NULL,
  config jsonb NOT NULL,
  checksum text NOT NULL,
  updated_by text NOT NULL,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, module_name, version)
);

CREATE INDEX IF NOT EXISTS module_config_versions_latest_idx
  ON module_config_versions (tenant_id, module_name, version DESC);

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

CREATE TABLE IF NOT EXISTS blockchain_chains (
  tenant_id text NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  chain_key text NOT NULL,
  display_name text NOT NULL,
  network text NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, chain_key)
);

CREATE TABLE IF NOT EXISTS blockchain_rpc_endpoints (
  tenant_id text NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  endpoint_id text NOT NULL,
  chain_key text NOT NULL,
  url text NOT NULL,
  priority integer NOT NULL DEFAULT 100,
  enabled boolean NOT NULL DEFAULT true,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, endpoint_id)
);

CREATE INDEX IF NOT EXISTS blockchain_rpc_endpoints_chain_idx
  ON blockchain_rpc_endpoints (tenant_id, chain_key, priority);

CREATE TABLE IF NOT EXISTS blockchain_address_watchlist (
  tenant_id text NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  entry_id text NOT NULL,
  chain_key text NOT NULL,
  address text NOT NULL,
  label text,
  enabled boolean NOT NULL DEFAULT true,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, entry_id)
);

CREATE INDEX IF NOT EXISTS blockchain_address_watchlist_chain_idx
  ON blockchain_address_watchlist (tenant_id, chain_key);

CREATE TABLE IF NOT EXISTS blockchain_contract_watchlist (
  tenant_id text NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  contract_id text NOT NULL,
  chain_key text NOT NULL,
  address text NOT NULL,
  label text,
  abi_ref text,
  enabled boolean NOT NULL DEFAULT true,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, contract_id)
);

CREATE INDEX IF NOT EXISTS blockchain_contract_watchlist_chain_idx
  ON blockchain_contract_watchlist (tenant_id, chain_key);

CREATE TABLE IF NOT EXISTS blockchain_token_watchlist (
  tenant_id text NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  token_id text NOT NULL,
  chain_key text NOT NULL,
  contract_address text NOT NULL,
  symbol text,
  decimals integer,
  enabled boolean NOT NULL DEFAULT true,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, token_id)
);

CREATE INDEX IF NOT EXISTS blockchain_token_watchlist_chain_idx
  ON blockchain_token_watchlist (tenant_id, chain_key);

CREATE TABLE IF NOT EXISTS blockchain_filter_rules (
  tenant_id text NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  rule_id text NOT NULL,
  chain_key text,
  name text NOT NULL,
  expression jsonb NOT NULL,
  action text NOT NULL DEFAULT 'keep',
  enabled boolean NOT NULL DEFAULT true,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, rule_id)
);

CREATE INDEX IF NOT EXISTS blockchain_filter_rules_chain_idx
  ON blockchain_filter_rules (tenant_id, chain_key);

CREATE TABLE IF NOT EXISTS blockchain_crawl_assignments (
  tenant_id text NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  assignment_id text NOT NULL,
  chain_key text NOT NULL,
  crawler_id text NOT NULL,
  start_cursor jsonb NOT NULL DEFAULT '{}'::jsonb,
  end_cursor jsonb,
  config jsonb NOT NULL DEFAULT '{}'::jsonb,
  enabled boolean NOT NULL DEFAULT true,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, assignment_id)
);

CREATE INDEX IF NOT EXISTS blockchain_crawl_assignments_crawler_idx
  ON blockchain_crawl_assignments (tenant_id, crawler_id, enabled);

CREATE TABLE IF NOT EXISTS blockchain_checkpoints (
  tenant_id text NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
  assignment_id text NOT NULL,
  chain_key text NOT NULL,
  cursor jsonb NOT NULL DEFAULT '{}'::jsonb,
  finalized_cursor jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_by text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, assignment_id)
);
