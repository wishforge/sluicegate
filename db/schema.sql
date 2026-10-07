CREATE TABLE IF NOT EXISTS migrations (
  id TEXT PRIMARY KEY,
  workload_id TEXT NOT NULL,
  source_agent TEXT NOT NULL,
  target_agent TEXT NOT NULL,
  state TEXT NOT NULL,
  chunk_size BIGINT NOT NULL,
  fence_token TEXT,
  fence_epoch BIGINT NOT NULL DEFAULT 0,
  checkpoint_id TEXT,
  manifest JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  error TEXT,
  last_error_code TEXT,
  stats JSONB NOT NULL DEFAULT '{}'::jsonb,
  version BIGINT NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_migrations_state_updated ON migrations(state, updated_at);
CREATE INDEX IF NOT EXISTS idx_migrations_workload ON migrations(workload_id, updated_at DESC);
CREATE TABLE IF NOT EXISTS workloads (
  workload_id TEXT PRIMARY KEY,
  current_node TEXT NOT NULL,
  fence_epoch BIGINT NOT NULL DEFAULT 0,
  active_migration_id TEXT,
  updated_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS idempotency_keys (
  key TEXT PRIMARY KEY,
  migration_id TEXT NOT NULL,
  request_hash TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS controller_leases (
  resource_id TEXT PRIMARY KEY,
  holder_id TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS audit_events (
  id BIGSERIAL PRIMARY KEY,
  migration_id TEXT NOT NULL,
  workload_id TEXT NOT NULL,
  state TEXT NOT NULL,
  fence_epoch BIGINT NOT NULL DEFAULT 0,
  event_time TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_audit_migration_time ON audit_events(migration_id, event_time DESC);
