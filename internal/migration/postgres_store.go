package migration

// PostgreSQL control-plane store.
//
// Design notes:
//   * no per-operation `psql` subprocesses
//   * one bounded pgxpool per controller process
//   * every DB operation is subject to a small timeout so pool pressure
//     becomes backpressure instead of an unbounded process explosion
//
// Data-plane files/chunks remain outside PostgreSQL. PostgreSQL is only the
// authoritative control-plane state.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	pool   *pgxpool.Pool
	opWait time.Duration
}

func OpenPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, errors.New("DATABASE_URL is required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}

	cfg.MaxConns = int32(envInt("DB_MAX_CONNS", 32))
	cfg.MinConns = int32(envInt("DB_MIN_CONNS", 4))
	cfg.MaxConnLifetime = envDuration("DB_MAX_CONN_LIFETIME", 30*time.Minute)
	cfg.MaxConnIdleTime = envDuration("DB_MAX_CONN_IDLE_TIME", 5*time.Minute)
	cfg.HealthCheckPeriod = envDuration("DB_HEALTH_CHECK_PERIOD", 30*time.Second)
	if cfg.MaxConns < 1 {
		cfg.MaxConns = 1
	}
	if cfg.MinConns < 0 {
		cfg.MinConns = 0
	}
	if cfg.MinConns > cfg.MaxConns {
		cfg.MinConns = cfg.MaxConns
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}
	s := &PostgresStore{pool: pool, opWait: envDuration("DB_OP_TIMEOUT", 8*time.Second)}
	pingCtx, cancel := context.WithTimeout(ctx, s.opWait)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	if err := s.exec(pingCtx, postgresSchema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres schema init: %w", err)
	}
	return s, nil
}

const postgresSchema = `
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

-- One table, two logical lease namespaces:
--   resource_id='controller' => singleton controller lease
--   resource_id=<migration_id> => per-migration action lease
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
`

func envInt(name string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}

func (s *PostgresStore) opContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, s.opWait)
}

func (s *PostgresStore) run(ctx context.Context, sql string, args ...any) ([]map[string]string, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	out := make([]map[string]string, 0, 8)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make(map[string]string, len(fields))
		for i, f := range fields {
			if i >= len(values) || values[i] == nil {
				row[string(f.Name)] = ""
				continue
			}
			row[string(f.Name)] = fmt.Sprint(values[i])
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *PostgresStore) exec(ctx context.Context, sql string, args ...any) error {
	_, err := s.pool.Exec(ctx, sql, args...)
	return err
}

func encodeJSON(v any) ([]byte, error) { return json.Marshal(v) }

func (s *PostgresStore) CreateMigration(m Migration, idem string) (Migration, bool, error) {
	ctx, cancel := s.opContext(context.Background())
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Migration{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if idem != "" {
		tag, err := tx.Exec(ctx, `INSERT INTO idempotency_keys(key,migration_id) VALUES($1,$2) ON CONFLICT DO NOTHING`, idem, m.ID)
		if err != nil {
			return Migration{}, false, err
		}
		if tag.RowsAffected() == 0 {
			var existingID string
			if err := tx.QueryRow(ctx, `SELECT migration_id FROM idempotency_keys WHERE key=$1`, idem).Scan(&existingID); err != nil {
				return Migration{}, false, err
			}
			if existingID == m.ID {
				// Same request replay after a retry from the exact caller.
				var existing Migration
				if err := scanMigration(tx.QueryRow(ctx, migrationSelect+` WHERE id=$1`, existingID), &existing); err != nil {
					return Migration{}, false, err
				}
				if err := tx.Commit(ctx); err != nil {
					return Migration{}, false, err
				}
				return existing, true, nil
			}
			var existing Migration
			if err := scanMigration(tx.QueryRow(ctx, migrationSelect+` WHERE id=$1`, existingID), &existing); err != nil {
				return Migration{}, false, err
			}
			if err := tx.Commit(ctx); err != nil {
				return Migration{}, false, err
			}
			return existing, true, nil
		}
	}

	manifest, err := encodeJSON(m.Manifest)
	if err != nil {
		return Migration{}, false, err
	}
	stats, err := encodeJSON(m.Stats)
	if err != nil {
		return Migration{}, false, err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO migrations(id,workload_id,source_agent,target_agent,state,chunk_size,fence_token,fence_epoch,checkpoint_id,manifest,created_at,updated_at,error,last_error_code,stats)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,$15::jsonb)`,
		m.ID, m.WorkloadID, m.SourceAgent, m.TargetAgent, string(m.State), m.ChunkSize, nullableString(m.FenceToken), m.FenceEpoch,
		nullableString(m.CheckpointID), string(manifest), m.CreatedAt, m.UpdatedAt, nullableString(m.Error), nullableString(m.LastErrorCode), string(stats)); err != nil {
		return Migration{}, false, err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO workloads(workload_id,current_node,fence_epoch,active_migration_id,updated_at)
VALUES($1,$2,$3,$4,now())
ON CONFLICT(workload_id) DO UPDATE SET current_node=EXCLUDED.current_node, updated_at=EXCLUDED.updated_at`,
		m.WorkloadID, m.SourceAgent, m.FenceEpoch, m.ID); err != nil {
		return Migration{}, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(migration_id,workload_id,state,fence_epoch) VALUES($1,$2,$3,$4)`, m.ID, m.WorkloadID, string(m.State), m.FenceEpoch); err != nil {
		return Migration{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Migration{}, false, err
	}
	return m, false, nil
}

func (s *PostgresStore) Put(m Migration) error {
	ctx, cancel := s.opContext(context.Background())
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	manifest, err := encodeJSON(m.Manifest)
	if err != nil {
		return err
	}
	stats, err := encodeJSON(m.Stats)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO migrations(id,workload_id,source_agent,target_agent,state,chunk_size,fence_token,fence_epoch,checkpoint_id,manifest,created_at,updated_at,error,last_error_code,stats,version)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,$15::jsonb,1)
ON CONFLICT(id) DO UPDATE SET
 workload_id=EXCLUDED.workload_id,
 source_agent=EXCLUDED.source_agent,
 target_agent=EXCLUDED.target_agent,
 state=EXCLUDED.state,
 chunk_size=EXCLUDED.chunk_size,
 fence_token=EXCLUDED.fence_token,
 fence_epoch=EXCLUDED.fence_epoch,
 checkpoint_id=EXCLUDED.checkpoint_id,
 manifest=EXCLUDED.manifest,
 updated_at=EXCLUDED.updated_at,
 error=EXCLUDED.error,
 last_error_code=EXCLUDED.last_error_code,
 stats=EXCLUDED.stats,
 version=migrations.version+1`,
		m.ID, m.WorkloadID, m.SourceAgent, m.TargetAgent, string(m.State), m.ChunkSize, nullableString(m.FenceToken), m.FenceEpoch,
		nullableString(m.CheckpointID), string(manifest), m.CreatedAt, m.UpdatedAt, nullableString(m.Error), nullableString(m.LastErrorCode), string(stats)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO workloads(workload_id,current_node,fence_epoch,active_migration_id,updated_at)
VALUES($1,$2,$3,$4,$5)
ON CONFLICT(workload_id) DO UPDATE SET
 current_node=CASE
   WHEN EXCLUDED.active_migration_id IS NULL THEN workloads.current_node
   WHEN EXCLUDED.active_migration_id=$6 AND $7='COMMITTED' THEN EXCLUDED.current_node
   ELSE workloads.current_node END,
 fence_epoch=GREATEST(workloads.fence_epoch, EXCLUDED.fence_epoch),
 active_migration_id=CASE WHEN $7 IN ('COMMITTED','ROLLED_BACK') THEN NULL ELSE EXCLUDED.active_migration_id END,
 updated_at=EXCLUDED.updated_at`,
		m.WorkloadID, m.SourceAgent, m.FenceEpoch, m.ID, m.UpdatedAt, m.ID, string(m.State)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(migration_id,workload_id,state,fence_epoch) VALUES($1,$2,$3,$4)`, m.ID, m.WorkloadID, string(m.State), m.FenceEpoch); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

const migrationSelect = `SELECT id,workload_id,source_agent,target_agent,state,chunk_size,COALESCE(fence_token,''),fence_epoch,COALESCE(checkpoint_id,''),manifest,created_at,updated_at,COALESCE(error,''),COALESCE(last_error_code,''),stats,version FROM migrations`

type rowScanner interface {
	Scan(...any) error
}

func scanMigration(row rowScanner, m *Migration) error {
	var state string
	var manifest, stats []byte
	if err := row.Scan(
		&m.ID, &m.WorkloadID, &m.SourceAgent, &m.TargetAgent, &state, &m.ChunkSize, &m.FenceToken, &m.FenceEpoch,
		&m.CheckpointID, &manifest, &m.CreatedAt, &m.UpdatedAt, &m.Error, &m.LastErrorCode, &stats, &m.Version,
	); err != nil {
		return err
	}
	m.State = State(state)
	if len(manifest) > 0 {
		if err := json.Unmarshal(manifest, &m.Manifest); err != nil {
			return err
		}
	}
	if len(stats) > 0 {
		if err := json.Unmarshal(stats, &m.Stats); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) Get(id string) (Migration, bool) {
	ctx, cancel := s.opContext(context.Background())
	defer cancel()
	var m Migration
	err := scanMigration(s.pool.QueryRow(ctx, migrationSelect+` WHERE id=$1`, id), &m)
	if errors.Is(err, pgx.ErrNoRows) || err != nil {
		return Migration{}, false
	}
	return m, true
}

func (s *PostgresStore) FindIdempotency(key string) (string, bool) {
	ctx, cancel := s.opContext(context.Background())
	defer cancel()
	var id string
	err := s.pool.QueryRow(ctx, `SELECT migration_id FROM idempotency_keys WHERE key=$1`, key).Scan(&id)
	return id, err == nil && id != ""
}

func (s *PostgresStore) PutIdempotency(key, id string) error {
	if key == "" {
		return nil
	}
	ctx, cancel := s.opContext(context.Background())
	defer cancel()
	return s.exec(ctx, `INSERT INTO idempotency_keys(key,migration_id) VALUES($1,$2) ON CONFLICT(key) DO NOTHING`, key, id)
}

func (s *PostgresStore) ReserveFenceEpoch(workloadID, migrationID string) (uint64, error) {
	ctx, cancel := s.opContext(context.Background())
	defer cancel()
	var epoch int64
	err := s.pool.QueryRow(ctx, `
INSERT INTO workloads(workload_id,current_node,fence_epoch,active_migration_id,updated_at)
VALUES($1,'',$3,$2,now())
ON CONFLICT(workload_id) DO UPDATE SET
 fence_epoch=workloads.fence_epoch+1,
 active_migration_id=EXCLUDED.active_migration_id,
 updated_at=now()
RETURNING fence_epoch`, workloadID, migrationID, int64(1)).Scan(&epoch)
	if err != nil {
		return 0, err
	}
	if epoch < 0 {
		return 0, fmt.Errorf("invalid fence epoch %d", epoch)
	}
	return uint64(epoch), nil
}

func (s *PostgresStore) ListReconcilable(ctx context.Context, limit int) ([]Migration, error) {
	if limit <= 0 {
		limit = 100
	}
	opCtx, cancel := context.WithTimeout(ctx, s.opWait)
	defer cancel()
	rows, err := s.pool.Query(opCtx, migrationSelect+fmt.Sprintf(` WHERE state NOT IN ('COMMITTED','ROLLED_BACK','FAILED') ORDER BY updated_at LIMIT %d`, limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Migration, 0, limit)
	for rows.Next() {
		var m Migration
		if err := scanMigration(rows, &m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *PostgresStore) TryAcquireMigrationLease(ctx context.Context, migrationID, holder string, ttl time.Duration) (bool, error) {
	opCtx, cancel := context.WithTimeout(ctx, s.opWait)
	defer cancel()
	var got string
	err := s.pool.QueryRow(opCtx, `
INSERT INTO controller_leases(resource_id,holder_id,expires_at,updated_at)
VALUES($1,$2,now()+$3::interval,now())
ON CONFLICT(resource_id) DO UPDATE SET
 holder_id=EXCLUDED.holder_id,
 expires_at=EXCLUDED.expires_at,
 updated_at=now()
WHERE controller_leases.expires_at < now() OR controller_leases.holder_id=EXCLUDED.holder_id
RETURNING holder_id`, migrationID, holder, ttl.String()).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return got == holder, nil
}

func (s *PostgresStore) RenewMigrationLease(ctx context.Context, migrationID, holder string, ttl time.Duration) error {
	opCtx, cancel := context.WithTimeout(ctx, s.opWait)
	defer cancel()
	var got string
	err := s.pool.QueryRow(opCtx, `UPDATE controller_leases SET expires_at=now()+$3::interval, updated_at=now() WHERE resource_id=$1 AND holder_id=$2 RETURNING holder_id`, migrationID, holder, ttl.String()).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("migration lease lost: migration_id=%s holder_id=%s", migrationID, holder)
	}
	return err
}

func (s *PostgresStore) ReleaseMigrationLease(ctx context.Context, migrationID, holder string) error {
	opCtx, cancel := context.WithTimeout(ctx, s.opWait)
	defer cancel()
	return s.exec(opCtx, `DELETE FROM controller_leases WHERE resource_id=$1 AND holder_id=$2`, migrationID, holder)
}

const controllerLeaseResource = "controller"

func (s *PostgresStore) TryAcquireControllerLease(ctx context.Context, holder string, ttl time.Duration) (bool, error) {
	opCtx, cancel := context.WithTimeout(ctx, s.opWait)
	defer cancel()
	var got string
	err := s.pool.QueryRow(opCtx, `
INSERT INTO controller_leases(resource_id,holder_id,expires_at,updated_at)
VALUES($1,$2,now()+$3::interval,now())
ON CONFLICT(resource_id) DO UPDATE SET
 holder_id=EXCLUDED.holder_id,
 expires_at=EXCLUDED.expires_at,
 updated_at=now()
WHERE controller_leases.expires_at < now() OR controller_leases.holder_id=EXCLUDED.holder_id
RETURNING holder_id`, controllerLeaseResource, holder, ttl.String()).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return got == holder, nil
}

func (s *PostgresStore) RenewControllerLease(ctx context.Context, holder string, ttl time.Duration) error {
	opCtx, cancel := context.WithTimeout(ctx, s.opWait)
	defer cancel()
	var got string
	err := s.pool.QueryRow(opCtx, `UPDATE controller_leases SET expires_at=now()+$2::interval, updated_at=now() WHERE resource_id=$1 AND holder_id=$3 RETURNING holder_id`, controllerLeaseResource, ttl.String(), holder).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("controller lease lost: holder_id=%s", holder)
	}
	return err
}

func (s *PostgresStore) ReleaseControllerLease(ctx context.Context, holder string) error {
	opCtx, cancel := context.WithTimeout(ctx, s.opWait)
	defer cancel()
	return s.exec(opCtx, `DELETE FROM controller_leases WHERE resource_id=$1 AND holder_id=$2`, controllerLeaseResource, holder)
}

// DBPoolStats exposes bounded control-plane capacity to readiness/logging tools.
type DBPoolStats struct {
	TotalConns    int32 `json:"total_conns"`
	IdleConns     int32 `json:"idle_conns"`
	AcquiredConns int32 `json:"acquired_conns"`
	MaxConns      int32 `json:"max_conns"`
}

func (s *PostgresStore) PoolStats() DBPoolStats {
	st := s.pool.Stat()
	return DBPoolStats{TotalConns: st.TotalConns(), IdleConns: st.IdleConns(), AcquiredConns: st.AcquiredConns(), MaxConns: st.MaxConns()}
}

func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func (s *PostgresStore) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}
