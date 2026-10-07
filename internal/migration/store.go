package migration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Store is the authoritative migration control-plane persistence contract.
// Production uses PostgreSQL/pgxpool; FileStore remains for
// deterministic unit tests.
type Store interface {
	Put(Migration) error
	Get(string) (Migration, bool)
	FindIdempotency(string) (string, bool)
	PutIdempotency(string, string) error
	CreateMigration(Migration, string) (Migration, bool, error)
	ReserveFenceEpoch(workloadID, migrationID string) (uint64, error)
	ListReconcilable(ctx context.Context, limit int) ([]Migration, error)
	TryAcquireMigrationLease(ctx context.Context, migrationID, holder string, ttl time.Duration) (bool, error)
	RenewMigrationLease(ctx context.Context, migrationID, holder string, ttl time.Duration) error
	ReleaseMigrationLease(ctx context.Context, migrationID, holder string) error
	TryAcquireControllerLease(ctx context.Context, holder string, ttl time.Duration) (bool, error)
	RenewControllerLease(ctx context.Context, holder string, ttl time.Duration) error
	ReleaseControllerLease(ctx context.Context, holder string) error
	Close() error
}

type StoreData struct {
	Migrations  map[string]Migration `json:"migrations"`
	Idempotency map[string]string    `json:"idempotency,omitempty"`
	Workloads   map[string]uint64    `json:"workloads,omitempty"`
}

// FileStore is retained only as a deterministic local/unit-test backend.
type FileStore struct {
	mu   sync.RWMutex
	path string
	data StoreData
}

func OpenFileStore(path string) (*FileStore, error) {
	s := &FileStore{path: path, data: StoreData{Migrations: map[string]Migration{}, Idempotency: map[string]string{}, Workloads: map[string]uint64{}}}
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return s, nil
		}
		return nil, err
	}
	if len(b) == 0 {
		return s, nil
	}
	if err := json.Unmarshal(b, &s.data); err != nil {
		return nil, err
	}
	if s.data.Migrations == nil {
		s.data.Migrations = map[string]Migration{}
	}
	if s.data.Idempotency == nil {
		s.data.Idempotency = map[string]string{}
	}
	if s.data.Workloads == nil {
		s.data.Workloads = map[string]uint64{}
	}
	return s, nil
}

func (s *FileStore) persistLocked() error {
	b, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(s.path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *FileStore) Put(m Migration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Migrations[m.ID] = m
	return s.persistLocked()
}
func (s *FileStore) Get(id string) (Migration, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.data.Migrations[id]
	return m, ok
}
func (s *FileStore) FindIdempotency(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data.Idempotency[key]
	return v, ok
}
func (s *FileStore) PutIdempotency(key, id string) error {
	if key == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Idempotency[key] = id
	return s.persistLocked()
}

func (s *FileStore) CreateMigration(m Migration, idem string) (Migration, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if idem != "" {
		if id, ok := s.data.Idempotency[idem]; ok {
			existing, exists := s.data.Migrations[id]
			if !exists {
				return Migration{}, false, errors.New("idempotency record points to missing migration")
			}
			return existing, true, nil
		}
	}
	s.data.Migrations[m.ID] = m
	if idem != "" {
		s.data.Idempotency[idem] = m.ID
	}
	if _, ok := s.data.Workloads[m.WorkloadID]; !ok {
		s.data.Workloads[m.WorkloadID] = 0
	}
	if err := s.persistLocked(); err != nil {
		return Migration{}, false, err
	}
	return m, false, nil
}

func (s *FileStore) ReserveFenceEpoch(workloadID, _ string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data.Workloads[workloadID]++
	if err := s.persistLocked(); err != nil {
		return 0, err
	}
	return s.data.Workloads[workloadID], nil
}

func (s *FileStore) ListReconcilable(_ context.Context, limit int) ([]Migration, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Migration, 0, len(s.data.Migrations))
	for _, m := range s.data.Migrations {
		switch m.State {
		case StateCommitted, StateRolledBack, StateFailed:
			continue
		default:
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.Before(out[j].UpdatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (s *FileStore) TryAcquireMigrationLease(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (s *FileStore) RenewMigrationLease(context.Context, string, string, time.Duration) error {
	return nil
}
func (s *FileStore) ReleaseMigrationLease(context.Context, string, string) error { return nil }
func (s *FileStore) TryAcquireControllerLease(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}
func (s *FileStore) RenewControllerLease(context.Context, string, time.Duration) error { return nil }
func (s *FileStore) ReleaseControllerLease(context.Context, string) error              { return nil }
func (s *FileStore) Close() error                                                      { return nil }
