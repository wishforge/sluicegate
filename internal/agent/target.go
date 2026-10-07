package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/wishforge/sluicegate/internal/common"
	"github.com/wishforge/sluicegate/internal/migration"
)

type TargetState struct {
	MigrationID string                `json:"migration_id"`
	WorkloadID  string                `json:"workload_id"`
	Files       map[string]TargetFile `json:"files"`
	Activated   bool                  `json:"activated"`
	Committed   bool                  `json:"committed"`
}

type TargetFile struct {
	Manifest migration.File `json:"manifest"`
	Chunks   map[int]string `json:"chunks"`
}

type TargetServer struct {
	Root            string
	Logger          *common.SLogger
	Metrics         *common.Metrics
	locks           stripedTargetLock
	stateMu         stateTableLock
	states          map[string]TargetState
	chaosCrashAfter int64
	chaos           chaosCounters
}

func NewTargetServer(root string, logger *common.SLogger, metrics *common.Metrics) (*TargetServer, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	s := &TargetServer{Root: root, Logger: logger, Metrics: metrics, states: map[string]TargetState{}}
	if v := os.Getenv("CRASH_AFTER_CHUNKS"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			s.chaosCrashAfter = n
		}
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *TargetServer) statePath(id string) string {
	return filepath.Join(s.Root, ".migration-state", id+".json")
}
func (s *TargetServer) stagingPath(id, workload string) string {
	return filepath.Join(s.Root, ".migration-staging", id, workload)
}
func (s *TargetServer) workloadPath(workload string) string {
	return filepath.Join(s.Root, "workloads", workload)
}

func (s *TargetServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		common.JSON(w, 200, map[string]string{"status": "ok", "role": "target-agent"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		common.JSON(w, 200, map[string]string{"status": "ready"})
	})
	mux.Handle("/metrics", http.HandlerFunc(s.Metrics.Handler))
	mux.HandleFunc("/v1/migrations/", s.migrations)
	return common.WrapRequest(mux, s.Logger)
}

func (s *TargetServer) migrations(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/migrations/"), "/")
	if len(parts) < 2 || parts[0] == "" {
		common.Error(w, 404, "NOT_FOUND", errors.New("migration path not found"))
		return
	}
	id, err := url.PathUnescape(parts[0])
	if err != nil {
		common.Error(w, 400, "INVALID_PATH", err)
		return
	}
	switch parts[1] {
	case "init":
		if r.Method != http.MethodPost {
			common.Error(w, 405, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
			return
		}
		s.handleInit(w, r, id)
	case "progress":
		if r.Method != http.MethodGet {
			common.Error(w, 405, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
			return
		}
		s.handleProgress(w, r, id)
	case "chunk":
		if r.Method != http.MethodPut {
			common.Error(w, 405, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
			return
		}
		s.handleChunk(w, r, id)
	case "chunks":
		if r.Method != http.MethodPut {
			common.Error(w, 405, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
			return
		}
		s.handleChunkBatch(w, r, id)
	case "activate":
		if r.Method != http.MethodPost {
			common.Error(w, 405, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
			return
		}
		s.handleActivate(w, r, id)
	case "commit":
		if r.Method != http.MethodPost {
			common.Error(w, 405, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
			return
		}
		s.handleCommit(w, r, id)
	case "rollback":
		if r.Method != http.MethodPost {
			common.Error(w, 405, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
			return
		}
		s.handleRollback(w, r, id)
	default:
		common.Error(w, 404, "NOT_FOUND", errors.New("route not found"))
	}
}

func (s *TargetServer) handleInit(w http.ResponseWriter, r *http.Request, id string) {
	var req struct {
		WorkloadID string           `json:"workload_id"`
		Files      []migration.File `json:"files"`
	}
	if err := common.DecodeJSON(r, &req); err != nil {
		common.Error(w, 400, "INVALID_REQUEST", err)
		return
	}
	if !safeRelativePath(req.WorkloadID) {
		common.Error(w, 400, "INVALID_WORKLOAD", errors.New("invalid workload id"))
		return
	}
	mu := s.locks.lockFor(id)
	mu.Lock()
	defer mu.Unlock()
	s.stateMu.RLock()
	st, ok := s.states[id]
	s.stateMu.RUnlock()
	if ok {
		if st.WorkloadID != req.WorkloadID {
			common.Error(w, 409, "MIGRATION_CONFLICT", errors.New("migration workload mismatch"))
			return
		}
		common.JSON(w, 200, st)
		return
	}
	st = TargetState{MigrationID: id, WorkloadID: req.WorkloadID, Files: map[string]TargetFile{}}
	for _, f := range req.Files {
		if !safeRelativePath(f.Path) {
			common.Error(w, 400, "INVALID_FILE_PATH", fmt.Errorf("invalid path %q", f.Path))
			return
		}
		st.Files[f.Path] = TargetFile{Manifest: f, Chunks: map[int]string{}}
		part := filepath.Join(s.stagingPath(id, req.WorkloadID), filepath.FromSlash(f.Path)+".part")
		if err := os.MkdirAll(filepath.Dir(part), 0o755); err != nil {
			common.Error(w, 500, "STAGING_INIT_FAILED", err)
			return
		}
		fout, err := os.OpenFile(part, os.O_CREATE|os.O_RDWR, os.FileMode(f.Mode))
		if err != nil {
			common.Error(w, 500, "STAGING_INIT_FAILED", err)
			return
		}
		if err := fout.Truncate(f.Size); err != nil {
			_ = fout.Close()
			common.Error(w, 500, "STAGING_INIT_FAILED", err)
			return
		}
		if err := fout.Close(); err != nil {
			common.Error(w, 500, "STAGING_INIT_FAILED", err)
			return
		}
	}
	if err := s.saveLocked(st); err != nil {
		common.Error(w, 500, "STATE_PERSIST_FAILED", err)
		return
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.states[id] = st
	s.Logger.Info("target_migration_initialized", "request_id", common.RequestID(r), "trace_id", common.TraceID(r), "migration_id", id, "workload_id", req.WorkloadID, "files", len(req.Files))
	common.JSON(w, 201, st)
}

func (s *TargetServer) handleProgress(w http.ResponseWriter, r *http.Request, id string) {
	mu := s.locks.lockFor(id)
	mu.Lock()
	defer mu.Unlock()
	s.stateMu.RLock()
	st, ok := s.states[id]
	s.stateMu.RUnlock()
	if !ok {
		common.Error(w, 404, "MIGRATION_NOT_FOUND", errors.New("migration not found"))
		return
	}
	out := migration.ProgressResponse{MigrationID: id, Files: map[string][]int{}}
	for p, f := range st.Files {
		idx := make([]int, 0, len(f.Chunks))
		for i := range f.Chunks {
			idx = append(idx, i)
		}
		sort.Ints(idx)
		out.Files[p] = idx
	}
	common.JSON(w, 200, out)
}

func (s *TargetServer) handleChunkBatch(w http.ResponseWriter, r *http.Request, id string) {
	started := time.Now()
	active := s.Metrics.TargetBatchInflight.Add(1)
	for {
		peak := s.Metrics.TargetBatchPeak.Load()
		if active <= peak || s.Metrics.TargetBatchPeak.CompareAndSwap(peak, active) {
			break
		}
	}
	defer s.Metrics.TargetBatchInflight.Add(-1)

	chunks, err := migration.DecodeBatch(r.Body, migration.MaxBatchBytes)
	if err != nil {
		common.Error(w, 400, "INVALID_BATCH", err)
		return
	}
	s.Metrics.TargetBatchRequests.Add(1)

	lockStart := time.Now()
	mu := s.locks.lockFor(id)
	mu.Lock()
	queueWait := time.Since(lockStart)
	s.Metrics.TargetBatchQueueWaitMs.Add(uint64(queueWait.Milliseconds()))
	defer mu.Unlock()

	s.stateMu.RLock()
	st, ok := s.states[id]
	s.stateMu.RUnlock()
	if !ok {
		common.Error(w, 404, "MIGRATION_NOT_FOUND", errors.New("migration not found"))
		return
	}
	stored := 0
	alreadyPresent := 0
	var storedBytes int64
	files := map[string]*os.File{}
	closeFiles := func(syncFiles bool) error {
		var first error
		for path, f := range files {
			if syncFiles {
				if err := f.Sync(); err != nil && first == nil {
					first = err
				}
			}
			if err := f.Close(); err != nil && first == nil {
				first = err
			}
			delete(files, path)
		}
		return first
	}

	writeStart := time.Now()
	for _, c := range chunks {
		if !safeRelativePath(c.Path) {
			_ = closeFiles(false)
			common.Error(w, 400, "INVALID_FILE_PATH", errors.New("invalid file path"))
			return
		}
		f, ok := st.Files[c.Path]
		if !ok {
			_ = closeFiles(false)
			common.Error(w, 404, "FILE_NOT_FOUND", errors.New("file not found in manifest"))
			return
		}
		if c.Index >= f.Manifest.ChunkCount || c.Offset != int64(c.Index)*f.Manifest.ChunkSize {
			_ = closeFiles(false)
			common.Error(w, 409, "INVALID_CHUNK", errors.New("chunk index/offset does not match manifest"))
			return
		}
		expectedSize := f.Manifest.ChunkSize
		remaining := f.Manifest.Size - c.Offset
		if remaining < expectedSize {
			expectedSize = remaining
		}
		if expectedSize < 0 || int64(len(c.Data)) != expectedSize {
			_ = closeFiles(false)
			common.Error(w, 409, "INVALID_CHUNK", errors.New("chunk size does not match manifest"))
			return
		}
		if prev, exists := f.Chunks[c.Index]; exists {
			if prev == c.SHA256 {
				alreadyPresent++
				continue
			}
			_ = closeFiles(false)
			common.Error(w, 409, "CHUNK_CONFLICT", errors.New("chunk already exists with different checksum"))
			return
		}
		part := filepath.Join(s.stagingPath(id, st.WorkloadID), filepath.FromSlash(c.Path)+".part")
		out := files[c.Path]
		if out == nil {
			out, err = os.OpenFile(part, os.O_WRONLY, 0o644)
			if err != nil {
				_ = closeFiles(false)
				common.Error(w, 500, "CHUNK_WRITE_FAILED", err)
				return
			}
			files[c.Path] = out
		}
		if _, err := out.WriteAt(c.Data, c.Offset); err != nil {
			_ = closeFiles(false)
			common.Error(w, 500, "CHUNK_WRITE_FAILED", err)
			return
		}
		f.Chunks[c.Index] = c.SHA256
		st.Files[c.Path] = f
		stored++
		storedBytes += int64(len(c.Data))
		s.Metrics.Chunks.Add(1)
		s.Metrics.Bytes.Add(uint64(len(c.Data)))
		if persisted := s.chaos.reach(s.chaosCrashAfter); persisted > 0 {
			if syncErr := closeFiles(true); syncErr == nil {
				_ = s.saveLocked(st)
			} else {
				_ = closeFiles(false)
			}
			s.Logger.Error("target_chaos_crash", "migration_id", id, "chunks_persisted", persisted)
			os.Exit(137)
		}
	}
	writeMs := time.Since(writeStart)
	s.Metrics.TargetBatchWriteMs.Add(uint64(writeMs.Milliseconds()))

	fsyncStart := time.Now()
	if err := closeFiles(true); err != nil {
		common.Error(w, 500, "CHUNK_SYNC_FAILED", err)
		return
	}
	fsyncMs := time.Since(fsyncStart)
	s.Metrics.TargetBatchFsyncMs.Add(uint64(fsyncMs.Milliseconds()))

	stateSaveMs := int64(0)
	if stored > 0 {
		stateStart := time.Now()
		if err := s.saveLocked(st); err != nil {
			common.Error(w, 500, "STATE_PERSIST_FAILED", err)
			return
		}
		stateSaveMs = time.Since(stateStart).Milliseconds()
		s.Metrics.TargetBatchStateSaveMs.Add(uint64(stateSaveMs))
	}

	h := w.Header()
	h.Set("X-Target-Batch-Queue-Wait-Ms", strconv.FormatInt(queueWait.Milliseconds(), 10))
	h.Set("X-Target-Batch-Write-Ms", strconv.FormatInt(writeMs.Milliseconds(), 10))
	h.Set("X-Target-Batch-Fsync-Ms", strconv.FormatInt(fsyncMs.Milliseconds(), 10))
	h.Set("X-Target-Batch-State-Save-Ms", strconv.FormatInt(stateSaveMs, 10))
	h.Set("X-Target-Batch-Peak-Inflight", strconv.FormatInt(s.Metrics.TargetBatchPeak.Load(), 10))
	h.Set("X-Target-Batch-Handler-Ms", strconv.FormatInt(time.Since(started).Milliseconds(), 10))
	s.Logger.Debug("target_batch_persisted", "request_id", common.RequestID(r), "trace_id", common.TraceID(r), "migration_id", id, "chunks_stored", stored, "chunks_already_present", alreadyPresent, "bytes", storedBytes, "queue_wait_ms", queueWait.Milliseconds(), "write_ms", writeMs.Milliseconds(), "fsync_ms", fsyncMs.Milliseconds(), "state_save_ms", stateSaveMs, "active", active, "handler_ms", time.Since(started).Milliseconds())
	common.JSON(w, 200, map[string]any{"status": "stored", "chunks": len(chunks), "stored": stored, "already_present": alreadyPresent, "bytes": storedBytes})
}

func (s *TargetServer) handleChunk(w http.ResponseWriter, r *http.Request, id string) {
	path := r.URL.Query().Get("path")
	idx, err := strconv.Atoi(r.URL.Query().Get("index"))
	if err != nil || idx < 0 {
		common.Error(w, 400, "INVALID_CHUNK", errors.New("valid index required"))
		return
	}
	offset, err := strconv.ParseInt(r.Header.Get("X-Chunk-Offset"), 10, 64)
	if err != nil || offset < 0 {
		common.Error(w, 400, "INVALID_CHUNK", errors.New("valid offset required"))
		return
	}
	wantSHA := r.Header.Get("X-Chunk-SHA256")
	chunkSize, err := strconv.ParseInt(r.Header.Get("X-Chunk-Size"), 10, 64)
	if err != nil || chunkSize < 0 {
		common.Error(w, 400, "INVALID_CHUNK", errors.New("valid chunk size required"))
		return
	}
	if !safeRelativePath(path) {
		common.Error(w, 400, "INVALID_FILE_PATH", errors.New("invalid file path"))
		return
	}
	if chunkSize > 8<<20 {
		common.Error(w, 400, "INVALID_CHUNK", errors.New("chunk too large"))
		return
	}
	mu := s.locks.lockFor(id)
	mu.Lock()
	s.stateMu.RLock()
	st, ok := s.states[id]
	s.stateMu.RUnlock()
	if !ok {
		mu.Unlock()
		common.Error(w, 404, "MIGRATION_NOT_FOUND", errors.New("migration not found"))
		return
	}
	f, ok := st.Files[path]
	if !ok {
		mu.Unlock()
		common.Error(w, 404, "FILE_NOT_FOUND", errors.New("file not found in manifest"))
		return
	}
	if idx >= f.Manifest.ChunkCount || offset != int64(idx)*f.Manifest.ChunkSize {
		mu.Unlock()
		common.Error(w, 409, "INVALID_CHUNK", errors.New("chunk index/offset does not match manifest"))
		return
	}
	expectedSize := f.Manifest.ChunkSize
	remaining := f.Manifest.Size - offset
	if remaining < expectedSize {
		expectedSize = remaining
	}
	if expectedSize < 0 || chunkSize != expectedSize {
		mu.Unlock()
		common.Error(w, 409, "INVALID_CHUNK", errors.New("chunk size does not match manifest"))
		return
	}
	mu.Unlock()
	data, err := io.ReadAll(io.LimitReader(r.Body, chunkSize+1))
	if err != nil {
		common.Error(w, 400, "CHUNK_READ_FAILED", err)
		return
	}
	if int64(len(data)) != chunkSize {
		common.Error(w, 400, "CHUNK_SIZE_MISMATCH", errors.New("body size does not match X-Chunk-Size"))
		return
	}
	h := sha256.Sum256(data)
	gotSHA := hex.EncodeToString(h[:])
	if gotSHA != wantSHA {
		common.Error(w, 422, "CHUNK_CHECKSUM_MISMATCH", fmt.Errorf("expected %s got %s", wantSHA, gotSHA))
		return
	}
	s.stateMu.RLock()
	st, ok = s.states[id]
	s.stateMu.RUnlock()
	if !ok {
		common.Error(w, 404, "MIGRATION_NOT_FOUND", errors.New("migration not found"))
		return
	}
	f, ok = st.Files[path]
	if !ok {
		common.Error(w, 404, "FILE_NOT_FOUND", errors.New("file not found in manifest"))
		return
	}
	if prev, ok := f.Chunks[idx]; ok {
		if prev == wantSHA {
			common.JSON(w, 200, map[string]any{"status": "already_present", "index": idx})
			return
		}
		common.Error(w, 409, "CHUNK_CONFLICT", errors.New("chunk already exists with different checksum"))
		return
	}
	part := filepath.Join(s.stagingPath(id, st.WorkloadID), filepath.FromSlash(path)+".part")
	if err := writeAtAndSync(part, offset, data); err != nil {
		common.Error(w, 500, "CHUNK_WRITE_FAILED", err)
		return
	}
	f.Chunks[idx] = wantSHA
	st.Files[path] = f
	if err := s.saveLocked(st); err != nil {
		common.Error(w, 500, "STATE_PERSIST_FAILED", err)
		return
	}
	s.Metrics.Chunks.Add(1)
	s.Metrics.Bytes.Add(uint64(len(data)))
	s.Logger.Debug("chunk_persisted", "request_id", common.RequestID(r), "trace_id", common.TraceID(r), "migration_id", id, "path", path, "chunk_index", idx, "bytes", len(data))
	if persisted := s.chaos.reach(s.chaosCrashAfter); persisted > 0 {
		s.Logger.Error("target_chaos_crash", "migration_id", id, "chunks_persisted", persisted)
		os.Exit(137)
	}
	common.JSON(w, 200, map[string]any{"status": "stored", "index": idx})
}

func (s *TargetServer) handleActivate(w http.ResponseWriter, r *http.Request, id string) {
	mu := s.locks.lockFor(id)
	mu.Lock()
	defer mu.Unlock()
	s.stateMu.RLock()
	st, ok := s.states[id]
	s.stateMu.RUnlock()
	if !ok {
		common.Error(w, 404, "MIGRATION_NOT_FOUND", errors.New("migration not found"))
		return
	}
	if st.Activated {
		common.JSON(w, 200, map[string]any{"status": "already_activated", "workload_id": st.WorkloadID})
		return
	}
	staging := s.stagingPath(id, st.WorkloadID)
	for path, f := range st.Files {
		if len(f.Chunks) != f.Manifest.ChunkCount {
			common.Error(w, 409, "INCOMPLETE_TRANSFER", fmt.Errorf("%s has %d/%d chunks", path, len(f.Chunks), f.Manifest.ChunkCount))
			return
		}
		part := filepath.Join(staging, filepath.FromSlash(path)+".part")
		final := filepath.Join(staging, filepath.FromSlash(path))
		if _, err := os.Stat(final); err != nil {
			got, err := shaFile(part)
			if err != nil {
				common.Error(w, 500, "VERIFY_FAILED", err)
				return
			}
			if got != f.Manifest.SHA256 {
				common.Error(w, 422, "FILE_CHECKSUM_MISMATCH", fmt.Errorf("%s checksum mismatch", path))
				return
			}
			if err := os.Rename(part, final); err != nil {
				common.Error(w, 500, "STAGING_FINALIZE_FAILED", err)
				return
			}
		} else {
			got, err := shaFile(final)
			if err != nil || got != f.Manifest.SHA256 {
				common.Error(w, 422, "FILE_CHECKSUM_MISMATCH", fmt.Errorf("%s final staged file invalid", path))
				return
			}
		}
	}
	marker := filepath.Join(staging, ".migration-activation.json")
	if _, err := os.Stat(marker); os.IsNotExist(err) {
		b, _ := json.Marshal(map[string]string{"migration_id": id, "workload_id": st.WorkloadID})
		if err := os.WriteFile(marker, b, 0o600); err != nil {
			common.Error(w, 500, "ACTIVATE_FAILED", err)
			return
		}
	}
	target := s.workloadPath(st.WorkloadID)
	if info, err := os.Stat(target); err == nil {
		markerTarget := filepath.Join(target, ".migration-activation.json")
		if info.IsDir() {
			b, _ := os.ReadFile(markerTarget)
			if strings.Contains(string(b), id) {
				st.Activated = true
				if err := s.saveLocked(st); err == nil {
					s.stateMu.Lock()
					defer s.stateMu.Unlock()
					s.states[id] = st
				}
				common.JSON(w, 200, map[string]any{"status": "already_activated", "workload_id": st.WorkloadID})
				return
			}
		}
		common.Error(w, 409, "TARGET_ALREADY_EXISTS", errors.New("target workload already exists"))
		return
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		common.Error(w, 500, "ACTIVATE_FAILED", err)
		return
	}
	if err := os.Rename(staging, target); err != nil {
		common.Error(w, 500, "ACTIVATE_FAILED", err)
		return
	}
	st.Activated = true
	if err := s.saveLocked(st); err != nil {
		common.Error(w, 500, "STATE_PERSIST_FAILED", err)
		return
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.states[id] = st
	s.Logger.Info("target_activated", "request_id", common.RequestID(r), "trace_id", common.TraceID(r), "migration_id", id, "workload_id", st.WorkloadID)
	common.JSON(w, 200, map[string]any{"status": "activated", "workload_id": st.WorkloadID})
}

func (s *TargetServer) handleCommit(w http.ResponseWriter, r *http.Request, id string) {
	mu := s.locks.lockFor(id)
	mu.Lock()
	defer mu.Unlock()
	s.stateMu.RLock()
	st, ok := s.states[id]
	s.stateMu.RUnlock()
	if !ok {
		common.Error(w, 404, "MIGRATION_NOT_FOUND", errors.New("migration not found"))
		return
	}
	if !st.Activated {
		common.Error(w, 409, "NOT_ACTIVATED", errors.New("target must be activated before commit"))
		return
	}
	st.Committed = true
	if err := s.saveLocked(st); err != nil {
		common.Error(w, 500, "STATE_PERSIST_FAILED", err)
		return
	}
	s.Logger.Info("target_commit_ack", "request_id", common.RequestID(r), "trace_id", common.TraceID(r), "migration_id", id, "workload_id", st.WorkloadID)
	common.JSON(w, 200, map[string]any{"status": "committed"})
}

func (s *TargetServer) handleRollback(w http.ResponseWriter, r *http.Request, id string) {
	mu := s.locks.lockFor(id)
	mu.Lock()
	defer mu.Unlock()
	s.stateMu.RLock()
	st, ok := s.states[id]
	s.stateMu.RUnlock()
	if !ok {
		common.JSON(w, 200, map[string]any{"status": "already_absent"})
		return
	}
	if st.Committed {
		common.Error(w, 409, "ALREADY_COMMITTED", errors.New("cannot rollback committed target"))
		return
	}
	_ = os.RemoveAll(s.stagingPath(id, st.WorkloadID))
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	delete(s.states, id)
	_ = os.Remove(s.statePath(id))
	common.JSON(w, 200, map[string]any{"status": "rolled_back"})
}

func (s *TargetServer) load() error {
	entries, err := os.ReadDir(filepath.Join(s.Root, ".migration-state"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.Root, ".migration-state", e.Name()))
		if err != nil {
			return err
		}
		var st TargetState
		if err := json.Unmarshal(b, &st); err != nil {
			return err
		}
		s.stateMu.Lock()
		defer s.stateMu.Unlock()
		s.states[st.MigrationID] = st
	}
	return nil
}

func (s *TargetServer) saveLocked(st TargetState) error {
	if err := os.MkdirAll(filepath.Dir(s.statePath(st.MigrationID)), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.statePath(st.MigrationID) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY, 0o644)
	if err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	return os.Rename(tmp, s.statePath(st.MigrationID))
}

func writeAtAndSync(path string, offset int64, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteAt(data, offset); err != nil {
		return err
	}
	return f.Sync()
}
