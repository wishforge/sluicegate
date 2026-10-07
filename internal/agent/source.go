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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wishforge/sluicegate/internal/common"
	"github.com/wishforge/sluicegate/internal/migration"
)

type Fence struct {
	MigrationID string    `json:"migration_id"`
	Token       string    `json:"token"`
	Epoch       uint64    `json:"epoch"`
	CreatedAt   time.Time `json:"created_at"`
}

type SourceServer struct {
	Root           string
	CheckpointRoot string
	Logger         *common.SLogger
	Metrics        *common.Metrics
	mu             sync.RWMutex
	Fences         map[string]Fence
	fenceFile      string
}

func NewSourceServer(root, checkpointRoot string, logger *common.SLogger, metrics *common.Metrics) (*SourceServer, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(checkpointRoot, 0o755); err != nil {
		return nil, err
	}
	s := &SourceServer{Root: root, CheckpointRoot: checkpointRoot, Logger: logger, Metrics: metrics, Fences: map[string]Fence{}, fenceFile: filepath.Join(checkpointRoot, "fences.json")}
	if err := s.loadFences(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *SourceServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		common.JSON(w, 200, map[string]string{"status": "ok", "role": "source-agent"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		common.JSON(w, 200, map[string]string{"status": "ready"})
	})
	mux.Handle("/metrics", http.HandlerFunc(s.Metrics.Handler))
	mux.HandleFunc("/v1/workloads/", s.workloads)
	return common.WrapRequest(mux, s.Logger)
}

func (s *SourceServer) workloads(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/workloads/"), "/")
	if len(parts) < 2 || parts[0] == "" {
		common.Error(w, 404, "NOT_FOUND", errors.New("workload path not found"))
		return
	}
	workloadID, err := url.PathUnescape(parts[0])
	if err != nil {
		common.Error(w, 400, "INVALID_PATH", err)
		return
	}
	switch parts[1] {
	case "fence":
		if r.Method != http.MethodPost {
			common.Error(w, 405, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
			return
		}
		s.handleFence(w, r, workloadID)
	case "release":
		if r.Method != http.MethodPost {
			common.Error(w, 405, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
			return
		}
		s.handleRelease(w, r, workloadID)
	case "checkpoint":
		if r.Method != http.MethodPost {
			common.Error(w, 405, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
			return
		}
		s.handleCheckpoint(w, r, workloadID)
	case "write":
		if r.Method != http.MethodPut {
			common.Error(w, 405, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
			return
		}
		s.handleWrite(w, r, workloadID, r.URL.Query().Get("path"))
	case "checkpoints":
		if len(parts) < 4 {
			common.Error(w, 404, "NOT_FOUND", errors.New("checkpoint route not found"))
			return
		}
		checkpointID, err := url.PathUnescape(parts[2])
		if err != nil {
			common.Error(w, 400, "INVALID_PATH", err)
			return
		}
		path := r.URL.Query().Get("path")
		if parts[3] == "chunk" {
			if r.Method != http.MethodGet {
				common.Error(w, 405, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
				return
			}
			s.handleChunk(w, r, workloadID, checkpointID, path)
			return
		}
		if parts[3] == "chunks" {
			if r.Method != http.MethodGet {
				common.Error(w, 405, "METHOD_NOT_ALLOWED", errors.New("method not allowed"))
				return
			}
			s.handleChunkBatch(w, r, workloadID, checkpointID, path)
			return
		}
		common.Error(w, 404, "NOT_FOUND", errors.New("checkpoint route not found"))
	default:
		common.Error(w, 404, "NOT_FOUND", errors.New("route not found"))
	}
}

func (s *SourceServer) handleFence(w http.ResponseWriter, r *http.Request, workloadID string) {
	migrationID := r.Header.Get("X-Migration-ID")
	if migrationID == "" {
		common.Error(w, 400, "MISSING_MIGRATION_ID", errors.New("X-Migration-ID required"))
		return
	}
	root, err := s.workloadRoot(workloadID)
	if err != nil {
		common.Error(w, 400, "INVALID_WORKLOAD", err)
		return
	}
	if st, err := os.Stat(root); err != nil || !st.IsDir() {
		common.Error(w, 404, "WORKLOAD_NOT_FOUND", errors.New("workload not found"))
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if f, ok := s.Fences[workloadID]; ok {
		if f.MigrationID == migrationID {
			common.JSON(w, 200, migration.AgentFenceResponse{MigrationID: migrationID, Token: f.Token, Epoch: f.Epoch})
			return
		}
		common.Error(w, 409, "FENCED_BY_OTHER_MIGRATION", errors.New("workload is already fenced by another migration"))
		return
	}
	epoch := uint64(time.Now().UnixNano())
	if raw := r.Header.Get("X-Fence-Epoch"); raw != "" {
		if parsed, err := strconv.ParseUint(raw, 10, 64); err == nil && parsed > 0 {
			epoch = parsed
		}
	}
	f := Fence{MigrationID: migrationID, Token: common.Token(), Epoch: epoch, CreatedAt: time.Now().UTC()}
	s.Fences[workloadID] = f
	if err := s.persistFencesLocked(); err != nil {
		delete(s.Fences, workloadID)
		common.Error(w, 500, "FENCE_PERSIST_FAILED", err)
		return
	}
	s.Logger.Info("workload_fenced", "request_id", common.RequestID(r), "trace_id", common.TraceID(r), "migration_id", migrationID, "workload_id", workloadID, "fence_epoch", f.Epoch)
	common.JSON(w, 200, migration.AgentFenceResponse{MigrationID: migrationID, Token: f.Token, Epoch: f.Epoch})
}

func (s *SourceServer) handleCheckpoint(w http.ResponseWriter, r *http.Request, workloadID string) {
	migrationID := r.Header.Get("X-Migration-ID")
	token := r.Header.Get("X-Fence-Token")
	if migrationID == "" || token == "" {
		common.Error(w, 400, "MISSING_FENCE", errors.New("X-Migration-ID and X-Fence-Token required"))
		return
	}
	if !s.validFence(workloadID, migrationID, token) {
		common.Error(w, 423, "FENCE_REQUIRED", errors.New("valid migration fence required"))
		return
	}
	root, err := s.workloadRoot(workloadID)
	if err != nil {
		common.Error(w, 400, "INVALID_WORKLOAD", err)
		return
	}
	checkpointID := migrationID
	checkpointRoot := filepath.Join(s.CheckpointRoot, checkpointID, workloadID)
	chunkSize, _ := strconv.ParseInt(r.URL.Query().Get("chunk_size"), 10, 64)
	if chunkSize <= 0 {
		chunkSize = 1 << 20
	}
	if _, err := os.Stat(checkpointRoot); err == nil {
		manifest, err := buildManifest(checkpointRoot, chunkSize)
		if err != nil {
			common.Error(w, 500, "CHECKPOINT_READ_FAILED", err)
			return
		}
		common.JSON(w, 200, migration.CheckpointResponse{MigrationID: migrationID, CheckpointID: checkpointID, Files: manifest})
		return
	}
	if err := os.MkdirAll(checkpointRoot, 0o755); err != nil {
		common.Error(w, 500, "CHECKPOINT_INIT_FAILED", err)
		return
	}
	if err := snapshotTree(root, checkpointRoot); err != nil {
		common.Error(w, 500, "CHECKPOINT_CREATE_FAILED", err)
		return
	}
	manifest, err := buildManifest(checkpointRoot, chunkSize)
	if err != nil {
		common.Error(w, 500, "CHECKPOINT_MANIFEST_FAILED", err)
		return
	}
	s.Logger.Info("checkpoint_created", "request_id", common.RequestID(r), "trace_id", common.TraceID(r), "migration_id", migrationID, "workload_id", workloadID, "checkpoint_id", checkpointID, "files", len(manifest))
	common.JSON(w, 201, migration.CheckpointResponse{MigrationID: migrationID, CheckpointID: checkpointID, Files: manifest})
}

func (s *SourceServer) handleRelease(w http.ResponseWriter, r *http.Request, workloadID string) {
	migrationID := r.Header.Get("X-Migration-ID")
	token := r.Header.Get("X-Fence-Token")
	if migrationID == "" || token == "" {
		common.Error(w, 400, "MISSING_FENCE", errors.New("X-Migration-ID and X-Fence-Token required"))
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.Fences[workloadID]
	if !ok {
		common.JSON(w, 200, map[string]any{"status": "already_released"})
		return
	}
	if f.MigrationID != migrationID || f.Token != token {
		common.Error(w, 409, "FENCE_MISMATCH", errors.New("fence token mismatch"))
		return
	}
	delete(s.Fences, workloadID)
	if err := s.persistFencesLocked(); err != nil {
		common.Error(w, 500, "FENCE_PERSIST_FAILED", err)
		return
	}
	s.Logger.Info("workload_fence_released", "request_id", common.RequestID(r), "trace_id", common.TraceID(r), "migration_id", migrationID, "workload_id", workloadID, "fence_epoch", f.Epoch)
	common.JSON(w, 200, map[string]any{"status": "released", "epoch": f.Epoch})
}

func (s *SourceServer) handleWrite(w http.ResponseWriter, r *http.Request, workloadID, rel string) {
	if !safeRelativePath(rel) {
		common.Error(w, 400, "INVALID_PATH", errors.New("invalid relative path"))
		return
	}
	s.mu.RLock()
	_, fenced := s.Fences[workloadID]
	if fenced {
		s.mu.RUnlock()
		common.Error(w, 423, "WORKLOAD_FENCED", errors.New("workload is fenced and cannot accept writes"))
		return
	}
	root, err := s.workloadRoot(workloadID)
	if err != nil {
		s.mu.RUnlock()
		common.Error(w, 400, "INVALID_WORKLOAD", err)
		return
	}
	path := filepath.Join(root, rel)
	defer s.mu.RUnlock()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		common.Error(w, 500, "WRITE_FAILED", err)
		return
	}
	defer r.Body.Close()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		common.Error(w, 500, "WRITE_FAILED", err)
		return
	}
	defer f.Close()
	if _, err := io.Copy(f, io.LimitReader(r.Body, 64<<20)); err != nil {
		common.Error(w, 500, "WRITE_FAILED", err)
		return
	}
	if err := f.Sync(); err != nil {
		common.Error(w, 500, "WRITE_FAILED", err)
		return
	}
	common.JSON(w, 200, map[string]any{"status": "written", "path": rel})
}

func (s *SourceServer) handleChunkBatch(w http.ResponseWriter, r *http.Request, workloadID, checkpointID, rel string) {
	started := time.Now()
	if !safeRelativePath(rel) {
		common.Error(w, 400, "INVALID_PATH", errors.New("invalid relative path"))
		return
	}
	fenceToken := r.Header.Get("X-Fence-Token")
	migrationID := r.Header.Get("X-Migration-ID")
	if !s.validFence(workloadID, migrationID, fenceToken) {
		common.Error(w, 423, "FENCE_REQUIRED", errors.New("valid migration fence required"))
		return
	}
	chunkSize, err := strconv.ParseInt(r.URL.Query().Get("chunk_size"), 10, 64)
	if err != nil || chunkSize <= 0 || chunkSize > 8<<20 {
		common.Error(w, 400, "INVALID_CHUNK", errors.New("valid chunk_size required"))
		return
	}
	start, err := strconv.Atoi(r.URL.Query().Get("start"))
	if err != nil || start < 0 {
		common.Error(w, 400, "INVALID_BATCH", errors.New("valid start required"))
		return
	}
	count, err := strconv.Atoi(r.URL.Query().Get("count"))
	if err != nil || count < 1 || count > migration.MaxBatchChunks {
		common.Error(w, 400, "INVALID_BATCH", errors.New("valid count required"))
		return
	}
	maxBytes := int64(migration.DefaultBatchBytes)
	if raw := r.URL.Query().Get("max_bytes"); raw != "" {
		maxBytes, err = strconv.ParseInt(raw, 10, 64)
		if err != nil || maxBytes <= 0 || maxBytes > migration.MaxBatchBytes {
			common.Error(w, 400, "INVALID_BATCH", errors.New("invalid max_bytes"))
			return
		}
	}
	filePath := filepath.Join(s.CheckpointRoot, checkpointID, workloadID, filepath.FromSlash(rel))
	f, err := os.Open(filePath)
	if err != nil {
		common.Error(w, 404, "FILE_NOT_FOUND", err)
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		common.Error(w, 500, "BATCH_STAT_FAILED", err)
		return
	}
	totalChunks := int((st.Size() + chunkSize - 1) / chunkSize)
	if start >= totalChunks {
		common.Error(w, 416, "BATCH_RANGE_EMPTY", errors.New("batch start is beyond file"))
		return
	}
	end := start + count
	if end > totalChunks {
		end = totalChunks
	}
	chunks := make([]migration.BatchChunk, 0, end-start)
	var totalBytes int64
	readStart := time.Now()
	for idx := start; idx < end; idx++ {
		offset := int64(idx) * chunkSize
		want := chunkSize
		if rem := st.Size() - offset; rem < want {
			want = rem
		}
		if want < 0 {
			want = 0
		}
		buf := make([]byte, want)
		n, readErr := f.ReadAt(buf, offset)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			common.Error(w, 500, "CHUNK_READ_FAILED", readErr)
			return
		}
		data := buf[:n]
		h := sha256.Sum256(data)
		chunks = append(chunks, migration.BatchChunk{Path: rel, Index: idx, Offset: offset, SHA256: hex.EncodeToString(h[:]), Data: data})
		totalBytes += int64(n)
	}
	readMs := time.Since(readStart).Milliseconds()
	encodeStart := time.Now()
	encoded, err := migration.EncodeBatch(chunks)
	if err != nil {
		common.Error(w, 500, "BATCH_ENCODE_FAILED", err)
		return
	}
	encodeMs := time.Since(encodeStart).Milliseconds()
	s.Metrics.SourceBatchRequests.Add(1)
	s.Metrics.SourceBatchReadMs.Add(uint64(readMs))
	s.Metrics.SourceBatchEncodeMs.Add(uint64(encodeMs))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Batch-Chunks", strconv.Itoa(len(chunks)))
	w.Header().Set("X-Batch-Bytes", strconv.FormatInt(totalBytes, 10))
	w.Header().Set("X-Source-Batch-Read-Ms", strconv.FormatInt(readMs, 10))
	w.Header().Set("X-Source-Batch-Encode-Ms", strconv.FormatInt(encodeMs, 10))
	w.Header().Set("X-Source-Batch-Handler-Ms", strconv.FormatInt(time.Since(started).Milliseconds(), 10))
	w.Header().Set("X-Request-ID", common.RequestID(r))
	_, _ = w.Write(encoded)
}

func (s *SourceServer) handleChunk(w http.ResponseWriter, r *http.Request, workloadID, checkpointID, rel string) {
	if !safeRelativePath(rel) {
		common.Error(w, 400, "INVALID_PATH", errors.New("invalid relative path"))
		return
	}
	fenceToken := r.Header.Get("X-Fence-Token")
	migrationID := r.Header.Get("X-Migration-ID")
	if !s.validFence(workloadID, migrationID, fenceToken) {
		common.Error(w, 423, "FENCE_REQUIRED", errors.New("valid migration fence required"))
		return
	}
	idx, err := strconv.Atoi(r.URL.Query().Get("index"))
	if err != nil || idx < 0 {
		common.Error(w, 400, "INVALID_CHUNK", errors.New("valid chunk index required"))
		return
	}
	chunkSize, err := strconv.ParseInt(r.URL.Query().Get("chunk_size"), 10, 64)
	if err != nil || chunkSize <= 0 {
		common.Error(w, 400, "INVALID_CHUNK", errors.New("valid chunk_size required"))
		return
	}
	path := filepath.Join(s.CheckpointRoot, checkpointID, workloadID, filepath.FromSlash(rel))
	f, err := os.Open(path)
	if err != nil {
		common.Error(w, 404, "FILE_NOT_FOUND", err)
		return
	}
	defer f.Close()
	offset := int64(idx) * chunkSize
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		common.Error(w, 500, "CHUNK_READ_FAILED", err)
		return
	}
	buf := make([]byte, chunkSize)
	n, err := io.ReadFull(f, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		common.Error(w, 500, "CHUNK_READ_FAILED", err)
		return
	}
	if n == 0 && idx == 0 {
		// Empty file: a zero-length response is valid.
	}
	data := buf[:n]
	h := sha256.Sum256(data)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Chunk-SHA256", hex.EncodeToString(h[:]))
	w.Header().Set("X-Chunk-Index", strconv.Itoa(idx))
	w.Header().Set("X-Chunk-Offset", strconv.FormatInt(offset, 10))
	w.Header().Set("X-Chunk-Size", strconv.Itoa(n))
	w.Header().Set("X-Request-ID", common.RequestID(r))
	w.WriteHeader(200)
	_, _ = w.Write(data)
}

func (s *SourceServer) loadFences() error {
	b, err := os.ReadFile(s.fenceFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(b) == 0 {
		return nil
	}
	var data map[string]Fence
	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}
	for k, v := range data {
		s.Fences[k] = v
	}
	return nil
}

func (s *SourceServer) persistFencesLocked() error {
	b, err := json.MarshalIndent(s.Fences, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.fenceFile + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, s.fenceFile)
}

func (s *SourceServer) validFence(workloadID, migrationID, token string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, ok := s.Fences[workloadID]
	return ok && f.MigrationID == migrationID && f.Token == token
}

func (s *SourceServer) workloadRoot(workloadID string) (string, error) {
	if !safeRelativePath(workloadID) {
		return "", errors.New("invalid workload id")
	}
	return filepath.Join(s.Root, workloadID), nil
}

func safeRelativePath(p string) bool {
	p = filepath.Clean(filepath.FromSlash(p))
	return p != "." && p != ".." && !filepath.IsAbs(p) && !strings.HasPrefix(p, ".."+string(os.PathSeparator)) && !strings.Contains(p, string(os.PathSeparator)+".migration"+string(os.PathSeparator)) && !strings.HasPrefix(p, ".migration"+string(os.PathSeparator))
}

func snapshotTree(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path == src {
			return nil
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if strings.HasPrefix(rel, ".migration") {
			return filepath.SkipDir
		}
		dstPath := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode().Perm())
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink not supported: %s", rel)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			return err
		}
		out, err := os.OpenFile(dstPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode().Perm())
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			_ = out.Close()
			return err
		}
		if err := out.Sync(); err != nil {
			_ = out.Close()
			return err
		}
		return out.Close()
	})
}

func buildManifest(root string, chunkSize int64) ([]migration.File, error) {
	var out []migration.File
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if !safeRelativePath(rel) {
			return fmt.Errorf("unsafe file path: %s", rel)
		}
		sha, err := shaFile(path)
		if err != nil {
			return err
		}
		cs := chunkSize
		if cs <= 0 {
			cs = 1 << 20
		}
		count := int((info.Size() + cs - 1) / cs)
		out = append(out, migration.File{Path: filepath.ToSlash(rel), Size: info.Size(), SHA256: sha, Mode: uint32(info.Mode().Perm()), ChunkSize: cs, ChunkCount: count})
		return nil
	})
	return out, err
}

func shaFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
