package migration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/wishforge/sluicegate/internal/common"
)

type Service struct {
	store              Store
	log                *common.SLogger
	metrics            *common.Metrics
	client             *AgentClient
	failAfterChunks    atomic.Int64
	failedOnce         atomic.Bool
	crashAfterChunks   atomic.Int64
	crashOnce          atomic.Bool
	commitPauseMillis  atomic.Int64
	transferBatchBytes atomic.Int64
}

func NewService(store Store, log *common.SLogger, metrics *common.Metrics) *Service {
	s := &Service{store: store, log: log, metrics: metrics}
	s.client = NewAgentClient(log)
	if v := os.Getenv("FAIL_AFTER_CHUNKS"); v != "" {
		var n int64
		_, _ = fmt.Sscanf(v, "%d", &n)
		if n > 0 {
			s.failAfterChunks.Store(n)
		}
	}
	if v := os.Getenv("CRASH_AFTER_CHUNKS"); v != "" {
		var n int64
		_, _ = fmt.Sscanf(v, "%d", &n)
		if n > 0 {
			s.crashAfterChunks.Store(n)
		}
	}
	if v := os.Getenv("CHAOS_COMMIT_PAUSE_MS"); v != "" {
		var n int64
		_, _ = fmt.Sscanf(v, "%d", &n)
		if n > 0 {
			s.commitPauseMillis.Store(n)
		}
	}
	s.transferBatchBytes.Store(DefaultBatchBytes)
	if v := os.Getenv("TRANSFER_BATCH_BYTES"); v != "" {
		var n int64
		_, _ = fmt.Sscanf(v, "%d", &n)
		if n >= 256<<10 && n <= MaxConfiguredBatchBytes {
			s.transferBatchBytes.Store(n)
		}
	}
	return s
}

func (s *Service) Create(req CreateRequest, idem string) (Migration, bool, error) {
	if strings.TrimSpace(req.WorkloadID) == "" || strings.TrimSpace(req.SourceAgent) == "" || strings.TrimSpace(req.TargetAgent) == "" {
		return Migration{}, false, errors.New("workload_id, source_agent, target_agent are required")
	}
	if req.ChunkSize <= 0 {
		req.ChunkSize = 1 << 20
	}
	if req.ChunkSize < 64<<10 || req.ChunkSize > 8<<20 {
		return Migration{}, false, errors.New("chunk_size must be between 64KiB and 8MiB")
	}
	now := time.Now().UTC()
	m := Migration{ID: common.ID("mig"), WorkloadID: req.WorkloadID, SourceAgent: strings.TrimRight(req.SourceAgent, "/"), TargetAgent: strings.TrimRight(req.TargetAgent, "/"), ChunkSize: req.ChunkSize, State: StatePending, CreatedAt: now, UpdatedAt: now, Version: 1}
	idemKey := ""
	if idem != "" {
		idemKey = "create:" + idem
	}
	m, replay, err := s.store.CreateMigration(m, idemKey)
	if err != nil {
		return Migration{}, false, err
	}
	if replay {
		s.log.Info("migration_idempotency_replay", "migration_id", m.ID, "workload_id", m.WorkloadID)
		return m, true, nil
	}
	s.log.Info("migration_created", "migration_id", m.ID, "workload_id", m.WorkloadID, "source_agent", m.SourceAgent, "target_agent", m.TargetAgent, "chunk_size", m.ChunkSize)
	return m, false, nil
}

func (s *Service) Get(id string) (Migration, bool) { return s.store.Get(id) }

func (s *Service) Prepare(ctx context.Context, id, requestID string) (Migration, error) {
	m, err := s.must(id)
	if err != nil {
		return Migration{}, err
	}
	if m.State == StateFrozen || m.State == StateTransferring || m.State == StateActivating || m.State == StateActivated || m.State == StateCommitting || m.State == StateCommitted {
		return m, nil
	}
	if m.State != StatePending && m.State != StateFencing && m.State != StatePreparing {
		return m, fmt.Errorf("prepare requires PENDING/FENCING/PREPARING, got %s", m.State)
	}
	start := time.Now()
	m.State = StateFencing
	m.UpdatedAt = time.Now().UTC()
	if err := s.store.Put(m); err != nil {
		return Migration{}, err
	}
	epoch := m.FenceEpoch
	if epoch == 0 {
		epoch, err = s.store.ReserveFenceEpoch(m.WorkloadID, m.ID)
		if err != nil {
			return s.retryableFailure(id, StateFencing, "FENCE_EPOCH_FAILED", err)
		}
		m.FenceEpoch = epoch
		m.UpdatedAt = time.Now().UTC()
		if err := s.store.Put(m); err != nil {
			return Migration{}, err
		}
	}
	fence, err := s.client.Fence(ctx, m.SourceAgent, m.WorkloadID, m.ID, requestID, epoch)
	if err != nil {
		return s.retryableFailure(id, StateFencing, "FENCE_FAILED", err)
	}
	m.FenceToken = fence.Token
	m.FenceEpoch = epoch
	if fence.Epoch > 0 {
		m.FenceEpoch = fence.Epoch
	}
	m.State = StatePreparing
	m.UpdatedAt = time.Now().UTC()
	if err := s.store.Put(m); err != nil {
		return Migration{}, err
	}
	cp, err := s.client.Checkpoint(ctx, m.SourceAgent, m.WorkloadID, m.ID, m.FenceToken, requestID, m.ChunkSize)
	if err != nil {
		return s.retryableFailure(id, StatePreparing, "CHECKPOINT_FAILED", err)
	}
	m.CheckpointID = cp.CheckpointID
	m.Manifest = cp.Files
	m.Stats.FilesTotal = len(cp.Files)
	if m.Stats.ChunksTotal == 0 {
		for _, f := range cp.Files {
			m.Stats.ChunksTotal += int64(f.ChunkCount)
		}
	}
	m.State = StateFrozen
	m.UpdatedAt = time.Now().UTC()
	if err := s.store.Put(m); err != nil {
		return Migration{}, err
	}
	s.log.Info("migration_prepared", "migration_id", m.ID, "workload_id", m.WorkloadID, "fence_epoch", m.FenceEpoch, "checkpoint_id", m.CheckpointID, "files", len(m.Manifest), "chunks_total", m.Stats.ChunksTotal, "duration_ms", time.Since(start).Milliseconds())
	return m, nil
}

func (s *Service) Transfer(ctx context.Context, id, requestID string) (Migration, error) {
	m, err := s.must(id)
	if err != nil {
		return Migration{}, err
	}
	if m.State == StateActivated || m.State == StateCommitting || m.State == StateCommitted {
		return m, nil
	}
	if m.State != StateFrozen && m.State != StateTransferring {
		return Migration{}, fmt.Errorf("transfer requires FROZEN/TRANSFERRING, got %s", m.State)
	}
	m.State = StateTransferring
	m.UpdatedAt = time.Now().UTC()
	if err := s.store.Put(m); err != nil {
		return Migration{}, err
	}
	if err := s.client.InitTarget(ctx, m.TargetAgent, m.ID, m.WorkloadID, m.Manifest, requestID); err != nil {
		return s.retryableFailure(id, StateTransferring, "TARGET_INIT_FAILED", err)
	}
	progress, err := s.client.Progress(ctx, m.TargetAgent, m.ID, requestID)
	if err != nil {
		return s.retryableFailure(id, StateTransferring, "PROGRESS_READ_FAILED", err)
	}
	batchBytes := s.transferBatchBytes.Load()
	if batchBytes <= 0 {
		batchBytes = DefaultBatchBytes
	}
	start := time.Now()
	transferredThisCall := int64(0)
	batchCalls := int64(0)
	for _, f := range m.Manifest {
		completed := map[int]bool{}
		for _, idx := range progress.Files[f.Path] {
			completed[idx] = true
		}
		for startIdx := 0; startIdx < f.ChunkCount; {
			chunksPerBatch := int(batchBytes / f.ChunkSize)
			if chunksPerBatch < 1 {
				chunksPerBatch = 1
			}
			endIdx := startIdx + chunksPerBatch
			if endIdx > f.ChunkCount {
				endIdx = f.ChunkCount
			}
			allDone := true
			for i := startIdx; i < endIdx; i++ {
				if !completed[i] {
					allDone = false
					break
				}
			}
			if allDone {
				for i := startIdx; i < endIdx; i++ {
					m.Stats.ChunksResumed++
					remaining := f.Size - int64(i)*f.ChunkSize
					chunkBytes := f.ChunkSize
					if remaining < chunkBytes {
						chunkBytes = remaining
					}
					if chunkBytes > 0 {
						m.Stats.BytesResumed += chunkBytes
					}
				}
				startIdx = endIdx
				continue
			}
			sourceHTTPStart := time.Now()
			batch, sourceStats, err := s.client.FetchChunkBatchWithStats(ctx, m.SourceAgent, m.WorkloadID, m.CheckpointID, f.Path, startIdx, endIdx-startIdx, f.ChunkSize, batchBytes, m.ID, m.FenceToken, requestID)
			m.Stats.SourceBatchHTTPMs += time.Since(sourceHTTPStart).Milliseconds()
			m.Stats.SourceReadMs += sourceStats.ServerReadMs
			m.Stats.SourceEncodeMs += sourceStats.ServerEncodeMs
			if err != nil {
				return s.retryableFailure(id, StateTransferring, "SOURCE_CHUNK_BATCH_FAILED", err)
			}
			if len(batch) == 0 {
				return s.fail(id, "SOURCE_CHUNK_BATCH_EMPTY", errors.New("source returned an empty chunk batch"))
			}
			missingInBatch := int64(0)
			for _, c := range batch {
				if completed[c.Index] {
					m.Stats.ChunksResumed++
					m.Stats.BytesResumed += int64(len(c.Data))
				} else {
					missingInBatch++
				}
			}
			targetHTTPStart := time.Now()
			targetStats, err := s.client.PutChunkBatchWithStats(ctx, m.TargetAgent, m.ID, batch, requestID)
			m.Stats.TargetBatchHTTPMs += time.Since(targetHTTPStart).Milliseconds()
			m.Stats.TargetQueueWaitMs += targetStats.TargetQueueWaitMs
			m.Stats.TargetWriteMs += targetStats.TargetWriteMs
			m.Stats.TargetFsyncMs += targetStats.TargetFsyncMs
			m.Stats.TargetStateSaveMs += targetStats.TargetStateSaveMs
			if targetStats.TargetPeakInflight > m.Stats.TargetBatchPeakIn {
				m.Stats.TargetBatchPeakIn = targetStats.TargetPeakInflight
			}
			if err != nil {
				return s.retryableFailure(id, StateTransferring, "TARGET_CHUNK_BATCH_FAILED", err)
			}
			m.Stats.ChunksTransferred += missingInBatch
			for _, c := range batch {
				if !completed[c.Index] {
					m.Stats.BytesTransferred += int64(len(c.Data))
				}
			}
			transferredThisCall += missingInBatch
			batchCalls++
			m.Stats.BatchesTransferred++
			if s.shouldCrash(transferredThisCall) {
				_ = s.store.Put(m)
				s.log.Error("migration_chaos_crash", "migration_id", m.ID, "chunks_this_call", transferredThisCall, "batch_calls", batchCalls)
				os.Exit(137)
			}
			if s.shouldInjectFailure(transferredThisCall) {
				_ = s.store.Put(m)
				s.log.Warn("migration_fault_injected", "migration_id", m.ID, "chunks_this_call", transferredThisCall, "batch_calls", batchCalls)
				return Migration{}, errors.New("fault injection after configured chunk count")
			}
			if err := s.store.Put(m); err != nil {
				return Migration{}, err
			}
			startIdx = endIdx
		}
		m.Stats.FilesCompleted++
	}
	m.Stats.FilesCompleted = len(m.Manifest)
	if err := s.store.Put(m); err != nil {
		return Migration{}, err
	}
	s.log.Info("migration_transfer_complete", "migration_id", m.ID, "workload_id", m.WorkloadID, "chunks_transferred", transferredThisCall, "chunks_resumed", m.Stats.ChunksResumed, "bytes_transferred", m.Stats.BytesTransferred, "bytes_resumed", m.Stats.BytesResumed, "batch_calls", batchCalls, "batch_bytes", batchBytes, "source_batch_http_ms", m.Stats.SourceBatchHTTPMs, "source_read_ms", m.Stats.SourceReadMs, "source_encode_ms", m.Stats.SourceEncodeMs, "target_batch_http_ms", m.Stats.TargetBatchHTTPMs, "target_queue_wait_ms", m.Stats.TargetQueueWaitMs, "target_write_ms", m.Stats.TargetWriteMs, "target_fsync_ms", m.Stats.TargetFsyncMs, "target_state_save_ms", m.Stats.TargetStateSaveMs, "target_peak_inflight", m.Stats.TargetBatchPeakIn, "duration_ms", time.Since(start).Milliseconds())
	return m, nil
}

func completedFileCount(m Migration, currentPath string, progress ProgressResponse) int {
	count := 0
	for _, mf := range m.Manifest {
		if mf.ChunkCount == 0 {
			count++
			continue
		}
		completed := len(progress.Files[mf.Path])
		if mf.Path == currentPath {
			// Current file has just been fully transferred.
			completed = mf.ChunkCount
		}
		if completed >= mf.ChunkCount {
			count++
		}
	}
	return count
}

func (s *Service) Activate(ctx context.Context, id, requestID string) (Migration, error) {
	m, err := s.must(id)
	if err != nil {
		return Migration{}, err
	}
	if m.State == StateActivated || m.State == StateCommitting || m.State == StateCommitted {
		return m, nil
	}
	if m.State != StateTransferring && m.State != StateActivating {
		return Migration{}, fmt.Errorf("activate requires TRANSFERRING/ACTIVATING, got %s", m.State)
	}
	m.State = StateActivating
	m.UpdatedAt = time.Now().UTC()
	if err := s.store.Put(m); err != nil {
		return Migration{}, err
	}
	if err := s.client.Activate(ctx, m.TargetAgent, m.ID, requestID); err != nil {
		return s.retryableFailure(id, StateActivating, "TARGET_ACTIVATE_FAILED", err)
	}
	m.State = StateActivated
	m.UpdatedAt = time.Now().UTC()
	if err := s.store.Put(m); err != nil {
		return Migration{}, err
	}
	s.log.Info("migration_activated", "migration_id", m.ID, "workload_id", m.WorkloadID)
	return m, nil
}

func (s *Service) Commit(ctx context.Context, id, requestID string) (Migration, error) {
	m, err := s.must(id)
	if err != nil {
		return Migration{}, err
	}
	if m.State == StateCommitted {
		return m, nil
	}
	if m.State != StateActivated && m.State != StateCommitting {
		return Migration{}, fmt.Errorf("commit requires ACTIVATED/COMMITTING, got %s", m.State)
	}
	m.State = StateCommitting
	m.UpdatedAt = time.Now().UTC()
	if err := s.store.Put(m); err != nil {
		return Migration{}, err
	}
	if err := s.client.CommitTarget(ctx, m.TargetAgent, m.ID, requestID); err != nil {
		m.Error, m.LastErrorCode, m.UpdatedAt = err.Error(), "TARGET_COMMIT_FAILED", time.Now().UTC()
		_ = s.store.Put(m)
		s.log.Error("migration_commit_retryable_failure", "migration_id", m.ID, "error_code", m.LastErrorCode, "error", err.Error())
		return m, fmt.Errorf("TARGET_COMMIT_FAILED: %w", err)
	}
	if pause := s.commitPauseMillis.Load(); pause > 0 {
		s.log.Warn("migration_chaos_commit_pause", "migration_id", m.ID, "pause_ms", pause)
		time.Sleep(time.Duration(pause) * time.Millisecond)
	}
	if err := s.client.Release(ctx, m.SourceAgent, m.WorkloadID, m.ID, m.FenceToken, requestID); err != nil {
		m.Error, m.LastErrorCode, m.UpdatedAt = err.Error(), "SOURCE_RELEASE_FAILED", time.Now().UTC()
		_ = s.store.Put(m)
		s.log.Error("migration_commit_retryable_failure", "migration_id", m.ID, "error_code", m.LastErrorCode, "error", err.Error())
		return m, fmt.Errorf("SOURCE_RELEASE_FAILED: %w", err)
	}
	m.State = StateCommitted
	m.UpdatedAt = time.Now().UTC()
	if err := s.store.Put(m); err != nil {
		return Migration{}, err
	}
	s.log.Info("migration_committed", "migration_id", m.ID, "workload_id", m.WorkloadID, "fence_epoch", m.FenceEpoch)
	return m, nil
}

func (s *Service) Rollback(ctx context.Context, id, requestID string) (Migration, error) {
	m, err := s.must(id)
	if err != nil {
		return Migration{}, err
	}
	if m.State == StateRolledBack {
		return m, nil
	}
	if m.State == StateCommitted {
		return Migration{}, errors.New("cannot rollback committed migration")
	}
	m.State = StateRollingBack
	m.UpdatedAt = time.Now().UTC()
	if err := s.store.Put(m); err != nil {
		return Migration{}, err
	}
	var errs []string
	if m.TargetAgent != "" {
		if err := s.client.RollbackTarget(ctx, m.TargetAgent, m.ID, requestID); err != nil {
			errs = append(errs, "target: "+err.Error())
		}
	}
	if m.FenceToken != "" {
		if err := s.client.Release(ctx, m.SourceAgent, m.WorkloadID, m.ID, m.FenceToken, requestID); err != nil {
			errs = append(errs, "source: "+err.Error())
		}
	}
	if len(errs) > 0 {
		return s.fail(id, "ROLLBACK_FAILED", errors.New(strings.Join(errs, "; ")))
	}
	m.State = StateRolledBack
	m.UpdatedAt = time.Now().UTC()
	_ = s.store.Put(m)
	s.log.Info("migration_rolled_back", "migration_id", m.ID, "workload_id", m.WorkloadID)
	return m, nil
}

func (s *Service) fail(id, code string, err error) (Migration, error) {
	m, ok := s.store.Get(id)
	if ok {
		m.State = StateFailed
		m.Error = err.Error()
		m.LastErrorCode = code
		m.UpdatedAt = time.Now().UTC()
		_ = s.store.Put(m)
	}
	s.metrics.Failures.Add(1)
	s.log.Error("migration_failed", "migration_id", id, "error_code", code, "error", err.Error())
	return m, fmt.Errorf("%s: %w", code, err)
}

func (s *Service) must(id string) (Migration, error) {
	m, ok := s.store.Get(id)
	if !ok {
		return Migration{}, os.ErrNotExist
	}
	return m, nil
}

func (s *Service) shouldInjectFailure(n int64) bool {
	limit := s.failAfterChunks.Load()
	return limit > 0 && n >= limit && s.failedOnce.CompareAndSwap(false, true)
}

func (s *Service) shouldCrash(n int64) bool {
	limit := s.crashAfterChunks.Load()
	return limit > 0 && n >= limit && s.crashOnce.CompareAndSwap(false, true)
}

func (s *Service) retryableFailure(id string, state State, code string, err error) (Migration, error) {
	m, ok := s.store.Get(id)
	if !ok {
		return Migration{}, err
	}
	m.State = state
	m.Error = err.Error()
	m.LastErrorCode = code
	m.UpdatedAt = time.Now().UTC()
	_ = s.store.Put(m)
	s.log.Warn("migration_retryable_failure", "migration_id", id, "state", state, "error_code", code, "error", err.Error())
	return m, fmt.Errorf("%s: %w", code, err)
}
