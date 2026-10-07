package migration

import "time"

type State string

const (
	StatePending      State = "PENDING"
	StateFencing      State = "FENCING"
	StatePreparing    State = "PREPARING"
	StateFrozen       State = "FROZEN"
	StateTransferring State = "TRANSFERRING"
	StateActivating   State = "ACTIVATING"
	StateActivated    State = "ACTIVATED"
	StateCommitting   State = "COMMITTING"
	StateCommitted    State = "COMMITTED"
	StateRollingBack  State = "ROLLING_BACK"
	StateRolledBack   State = "ROLLED_BACK"
	StateFailed       State = "FAILED"
)

type CreateRequest struct {
	WorkloadID  string `json:"workload_id"`
	SourceAgent string `json:"source_agent"`
	TargetAgent string `json:"target_agent"`
	ChunkSize   int64  `json:"chunk_size,omitempty"`
}

type Migration struct {
	ID            string        `json:"id"`
	WorkloadID    string        `json:"workload_id"`
	SourceAgent   string        `json:"source_agent"`
	TargetAgent   string        `json:"target_agent"`
	State         State         `json:"state"`
	ChunkSize     int64         `json:"chunk_size"`
	FenceToken    string        `json:"fence_token,omitempty"`
	FenceEpoch    uint64        `json:"fence_epoch,omitempty"`
	CheckpointID  string        `json:"checkpoint_id,omitempty"`
	Manifest      []File        `json:"manifest,omitempty"`
	CreatedAt     time.Time     `json:"created_at"`
	UpdatedAt     time.Time     `json:"updated_at"`
	Error         string        `json:"error,omitempty"`
	LastErrorCode string        `json:"last_error_code,omitempty"`
	Stats         TransferStats `json:"stats"`
	Version       int64         `json:"version"`
}

type File struct {
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	SHA256     string `json:"sha256"`
	Mode       uint32 `json:"mode"`
	ChunkSize  int64  `json:"chunk_size"`
	ChunkCount int    `json:"chunk_count"`
}

type TransferStats struct {
	FilesTotal         int   `json:"files_total"`
	FilesCompleted     int   `json:"files_completed"`
	ChunksTotal        int64 `json:"chunks_total"`
	ChunksTransferred  int64 `json:"chunks_transferred"`
	ChunksResumed      int64 `json:"chunks_resumed"`
	BatchesTransferred int64 `json:"batches_transferred"`
	BytesTransferred   int64 `json:"bytes_transferred"`
	BytesResumed       int64 `json:"bytes_resumed"`
	SourceBatchHTTPMs  int64 `json:"source_batch_http_ms"`
	SourceReadMs       int64 `json:"source_read_ms"`
	SourceEncodeMs     int64 `json:"source_encode_ms"`
	TargetBatchHTTPMs  int64 `json:"target_batch_http_ms"`
	TargetQueueWaitMs  int64 `json:"target_queue_wait_ms"`
	TargetWriteMs      int64 `json:"target_write_ms"`
	TargetFsyncMs      int64 `json:"target_fsync_ms"`
	TargetStateSaveMs  int64 `json:"target_state_save_ms"`
	TargetBatchPeakIn  int64 `json:"target_batch_peak_inflight"`
}

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type AgentFenceResponse struct {
	MigrationID string `json:"migration_id"`
	Token       string `json:"token"`
	Epoch       uint64 `json:"epoch"`
}

type CheckpointResponse struct {
	MigrationID  string `json:"migration_id"`
	CheckpointID string `json:"checkpoint_id"`
	Files        []File `json:"files"`
}

type ProgressResponse struct {
	MigrationID string           `json:"migration_id"`
	Files       map[string][]int `json:"files"`
}
