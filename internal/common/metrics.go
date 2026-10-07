package common

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

type Metrics struct {
	Requests atomic.Uint64
	Actions  atomic.Uint64
	Failures atomic.Uint64
	Chunks   atomic.Uint64
	Bytes    atomic.Uint64

	SourceBatchRequests atomic.Uint64
	SourceBatchReadMs   atomic.Uint64
	SourceBatchEncodeMs atomic.Uint64

	TargetBatchRequests    atomic.Uint64
	TargetBatchInflight    atomic.Int64
	TargetBatchPeak        atomic.Int64
	TargetBatchQueueWaitMs atomic.Uint64
	TargetBatchWriteMs     atomic.Uint64
	TargetBatchFsyncMs     atomic.Uint64
	TargetBatchStateSaveMs atomic.Uint64
}

func (m *Metrics) Handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "migration_http_requests_total %d\n", m.Requests.Load())
	fmt.Fprintf(w, "migration_actions_total %d\n", m.Actions.Load())
	fmt.Fprintf(w, "migration_failures_total %d\n", m.Failures.Load())
	fmt.Fprintf(w, "migration_transfer_chunks_total %d\n", m.Chunks.Load())
	fmt.Fprintf(w, "migration_transfer_bytes_total %d\n", m.Bytes.Load())
	fmt.Fprintf(w, "migration_source_batch_requests_total %d\n", m.SourceBatchRequests.Load())
	fmt.Fprintf(w, "migration_source_batch_read_ms_total %d\n", m.SourceBatchReadMs.Load())
	fmt.Fprintf(w, "migration_source_batch_encode_ms_total %d\n", m.SourceBatchEncodeMs.Load())
	fmt.Fprintf(w, "migration_target_batch_requests_total %d\n", m.TargetBatchRequests.Load())
	fmt.Fprintf(w, "migration_target_batch_inflight %d\n", m.TargetBatchInflight.Load())
	fmt.Fprintf(w, "migration_target_batch_peak_inflight %d\n", m.TargetBatchPeak.Load())
	fmt.Fprintf(w, "migration_target_batch_queue_wait_ms_total %d\n", m.TargetBatchQueueWaitMs.Load())
	fmt.Fprintf(w, "migration_target_batch_write_ms_total %d\n", m.TargetBatchWriteMs.Load())
	fmt.Fprintf(w, "migration_target_batch_fsync_ms_total %d\n", m.TargetBatchFsyncMs.Load())
	fmt.Fprintf(w, "migration_target_batch_state_save_ms_total %d\n", m.TargetBatchStateSaveMs.Load())
}
