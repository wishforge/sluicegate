package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wishforge/sluicegate/internal/common"
	"github.com/wishforge/sluicegate/internal/migration"
)

func TestBatchTransferHTTP(t *testing.T) {
	t.Parallel()
	srcRoot := t.TempDir()
	cpRoot := filepath.Join(t.TempDir(), "checkpoints")
	targetRoot := t.TempDir()
	logger := common.NewLogger("batch-test")
	metrics := &common.Metrics{}

	src, err := NewSourceServer(srcRoot, cpRoot, logger, metrics)
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewTargetServer(targetRoot, logger, metrics)
	if err != nil {
		t.Fatal(err)
	}

	workload := "w1"
	migrationID := "m1"
	checkpoint := migrationID
	data := []byte("abcdefghijkl")
	checkpointFile := filepath.Join(cpRoot, checkpoint, workload, "data.bin")
	if err := os.MkdirAll(filepath.Dir(checkpointFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(checkpointFile, data, 0o600); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(data)
	sha := hex.EncodeToString(h[:])

	src.Fences[workload] = Fence{MigrationID: migrationID, Token: "token", Epoch: 1}
	chunkSize := int64(4)
	manifest := []migration.File{{Path: "data.bin", Size: int64(len(data)), SHA256: sha, Mode: 0o600, ChunkSize: chunkSize, ChunkCount: 3}}
	targetReq := httptest.NewRecorder()
	initBody := fmt.Sprintf(`{"workload_id":%q,"files":[{"path":"data.bin","size":%d,"sha256":%q,"mode":384,"chunk_size":4,"chunk_count":3}]}`, workload, len(data), sha)
	initHTTP := httptest.NewRequest(http.MethodPost, "/v1/migrations/"+migrationID+"/init", strings.NewReader(initBody))
	initHTTP.Header.Set("Content-Type", "application/json")
	target.Handler().ServeHTTP(targetReq, initHTTP)
	if targetReq.Code/100 != 2 {
		t.Fatalf("init status=%d body=%s", targetReq.Code, targetReq.Body.String())
	}

	srcServer := httptest.NewServer(src.Handler())
	defer srcServer.Close()
	targetServer := httptest.NewServer(target.Handler())
	defer targetServer.Close()
	client := migration.NewAgentClient(logger)
	chunks, sourceStats, err := client.FetchChunkBatchWithStats(context.Background(), srcServer.URL, workload, checkpoint, "data.bin", 0, 3, chunkSize, migration.DefaultBatchBytes, migrationID, "token", "req-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatalf("chunks=%d want=3", len(chunks))
	}
	if sourceStats.ServerReadMs < 0 || sourceStats.ServerEncodeMs < 0 {
		t.Fatalf("invalid source telemetry: %+v", sourceStats)
	}
	targetStats, err := client.PutChunkBatchWithStats(context.Background(), targetServer.URL, migrationID, chunks, "req-1")
	if err != nil {
		t.Fatal(err)
	}
	if targetStats.TargetPeakInflight < 1 {
		t.Fatalf("target peak inflight=%d want>=1", targetStats.TargetPeakInflight)
	}
	if targetStats.TargetQueueWaitMs < 0 || targetStats.TargetWriteMs < 0 || targetStats.TargetFsyncMs < 0 || targetStats.TargetStateSaveMs < 0 {
		t.Fatalf("invalid target telemetry: %+v", targetStats)
	}

	if metrics.TargetBatchRequests.Load() != 1 {
		t.Fatalf("target batch requests=%d want=1", metrics.TargetBatchRequests.Load())
	}
	if metrics.SourceBatchRequests.Load() != 1 {
		t.Fatalf("source batch requests=%d want=1", metrics.SourceBatchRequests.Load())
	}

	progress, err := client.Progress(context.Background(), targetServer.URL, migrationID, "req-2")
	if err != nil {
		t.Fatal(err)
	}
	if got := len(progress.Files["data.bin"]); got != 3 {
		t.Fatalf("progress chunks=%d want=3", got)
	}

	activateReq := httptest.NewRequest(http.MethodPost, "/v1/migrations/"+migrationID+"/activate", nil)
	activateRec := httptest.NewRecorder()
	target.Handler().ServeHTTP(activateRec, activateReq)
	if activateRec.Code/100 != 2 {
		t.Fatalf("activate status=%d body=%s", activateRec.Code, activateRec.Body.String())
	}
	final := filepath.Join(targetRoot, "workloads", workload, "data.bin")
	got, err := os.ReadFile(final)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("payload mismatch: got=%q want=%q", string(got), string(data))
	}
	_ = manifest
}
