package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wishforge/sluicegate/internal/common"
	"github.com/wishforge/sluicegate/internal/migration"
)

// Finding 1 RED: batch writes for DIFFERENT migrations are fully serialized.
//
// TargetServer holds a single sync.Mutex (target.go:41) that handleChunkBatch
// acquires at target.go:245 and holds until fsync and state save complete
// (target.go:248, via defer). Nothing in the critical section is shared
// between two different migration ids: each has its own TargetState, its own
// staging directory, and its own file set.
//
// Consequence: concurrency above 1 cannot increase target-side throughput.
// The saturation matrix shows exactly this - throughput peaks at
// concurrency 8 then stays flat while p50 latency grows linearly.
//
// The test below runs batches for independent migrations concurrently and
// asserts they overlap. It is RED against the current single lock: the
// measured overlap is zero because every batch waits for the previous one.
//
// What a fix must preserve (NOT a defect to fix):
//   - batches for the SAME migration must stay serialized, because
//     f.Chunks[c.Index] is the idempotency record and concurrent mutation
//     of it would break replay semantics.
//   - state save ordering per migration must stay stable.

func TestIndependentMigrationsSerializeOnTargetLock(t *testing.T) {
	t.Parallel()

	const migrations = 4
	const chunkSize = int64(4)
	const chunkCount = 3
	payload := []byte("abcdefghijkl")

	targetRoot := t.TempDir()
	logger := common.NewLogger("lock-red")
	metrics := &common.Metrics{}

	target, err := NewTargetServer(targetRoot, logger, metrics)
	if err != nil {
		t.Fatal(err)
	}

	// Register `migrations` independent migrations, each with identical
	// content but its own id, so nothing is shared between them.
	sum := sha256.Sum256(payload)
	sha := hex.EncodeToString(sum[:])
	ids := make([]string, 0, migrations)
	for i := 0; i < migrations; i++ {
		id := fmt.Sprintf("m%d", i)
		ids = append(ids, id)

		body := fmt.Sprintf(`{"workload_id":%q,"files":[{"path":"data.bin","size":%d,"sha256":%q,"mode":384,"chunk_size":4,"chunk_count":3}]}`,
			"w", len(payload), sha)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/migrations/"+id+"/init", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		target.Handler().ServeHTTP(rec, req)
		if rec.Code/100 != 2 {
			t.Fatalf("init %s status=%d body=%s", id, rec.Code, rec.Body.String())
		}
	}

	// Build one batch per migration. DecodeBatch verifies each chunk's
	// SHA256 independently (batch.go:127), so each chunk needs its own
	// digest over its own bytes, not the whole-file digest.
	batches := make([][]migration.BatchChunk, migrations)
	for i := range ids {
		batches[i] = buildChunks(t, payload, chunkSize, chunkCount)
	}

	// Fire all batches concurrently.
	ts := httptest.NewServer(target.Handler())
	defer ts.Close()
	client := migration.NewAgentClient(logger)

	var wg sync.WaitGroup
	start := time.Now()
	for i, id := range ids {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			if _, err := client.PutChunkBatchWithStats(context.Background(), ts.URL, id, batches[i], "req-lock"); err != nil {
				t.Errorf("batch %s: %v", id, err)
			}
		}(i, id)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// The lock serializes everything, so total time is the sum of all
	// batches. Measure the per-batch critical-section cost to show there
	// is headroom that concurrency cannot currently reach.
	queueTotal := time.Duration(metrics.TargetBatchQueueWaitMs.Load()) * time.Millisecond
	t.Logf("migrations=%d elapsed=%v queue_wait_total=%v", migrations, elapsed, queueTotal)

	// RED assertion: with a single global lock, queue wait grows with the
	// number of concurrent independent migrations. If batches overlapped,
	// the later ones would not each pay a full lock wait.
	perBatchWait := queueTotal / time.Duration(migrations)
	t.Logf("mean queue wait per batch = %v", perBatchWait)

	if perBatchWait > 0 {
		t.Fatalf("RED: independent migrations are serialized on one lock; "+
			"mean queue wait %v across %d concurrent batches (elapsed %v). "+
			"Nothing is shared between these migrations, so this wait buys nothing.",
			perBatchWait, migrations, elapsed)
	}
}

// TestSameMigrationStaysSerialized pins the invariant that a fix must not
// break: batches for one migration are still applied one at a time, so the
// idempotency map f.Chunks is never mutated concurrently.
func TestSameMigrationStaysSerialized(t *testing.T) {
	t.Parallel()

	payload := []byte("abcdefghijkl")
	chunkSize := int64(4)
	sum := sha256.Sum256(payload)
	sha := hex.EncodeToString(sum[:])

	targetRoot := t.TempDir()
	target, err := NewTargetServer(targetRoot, common.NewLogger("same-mig"), &common.Metrics{})
	if err != nil {
		t.Fatal(err)
	}

	id := "same"
	body := fmt.Sprintf(`{"workload_id":"w","files":[{"path":"data.bin","size":%d,"sha256":%q,"mode":384,"chunk_size":4,"chunk_count":3}]}`,
		len(payload), sha)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/migrations/"+id+"/init", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	target.Handler().ServeHTTP(rec, req)
	if rec.Code/100 != 2 {
		t.Fatalf("init status=%d", rec.Code)
	}

	// Replay the identical batch many times. Idempotency must hold: the
	// second and later sends report already_present, never a conflict.
	ts := httptest.NewServer(target.Handler())
	defer ts.Close()
	client := migration.NewAgentClient(logger(t))

	chunks := buildChunks(t, payload, chunkSize, 3)

	for attempt := 0; attempt < 5; attempt++ {
		if _, err := client.PutChunkBatchWithStats(context.Background(), ts.URL, id, chunks, "req-replay"); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}

	progress, err := client.Progress(context.Background(), ts.URL, id, "req-progress")
	if err != nil {
		t.Fatal(err)
	}
	if got := len(progress.Files["data.bin"]); got != 3 {
		t.Fatalf("idempotency broken: progress has %d chunks, want 3", got)
	}
}

// buildChunks returns count chunks of size chunkSize, each carrying the
// SHA256 of its own bytes as DecodeBatch requires.
func buildChunks(t *testing.T, payload []byte, chunkSize int64, count int) []migration.BatchChunk {
	t.Helper()
	out := make([]migration.BatchChunk, 0, count)
	for c := 0; c < count; c++ {
		off := int64(c) * chunkSize
		end := off + chunkSize
		if end > int64(len(payload)) {
			end = int64(len(payload))
		}
		data := payload[off:end]
		sum := sha256.Sum256(data)
		out = append(out, migration.BatchChunk{
			Path:   "data.bin",
			Index:  c,
			Offset: off,
			SHA256: hex.EncodeToString(sum[:]),
			Data:   data,
		})
	}
	return out
}

func logger(t *testing.T) *common.SLogger {
	t.Helper()
	return common.NewLogger("test")
}

// TestStripedLockHoldsUnderMixedLoad stresses the striped lock the way the
// saturation matrix does: many migrations, many batches each, interleaved
// progress reads and replays. Under -race this is the check that the
// per-migration sharding did not introduce a data race on s.states or on
// the chunk idempotency map.
func TestStripedLockHoldsUnderMixedLoad(t *testing.T) {
	t.Parallel()

	const migrations = 24
	const batchesPer = 4
	const chunkSize = int64(8)

	payload := make([]byte, chunkSize*batchesPer)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	sum := sha256.Sum256(payload)
	fileSHA := hex.EncodeToString(sum[:])

	target, err := NewTargetServer(t.TempDir(), common.NewLogger("mixed"), &common.Metrics{})
	if err != nil {
		t.Fatal(err)
	}

	ids := make([]string, migrations)
	for i := range ids {
		ids[i] = fmt.Sprintf("mix%d", i)
		body := fmt.Sprintf(`{"workload_id":"w","files":[{"path":"data.bin","size":%d,"sha256":%q,"mode":384,"chunk_size":8,"chunk_count":%d}]}`,
			len(payload), fileSHA, batchesPer)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/migrations/"+ids[i]+"/init", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		target.Handler().ServeHTTP(rec, req)
		if rec.Code/100 != 2 {
			t.Fatalf("init %s status=%d", ids[i], rec.Code)
		}
	}

	ts := httptest.NewServer(target.Handler())
	defer ts.Close()
	client := migration.NewAgentClient(common.NewLogger("mixed-client"))

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			chunks := buildChunks(t, payload, chunkSize, batchesPer)
			// Send each batch twice: the second send must be idempotent, and
			// interleaving duplicates with progress reads is what would trip
			// a race on the shared state.
			for b := 0; b < 2; b++ {
				if _, err := client.PutChunkBatchWithStats(context.Background(), ts.URL, id, chunks, "req-mixed"); err != nil {
					t.Errorf("%s: %v", id, err)
					return
				}
				if _, err := client.Progress(context.Background(), ts.URL, id, "req-mixed-progress"); err != nil {
					t.Errorf("%s progress: %v", id, err)
					return
				}
			}
		}(id)
	}
	wg.Wait()

	for _, id := range ids {
		progress, err := client.Progress(context.Background(), ts.URL, id, "req-final")
		if err != nil {
			t.Fatalf("%s final progress: %v", id, err)
		}
		if got := len(progress.Files["data.bin"]); got != batchesPer {
			t.Fatalf("%s: progress has %d chunks, want %d (idempotency broken)", id, got, batchesPer)
		}
	}
}

// TestStripeDistributionIsActuallyStriped guards the mechanism itself.
//
// The mixed-load test above passes even when every id maps to stripe 0,
// because a single lock is trivially race-free. So it verifies safety but not
// the fix. This test pins the distribution: distinct ids must be able to land
// on different stripes, otherwise "striping" is a no-op that happens to be
// safe.
func TestStripeDistributionIsActuallyStriped(t *testing.T) {
	t.Parallel()

	const target = 4096
	var l stripedTargetLock

	distinct := map[*sync.Mutex]struct{}{}
	for i := 0; i < target; i++ {
		distinct[l.lockFor(fmt.Sprintf("m%d", i))] = struct{}{}
	}

	// With 256 stripes and 4096 keys, a working hash should touch nearly all
	// of them. Requiring >= 200 keeps this from being flaky while still
	// failing loudly if the index collapses to a constant.
	if len(distinct) < 200 {
		t.Fatalf("only %d distinct stripes for %d keys; striping is not working", len(distinct), target)
	}
	t.Logf("%d keys -> %d distinct stripes", target, len(distinct))
}

// TestSameIDAlwaysMapsToSameStripe is the safety property: idempotency relies
// on one migration always taking one lock, so this must never change.
func TestSameIDAlwaysMapsToSameStripe(t *testing.T) {
	t.Parallel()

	var l stripedTargetLock
	first := l.lockFor("stable-id")
	for i := 0; i < 1000; i++ {
		if l.lockFor("stable-id") != first {
			t.Fatal("same id mapped to different stripes; per-migration serialization is broken")
		}
	}
}
