package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	migrationpkg "github.com/wishforge/sluicegate/internal/migration"
)

type migration struct {
	ID         string `json:"id"`
	WorkloadID string `json:"workload_id"`
	State      string `json:"state"`
	Stats      struct {
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
	} `json:"stats"`
}

type result struct {
	ok                bool
	total             time.Duration
	create            time.Duration
	prepare           time.Duration
	transfer          time.Duration
	activate          time.Duration
	commit            time.Duration
	bytes             int64
	chunks            int64
	batches           int64
	sourceBatchHTTPMs int64
	sourceReadMs      int64
	sourceEncodeMs    int64
	targetBatchHTTPMs int64
	targetQueueWaitMs int64
	targetWriteMs     int64
	targetFsyncMs     int64
	targetStateSaveMs int64
	targetBatchPeakIn int64
	error             string
	errorCode         string
}

type summary struct {
	Workloads             int            `json:"workloads"`
	Concurrency           int            `json:"concurrency"`
	PayloadBytes          int            `json:"payload_bytes"`
	ChunkSize             int64          `json:"chunk_size"`
	SeedConcurrency       int            `json:"seed_concurrency"`
	SeedDurationMs        int64          `json:"seed_duration_ms"`
	DurationMs            int64          `json:"duration_ms"`
	Completed             int            `json:"completed"`
	Failed                int            `json:"failed"`
	SuccessRate           float64        `json:"success_rate"`
	MigrationsPerSec      float64        `json:"migrations_per_sec"`
	DataBytes             int64          `json:"data_bytes"`
	DataMBPerSec          float64        `json:"data_mb_per_sec"`
	TotalP50Ms            float64        `json:"total_p50_ms"`
	TotalP95Ms            float64        `json:"total_p95_ms"`
	TotalP99Ms            float64        `json:"total_p99_ms"`
	CreateP50Ms           float64        `json:"create_p50_ms"`
	CreateP95Ms           float64        `json:"create_p95_ms"`
	CreateP99Ms           float64        `json:"create_p99_ms"`
	PrepareP50Ms          float64        `json:"prepare_p50_ms"`
	PrepareP95Ms          float64        `json:"prepare_p95_ms"`
	PrepareP99Ms          float64        `json:"prepare_p99_ms"`
	TransferP50Ms         float64        `json:"transfer_p50_ms"`
	TransferP95Ms         float64        `json:"transfer_p95_ms"`
	TransferP99Ms         float64        `json:"transfer_p99_ms"`
	ActivateP50Ms         float64        `json:"activate_p50_ms"`
	ActivateP95Ms         float64        `json:"activate_p95_ms"`
	ActivateP99Ms         float64        `json:"activate_p99_ms"`
	CommitP50Ms           float64        `json:"commit_p50_ms"`
	CommitP95Ms           float64        `json:"commit_p95_ms"`
	CommitP99Ms           float64        `json:"commit_p99_ms"`
	MaxInFlight           int64          `json:"max_inflight"`
	BatchCountP50         float64        `json:"batch_count_p50"`
	BatchCountP95         float64        `json:"batch_count_p95"`
	BatchCountP99         float64        `json:"batch_count_p99"`
	TransferBatchBytes    int64          `json:"transfer_batch_bytes"`
	SourceBatchHTTPP50Ms  float64        `json:"source_batch_http_p50_ms"`
	SourceBatchHTTPP95Ms  float64        `json:"source_batch_http_p95_ms"`
	SourceBatchHTTPP99Ms  float64        `json:"source_batch_http_p99_ms"`
	SourceReadP50Ms       float64        `json:"source_read_p50_ms"`
	SourceReadP95Ms       float64        `json:"source_read_p95_ms"`
	SourceReadP99Ms       float64        `json:"source_read_p99_ms"`
	SourceEncodeP50Ms     float64        `json:"source_encode_p50_ms"`
	SourceEncodeP95Ms     float64        `json:"source_encode_p95_ms"`
	SourceEncodeP99Ms     float64        `json:"source_encode_p99_ms"`
	TargetBatchHTTPP50Ms  float64        `json:"target_batch_http_p50_ms"`
	TargetBatchHTTPP95Ms  float64        `json:"target_batch_http_p95_ms"`
	TargetBatchHTTPP99Ms  float64        `json:"target_batch_http_p99_ms"`
	TargetQueueWaitP50Ms  float64        `json:"target_queue_wait_p50_ms"`
	TargetQueueWaitP95Ms  float64        `json:"target_queue_wait_p95_ms"`
	TargetQueueWaitP99Ms  float64        `json:"target_queue_wait_p99_ms"`
	TargetWriteP50Ms      float64        `json:"target_write_p50_ms"`
	TargetWriteP95Ms      float64        `json:"target_write_p95_ms"`
	TargetWriteP99Ms      float64        `json:"target_write_p99_ms"`
	TargetFsyncP50Ms      float64        `json:"target_fsync_p50_ms"`
	TargetFsyncP95Ms      float64        `json:"target_fsync_p95_ms"`
	TargetFsyncP99Ms      float64        `json:"target_fsync_p99_ms"`
	TargetStateSaveP50Ms  float64        `json:"target_state_save_p50_ms"`
	TargetStateSaveP95Ms  float64        `json:"target_state_save_p95_ms"`
	TargetStateSaveP99Ms  float64        `json:"target_state_save_p99_ms"`
	TargetPeakInflightP50 float64        `json:"target_peak_inflight_p50"`
	TargetPeakInflightP95 float64        `json:"target_peak_inflight_p95"`
	TargetPeakInflightP99 float64        `json:"target_peak_inflight_p99"`
	ErrorCounts           map[string]int `json:"error_counts,omitempty"`
	ErrorSamples          []string       `json:"error_samples,omitempty"`
}

type seedSummary struct {
	Completed int
	Failed    int
	FirstErr  string
}

func main() {
	var (
		controller      = flag.String("controller", "http://127.0.0.1:21280", "migration controller URL")
		source          = flag.String("source", "http://127.0.0.1:21281", "source agent URL")
		target          = flag.String("target", "http://127.0.0.1:21282", "target agent URL")
		workloads       = flag.Int("workloads", 100, "number of workloads to migrate")
		concurrency     = flag.Int("concurrency", 20, "concurrent migrations")
		seedConcurrency = flag.Int("seed-concurrency", 20, "concurrent source seeding workers")
		payloadBytes    = flag.Int("payload-bytes", 4096, "payload bytes per workload")
		tasks           = flag.Int("tasks", 10, "number of logical tasks represented in the ledger")
		chunkSize       = flag.Int64("chunk-size", 65536, "migration chunk size")
		timeout         = flag.Duration("timeout", 30*time.Minute, "per-migration timeout")
		globalTimeout   = flag.Duration("global-timeout", 90*time.Minute, "whole benchmark timeout")
		progressEvery   = flag.Duration("progress-interval", 5*time.Second, "progress print interval; 0 disables")
		httpMaxConns    = flag.Int("http-max-conns", 1024, "maximum idle HTTP connections per host")
		seedTimeout     = flag.Duration("seed-timeout", 60*time.Second, "per-source-write timeout")
		output          = flag.String("output", "", "optional JSON output path")
		seed            = flag.Int64("seed", 20261007, "deterministic payload seed")
	)
	flag.Parse()
	if *workloads < 1 || *concurrency < 1 || *seedConcurrency < 1 || *payloadBytes < 1 || *tasks < 1 {
		log.Fatal("workloads/concurrency/seed-concurrency/payload-bytes/tasks must be > 0")
	}
	if *chunkSize < 64<<10 || *chunkSize > 8<<20 {
		log.Fatal("chunk-size must be between 64KiB and 8MiB")
	}
	if *globalTimeout <= 0 || *timeout <= 0 {
		log.Fatal("timeout/global-timeout must be > 0")
	}

	client := newHTTPClient(*httpMaxConns)
	transferBatchBytes := int64(migrationpkg.DefaultBatchBytes)
	if raw := os.Getenv("TRANSFER_BATCH_BYTES"); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n >= 256<<10 && n <= migrationpkg.MaxBatchBytes {
			transferBatchBytes = n
		}
	}
	fmt.Printf("[loadtest] workloads=%d concurrency=%d seed_concurrency=%d payload=%dB tasks=%d chunk=%d transfer_batch=%dB db_pool_expectation=bounded\n", *workloads, *concurrency, *seedConcurrency, *payloadBytes, *tasks, *chunkSize, transferBatchBytes)

	seedStart := time.Now()
	seedResult := seedWorkloads(client, *source, *workloads, *tasks, *payloadBytes, *seedConcurrency, *seed, *seedTimeout)
	seedDuration := time.Since(seedStart)
	fmt.Printf("[loadtest] seed_complete duration=%s completed=%d failed=%d", seedDuration.Round(time.Millisecond), seedResult.Completed, seedResult.Failed)
	if seedResult.FirstErr != "" {
		fmt.Printf(" first_error=%s", seedResult.FirstErr)
	}
	fmt.Println()

	runCtx, cancel := context.WithTimeout(context.Background(), *globalTimeout)
	defer cancel()
	start := time.Now()
	jobs := make(chan int, *concurrency*2)
	results := make(chan result, *workloads)
	var wg sync.WaitGroup
	var active atomic.Int64
	var maxActive atomic.Int64
	var finished atomic.Int64
	var succeeded atomic.Int64
	var failed atomic.Int64

	progressDone := make(chan struct{})
	if *progressEvery > 0 {
		go func() {
			ticker := time.NewTicker(*progressEvery)
			defer ticker.Stop()
			for {
				select {
				case <-progressDone:
					return
				case <-ticker.C:
					finishedNow := finished.Load()
					elapsedNow := time.Since(start).Seconds()
					throughput := 0.0
					if elapsedNow > 0 {
						throughput = float64(succeeded.Load()) / elapsedNow
					}
					fmt.Printf("[loadtest] progress finished=%d/%d success=%d failed=%d active=%d throughput=%.2f/s\n", finishedNow, *workloads, succeeded.Load(), failed.Load(), active.Load(), throughput)
				}
			}
		}()
	}

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				n := active.Add(1)
				for {
					old := maxActive.Load()
					if n <= old || maxActive.CompareAndSwap(old, n) {
						break
					}
				}
				r := safeRunMigration(runCtx, client, *controller, *source, *target, idx, *chunkSize, *timeout)
				active.Add(-1)
				finished.Add(1)
				if r.ok {
					succeeded.Add(1)
				} else {
					failed.Add(1)
				}
				results <- r
			}
		}()
	}

	scheduled := 0
schedule:
	for i := 1; i <= *workloads; i++ {
		select {
		case jobs <- i:
			scheduled++
		case <-runCtx.Done():
			break schedule
		}
	}
	close(jobs)
	wg.Wait()
	close(progressDone)

	// If the global timeout fired before all jobs were scheduled, make the
	// missing jobs explicit failures instead of silently dropping them.
	for i := scheduled + 1; i <= *workloads; i++ {
		results <- result{error: "global timeout before job was scheduled", errorCode: "GLOBAL_TIMEOUT"}
		failed.Add(1)
	}
	close(results)

	elapsed := time.Since(start)
	var all []result
	for r := range results {
		all = append(all, r)
	}

	s := buildSummary(all, *workloads, *concurrency, *payloadBytes, *chunkSize, *seedConcurrency, seedDuration, elapsed, maxActive.Load())
	s.TransferBatchBytes = transferBatchBytes
	b, _ := json.MarshalIndent(s, "", "  ")
	fmt.Println(string(b))
	fmt.Printf("[loadtest] done duration=%s finished=%d succeeded=%d failed=%d\n", elapsed.Round(time.Millisecond), finished.Load(), succeeded.Load(), failed.Load())
	fmt.Printf("LOADTEST %s\n", map[bool]string{true: "PASS", false: "FAIL"}[s.Completed == s.Workloads])
	fmt.Printf("completed=%d failed=%d throughput=%.2f migrations/s p50=%.1fms p95=%.1fms p99=%.1fms data=%.2fMB data_throughput=%.2fMB/s max_inflight=%d\n",
		s.Completed, s.Failed, s.MigrationsPerSec, s.TotalP50Ms, s.TotalP95Ms, s.TotalP99Ms,
		float64(s.DataBytes)/1e6, s.DataMBPerSec, s.MaxInFlight)
	fmt.Printf("[loadtest] transfer_profile target_queue_p50=%.1fms target_write_p50=%.1fms target_fsync_p50=%.1fms target_state_save_p50=%.1fms source_read_p50=%.1fms target_peak_inflight_p50=%.1f\n",
		s.TargetQueueWaitP50Ms, s.TargetWriteP50Ms, s.TargetFsyncP50Ms, s.TargetStateSaveP50Ms, s.SourceReadP50Ms, s.TargetPeakInflightP50)
	if *output != "" {
		if err := os.WriteFile(*output, b, 0o644); err != nil {
			log.Fatalf("write output: %v", err)
		}
	}
	if s.Completed != s.Workloads {
		os.Exit(2)
	}
}

func newHTTPClient(maxIdle int) *http.Client {
	if maxIdle < 1 {
		maxIdle = 1
	}
	tr := &http.Transport{
		MaxIdleConns:        maxIdle,
		MaxIdleConnsPerHost: maxIdle,
		MaxConnsPerHost:     maxIdle,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	return &http.Client{Transport: tr, Timeout: 0}
}

func seedWorkloads(client *http.Client, source string, workloads, tasks, payloadBytes, concurrency int, seed int64, timeout time.Duration) seedSummary {
	jobs := make(chan int)
	var wg sync.WaitGroup
	var completed atomic.Int64
	var failed atomic.Int64
	var firstErrMu sync.Mutex
	var firstErr string
	payload := make([]byte, payloadBytes)
	for i := range payload {
		payload[i] = byte((int64(i) + seed) % 251)
	}
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				id := fmt.Sprintf("load-%06d", idx)
				state := map[string]any{"workload_id": id, "last_completed_task": tasks, "business_counter": tasks * 10}
				stateBytes, _ := json.Marshal(state)
				ok := true
				if err := putWithTimeout(client, source+"/v1/workloads/"+id+"/write?path=runtime/state.json", stateBytes, nil, timeout); err != nil {
					storeFirstErr(&firstErrMu, &firstErr, err)
					ok = false
				}
				if ok {
					for task := 1; task <= tasks; task++ {
						ledger := fmt.Sprintf(`{"task_id":%d,"business_counter":%d}\n`, task, task*10)
						p := fmt.Sprintf("ledger/%06d.json", task)
						if err := putWithTimeout(client, source+"/v1/workloads/"+id+"/write?path="+p, []byte(strings.TrimSpace(ledger)), nil, timeout); err != nil {
							storeFirstErr(&firstErrMu, &firstErr, err)
							ok = false
							break
						}
					}
				}
				if err := putWithTimeout(client, source+"/v1/workloads/"+id+"/write?path=data/payload.bin", payload, nil, timeout); err != nil {
					storeFirstErr(&firstErrMu, &firstErr, err)
					ok = false
				}
				if ok {
					completed.Add(1)
				} else {
					failed.Add(1)
				}
			}
		}()
	}
	go func() {
		for i := 1; i <= workloads; i++ {
			jobs <- i
		}
		close(jobs)
	}()
	wg.Wait()
	out := seedSummary{Completed: int(completed.Load()), Failed: int(failed.Load())}
	firstErrMu.Lock()
	out.FirstErr = firstErr
	firstErrMu.Unlock()
	return out
}

func storeFirstErr(mu *sync.Mutex, dst *string, err error) {
	if err == nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	if *dst == "" {
		*dst = err.Error()
	}
}

func safeRunMigration(parent context.Context, client *http.Client, controller, source, target string, idx int, chunkSize int64, timeout time.Duration) (r result) {
	defer func() {
		if v := recover(); v != nil {
			r.ok = false
			r.errorCode = "WORKER_PANIC"
			r.error = fmt.Sprintf("worker panic: %v", v)
			r.total = time.Second
		}
	}()
	return runMigration(parent, client, controller, source, target, idx, chunkSize, timeout)
}

func runMigration(parent context.Context, client *http.Client, controller, source, target string, idx int, chunkSize int64, timeout time.Duration) result {
	started := time.Now()
	id := fmt.Sprintf("load-%06d", idx)
	idem := "load-" + id
	r := result{}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	payload := map[string]any{"workload_id": id, "source_agent": source, "target_agent": target, "chunk_size": chunkSize}

	phase := func(method, url, idemKey string) (time.Duration, []byte, int, error) {
		t := time.Now()
		var h http.Header
		if idemKey != "" {
			h = http.Header{"Idempotency-Key": []string{idemKey}}
		}
		body, status, err := do(ctx, client, method, url, jsonBody(payload), h)
		return time.Since(t), body, status, err
	}
	fail := func(action string, d time.Duration, body []byte, status int, err error) result {
		r.total = time.Since(started)
		r.error = httpErr(action, status, body, err)
		r.errorCode = errorCode(body)
		if errorsContainTimeout(ctx, err) {
			r.errorCode = "TIMEOUT"
		}
		return r
	}

	d, body, status, err := phase(http.MethodPost, controller+"/v1/migrations", idem)
	r.create = d
	if err != nil || status/100 != 2 {
		return fail("create", d, body, status, err)
	}
	var m migration
	if err := json.Unmarshal(body, &m); err != nil {
		r.total = time.Since(started)
		r.error = err.Error()
		r.errorCode = "INVALID_CREATE_RESPONSE"
		return r
	}

	r.prepare, body, status, err = action(ctx, client, controller, m.ID, "prepare")
	if err != nil || status/100 != 2 {
		return fail("prepare", r.prepare, body, status, err)
	}
	r.transfer, body, status, err = action(ctx, client, controller, m.ID, "transfer")
	if err != nil || status/100 != 2 {
		return fail("transfer", r.transfer, body, status, err)
	}
	r.activate, body, status, err = action(ctx, client, controller, m.ID, "activate")
	if err != nil || status/100 != 2 {
		return fail("activate", r.activate, body, status, err)
	}
	r.commit, body, status, err = action(ctx, client, controller, m.ID, "commit")
	if err != nil || status/100 != 2 {
		return fail("commit", r.commit, body, status, err)
	}
	var final migration
	if err := json.Unmarshal(body, &final); err != nil {
		r.total = time.Since(started)
		r.error = err.Error()
		r.errorCode = "INVALID_COMMIT_RESPONSE"
		return r
	}
	if final.State != "COMMITTED" {
		r.total = time.Since(started)
		r.error = "final state is " + final.State
		r.errorCode = "FINAL_STATE_NOT_COMMITTED"
		return r
	}
	r.ok = true
	r.bytes = final.Stats.BytesTransferred
	r.chunks = final.Stats.ChunksTransferred
	r.batches = final.Stats.BatchesTransferred
	r.sourceBatchHTTPMs = final.Stats.SourceBatchHTTPMs
	r.sourceReadMs = final.Stats.SourceReadMs
	r.sourceEncodeMs = final.Stats.SourceEncodeMs
	r.targetBatchHTTPMs = final.Stats.TargetBatchHTTPMs
	r.targetQueueWaitMs = final.Stats.TargetQueueWaitMs
	r.targetWriteMs = final.Stats.TargetWriteMs
	r.targetFsyncMs = final.Stats.TargetFsyncMs
	r.targetStateSaveMs = final.Stats.TargetStateSaveMs
	r.targetBatchPeakIn = final.Stats.TargetBatchPeakIn
	r.total = time.Since(started)
	return r
}

func errorsContainTimeout(ctx context.Context, err error) bool {
	return ctx.Err() != nil && err != nil
}

func action(ctx context.Context, client *http.Client, controller, id, action string) (time.Duration, []byte, int, error) {
	t := time.Now()
	body, status, err := do(ctx, client, http.MethodPost, controller+"/v1/migrations/"+id+"/"+action, nil, nil)
	return time.Since(t), body, status, err
}

func jsonBody(v any) io.Reader {
	b, _ := json.Marshal(v)
	return bytes.NewReader(b)
}

func putWithTimeout(client *http.Client, url string, body []byte, headers http.Header, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	b, status, err := do(ctx, client, http.MethodPut, url, bytes.NewReader(body), headers)
	if err != nil {
		return err
	}
	if status/100 != 2 {
		return fmt.Errorf("PUT %s status=%d body=%s", url, status, strings.TrimSpace(string(b)))
	}
	return nil
}

func do(ctx context.Context, client *http.Client, method, url string, body io.Reader, headers http.Header) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, vals := range headers {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, resp.StatusCode, readErr
	}
	return data, resp.StatusCode, nil
}

func httpErr(action string, status int, body []byte, err error) string {
	if err != nil {
		return action + ": " + err.Error()
	}
	return fmt.Sprintf("%s: status=%d body=%s", action, status, strings.TrimSpace(string(body)))
}

func errorCode(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var v struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(body, &v) == nil && v.Code != "" {
		return v.Code
	}
	return ""
}

func buildSummary(results []result, workloads, concurrency, payloadBytes int, chunkSize int64, seedConcurrency int, seedDuration, elapsed time.Duration, maxInFlight int64) summary {
	s := summary{Workloads: workloads, Concurrency: concurrency, PayloadBytes: payloadBytes, ChunkSize: chunkSize, SeedConcurrency: seedConcurrency, SeedDurationMs: seedDuration.Milliseconds(), DurationMs: elapsed.Milliseconds(), MaxInFlight: maxInFlight, ErrorCounts: map[string]int{}}
	var total, create, prepare, transfer, activate, commit, batches []float64
	var sourceHTTP, sourceRead, sourceEncode, targetHTTP, targetQueue, targetWrite, targetFsync, targetStateSave, targetPeak []float64
	var bytes int64
	errorsSeen := make([]string, 0, 8)
	for _, r := range results {
		if r.ok {
			s.Completed++
			total = append(total, float64(r.total)/float64(time.Millisecond))
			create = append(create, float64(r.create)/float64(time.Millisecond))
			prepare = append(prepare, float64(r.prepare)/float64(time.Millisecond))
			transfer = append(transfer, float64(r.transfer)/float64(time.Millisecond))
			activate = append(activate, float64(r.activate)/float64(time.Millisecond))
			commit = append(commit, float64(r.commit)/float64(time.Millisecond))
			batches = append(batches, float64(r.batches))
			sourceHTTP = append(sourceHTTP, float64(r.sourceBatchHTTPMs))
			sourceRead = append(sourceRead, float64(r.sourceReadMs))
			sourceEncode = append(sourceEncode, float64(r.sourceEncodeMs))
			targetHTTP = append(targetHTTP, float64(r.targetBatchHTTPMs))
			targetQueue = append(targetQueue, float64(r.targetQueueWaitMs))
			targetWrite = append(targetWrite, float64(r.targetWriteMs))
			targetFsync = append(targetFsync, float64(r.targetFsyncMs))
			targetStateSave = append(targetStateSave, float64(r.targetStateSaveMs))
			targetPeak = append(targetPeak, float64(r.targetBatchPeakIn))
			bytes += r.bytes
		} else {
			s.Failed++
			code := r.errorCode
			if code == "" {
				code = "UNKNOWN"
			}
			s.ErrorCounts[code]++
			if len(errorsSeen) < 8 && r.error != "" {
				errorsSeen = append(errorsSeen, r.error)
			}
		}
	}
	if len(s.ErrorCounts) == 0 {
		s.ErrorCounts = nil
	}
	if workloads > 0 {
		s.SuccessRate = float64(s.Completed) / float64(workloads)
	}
	if elapsed > 0 {
		s.MigrationsPerSec = float64(s.Completed) / elapsed.Seconds()
		s.DataMBPerSec = float64(bytes) / (1e6 * elapsed.Seconds())
	}
	s.DataBytes = bytes
	s.TotalP50Ms = percentile(total, .50)
	s.TotalP95Ms = percentile(total, .95)
	s.TotalP99Ms = percentile(total, .99)
	s.CreateP50Ms, s.CreateP95Ms, s.CreateP99Ms = percentile(create, .50), percentile(create, .95), percentile(create, .99)
	s.PrepareP50Ms, s.PrepareP95Ms, s.PrepareP99Ms = percentile(prepare, .50), percentile(prepare, .95), percentile(prepare, .99)
	s.TransferP50Ms, s.TransferP95Ms, s.TransferP99Ms = percentile(transfer, .50), percentile(transfer, .95), percentile(transfer, .99)
	s.ActivateP50Ms, s.ActivateP95Ms, s.ActivateP99Ms = percentile(activate, .50), percentile(activate, .95), percentile(activate, .99)
	s.CommitP50Ms, s.CommitP95Ms, s.CommitP99Ms = percentile(commit, .50), percentile(commit, .95), percentile(commit, .99)
	s.BatchCountP50, s.BatchCountP95, s.BatchCountP99 = percentile(batches, .50), percentile(batches, .95), percentile(batches, .99)
	s.SourceBatchHTTPP50Ms, s.SourceBatchHTTPP95Ms, s.SourceBatchHTTPP99Ms = percentile(sourceHTTP, .50), percentile(sourceHTTP, .95), percentile(sourceHTTP, .99)
	s.SourceReadP50Ms, s.SourceReadP95Ms, s.SourceReadP99Ms = percentile(sourceRead, .50), percentile(sourceRead, .95), percentile(sourceRead, .99)
	s.SourceEncodeP50Ms, s.SourceEncodeP95Ms, s.SourceEncodeP99Ms = percentile(sourceEncode, .50), percentile(sourceEncode, .95), percentile(sourceEncode, .99)
	s.TargetBatchHTTPP50Ms, s.TargetBatchHTTPP95Ms, s.TargetBatchHTTPP99Ms = percentile(targetHTTP, .50), percentile(targetHTTP, .95), percentile(targetHTTP, .99)
	s.TargetQueueWaitP50Ms, s.TargetQueueWaitP95Ms, s.TargetQueueWaitP99Ms = percentile(targetQueue, .50), percentile(targetQueue, .95), percentile(targetQueue, .99)
	s.TargetWriteP50Ms, s.TargetWriteP95Ms, s.TargetWriteP99Ms = percentile(targetWrite, .50), percentile(targetWrite, .95), percentile(targetWrite, .99)
	s.TargetFsyncP50Ms, s.TargetFsyncP95Ms, s.TargetFsyncP99Ms = percentile(targetFsync, .50), percentile(targetFsync, .95), percentile(targetFsync, .99)
	s.TargetStateSaveP50Ms, s.TargetStateSaveP95Ms, s.TargetStateSaveP99Ms = percentile(targetStateSave, .50), percentile(targetStateSave, .95), percentile(targetStateSave, .99)
	s.TargetPeakInflightP50, s.TargetPeakInflightP95, s.TargetPeakInflightP99 = percentile(targetPeak, .50), percentile(targetPeak, .95), percentile(targetPeak, .99)
	s.ErrorSamples = errorsSeen
	return s
}

func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sort.Float64s(xs)
	if len(xs) == 1 {
		return xs[0]
	}
	rank := p * float64(len(xs)-1)
	lo := int(rank)
	hi := lo + 1
	if hi >= len(xs) {
		return xs[lo]
	}
	frac := rank - float64(lo)
	return xs[lo] + (xs[hi]-xs[lo])*frac
}
