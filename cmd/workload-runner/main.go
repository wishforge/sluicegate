package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/wishforge/sluicegate/internal/common"
)

const (
	roleSource = "source"
	roleTarget = "target"
)

type BusinessState struct {
	WorkloadID        string    `json:"workload_id"`
	LastCompletedTask int64     `json:"last_completed_task"`
	BusinessCounter   int64     `json:"business_counter"`
	Owner             string    `json:"owner"`
	UpdatedAt         time.Time `json:"updated_at"`
}

type TaskRecord struct {
	WorkloadID string    `json:"workload_id"`
	TaskID     int64     `json:"task_id"`
	Sequence   int64     `json:"sequence"`
	Owner      string    `json:"owner"`
	CreatedAt  time.Time `json:"created_at"`
}

type runner struct {
	role        string
	workloadID  string
	totalTasks  int64
	interval    time.Duration
	sourceAgent string
	targetRoot  string
	log         *common.SLogger
	httpClient  *http.Client
}

func main() {
	log := common.NewLogger("stateful-workload-runner")
	r, err := newRunner(log)
	if err != nil {
		log.Error("startup_failed", "error_code", "INVALID_CONFIG", "error", err.Error())
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("workload_started", "role", r.role, "workload_id", r.workloadID, "total_tasks", r.totalTasks, "interval_ms", r.interval.Milliseconds())
	var runErr error
	switch r.role {
	case roleSource:
		runErr = r.runSource(ctx)
	case roleTarget:
		runErr = r.runTarget(ctx)
	default:
		runErr = fmt.Errorf("unsupported role %q", r.role)
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		log.Error("workload_failed", "error_code", "WORKLOAD_RUN_FAILED", "error", runErr.Error(), "role", r.role, "workload_id", r.workloadID)
		os.Exit(1)
	}
}

func newRunner(log *common.SLogger) (*runner, error) {
	role := strings.ToLower(getenv("WORKLOAD_ROLE", roleSource))
	workloadID := getenv("WORKLOAD_ID", "agent-001")
	total, err := strconv.ParseInt(getenv("TOTAL_TASKS", "120"), 10, 64)
	if err != nil || total <= 0 {
		return nil, errors.New("TOTAL_TASKS must be > 0")
	}
	ms, err := strconv.Atoi(getenv("TASK_INTERVAL_MS", "30"))
	if err != nil || ms <= 0 {
		return nil, errors.New("TASK_INTERVAL_MS must be > 0")
	}
	return &runner{
		role:        role,
		workloadID:  workloadID,
		totalTasks:  total,
		interval:    time.Duration(ms) * time.Millisecond,
		sourceAgent: strings.TrimRight(getenv("SOURCE_AGENT", "http://127.0.0.1:8081"), "/"),
		targetRoot:  getenv("TARGET_ROOT", "./target-data"),
		log:         log,
		httpClient:  &http.Client{Timeout: 10 * time.Second},
	}, nil
}

func (r *runner) runSource(ctx context.Context) error {
	var next int64 = 1
	for next <= r.totalTasks {
		state := BusinessState{
			WorkloadID:        r.workloadID,
			LastCompletedTask: next,
			BusinessCounter:   next * 10,
			Owner:             roleSource,
			UpdatedAt:         time.Now().UTC(),
		}
		task := TaskRecord{WorkloadID: r.workloadID, TaskID: next, Sequence: next, Owner: roleSource, CreatedAt: time.Now().UTC()}
		// Ledger is the durable business fact. The summary state is written second.
		if err := r.putJSON(ctx, "/v1/workloads/"+r.workloadID+"/write?path="+taskPath(next), task); err != nil {
			if isFencedError(err) {
				r.log.Info("workload_fenced_stop", "role", roleSource, "workload_id", r.workloadID, "last_completed_task", next-1, "error_code", "WORKLOAD_FENCED")
				return nil
			}
			return err
		}
		if err := r.putJSON(ctx, "/v1/workloads/"+r.workloadID+"/write?path=runtime/state.json", state); err != nil {
			if isFencedError(err) {
				r.log.Info("workload_fenced_after_task", "role", roleSource, "workload_id", r.workloadID, "last_completed_task", next, "error_code", "WORKLOAD_FENCED")
				return nil
			}
			return err
		}
		if next%10 == 0 || next == 1 {
			r.log.Info("workload_task_completed", "role", roleSource, "workload_id", r.workloadID, "task_id", next, "business_counter", next*10)
		} else {
			r.log.Debug("workload_task_completed", "role", roleSource, "workload_id", r.workloadID, "task_id", next, "business_counter", next*10)
		}
		next++
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.interval):
		}
	}
	r.log.Info("workload_completed", "role", roleSource, "workload_id", r.workloadID, "last_completed_task", r.totalTasks)
	return nil
}

func (r *runner) runTarget(ctx context.Context) error {
	workloadRoot := filepath.Join(r.targetRoot, "workloads", r.workloadID)
	statePath := filepath.Join(workloadRoot, "runtime", "state.json")
	for {
		if _, err := os.Stat(filepath.Join(workloadRoot, ".migration-activation.json")); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	state, err := readState(statePath)
	if err != nil {
		return fmt.Errorf("restore state: %w", err)
	}
	if state.LastCompletedTask < 0 || state.LastCompletedTask >= r.totalTasks {
		return fmt.Errorf("invalid restored task %d for total %d", state.LastCompletedTask, r.totalTasks)
	}
	highestLedger, err := highestContiguousTask(workloadRoot)
	if err != nil {
		return err
	}
	if state.LastCompletedTask > highestLedger {
		return fmt.Errorf("summary state %d is ahead of durable ledger %d", state.LastCompletedTask, highestLedger)
	}
	restoredFrom := highestLedger
	r.log.Info("workload_restored", "role", roleTarget, "workload_id", r.workloadID, "restored_from_task", restoredFrom, "summary_state_task", state.LastCompletedTask, "business_counter", restoredFrom*10)

	for next := restoredFrom + 1; next <= r.totalTasks; next++ {
		state = BusinessState{WorkloadID: r.workloadID, LastCompletedTask: next, BusinessCounter: next * 10, Owner: roleTarget, UpdatedAt: time.Now().UTC()}
		task := TaskRecord{WorkloadID: r.workloadID, TaskID: next, Sequence: next, Owner: roleTarget, CreatedAt: time.Now().UTC()}
		if err := writeJSONAtomic(filepath.Join(workloadRoot, taskPath(next)), task, 0o644); err != nil {
			return err
		}
		if err := writeJSONAtomic(statePath, state, 0o644); err != nil {
			return err
		}
		if next%10 == 0 || next == state.LastCompletedTask+1 || next == r.totalTasks {
			r.log.Info("workload_task_completed", "role", roleTarget, "workload_id", r.workloadID, "task_id", next, "business_counter", next*10)
		} else {
			r.log.Debug("workload_task_completed", "role", roleTarget, "workload_id", r.workloadID, "task_id", next, "business_counter", next*10)
		}
		if next == r.totalTasks {
			r.log.Info("workload_completed", "role", roleTarget, "workload_id", r.workloadID, "last_completed_task", next)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.interval):
		}
	}
	return nil
}

func (r *runner) putJSON(ctx context.Context, path string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, r.sourceAgent+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", newID("task"))
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusLocked {
			return fmt.Errorf("WORKLOAD_FENCED: %s", strings.TrimSpace(string(respBody)))
		}
		return fmt.Errorf("source write HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func highestContiguousTask(workloadRoot string) (int64, error) {
	ledger := filepath.Join(workloadRoot, "ledger")
	entries, err := os.ReadDir(ledger)
	if err != nil {
		return 0, err
	}
	seen := map[int64]bool{}
	var highest int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		base := strings.TrimSuffix(e.Name(), ".json")
		n, err := strconv.ParseInt(base, 10, 64)
		if err != nil || n <= 0 {
			return 0, fmt.Errorf("invalid ledger entry %q", e.Name())
		}
		var rec TaskRecord
		b, err := os.ReadFile(filepath.Join(ledger, e.Name()))
		if err != nil {
			return 0, err
		}
		if err := json.Unmarshal(b, &rec); err != nil {
			return 0, fmt.Errorf("invalid ledger entry %q: %w", e.Name(), err)
		}
		if rec.TaskID != n || rec.Sequence != n {
			return 0, fmt.Errorf("ledger task mismatch in %q", e.Name())
		}
		seen[n] = true
		if n > highest {
			highest = n
		}
	}
	for i := int64(1); i <= highest; i++ {
		if !seen[i] {
			return 0, fmt.Errorf("ledger gap at task %d", i)
		}
	}
	return highest, nil
}

func readState(path string) (BusinessState, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return BusinessState{}, err
	}
	var s BusinessState
	if err := json.Unmarshal(b, &s); err != nil {
		return BusinessState{}, err
	}
	return s, nil
}

func writeJSONAtomic(path string, v any, mode os.FileMode) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
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
	return os.Rename(tmp, path)
}

func taskPath(task int64) string {
	return fmt.Sprintf("ledger/%08d.json", task)
}

func isFencedError(err error) bool { return strings.Contains(err.Error(), "WORKLOAD_FENCED") }

func newID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

func getenv(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
