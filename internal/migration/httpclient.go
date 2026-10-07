package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type AgentClient struct {
	HTTP *http.Client
	Log  interface {
		Warn(string, ...any)
		Debug(string, ...any)
	}
}

func NewAgentClient(log interface {
	Warn(string, ...any)
	Debug(string, ...any)
}) *AgentClient {
	return &AgentClient{HTTP: &http.Client{Timeout: 90 * time.Second}, Log: log}
}

func (c *AgentClient) doJSON(ctx context.Context, method, endpoint string, headers map[string]string, body any, out any) error {
	var bodyBytes []byte
	if body != nil {
		bb, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyBytes = bb
	}
	for attempt := 1; attempt <= 3; attempt++ {
		var b io.Reader
		if body != nil {
			b = bytes.NewReader(bodyBytes)
		}
		req, err := http.NewRequestWithContext(ctx, method, endpoint, b)
		if err != nil {
			return err
		}
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		if req.Header.Get("X-Trace-ID") == "" && req.Header.Get("X-Request-ID") != "" {
			req.Header.Set("X-Trace-ID", req.Header.Get("X-Request-ID"))
		}
		resp, err := c.HTTP.Do(req)
		if err != nil {
			if attempt < 3 {
				sleepBackoff(attempt)
				continue
			}
			return err
		}
		defer resp.Body.Close()
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 500 && attempt < 3 {
			sleepBackoff(attempt)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return parseAgentError(resp.StatusCode, data)
		}
		if out != nil && len(data) > 0 {
			return json.Unmarshal(data, out)
		}
		return nil
	}
	return errors.New("unreachable")
}

func (c *AgentClient) postJSON(ctx context.Context, endpoint string, headers map[string]string, body, out any) error {
	return c.doJSON(ctx, http.MethodPost, endpoint, headers, body, out)
}

func (c *AgentClient) getJSON(ctx context.Context, endpoint string, headers map[string]string, out any) error {
	return c.doJSON(ctx, http.MethodGet, endpoint, headers, nil, out)
}

func (c *AgentClient) Fence(ctx context.Context, agent, workload, migrationID string, requestID string, epoch uint64) (AgentFenceResponse, error) {
	var out AgentFenceResponse
	err := c.postJSON(ctx, strings.TrimRight(agent, "/")+"/v1/workloads/"+url.PathEscape(workload)+"/fence", map[string]string{"X-Migration-ID": migrationID, "X-Request-ID": requestID, "X-Fence-Epoch": strconv.FormatUint(epoch, 10)}, nil, &out)
	return out, err
}

func (c *AgentClient) Release(ctx context.Context, agent, workload, migrationID, token, requestID string) error {
	return c.postJSON(ctx, strings.TrimRight(agent, "/")+"/v1/workloads/"+url.PathEscape(workload)+"/release", map[string]string{"X-Migration-ID": migrationID, "X-Fence-Token": token, "X-Request-ID": requestID}, nil, nil)
}

func (c *AgentClient) Checkpoint(ctx context.Context, agent, workload, migrationID, token, requestID string, chunkSize int64) (CheckpointResponse, error) {
	var out CheckpointResponse
	q := url.Values{}
	q.Set("chunk_size", strconv.FormatInt(chunkSize, 10))
	endpoint := strings.TrimRight(agent, "/") + "/v1/workloads/" + url.PathEscape(workload) + "/checkpoint?" + q.Encode()
	err := c.postJSON(ctx, endpoint, map[string]string{"X-Migration-ID": migrationID, "X-Fence-Token": token, "X-Request-ID": requestID}, nil, &out)
	return out, err
}

func (c *AgentClient) InitTarget(ctx context.Context, agent, migrationID, workload string, files []File, requestID string) error {
	return c.postJSON(ctx, strings.TrimRight(agent, "/")+"/v1/migrations/"+url.PathEscape(migrationID)+"/init", map[string]string{"X-Request-ID": requestID}, map[string]any{"workload_id": workload, "files": files}, nil)
}

func (c *AgentClient) Progress(ctx context.Context, agent, migrationID, requestID string) (ProgressResponse, error) {
	var out ProgressResponse
	err := c.getJSON(ctx, strings.TrimRight(agent, "/")+"/v1/migrations/"+url.PathEscape(migrationID)+"/progress", map[string]string{"X-Request-ID": requestID}, &out)
	return out, err
}

type BatchTransferHTTPStats struct {
	ServerReadMs       int64
	ServerEncodeMs     int64
	TargetQueueWaitMs  int64
	TargetWriteMs      int64
	TargetFsyncMs      int64
	TargetStateSaveMs  int64
	TargetPeakInflight int64
}

func headerInt64(h http.Header, key string) int64 {
	v, _ := strconv.ParseInt(h.Get(key), 10, 64)
	return v
}

func (c *AgentClient) FetchChunkBatch(ctx context.Context, agent, workload, checkpoint, path string, start, count int, chunkSize, maxBytes int64, migrationID, token, requestID string) ([]BatchChunk, error) {
	chunks, _, err := c.FetchChunkBatchWithStats(ctx, agent, workload, checkpoint, path, start, count, chunkSize, maxBytes, migrationID, token, requestID)
	return chunks, err
}

func (c *AgentClient) FetchChunkBatchWithStats(ctx context.Context, agent, workload, checkpoint, path string, start, count int, chunkSize, maxBytes int64, migrationID, token, requestID string) ([]BatchChunk, BatchTransferHTTPStats, error) {
	q := url.Values{}
	q.Set("path", path)
	q.Set("start", strconv.Itoa(start))
	q.Set("count", strconv.Itoa(count))
	q.Set("chunk_size", strconv.FormatInt(chunkSize, 10))
	q.Set("max_bytes", strconv.FormatInt(maxBytes, 10))
	endpoint := fmt.Sprintf("%s/v1/workloads/%s/checkpoints/%s/chunks?%s", strings.TrimRight(agent, "/"), url.PathEscape(workload), url.PathEscape(checkpoint), q.Encode())
	for attempt := 1; attempt <= 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, BatchTransferHTTPStats{}, err
		}
		req.Header.Set("X-Migration-ID", migrationID)
		req.Header.Set("X-Fence-Token", token)
		req.Header.Set("X-Request-ID", requestID)
		req.Header.Set("X-Trace-ID", requestID)
		resp, err := c.HTTP.Do(req)
		if err != nil {
			if attempt < 3 {
				sleepBackoff(attempt)
				continue
			}
			return nil, BatchTransferHTTPStats{}, err
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, MaxBatchBytes+1))
		stats := BatchTransferHTTPStats{
			ServerReadMs:   headerInt64(resp.Header, "X-Source-Batch-Read-Ms"),
			ServerEncodeMs: headerInt64(resp.Header, "X-Source-Batch-Encode-Ms"),
		}
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, stats, readErr
		}
		if resp.StatusCode >= 500 && attempt < 3 {
			sleepBackoff(attempt)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, stats, parseAgentError(resp.StatusCode, data)
		}
		chunks, err := DecodeBatch(bytes.NewReader(data), MaxBatchBytes)
		return chunks, stats, err
	}
	return nil, BatchTransferHTTPStats{}, errors.New("unreachable")
}

func (c *AgentClient) PutChunkBatch(ctx context.Context, agent, migrationID string, chunks []BatchChunk, requestID string) error {
	_, err := c.PutChunkBatchWithStats(ctx, agent, migrationID, chunks, requestID)
	return err
}

func (c *AgentClient) PutChunkBatchWithStats(ctx context.Context, agent, migrationID string, chunks []BatchChunk, requestID string) (BatchTransferHTTPStats, error) {
	body, err := EncodeBatch(chunks)
	if err != nil {
		return BatchTransferHTTPStats{}, err
	}
	endpoint := fmt.Sprintf("%s/v1/migrations/%s/chunks", strings.TrimRight(agent, "/"), url.PathEscape(migrationID))
	for attempt := 1; attempt <= 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(body))
		if err != nil {
			return BatchTransferHTTPStats{}, err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Content-Length", strconv.Itoa(len(body)))
		req.Header.Set("X-Request-ID", requestID)
		req.Header.Set("X-Trace-ID", requestID)
		resp, err := c.HTTP.Do(req)
		if err != nil {
			if attempt < 3 {
				sleepBackoff(attempt)
				continue
			}
			return BatchTransferHTTPStats{}, err
		}
		stats := BatchTransferHTTPStats{
			TargetQueueWaitMs:  headerInt64(resp.Header, "X-Target-Batch-Queue-Wait-Ms"),
			TargetWriteMs:      headerInt64(resp.Header, "X-Target-Batch-Write-Ms"),
			TargetFsyncMs:      headerInt64(resp.Header, "X-Target-Batch-Fsync-Ms"),
			TargetStateSaveMs:  headerInt64(resp.Header, "X-Target-Batch-State-Save-Ms"),
			TargetPeakInflight: headerInt64(resp.Header, "X-Target-Batch-Peak-Inflight"),
		}
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			return stats, readErr
		}
		if resp.StatusCode >= 500 && attempt < 3 {
			sleepBackoff(attempt)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return stats, parseAgentError(resp.StatusCode, respBody)
		}
		return stats, nil
	}
	return BatchTransferHTTPStats{}, errors.New("unreachable")
}

func (c *AgentClient) FetchChunk(ctx context.Context, agent, workload, checkpoint, path string, index int, chunkSize int64, migrationID, token, requestID string) ([]byte, map[string]string, error) {
	q := url.Values{}
	q.Set("path", path)
	q.Set("index", strconv.Itoa(index))
	q.Set("chunk_size", strconv.FormatInt(chunkSize, 10))
	endpoint := fmt.Sprintf("%s/v1/workloads/%s/checkpoints/%s/chunk?%s", strings.TrimRight(agent, "/"), url.PathEscape(workload), url.PathEscape(checkpoint), q.Encode())
	for attempt := 1; attempt <= 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, nil, err
		}
		req.Header.Set("X-Migration-ID", migrationID)
		req.Header.Set("X-Fence-Token", token)
		req.Header.Set("X-Request-ID", requestID)
		req.Header.Set("X-Trace-ID", requestID)
		resp, err := c.HTTP.Do(req)
		if err != nil {
			if attempt < 3 {
				sleepBackoff(attempt)
				continue
			}
			return nil, nil, err
		}
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, chunkSize+1))
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, nil, readErr
		}
		if resp.StatusCode >= 500 && attempt < 3 {
			sleepBackoff(attempt)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, nil, parseAgentError(resp.StatusCode, data)
		}
		headers := map[string]string{"sha256": resp.Header.Get("X-Chunk-SHA256"), "index": resp.Header.Get("X-Chunk-Index"), "offset": resp.Header.Get("X-Chunk-Offset"), "size": resp.Header.Get("X-Chunk-Size")}
		return data, headers, nil
	}
	return nil, nil, errors.New("unreachable")
}

func (c *AgentClient) PutChunk(ctx context.Context, agent, migrationID, path string, index int, offset int64, data []byte, sha string, requestID string) error {
	q := url.Values{}
	q.Set("path", path)
	q.Set("index", strconv.Itoa(index))
	endpoint := fmt.Sprintf("%s/v1/migrations/%s/chunk?%s", strings.TrimRight(agent, "/"), url.PathEscape(migrationID), q.Encode())
	for attempt := 1; attempt <= 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Chunk-Offset", strconv.FormatInt(offset, 10))
		req.Header.Set("X-Chunk-Size", strconv.Itoa(len(data)))
		req.Header.Set("X-Chunk-SHA256", sha)
		req.Header.Set("X-Request-ID", requestID)
		req.Header.Set("X-Trace-ID", requestID)
		resp, err := c.HTTP.Do(req)
		if err != nil {
			if attempt < 3 {
				sleepBackoff(attempt)
				continue
			}
			return err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 500 && attempt < 3 {
			sleepBackoff(attempt)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return parseAgentError(resp.StatusCode, body)
		}
		return nil
	}
	return errors.New("unreachable")
}

func (c *AgentClient) Activate(ctx context.Context, agent, migrationID, requestID string) error {
	return c.postJSON(ctx, strings.TrimRight(agent, "/")+"/v1/migrations/"+url.PathEscape(migrationID)+"/activate", map[string]string{"X-Request-ID": requestID}, nil, nil)
}
func (c *AgentClient) CommitTarget(ctx context.Context, agent, migrationID, requestID string) error {
	return c.postJSON(ctx, strings.TrimRight(agent, "/")+"/v1/migrations/"+url.PathEscape(migrationID)+"/commit", map[string]string{"X-Request-ID": requestID}, nil, nil)
}
func (c *AgentClient) RollbackTarget(ctx context.Context, agent, migrationID, requestID string) error {
	return c.postJSON(ctx, strings.TrimRight(agent, "/")+"/v1/migrations/"+url.PathEscape(migrationID)+"/rollback", map[string]string{"X-Request-ID": requestID}, nil, nil)
}

func parseAgentError(status int, b []byte) error {
	var x struct {
		Error ErrorResponse `json:"error"`
	}
	if json.Unmarshal(b, &x) == nil && x.Error.Code != "" {
		return fmt.Errorf("agent http %d %s: %s", status, x.Error.Code, x.Error.Message)
	}
	return fmt.Errorf("agent http %d: %s", status, strings.TrimSpace(string(b)))
}

func sleepBackoff(attempt int) {
	time.Sleep(time.Duration(attempt*50+rand.Intn(25)) * time.Millisecond)
}
