package common

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

type ctxKey string

const (
	RequestIDKey ctxKey = "request_id"
	TraceIDKey   ctxKey = "trace_id"
)

func RequestID(r *http.Request) string {
	v := r.Context().Value(RequestIDKey)
	s, _ := v.(string)
	return s
}

func TraceID(r *http.Request) string {
	v := r.Context().Value(TraceIDKey)
	s, _ := v.(string)
	return s
}

func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func Error(w http.ResponseWriter, status int, code string, err error) {
	JSON(w, status, map[string]any{"error": map[string]any{"code": code, "message": err.Error()}})
}

func DecodeJSON(r *http.Request, dst any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.Contains(ct, "application/json") {
		return errors.New("content-type must be application/json")
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, 4<<20))
	if err := dec.Decode(dst); err != nil {
		return err
	}
	return nil
}

type StatusWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (w *StatusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *StatusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	return n, err
}

func (w *StatusWriter) statusOrOK() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

type Logger interface {
	Info(string, ...any)
	Warn(string, ...any)
	Error(string, ...any)
	Debug(string, ...any)
}

func WrapRequest(next http.Handler, logger Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rid := r.Header.Get("X-Request-ID")
		if strings.TrimSpace(rid) == "" {
			rid = ID("req")
		}
		tid := r.Header.Get("X-Trace-ID")
		if strings.TrimSpace(tid) == "" {
			tid = ID("trace")
		}
		ctx := context.WithValue(r.Context(), RequestIDKey, rid)
		ctx = context.WithValue(ctx, TraceIDKey, tid)
		r = r.WithContext(ctx)
		w.Header().Set("X-Request-ID", rid)
		sw := &StatusWriter{ResponseWriter: w}
		next.ServeHTTP(sw, r)
		logger.Info("http_request", "request_id", rid, "trace_id", tid, "method", r.Method, "path", r.URL.Path, "status", sw.statusOrOK(), "bytes", sw.bytes, "duration_ms", time.Since(start).Milliseconds())
	})
}
