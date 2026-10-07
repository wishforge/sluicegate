package main

import (
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	got := percentile([]float64{1, 2, 3, 4, 5}, .50)
	if got != 3 {
		t.Fatalf("p50=%v want 3", got)
	}
	got = percentile([]float64{1, 2, 3, 4, 5}, .95)
	if got < 4.7 || got > 4.8 {
		t.Fatalf("p95=%v want ~4.8", got)
	}
}

func TestBuildSummary(t *testing.T) {
	results := []result{
		{ok: true, total: 100 * time.Millisecond, create: 10 * time.Millisecond, prepare: 20 * time.Millisecond, transfer: 40 * time.Millisecond, activate: 20 * time.Millisecond, commit: 10 * time.Millisecond, bytes: 1000},
		{ok: true, total: 200 * time.Millisecond, create: 20 * time.Millisecond, prepare: 40 * time.Millisecond, transfer: 80 * time.Millisecond, activate: 40 * time.Millisecond, commit: 20 * time.Millisecond, bytes: 2000},
		{ok: false, error: "boom"},
	}
	s := buildSummary(results, 3, 2, 4096, 65536, 2, 50*time.Millisecond, time.Second, 2)
	if s.Completed != 2 || s.Failed != 1 {
		t.Fatalf("completed=%d failed=%d", s.Completed, s.Failed)
	}
	if s.MaxInFlight != 2 {
		t.Fatalf("max_inflight=%d", s.MaxInFlight)
	}
	if s.DataBytes != 3000 {
		t.Fatalf("data_bytes=%d", s.DataBytes)
	}
}
