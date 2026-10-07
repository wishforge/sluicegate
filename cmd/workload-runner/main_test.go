package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestHighestContiguousTask(t *testing.T) {
	root := t.TempDir()
	ledger := filepath.Join(root, "ledger")
	if err := os.MkdirAll(ledger, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 3; i++ {
		b, _ := json.Marshal(TaskRecord{TaskID: i, Sequence: i})
		if err := os.WriteFile(filepath.Join(root, taskPath(i)), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := highestContiguousTask(root)
	if err != nil {
		t.Fatal(err)
	}
	if got != 3 {
		t.Fatalf("got %d, want 3", got)
	}
}

func TestHighestContiguousTaskDetectsGap(t *testing.T) {
	root := t.TempDir()
	ledger := filepath.Join(root, "ledger")
	if err := os.MkdirAll(ledger, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, i := range []int64{1, 3} {
		b, _ := json.Marshal(TaskRecord{TaskID: i, Sequence: i})
		if err := os.WriteFile(filepath.Join(root, taskPath(i)), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := highestContiguousTask(root); err == nil {
		t.Fatal("expected gap error")
	}
}
