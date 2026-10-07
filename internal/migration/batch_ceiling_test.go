package migration

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

// Finding 2: the documented maximum TRANSFER_BATCH_BYTES could never be used.
//
// MaxBatchBytes used to serve two different limits:
//
//   - service.go accepted a configured batch size up to MaxBatchBytes
//   - batch.go rejects an encoded batch whose size exceeds MaxBatchBytes
//
// Encoding adds BatchHeaderSize plus the path per chunk, so a configured size
// equal to MaxBatchBytes always encoded to something larger and failed with
// "encoded batch exceeds N bytes" for every workload.
//
// The fix splits the two: MaxConfiguredBatchBytes caps TRANSFER_BATCH_BYTES
// with headroom for encoding, while MaxBatchBytes stays the wire-format
// ceiling so already-persisted batches remain readable. These tests pin both
// halves so the two constants cannot drift back together.

func TestConfiguredCeilingEncodesSuccessfully(t *testing.T) {
	const chunkSize = 64 << 10
	const path = "data.bin"

	sum := fmt.Sprintf("%x", sha256.Sum256([]byte("seed")))
	data := make([]byte, chunkSize)
	chunks := make([]BatchChunk, 0, MaxBatchChunks)
	var payload int64
	for payload < MaxConfiguredBatchBytes {
		chunks = append(chunks, BatchChunk{
			Path: path, Index: len(chunks), Offset: payload, SHA256: sum, Data: data,
		})
		payload += chunkSize
	}

	// A batch built to the configured ceiling must encode. Before the fix this
	// failed with "encoded batch exceeds 16777216 bytes".
	encoded, err := EncodeBatch(chunks)
	if err != nil {
		t.Fatalf("a batch sized to the configured ceiling %d must encode, got: %v",
			MaxConfiguredBatchBytes, err)
	}
	if len(encoded) > MaxBatchBytes {
		t.Fatalf("encoded %d exceeds the wire ceiling %d", len(encoded), MaxBatchBytes)
	}
	t.Logf("configured ceiling %d encodes to %d bytes (wire ceiling %d)",
		MaxConfiguredBatchBytes, len(encoded), MaxBatchBytes)
}

func TestConfiguredCeilingStaysBelowWireCeiling(t *testing.T) {
	// The two constants must remain distinct. How much headroom is enough is
	// already pinned behaviourally by TestConfiguredCeilingEncodesSuccessfully,
	// which builds a real batch at the ceiling; a hand-computed worst case
	// here would only re-derive MaxBatchChunks arithmetic.
	if MaxConfiguredBatchBytes >= MaxBatchBytes {
		t.Fatalf("MaxConfiguredBatchBytes (%d) must be below MaxBatchBytes (%d)",
			MaxConfiguredBatchBytes, MaxBatchBytes)
	}
	t.Logf("configured ceiling %d, wire ceiling %d, headroom %d",
		MaxConfiguredBatchBytes, MaxBatchBytes, MaxBatchBytes-MaxConfiguredBatchBytes)
}

func TestWireCeilingStillRejectsOversizedBatch(t *testing.T) {
	// The wire ceiling must keep rejecting anything above it, otherwise
	// DecodeBatch would have to buffer unbounded input.
	sum := strings.Repeat("a", 64)
	one := BatchChunk{Path: "a", Index: 0, SHA256: sum, Data: make([]byte, 1<<20)}

	// Build past the ceiling one megabyte at a time.
	var chunks []BatchChunk
	for i := 0; i < 17; i++ {
		chunks = append(chunks, one)
	}
	if _, err := EncodeBatch(chunks); err == nil {
		t.Fatalf("EncodeBatch accepted a batch above the wire ceiling %d", MaxBatchBytes)
	}
}

func TestEncodedOverheadIsQuantified(t *testing.T) {
	sum := strings.Repeat("a", 64)
	one := BatchChunk{Path: "a", Index: 0, Offset: 0, SHA256: sum, Data: make([]byte, 1024)}

	encoded, err := EncodeBatch([]BatchChunk{one})
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	overhead := len(encoded) - len(one.Data)
	// Per chunk the encoder writes: magic(5) + pathLen(4) + index(4) +
	// offset(8) + dataLen(4) + sha256(32) + path bytes. BatchHeaderSize is
	// 5+4+4+8+4+32 = 57 and already includes the magic, so the total is
	// BatchHeaderSize + len(path).
	wantOverhead := BatchHeaderSize + len("a")
	if overhead != wantOverhead {
		t.Fatalf("per-chunk overhead = %d, want BatchHeaderSize(%d) + len(path)(%d) = %d",
			overhead, BatchHeaderSize, len("a"), wantOverhead)
	}
	t.Logf("per-chunk overhead = %d bytes", overhead)
}
