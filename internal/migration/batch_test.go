package migration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestBatchCodecRoundTrip(t *testing.T) {
	data1 := []byte("hello")
	sum1 := sha256.Sum256(data1)
	data2 := []byte("world-world")
	sum2 := sha256.Sum256(data2)
	want := []BatchChunk{
		{Path: "a.txt", Index: 0, Offset: 0, SHA256: hex.EncodeToString(sum1[:]), Data: data1},
		{Path: "b.txt", Index: 2, Offset: 16, SHA256: hex.EncodeToString(sum2[:]), Data: data2},
	}
	raw, err := EncodeBatch(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBatch(bytes.NewReader(raw), MaxBatchBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("len=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i].Path != want[i].Path || got[i].Index != want[i].Index || got[i].Offset != want[i].Offset || got[i].SHA256 != want[i].SHA256 || !bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("chunk %d mismatch: got=%+v want=%+v", i, got[i], want[i])
		}
	}
}

func TestBatchCodecRejectsBadChecksum(t *testing.T) {
	data := []byte("bad")
	sum := sha256.Sum256(data)
	chunks := []BatchChunk{{Path: "x", Index: 0, Offset: 0, SHA256: hex.EncodeToString(sum[:]), Data: data}}
	raw, err := EncodeBatch(chunks)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	if _, err := DecodeBatch(bytes.NewReader(raw), MaxBatchBytes); err == nil {
		t.Fatal("expected checksum error")
	}
}
