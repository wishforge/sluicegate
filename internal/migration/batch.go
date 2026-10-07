package migration

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

const (
	BatchMagic        = "MIGB1"
	BatchHeaderSize   = 5 + 4 + 4 + 8 + 4 + 32
	DefaultBatchBytes = 4 << 20
	MaxBatchBytes     = 16 << 20
	MaxBatchPathBytes = 4 << 10
	MaxBatchChunks    = 4096
)

type BatchChunk struct {
	Path   string
	Index  int
	Offset int64
	SHA256 string
	Data   []byte
}

func EncodeBatch(chunks []BatchChunk) ([]byte, error) {
	if len(chunks) == 0 {
		return nil, errors.New("batch must contain at least one chunk")
	}
	if len(chunks) > MaxBatchChunks {
		return nil, fmt.Errorf("batch contains too many chunks: %d", len(chunks))
	}
	var out bytes.Buffer
	for _, c := range chunks {
		if c.Path == "" || len(c.Path) > MaxBatchPathBytes {
			return nil, errors.New("invalid batch chunk path")
		}
		if c.Index < 0 || c.Offset < 0 {
			return nil, errors.New("invalid batch chunk index/offset")
		}
		if len(c.Data) > MaxBatchBytes {
			return nil, errors.New("batch chunk too large")
		}
		rawSHA, err := hex.DecodeString(c.SHA256)
		if err != nil || len(rawSHA) != sha256.Size {
			return nil, errors.New("invalid batch chunk sha256")
		}
		if _, err := out.WriteString(BatchMagic); err != nil {
			return nil, err
		}
		if err := writeU32(&out, uint32(len(c.Path))); err != nil {
			return nil, err
		}
		if err := writeU32(&out, uint32(c.Index)); err != nil {
			return nil, err
		}
		if err := writeU64(&out, uint64(c.Offset)); err != nil {
			return nil, err
		}
		if err := writeU32(&out, uint32(len(c.Data))); err != nil {
			return nil, err
		}
		if _, err := out.Write(rawSHA); err != nil {
			return nil, err
		}
		if _, err := out.WriteString(c.Path); err != nil {
			return nil, err
		}
		if _, err := out.Write(c.Data); err != nil {
			return nil, err
		}
		if out.Len() > MaxBatchBytes {
			return nil, fmt.Errorf("encoded batch exceeds %d bytes", MaxBatchBytes)
		}
	}
	return out.Bytes(), nil
}

func DecodeBatch(r io.Reader, maxBytes int64) ([]BatchChunk, error) {
	if maxBytes <= 0 || maxBytes > MaxBatchBytes {
		maxBytes = MaxBatchBytes
	}
	body, err := io.ReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("batch request exceeds %d bytes", maxBytes)
	}
	var chunks []BatchChunk
	pos := 0
	for pos < len(body) {
		if len(body)-pos < BatchHeaderSize {
			return nil, errors.New("truncated batch header")
		}
		if string(body[pos:pos+5]) != BatchMagic {
			return nil, errors.New("invalid batch magic")
		}
		pos += 5
		pathLen := int(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4
		index := int(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4
		offset := int64(binary.BigEndian.Uint64(body[pos : pos+8]))
		pos += 8
		dataLen := int(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4
		if pathLen <= 0 || pathLen > MaxBatchPathBytes {
			return nil, errors.New("invalid batch path length")
		}
		if index < 0 || offset < 0 || dataLen < 0 || dataLen > MaxBatchBytes {
			return nil, errors.New("invalid batch chunk metadata")
		}
		shaRaw := body[pos : pos+sha256.Size]
		pos += sha256.Size
		if len(body)-pos < pathLen+dataLen {
			return nil, errors.New("truncated batch payload")
		}
		path := string(body[pos : pos+pathLen])
		pos += pathLen
		data := append([]byte(nil), body[pos:pos+dataLen]...)
		pos += dataLen
		got := sha256.Sum256(data)
		if !bytes.Equal(got[:], shaRaw) {
			return nil, errors.New("batch chunk checksum mismatch")
		}
		chunks = append(chunks, BatchChunk{Path: path, Index: index, Offset: offset, SHA256: hex.EncodeToString(shaRaw), Data: data})
		if len(chunks) > MaxBatchChunks {
			return nil, fmt.Errorf("batch contains too many chunks: %d", len(chunks))
		}
	}
	if len(chunks) == 0 {
		return nil, errors.New("empty batch")
	}
	return chunks, nil
}

func writeU32(w io.Writer, n uint32) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], n)
	_, err := w.Write(b[:])
	return err
}

func writeU64(w io.Writer, n uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	_, err := w.Write(b[:])
	return err
}
