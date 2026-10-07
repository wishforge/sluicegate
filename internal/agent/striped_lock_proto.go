package agent

// Step 4 prototype: verify that a striped lock keyed by migration id removes
// the serialization measured in finding 1, while preserving the invariants
// that must not break.
//
// This file is a PROTOTYPE, not the fix. It exists so the recommended design
// can be measured before any production edit. The mechanism under test is the
// same one Prometheus uses in tsdb/agent/series.go: a fixed array of mutexes
// indexed by `id & (n-1)`, with cache-line padding between them.
//
// Invariants under test:
//  1. Batches for different migrations overlap (queue wait collapses).
//  2. Batches for the same migration stay serialized, because
//     f.Chunks[c.Index] is the idempotency record.
//  3. Chaos counters do not race.

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// targetLockStripes is a lock striping implementation.
//
// Design basis, from Prometheus tsdb/agent/series.go:
//   - series.go:130-141  "stripeSeries locks modulo ranges of IDs and hashes
//     to reduce lock contention. The locks are padded to not be on the same
//     cache line."
//   - series.go:320-326  hashLock/refLock use `& uint64(s.size-1)`, so the
//     stripe count must be a power of two.
//   - series.go:151-157  all stripes are allocated up front, so no map write
//     happens under lock selection.
//
// Divergence from Prometheus: those stripes guard a map, so they use RWMutex
// and a fast-path read lock. Here every critical section is a write that ends
// in fsync, so plain Mutex is correct and cheaper.
const targetLockStripes = 256

type paddedMutex struct {
	sync.Mutex
	_ [40]byte // same padding rationale as Prometheus stripeLock
}

type stripedTargetLock struct {
	stripes [targetLockStripes]paddedMutex
}

func (l *stripedTargetLock) lockFor(id string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return &l.stripes[h.Sum32()&(targetLockStripes-1)].Mutex
}

// chaosCounters replaces the two int64 fields that the global lock used to
// serialize. go test -race flags a plain ++ under concurrency, and the single
// global lock is exactly what we are removing, so the counters must become
// atomic on their own.
type chaosCounters struct {
	chunkCount atomic.Int64
	triggered  atomic.Bool
}

func (c *chaosCounters) reach(limit int64) int64 {
	n := c.chunkCount.Add(1)
	if limit > 0 && !c.triggered.Load() && n >= limit {
		if c.triggered.CompareAndSwap(false, true) {
			return n
		}
	}
	return 0
}

// stateTableLock guards the s.states map itself.
//
// Striping by migration id does NOT protect the map: two different ids
// usually land on different stripes, so concurrent inserts into the same map
// race. A dedicated lock for the map is the minimal fix; it is held only for
// the map operation, never across fsync, so it does not reintroduce
// serialization on the slow path.
//
// Verified by internal/agent/maprace_test.go under -race.
type stateTableLock struct {
	sync.RWMutex
	_ [40]byte
}

// withState runs fn under the map's read lock.
func (s *TargetServer) withState(fn func(states map[string]TargetState)) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	fn(s.states)
}

// withStateWrite runs fn under the map's write lock.
func (s *TargetServer) withStateWrite(fn func(states map[string]TargetState)) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	fn(s.states)
}
