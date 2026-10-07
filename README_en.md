# sluicegate

[中文文档](README.md)

A recoverable, idempotent data migration chain whose control plane makes resource bounds explicit. The transfer path batches data to cut round-trips and fsyncs, and segmented timing locates the bottleneck.


> **The problem: 1000 Migrations do not mean 1000 `psql` processes.**
>
> **The design principle: Migrations may be highly concurrent, database connections must be bounded; the controller lease is controller-scoped, not migration-scoped.**

The core migration chain:

```text
Fence → Checkpoint → Chunk Transfer → Resume → Verify → Activate → Commit
```

## Key Design Decisions

### 1. PostgreSQL goes through `pgxpool`, not `psql` subprocesses

Spawning one `psql` process per Store operation means 100 Migrations running 4 actions each, with every action also doing lease renew and state write, produces a large number of PostgreSQL client processes and eventually triggers:

```text
FATAL: sorry, too many clients already
```

The approach here is:

```text
100 / 500 / 1000 Migration
        │
        ▼
Controller HTTP goroutines
        │
        ▼
      pgxpool
   MaxConns=32 (default)
        │
        ▼
   PostgreSQL
```

Database connections are a **hard ceiling**. When concurrent Migrations exceed the DB pool's capacity, work queues up instead of spawning more processes until PostgreSQL falls over.

Main environment variables:

```bash
DB_MAX_CONNS=32
DB_MIN_CONNS=4
DB_OP_TIMEOUT=8s
DB_MAX_CONN_LIFETIME=30m
DB_MAX_CONN_IDLE_TIME=5m
```

### 2. Controller Lease and Migration Lease Split Apart

Another easy trap: every Migration action maintained a so-called `controller_lease`, so a thousand Migrations meant a flood of lease renewals.

Here it is split into two distinct semantics:

```text
Controller Lease
  resource_id = controller
  one per Controller instance
  one renewal goroutine

Migration Lease
  resource_id = migration_id
  protects only one Migration's action
```

So the logs go from "a pile of renew failures carrying migration_id" to two distinguishable classes:

```text
controller_lease_renew_failed resource_id=controller holder_id=...
migration_lease_renew_failed migration_id=... holder_id=...
```

### 3. LoadTest Does Not Abort a Whole Run on a Single Error

LoadTest provides:

- Global benchmark timeout
- Per-Migration timeout
- Progress output every 5 seconds
- Live success / failed / active / throughput statistics
- Failure code aggregation, e.g. `MIGRATION_BUSY` / `FENCE_FAILED` / `TIMEOUT`
- Worker panic isolation
- Tasks never scheduled before the global timeout are recorded explicitly as `GLOBAL_TIMEOUT` rather than silently dropped
- HTTP idle connection pool

So 1000 workloads do not simply print `seed_complete` and then have the process disappear; you get a complete result set instead.

## First Principles

```text
Migration concurrency
    ≠
PostgreSQL connection concurrency

Business concurrency can be very high
Database connections must be bounded

Controller Instance Lease
    ≠
Migration Ownership

The former answers "which Controller is current"
The latter answers "who may advance this Migration"
```


## Layout

```text
cmd/
  migration-controller/   Control Plane
  source-agent/            Source Data Plane
  target-agent/            Target Data Plane
  workload-runner/         Stateful Business Workload
  loadtest/                High-concurrency benchmark

internal/migration/
  postgres_store.go        pgxpool bounded PostgreSQL Store
  store.go                 Store contract + FileStore test backend
  service.go               Migration state machine

scripts/
  db-start.sh              PostgreSQL 18
  db-reset.sh              Control-plane reset
  e2e.sh                   Stateful migration E2E
  chaos.sh                 4 crash/recovery scenarios
  load-test.sh             Single benchmark
  load-matrix.sh           100/500/1000 matrix
```

## Requirements

- Go 1.23+
- Docker Desktop
- `curl`
- `python3`
- PostgreSQL 18 (the script starts Docker automatically)

The first build downloads `github.com/jackc/pgx/v5` automatically.

## 1. Start PostgreSQL

```bash
cd sluicegate
./scripts/db-start.sh
```

Defaults:

```text
127.0.0.1:55432
migration / migration
database=migration
container=sluicegate-postgres
```

> **Port conflicts no longer fail outright.** If an existing MVP PostgreSQL container already owns `55432` (`sluicegate-postgres` or `sluicegate-postgres`), the script reuses it. Otherwise it picks the next free port and writes it to `.runtime.env`. Subsequent `e2e.sh`, `chaos.sh`, and `load-test.sh` read that runtime config automatically.
> `.runtime.env` is local runtime configuration and is excluded by `.gitignore`.

You can also specify a port explicitly:

```bash
DB_PORT=55433 ./scripts/db-start.sh
```

## 2. Test / Build

```bash
make test
make vet
make build
```

## 3. Stateful E2E

```bash
./scripts/e2e.sh
```

On success you should see something like:

```text
business_sequence=CONTIGUOUS 1..120
PASS migration=... state=COMMITTED ... store=postgres
```

## 4. Chaos

```bash
./scripts/chaos.sh
```

Covers:

```text
CASE 1 Controller crash during Transfer
CASE 2 Target crash during Transfer
CASE 3 Controller crash after Activate
CASE 4 Controller crash between Target Commit and Source Release
```

The Controller Lease TTL still defaults to 15s. The chaos script actively sets it to 3s and waits for lease expiry before restarting the Controller, so a stale instance cannot block the new one.

## 5. Single Benchmark Run

Start with:

```bash
WORKLOADS=100 \
CONCURRENCY=20 \
SEED_CONCURRENCY=20 \
PAYLOAD_BYTES=4096 \
TASKS=10 \
DB_MAX_CONNS=32 \
./scripts/load-test.sh
```

### 1000-Concurrency Benchmark

Reproduces the problem exposed earlier:

```bash
WORKLOADS=1000 \
CONCURRENCY=100 \
SEED_CONCURRENCY=100 \
PAYLOAD_BYTES=1024 \
TASKS=1 \
CHUNK_SIZE=65536 \
DB_MAX_CONNS=32 \
LOAD_GLOBAL_TIMEOUT=90m \
./scripts/load-test.sh
```

You should see continuous progress, for example:

```text
[loadtest] progress finished=100/1000 success=100 failed=0 active=100 throughput=.../s
[loadtest] progress finished=200/1000 success=200 failed=0 active=100 throughput=.../s
...
```

The final JSON includes:

```json
{
  "completed": 1000,
  "failed": 0,
  "success_rate": 1,
  "migrations_per_sec":  ...,
  "total_p95_ms": ...,
  "error_counts": {}
}
```

## 6. Load Matrix

```bash
MATRIX="100 500 1000" \
CONCURRENCY=100 \
SEED_CONCURRENCY=100 \
DB_MAX_CONNS=32 \
./scripts/load-matrix.sh
```

### Why DB_MAX_CONNS Defaults to 32

32 is not claimed to be "the optimal value". It is a safe starting point:

```text
Migration concurrency = 100
         ↓
DB connection concurrency ≈ bounded by 32
         ↓
Remaining DB operations queue
         ↓
PostgreSQL is not blown out by client process count
```

For a real production deployment, this should be derived from PostgreSQL `max_connections`, the number of Controller replicas, query latency, and any other business connection pools sharing the same database.

## 7. Running the Controller Manually

```bash
export DATABASE_URL='postgres://migration:migration@127.0.0.1:55432/migration?sslmode=disable'
export DB_MAX_CONNS=32
export DB_MIN_CONNS=4
export CONTROLLER_ID='controller-1'
./bin/migration-controller
```

On startup the Controller acquires:

```text
resource_id=controller
holder_id=controller-1
```

and only then starts **one** Controller Lease renewal loop.

An individual Migration action uses:

```text
resource_id=<migration-id>
```

as its Migration Lease.

## 8. Control Plane / Data Plane

```text
                 Control Plane
        ┌────────────────────────────┐
        │ Migration Controller       │
        │    └── pgxpool ──> PG      │
        └────────────┬───────────────┘
                     │
             migration state
                     │
       ┌─────────────┴─────────────┐
       ▼                           ▼
 Source Agent                  Target Agent
       │                           │
 checkpoint/files              chunks/staging
       └────────── local FS / data plane ──────┘
```

PostgreSQL holds the "business facts": state, epoch, manifest, lease, audit.

Large files and chunks still travel through the Data Plane and are not stuffed into PostgreSQL.

## 9. Why It Is Built This Way

This is not the result of stacking up "performance optimizations". It is three layers tightening in sequence:

```text
PostgreSQL as the authoritative state store
        ↓
Concurrent load testing exposes the real problems
(psql subprocess amplification + lease renewal amplification)
        ↓
pgxpool bounded connection
+ singleton Controller Lease
+ per-Migration Lease
+ resilient LoadTest
```

The key point: the Control Plane moves from *it runs* to *it still has bounds and backpressure under high concurrency*. Without that step, concurrency only postpones "process count explodes" into "connection count explodes".

## Dependency Bootstrap

`make test`, `make vet`, and `make build` automatically run `go mod download` before compiling, so a fresh checkout does not require a manual `go get`.

## 10. Batch Data Plane

What changes here is not migration semantics, but how bytes are moved. Instead of:

`Fetch chunk -> HTTP -> Put chunk -> fsync -> persist state`

with:

`Fetch batch -> one HTTP response -> Put batch -> per-file fsync -> one state persist`

New protocol:

- Source: `GET /v1/workloads/{workload}/checkpoints/{checkpoint}/chunks`, returning a binary batch keyed by file + chunk range.
- Target: `PUT /v1/migrations/{migration}/chunks`, committing multiple chunks at once.
- `TRANSFER_BATCH_BYTES=8MiB` by default, accepting `256KiB..15MiB` (encoding needs headroom, so the configurable ceiling sits just below the 16MiB wire limit).
- When a Target batch writes to the same file it reuses one file descriptor, and issues a single `fsync` after the whole batch completes.
- State JSON is still persisted after the batch completes. So on crash, the latest unpersisted metadata is re-sent by the next progress/resume, which leaves the "recoverable + idempotent" semantics unchanged.

New benchmark metrics: `batches_transferred`, `batch_count_p50/p95/p99`.

### Recommended Benchmark Matrix

```bash
PAYLOAD_MATRIX="1024 65536 1048576 16777216 268435456" \
CONCURRENCY_MATRIX="10 50 100" \
MATRIX_WORKLOADS=100 \
CHUNK_SIZE=65536 \
TRANSFER_BATCH_BYTES=$((4*1024*1024)) \
./scripts/perf-matrix.sh
```

First principles: each layer solves one thing. The control plane solves "PostgreSQL connection count explodes with concurrency"; the transfer path solves "tiny data still paid for too many round-trips / fsyncs"; the diagnostics layer stops optimizing blindly and measures the bottleneck in pieces.

## 11. Performance Diagnosis

Segmented Transfer timing, without changing the migration state machine. The focus is on Source read/encode, Target queue wait, Target write, Target fsync, Target state save, and Target batch peak in-flight.

Run the 16MiB saturation matrix:

```bash
BOTTLENECK_PAYLOAD_BYTES=$((16*1024*1024)) \
BOTTLENECK_CONCURRENCY_MATRIX="1 2 4 8 16 32 64 100" \
BOTTLENECK_WORKLOADS=100 \
./scripts/bottleneck-matrix.sh
```

Output: `/tmp/sluicegate-bottleneck/summary.csv`.

Key fact at the code level: in `internal/agent/target.go`, `handleChunkBatch` holds `s.mu.Lock()` from acquisition until the entire `WriteAt → fsync → saveLocked` sequence completes. In other words, the current Target batch critical section inherently serializes batches belonging to different Migrations. `target_queue_wait_ms` makes that wait explicit, so you can first prove how much it costs before deciding whether to add concurrency.

## License

Apache-2.0. See [LICENSE](LICENSE).
