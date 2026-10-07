# sluicegate

> **English readers:** see [README_en.md](README_en.md) for the English version.

一条可恢复、幂等的数据迁移链，控制面的资源边界做成显式约束。传输侧用批量化把 round-trip 和 fsync 次数压下来，并提供分段计时来定位瓶颈。


> **要解决的问题：1000 个 Migration 不等于 1000 个 `psql` 进程。**
>
> **设计原则：Migration 可以高并发，数据库连接必须有界；Controller 租约是 Controller 级，而不是 Migration 级。**

核心迁移链：

```text
Fence → Checkpoint → Chunk Transfer → Resume → Verify → Activate → Commit
```

## 关键设计决策

### 1. PostgreSQL 走 `pgxpool`，不用 `psql` 子进程

如果每个 Store 操作都启动一次 `psql`，那么 100 个 Migration 各跑 4 个 action、每个 action 还要做 lease renew 和 state write，就会产生大量 PostgreSQL client process，最终触发：

```text
FATAL: sorry, too many clients already
```

现在的做法是：

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

数据库连接是**硬上限**。并发 Migration 超过 DB pool 能力时，任务排队，而不是不断创建进程把 PostgreSQL 打爆。

主要环境变量：

```bash
DB_MAX_CONNS=32
DB_MIN_CONNS=4
DB_OP_TIMEOUT=8s
DB_MAX_CONN_LIFETIME=30m
DB_MAX_CONN_IDLE_TIME=5m
```

### 2. Controller Lease 与 Migration Lease 分开

另一个容易踩的坑：每个 Migration action 都在维护所谓的 `controller_lease`，那么 1000 个 Migration 就会产生大量 lease renew。

现在的做法是拆成两个语义：

```text
Controller Lease
  resource_id = controller
  一个 Controller 实例只有一份
  一个 renewal goroutine

Migration Lease
  resource_id = migration_id
  只保护某个 Migration 的 action
```

因此日志从「一堆带 migration_id 的 renew 失败」变成可区分的两类：

```text
controller_lease_renew_failed resource_id=controller holder_id=...
migration_lease_renew_failed migration_id=... holder_id=...
```

### 3. LoadTest 不因为单个错误把整轮压测打断

LoadTest 具备这些保护：

- 全局 benchmark timeout
- 单 Migration timeout
- 每 5 秒进度输出
- success / failed / active / throughput 实时统计
- failure code 聚合，例如 `MIGRATION_BUSY` / `FENCE_FAILED` / `TIMEOUT`
- worker panic 隔离
- global timeout 未调度的任务会被明确记为 `GLOBAL_TIMEOUT`，不会静默丢失
- HTTP idle connection pool

因此 1000 workloads 不会在 `seed_complete` 之后「进程消失」，而是拿到完整结果。

## 第一性原理

```text
Migration 并发
    ≠
PostgreSQL Connection 并发

业务并发可以很高
数据库连接必须有界

Controller Instance Lease
    ≠
Migration Ownership

前者回答“谁是当前 Controller”
后者回答“谁可以推进这个 Migration”
```

## 目录

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

## 运行环境

- Go 1.23+
- Docker Desktop
- `curl`
- `python3`
- PostgreSQL 18（脚本自动启动 Docker）

首次构建会自动下载 `github.com/jackc/pgx/v5`。

## 1. 启动 PostgreSQL

```bash
cd sluicegate
./scripts/db-start.sh
```

默认使用：

```text
127.0.0.1:55432
migration / migration
database=migration
container=sluicegate-postgres
```

> **端口冲突不会再直接失败。** 如果已有一个 MVP PostgreSQL 容器占用了 `55432`（`sluicegate-postgres` 或 `sluicegate-postgres`），脚本会优先复用它；否则会自动寻找下一个空闲端口并写入 `.runtime.env`。后续 `e2e.sh`、`chaos.sh`、`load-test.sh` 会自动读取这个运行配置。
> `.runtime.env` 是本地运行配置，已被 `.gitignore` 排除。
>
> 也可以显式指定：
> ```bash
> DB_PORT=55433 ./scripts/db-start.sh
> ```

## 2. 测试 / 构建

```bash
make test
make vet
make build
```

## 3. Stateful E2E

```bash
./scripts/e2e.sh
```

成功时应看到类似：

```text
business_sequence=CONTIGUOUS 1..120
PASS migration=... state=COMMITTED ... store=postgres
```

## 4. Chaos

```bash
./scripts/chaos.sh
```

覆盖：

```text
CASE 1 Controller crash during Transfer
CASE 2 Target crash during Transfer
CASE 3 Controller crash after Activate
CASE 4 Controller crash between Target Commit and Source Release
```

Controller Lease TTL 默认为 15s；Chaos 脚本会主动设置 3s，并在 Controller 重启前等待 lease expiry，避免旧实例的 lease 阻塞新实例。

## 5. 单轮压测

推荐先跑：

```bash
WORKLOADS=100 \
CONCURRENCY=20 \
SEED_CONCURRENCY=20 \
PAYLOAD_BYTES=4096 \
TASKS=10 \
DB_MAX_CONNS=32 \
./scripts/load-test.sh
```

### 1000 并发压测

针对之前暴露问题的复现实验：

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

你应该看到持续进度，例如：

```text
[loadtest] progress finished=100/1000 success=100 failed=0 active=100 throughput=.../s
[loadtest] progress finished=200/1000 success=200 failed=0 active=100 throughput=.../s
...
```

最终 JSON 会包含：

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

## 6. 压测矩阵

```bash
MATRIX="100 500 1000" \
CONCURRENCY=100 \
SEED_CONCURRENCY=100 \
DB_MAX_CONNS=32 \
./scripts/load-matrix.sh
```

### 为什么默认 DB_MAX_CONNS=32？

不是说 32 是“最佳值”。它只是一个安全起点：

```text
Migration concurrency = 100
         ↓
DB connection concurrency ≈ bounded by 32
         ↓
剩余 DB 操作排队
         ↓
PostgreSQL 不被 client process 数量打爆
```

真正生产部署时，应根据 PostgreSQL `max_connections`、Controller 副本数、查询耗时以及其它业务连接池共同确定。

## 7. 手工运行 Controller

```bash
export DATABASE_URL='postgres://migration:migration@127.0.0.1:55432/migration?sslmode=disable'
export DB_MAX_CONNS=32
export DB_MIN_CONNS=4
export CONTROLLER_ID='controller-1'
./bin/migration-controller
```

Controller 启动时会先拿：

```text
resource_id=controller
holder_id=controller-1
```

拿到后仅启动**一个** Controller Lease renewal loop。

单个 Migration action 再使用：

```text
resource_id=<migration-id>
```

作为 Migration Lease。

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

PostgreSQL 保存“业务事实”：状态、epoch、manifest、lease、audit。

大文件 / chunk 仍然走 Data Plane，不塞 PostgreSQL。

## 9. 为什么这样设计

这套设计不是靠“性能优化”堆出来的，而是三个层次依次收紧的结果：

```text
PostgreSQL 作为权威状态存储
        ↓
加入并发压测，真实暴露问题
（psql 子进程放大 + lease renewal 放大）
        ↓
pgxpool 有界连接
+ 单例 Controller Lease
+ per-Migration Lease
+ 可恢复的 LoadTest
```

关键点在于：Control Plane 从**能跑**推进到**高并发下仍然有边界和背压**。没有这一步，并发只是把「进程数爆炸」推迟成「连接数爆炸」。

## Dependency bootstrap

`make test`, `make vet`, and `make build` automatically run `go mod download` before compiling, so a fresh checkout does not require a manual `go get`.

## 10. Batch Data Plane

这里改变的不是迁移语义，而是传输的搬运方式。从原来的：

`Fetch chunk -> HTTP -> Put chunk -> fsync -> persist state`

改成：

`Fetch batch -> one HTTP response -> Put batch -> per-file fsync -> one state persist`

新增协议：
- Source: `GET /v1/workloads/{workload}/checkpoints/{checkpoint}/chunks`，按 file + chunk range 返回二进制 batch。
- Target: `PUT /v1/migrations/{migration}/chunks`，一次提交多个 chunk。
- 默认 `TRANSFER_BATCH_BYTES=4MiB`，允许 `256KiB..16MiB`。
- Target batch 写入同一文件时复用一个 file descriptor，并在整个 batch 完成后执行一次 `fsync`。
- State JSON 仍然在 batch 完成后持久化；因此 crash 时，最新未持久化 metadata 会被下一次 progress/resume 重新发送，不改变“可恢复 + 幂等”的语义。

新增 benchmark 指标：`batches_transferred`、`batch_count_p50/p95/p99`。

### 建议压测矩阵

```bash
PAYLOAD_MATRIX="1024 65536 1048576 16777216 268435456" \
CONCURRENCY_MATRIX="10 50 100" \
MATRIX_WORKLOADS=100 \
CHUNK_SIZE=65536 \
TRANSFER_BATCH_BYTES=$((4*1024*1024)) \
./scripts/perf-matrix.sh
```

三个层次各自解决一件事：控制面解决“连接数随并发爆炸”；传输面解决“数据很小却做了太多 round-trip / fsync”；诊断层不再盲目优化，而是把瓶颈拆开测量。

## 11. 性能诊断

Transfer 分段计时，不改变迁移状态机。重点观测：Source read/encode、Target queue wait、Target write、Target fsync、Target state save，以及 Target batch peak in-flight。

运行 16MiB 饱和矩阵：

```bash
BOTTLENECK_PAYLOAD_BYTES=$((16*1024*1024)) \
BOTTLENECK_CONCURRENCY_MATRIX="1 2 4 8 16 32 64 100" \
BOTTLENECK_WORKLOADS=100 \
./scripts/bottleneck-matrix.sh
```

输出：`/tmp/sluicegate-bottleneck/summary.csv`。

代码层面的关键事实：`internal/agent/target.go` 的 `handleChunkBatch` 在 `s.mu.Lock()` 后，直到整个 `WriteAt → fsync → saveLocked` 完成才释放锁。也就是说，当前 Target 的 batch critical section 天然会把不同 Migration 的 batch 串行化。`target_queue_wait_ms` 把这段等待显式测出来——先证明它占多少，再决定要不要并发化。
