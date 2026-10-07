# Load Test

压测目标是在保证控制面正确性的前提下，回答一个更重要的问题：

> **Migration 可以高并发时，Control Plane 是否仍然有明确的资源边界、背压和失败可观测性？**

## 早期实现暴露的问题

早期实现用 `psql` 子进程访问 PostgreSQL。100~1000 并发 Migration 下，Create / Prepare / Transfer / Activate / Commit 与 lease renew 会不断 spawn `psql`，最终触发：

```text
FATAL: sorry, too many clients already
```

这一步的真实价值是把瓶颈暴露出来。

## 当前基线

现在使用一个 Controller 进程内的 bounded `pgxpool`：

```text
Migration concurrency = 100 / 500 / 1000
                    ↓
             Controller goroutines
                    ↓
                 pgxpool
              MaxConns = 32
                    ↓
                PostgreSQL
```

数据库连接数量不再随 Migration 数量线性增长。

默认：

```bash
DB_MAX_CONNS=32
DB_MIN_CONNS=4
DB_OP_TIMEOUT=8s
```

`DB_OP_TIMEOUT` 的意义是：连接池满时，查询等待也必须有边界；否则“有界连接”会重新退化成“无限请求等待”。

## 推荐基线

```bash
WORKLOADS=100 \\
CONCURRENCY=20 \\
SEED_CONCURRENCY=20 \\
PAYLOAD_BYTES=4096 \\
TASKS=10 \\
DB_MAX_CONNS=32 \\
./scripts/load-test.sh
```

## 重点实验：1000 Workloads / 100 Concurrent Migrations

```bash
WORKLOADS=1000 \\
CONCURRENCY=100 \\
SEED_CONCURRENCY=100 \\
PAYLOAD_BYTES=1024 \\
TASKS=1 \\
CHUNK_SIZE=65536 \\
DB_MAX_CONNS=32 \\
LOAD_GLOBAL_TIMEOUT=90m \\
./scripts/load-test.sh
```

LoadTest 每 5 秒报告：

```text
finished / total
success
failed
active
throughput
```

最终 JSON 报告包含：

- migration success rate
- migration throughput
- P50/P95/P99 latency
- Create / Prepare / Transfer / Activate / Commit P50/P95/P99
- data throughput
- failure code histogram
- error samples
- max in-flight

## 失败不应该让压测进程消失

这里明确区分三件事：

```text
Migration Failed
    ≠
Worker Failed
    ≠
LoadTest Process Failed
```

一个 Migration 因 `MIGRATION_BUSY`、`FENCE_FAILED` 或 `TIMEOUT` 失败，只影响自己的 result；Worker 会继续消费后续任务；LoadTest 最终仍然输出完整结果。

全局 timeout 尚未调度的任务会被显式记为 `GLOBAL_TIMEOUT`，不会静默丢失。

## Lease 观测

日志应该看到：

```text
controller_lease_acquired
controller_lease_renew_failed
migration_lease_renew_failed migration_id=...
```

注意语义：

```text
Controller Lease
  保护“谁是当前 Controller”

Migration Lease
  保护“谁可以推进这个 Migration”
```

Controller Lease 不再随着 Migration 数量复制。

## 结果解读

即使 1000 workloads 最终全部成功，也不能仅凭 migrations/s 判断系统性能已经很好。应该同时看：

```text
Migration Throughput
DB Pool Max / Acquired / Idle
Controller P95
Transfer P95
PostgreSQL transaction / tuple / block metrics
```

迁移数据量很小时，`Transfer` 时间往往不是网络吞吐的纯体现，而是多个 HTTP + filesystem + checkpoint + state persistence 固定开销的叠加。


## Batched Transfer

批传输默认启用 `TRANSFER_BATCH_BYTES=4MiB`。同一 file 的多个 chunk 会在 Source 侧一次读取、在 Target 侧一次 HTTP 提交，并在 batch 完成后执行一次文件同步和一次 state 持久化。

新增结果字段：
- `transfer_p50/p95/p99`：Transfer 阶段延迟
- `batch_count_p50/p95/p99`：每个 Migration 使用的 batch 数
- `transfer_batch_bytes`：本轮 batch budget

矩阵入口：`./scripts/perf-matrix.sh`。默认覆盖多个 payload 与 concurrency 组合，结果写入 `OUT_DIR/summary.csv`。
