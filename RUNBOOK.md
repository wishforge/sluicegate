# sluicegate — 运行说明

## 环境要求

- Go 1.23+
- Docker Desktop（用于启动 PostgreSQL 18）
- `curl`、`python3`
- macOS / Linux

## 快速开始

```bash
# 1. 启动 PostgreSQL（自动 docker compose up）
./scripts/db-start.sh

# 2. 测试
make test

# 3. 端到端验证（会看到 business_sequence=CONTIGUOUS 1..120）
./scripts/e2e.sh

# 4. 崩溃恢复验证（4 个 case）
./scripts/chaos.sh

# 5. 压测
WORKLOADS=100 CONCURRENCY=32 SEED_CONCURRENCY=100 \
PAYLOAD_BYTES=$((16*1024*1024)) TASKS=1 CHUNK_SIZE=65536 \
./scripts/load-test.sh

# 6. 并发饱和矩阵
BOTTLENECK_PAYLOAD_BYTES=$((16*1024*1024)) \
BOTTLENECK_CONCURRENCY_MATRIX="1 4 8 16 32 64 100" \
BOTTLENECK_WORKLOADS=100 \
./scripts/bottleneck-matrix.sh
```

停止数据库：`./scripts/db-stop.sh`

## 文档

- `README.md` / `README_en.md` — 设计原理与完整命令
- `docs/perf/findings-fix-design.md` — 性能瓶颈分析与优化证据台账
- `docs/bottleneck.md` — Transfer 分段计时说明
- `docs/chaos.md` — 崩溃恢复场景
- `docs/logging.md` — 日志字段契约

## 关键环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `TRANSFER_BATCH_BYTES` | 8MiB | 每次 HTTP 传输的批大小，允许 256KiB..15MiB |
| `DB_MAX_CONNS` | 32 | 控制面连接池硬上限 |
| `CHUNK_SIZE` | 65536 | chunk 大小（batch 内部切分用） |
| `CRASH_AFTER_CHUNKS` | — | chaos 测试用：写入 N 个 chunk 后自杀 |

## 已知未优化项

`prepare` 阶段每次迁移都会完整复制一份 payload 到 checkpoint 目录
（`source.go:194` 的 `checkpointID = migrationID` 导致 checkpointRoot 每次全新）。
在 page cache 紧张时会从磁盘重读，成为新的瓶颈。修复方向与风险见
`docs/perf/findings-fix-design.md` 末章（涉及快照隔离语义变更，待决策）。
