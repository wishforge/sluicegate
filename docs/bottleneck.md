# Transfer Bottleneck Instrumentation

这一层不改变迁移状态机或 Transfer 语义，只增加可观测性，并提供固定 16MiB payload 的并发饱和实验。

## 为什么要测

压测已经显示：并发从 10 增加到 50/100 时，总 throughput 基本不增长，而 P50/P95/P99 持续上涨。所以这里把 Transfer 拆成：

`Source HTTP → Source Read/Encode → Target HTTP → Target Queue Wait → Target Write → Target Fsync → Target State Save`

## 关键指标

`target_queue_wait_ms`：进入 Target batch handler 后，等待 `TargetServer.mu` 的时间。

`target_write_ms`：实际 `WriteAt` 阶段耗时。

`target_fsync_ms`：batch 文件 `Sync` 阶段耗时。

`target_state_save_ms`：Target migration state 持久化耗时。

`target_peak_inflight`：Target batch HTTP handler 的同时在途请求峰值。

`source_read_ms` / `source_encode_ms`：Source batch 读取、哈希和编码耗时。

## 运行饱和曲线

```bash
BOTTLENECK_PAYLOAD_BYTES=$((16*1024*1024)) \
BOTTLENECK_CONCURRENCY_MATRIX="1 2 4 8 16 32 64 100" \
BOTTLENECK_WORKLOADS=100 \
./scripts/bottleneck-matrix.sh
```

结果：

```text
/tmp/sluicegate-bottleneck/summary.csv
```

## 如何判断瓶颈

如果 concurrency 增加后：

- `migrations_per_sec` 基本不再增加；
- `target_queue_wait_p50_ms` 快速增加；
- `target_write_ms / target_fsync_ms` 没有同步数量级增加；

那么主要问题是 Target batch critical section 的排队，而不是磁盘带宽本身。

当前默认不缩短这个 critical section，因此不会把“诊断”和“优化”混在一起。等瓶颈被量化之后，再针对它做并发化，并重新跑 E2E/Chaos。
