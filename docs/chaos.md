# Chaos / Crash / Recovery

这里的目标不是增加新的迁移原语，而是验证现有原语在进程崩溃时仍然保持业务正确性：

- Controller crash：控制面重启后从 durable migration state 继续。
- Target crash：已经落盘的 chunk 不丢，重新启动后从 target progress resume。
- Activate boundary crash：目标已经 activated，Controller 重启后可以继续 commit。
- Commit boundary crash：Target commit 已确认但 Source 尚未 release 时 Controller 崩溃，Source 必须保持 fenced；Controller 恢复后 commit 可以安全重放。
- Source restart after fence：Source 重启后 durable fence 仍然拒绝业务写入。

## 验收不只看文件 checksum

每个 case 最终都验证：

1. `business_sequence=CONTIGUOUS 1..N`
2. 最终 `state=COMMITTED`
3. checkpoint / payload SHA256 一致
4. target 只有一个 active workload
5. commit 前 Source write 返回 `423 WORKLOAD_FENCED`
6. commit 后 Source write 恢复 `200`
7. Activate / Commit 重放不会产生第二个 active workload

## 故障注入

### Controller Transfer crash

`CRASH_AFTER_CHUNKS=N`：Controller 在第 N 个 chunk 持久化迁移统计后直接退出 `137`。Target 已落盘 chunk 保留，Controller 重启后继续 Transfer。

### Target Transfer crash

`CRASH_AFTER_CHUNKS=N`：Target 在第 N 个 chunk 已完成 `write + fsync + durable state` 后退出 `137`。Controller 将该错误视为 retryable，Migration 保持 `TRANSFERRING`，Target 重启后继续。

### Commit boundary crash

`CHAOS_COMMIT_PAUSE_MS=N`：Controller 在 Target Commit 成功后、Source Release 前暂停 N ms。测试期间强制杀死 Controller，验证 Source 仍被 fence；Controller 重启后可以安全重放 Commit。

## 运行

```bash
go test ./...
go vet ./...
./scripts/e2e.sh
./scripts/chaos.sh
```

日志位于 `/tmp/sluicegate-chaos/case-*/logs/`。
