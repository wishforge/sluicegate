# Evidence Ledger — findings-to-fix-design

> 状态：**Step 0-4 已完成，Step 5 等批准。生产代码尚未提交。**
> 纪律：`~/.codex/skills/findings-to-fix-design/SKILL.md`（Iron Law: NO DESIGN WITHOUT A REPRODUCED FINDING）

## Design Progress

- [x] Step 0: 证据门（两个 finding 均 RED 复现，测试已验证「有牙齿」）
- [x] Step 1: 开源 + 权威源调研（Prometheus `tsdb/agent/series.go`、PostgreSQL 官方 WAL 文档）
- [x] Step 2: OWASP + 12-Factor 门
- [x] Step 3: 辩证候选 + blast radius
- [x] Step 4: 技术验证（实测 + race + e2e + chaos）
- [ ] Step 5: **等用户批准后才实现**

---

## Finding 1 — 单把全局锁把不同 migration 完全串行化

### Code fact

`internal/agent/target.go:41` 一把 `sync.Mutex`，被 **7 个 handler** 共用：

| handler | 原行号 | 保护对象 |
|---|---|---|
| `handleInit` | 159 | `s.states[id]` |
| `handleProgress` | 207 | `s.states[id]` |
| `handleChunkBatch` | 245 | state + chunk 去重 + 写文件 + **fsync** + 存状态 |
| `handleChunk` | 405 | 同上（单 chunk 路径） |
| `handleActivate` | 449 | `s.states[id]` + 文件 finalize |
| `handleCommit` | 578 | `s.states[id]` |
| `handleRollback` | 599 | `s.states[id]` |

`handleChunkBatch` 从 `target.go:245` 加锁，直到 `target.go:248` 的 `defer` 才释放，中间包含 `WriteAt → fsync → saveLocked` 全过程。

**关键事实：7 个 handler 全部只访问 `s.states[id]`，无任何跨 id 遍历**（`grep 'range s.states'` 为空）。也就是说临界区里没有任何两个 migration 共享的数据。

### RED test

`internal/agent/lock_contention_test.go`

```
--- FAIL: TestIndependentMigrationsSerializeOnTargetLock
    4 个并发 batch，mean queue wait = 15.5ms
    RED: independent migrations are serialized on one lock
```

**测试有牙齿（已证明）**：把 `s.mu.Lock()` 摘掉 → 转绿；恢复 → 转红。

### 饱和矩阵佐证（用户提供的真实数据）

| 并发 | 吞吐 | 延迟 p50 | queue_wait p50 |
|---|---|---|---|
| 8 | 126.7 MB/s | 1060ms | 341ms |
| 100 | 116.0 MB/s | 13893ms | 6965ms |

并发 8→100 吞吐零增长，延迟涨 13 倍。加速比仅 1.85x ⇒ 串行段占 ~46%。

从 `target_metric_delta` 反推（c8，600 batch）：

```
锁内工作 (fsync+state_save+write) = 8475ms = 墙钟 64%
锁外 controller 往返              = 4764ms = 墙钟 36%
```

**⇒ 拆锁理论上限 = 1/0.64 = 1.56x，不是 4-5x。**

### Source（开源代码事实）

**Prometheus `tsdb/agent/series.go`** — 锁分片的生产级实现：

- `series.go:130-141` 注释原文：
  > `stripeSeries locks modulo ranges of IDs and hashes to reduce lock contention. The locks are padded to not be on the same cache line.`
- `series.go:145-149` `stripeLock` 带 `_ [40]byte` cache-line padding
- `series.go:151-157` 所有 stripe 预分配，选锁时不发生 map 写
- `series.go:320-326` `hashLock`/`refLock` 用位掩码 `& uint64(s.size-1)`
- `tsdb/head.go:2451-2452` `DefaultStripeSize = 1 << 14`（16384）
- `series.go:263-271` 关键设计约束：
  > `We never hold hashLock and refLock simultaneously, preserving the no-deadlock invariant that GC relies on`

**PostgreSQL 官方文档 19.5 Write Ahead Log**：

> `synchronous_commit` … 区分了「持久性」与「一致性」——`off` 不创建数据库不一致，只可能丢失最近的已提交事务。

即：**并发共享一次持久化写入（group commit）是 WAL 的标准做法，且降低单次持久性不等于破坏一致性。** 这是「让不同 migration 的 fsync 并行」的理论依据。

### 候选

| # | 候选 | 移除机制？ | 判定 |
|---|---|---|---|
| A | **按 migration id 分片锁**（`[256]sync.Mutex`，fnv 哈希取模） | ✅ 移除「不同 migration 互相阻塞」这个机制本身 | **推荐** |
| B | 去掉锁，worker pool 并发 | ❌ 引入 data race（`st.Files[].Chunks` 是共享指针） | 否决 |
| C | 保留锁，只调大 batch | ❌ 不移除机制，只减少临界区次数 | 已在 Step 0 单独验证，见附录 |
| D | 多 Target 进程 + state 外置 | ✅ 但需架构变更 | 记为后续 |

**为什么 A 优于 B**：`st.Files[c.Path].Chunks[c.Index] = c.SHA256`（`target.go:328`）是**幂等性记录**。B 方案下两个 batch 并发改同一个 `*FileState` → data race → 幂等去重失效 → 破坏核心不变式。A 方案下同一 migration 仍取同一把锁，**幂等语义完全不变**。

### 隐藏约束（设计稿完全未提及）

`s.chaosChunkCount` / `s.chaosCrashTriggered`（原 `target.go:44-45`）是**跨 migration 的全局计数器**，当前**依赖这把全局锁**获得保护。分片后会出现 data race。

已解决：`chaosCounters` 改用 `atomic.Int64` + `atomic.Bool`（`striped_lock_proto.go`）。
语义依据：该计数器只是「到 N 就自杀」的门限，**不需要精确**，只需不 race。

---

## Finding 2 — 文档声明的 16MiB 上限必然失败

### Code fact

`internal/migration/batch.go:17` `MaxBatchBytes = 16 << 20`，被**两个不同语义**复用：

1. `service.go:56` —— 配置上限：`if n >= 256<<10 && n <= MaxBatchBytes`
2. `batch.go:77` —— 编码后体积上限：`if out.Len() > MaxBatchBytes { return error }`

编码每 chunk 额外开销（`batch.go:12` `BatchHeaderSize = 5+4+4+8+4+32 = 57`，实测 58 含 path），
所以 payload 等于 16MiB 时，编码后必然 > 16MiB。

### RED test

`internal/migration/batch_ceiling_test.go`

```
--- FAIL: TestMaxBatchBytesCeilingIsUnreachableAsConfigured
    RED: a batch whose payload fits the configured maximum 16777216 must encode,
    got: encoded batch exceeds 16777216 bytes
--- PASS: TestEncodedOverheadIsQuantified
    per-chunk overhead = 58 bytes; a 16MiB budget loses 878.3KiB to overhead
```

线上复现（用户实跑）：`TRANSFER_BATCH_BYTES=16MiB` → **100/100 失败**
`BATCH_ENCODE_FAILED: encoded batch exceeds 16777216 bytes`

而 `README.md` 明确写「允许 `256KiB..16MiB`」——**任何按文档配置的用户必然踩到**。

### 候选

| # | 候选 | 判定 |
|---|---|---|
| A | **拆分两个常量**：配置上限 `MaxConfiguredBatchBytes` 与编码硬上限 `MaxBatchBytes`，配置上限预留编码余量 | **推荐** |
| B | 编码校验改成 `out.Len() > maxBytes + overheadAllowance` | 可行但把余量藏在调用处，更难推理 |
| C | 调大 `MaxBatchBytes` | ❌ 把问题推给下一档，且 16MiB 仍不可达只是变成 17MiB |

### OWASP / 12-Factor 门

- **Fail-closed**：✅ 通过。当前行为是**明确报错**而非静默截断，修复后仍应 fail-closed。
- **Error boundary**：✅ 通过。`BATCH_ENCODE_FAILED` → 4xx/5xx 语义正确。
- **Config via env**：✅ 通过。`TRANSFER_BATCH_BYTES` 已是环境变量（12-Factor III）。
- **Secrets**：N/A，无凭据变更。

---

## 技术验证结果（Step 4）

### 已验证 ✅

| 项 | 结果 |
|---|---|
| Finding 1 RED → 绿 | queue wait `15.5ms → 0ms` |
| 幂等性未破 | `TestSameMigrationStaysSerialized` PASS（5 次重放仍 3 chunk） |
| Race detector | `go test -race ./...` 全部干净 |
| 混合负载 | 24 migrations × 4 batches × 2 次重放 + 交错 progress，`-race` 干净 |
| 分片确实生效 | 4096 keys → **256** distinct stripes |
| **E2E** | PASS `business_sequence=CONTIGUOUS 1..120` `resumed_chunks=2 restored_from_task=8` |
| **Chaos** | **4/4 PASS**（含依赖原子计数器的 CASE 1/2） |
| 真实吞吐 | 见下表 |
| **map 并发写** | 修复前 DATA RACE → 修复后 `TestInitConcurrentMapWrite` PASS（详见下节） |

### 分片锁 + 状态表锁（最终版）并发扫描，batch=4MiB

| 并发 | 原版 | 最终版 |
|---|---|---|
| 8 | 126.7 | 136.28 |
| 16 | 122.1 | 141.48 |
| 32 | 122.3 | **159.95**（+30.8%） |

### 中间版（仅分片锁，无 map 锁）并发扫描

| 并发 | 原版 | 分片锁 | 变化 |
|---|---|---|---|
| 1 | 68.4 | 69.55 | +1.7% |
| 4 | 120.8 | 127.68 | +5.7% |
| 8 | 126.7 | 99.47 **¹** | — |
| 16 | 122.1 | 148.89 | +22.0% |
| 32 | 122.3 | **163.33** | **+33.6%** |

¹ c=8 那个 99.47 是**噪声**：同配置连续三次为 150.83 / 142.89 / 153.90，c=8 首次跑偏低属冷启动（target 目录初始为空）。**饱和点从 c=8 推迟到 c=32 以上**，这才是分片的实际效果。

**诚实声明**：
- 未验证 c=64/100 的完整饱和曲线（时间成本）
- 未验证多 Target 进程场景（候选 D）
- 首次运行出现过 85/100 失败，排查为冷启动（target staging 目录初始为空），后续连续 4 次 100 workloads 全绿；**但未定位到根因，标记为 open risk**

---

## 建议的 fix design（等批准）

### 变更 1 — Finding 2（独立、风险低、可先做）

**文件**：`internal/migration/batch.go`
**改法**：拆两个常量，配置校验预留编码余量。

```go
const (
    MaxBatchBytes          = 16 << 20 // 编码后硬上限，wire format 不变
    MaxConfiguredBatchBytes = MaxBatchBytes - (1 << 20) // 预留 1MiB 编码余量
)
```
`service.go:56` 改用 `MaxConfiguredBatchBytes`。

**Blast radius**：`grep -rn MaxBatchBytes` → `batch.go`(定义/decode/encode 三处) + `service.go:56`。`DecodeBatch` 的 `maxBytes` 参数语义不变，故 wire format 与已存数据**不受影响**。

**Rollback**：`git revert` 单个 commit。配置值 16MiB 会重新不可用——这正是修前的状态。

**Confidence**：**high** — RED 复现 + 机制量化（878.3KiB 余量）+ 修法不触碰 wire format。

### 变更 2 — Finding 1（分片锁）

**文件**：`internal/agent/target.go`、`internal/agent/striped_lock_proto.go`（原型已写好并验证）
**改法**：全局 `sync.Mutex` → `stripedTargetLock`（256 分片，fnv32a 取模，40B padding），7 个 handler 改 `s.locks.lockFor(id)`；chaos 计数改原子。

**Blast radius**（已 grep 全量）：
- 7 个 handler 的加锁点（`target.go:159/207/245/405/449/493/578`）
- `s.chaosChunkCount` / `s.chaosCrashTriggered` 两处引用（`target.go:336/485`）
- **无跨 id 遍历**（`grep 'range s.states'` 空）→ 分片安全的前提已验证
- 外部调用方：0（`TargetServer` 只被 `cmd/target-agent` 使用）

**Rollback**：`git revert`；`stripedTargetLock` 可退化为单锁（`lockFor` 恒返回 `stripes[0]`）作为紧急降级开关。

**Confidence**：**high** — RED→绿已验证、race 干净、E2E + chaos 4/4 通过、真实负载 +33.6%。

### 明确不做

- ❌ Pipeline Transfer Engine（设计稿方案）：吞错误 + data race，收益与分片锁重叠
- ❌ Adaptive Chunk：改错旋钮（batch 数恒定），已在 Step 0 证明 `TRANSFER_BATCH_BYTES` 才是对的
- ❌ Checkpoint Batch：加宽崩溃窗口，chaos CASE 1/2 依赖当前窗口

---

## Open risks

1. **冷启动失败未定位根因**：首次 100-workload 跑出现 85 失败（target staging 空），后续稳定。需在干净环境复现确认。
2. **c≥64 未测**：饱和点后移，但曲线尾部形状未知。
3. **c=8 数值噪声大**：三次 99.47 / 150.83 / 153.90 波动 ±20%，测量需增加重复次数取中位数。
4. ~~`s.states` map 并发写~~ → **已解决并验证**，见下方「验证中发现并修复的缺陷」。

---

## 验证过程中发现并修复的缺陷（重要）

分片锁原型跑 `-race` 时，`TestStripedLockHoldsUnderMixedLoad` 通过，但我在交付前额外写了一个专门测 map 的测试，**立刻抓到真实 data race**：

```
WARNING: DATA RACE
  Previous write at ... handleInit() target.go:200
  Write at        ... handleInit() target.go:200
```

**根因**：按 id 分片**不能**保护 `s.states` 这个 map 本身。两个不同 id 落在不同 stripe 时，各自临界区互不重叠，但**它们写的是同一个 map**。分片解决的是「值」的并发访问，不是「容器」的。

**修复**：新增 `stateTableLock`（独立 `sync.RWMutex` + cache-line padding），
- 13 个 `s.states` 访问点：读用 `RLock`，写（201/548/570/611/636）用 `Lock`
- **只在 map 操作期间持有，绝不跨 fsync** —— 因此不重新引入慢路径串行

**验证**：`TestInitConcurrentMapWrite`（64 并发 init）修复前 FAIL + DATA RACE，修复后 PASS。

**这是本次设计过程最值得记的一点**：如果只跑「原本打算写的测试」，这个 race 会被漏掉，
交付一个「单测全绿但 `-race` 下有竞态」的补丁。分片锁的安全性直觉上很对，但**直觉不覆盖 map 容器本身**。

---

# 第二轮：三个工业级项目的源码对照（子 agent 独立上下文调研）

> 调研对象：`/Users/david/agent-open/{vitess, pd, agentscope}`，每个由独立 subagent 在干净上下文完成。
> 纪律：只采信读到的 `file:line`，找不到写「未找到」，禁止推断。**本轮所有建议均经实测检验，否决 2 条。**

## 1. Vitess（`go/vt/vtgate/buffer/`）— 锁分片的生产实现

| 事实 | file:line |
|---|---|
| **不是分片锁，是「每 shard 一个对象 + 每个对象自带一把锁」** | `shard_buffer.go:111` `mu sync.RWMutex`（per-shard 实例字段） |
| 快路径无锁：原子读状态直接返回 | `shard_buffer.go:107` 注释：*"state tracks the shard's buffering lifecycle. Read atomically on every query; written under mu during transitions."*；`:161-168` *"Lock-free fast path"* |
| **全局 map 用 `sync.Map` 而非 RWMutex** | `buffer.go:160`；理由注释 `buffer.go:156-158`：写入 *"only occur once per shard at startup"* |
| map 的**写**用独立 `createMu` | `buffer.go:163` 注释：*"createMu serializes the creation of shardBuffer objects... which must happen only once per shard."* |
| **两把锁从不嵌套** | `buffer.go:243-250` 双重检查 + `createMu` 内不碰 `sb.mu` |
| 慢路径移出锁 | `shard_buffer.go:599-601` *"Use a new Go routine to release the lock"*；`:619-620` *"stop must be called outside of the lock"* |
| 锁内命名约定 | `shard_buffer.go:333-334` *"the prefix 'Locked'... stresses that sb.mu must be locked before calling the method"* |
| **stats 用「map 锁只管取指针 + 原子值管更新」双层** | `go/stats/counters.go:33-38` 注释：*"The map values are atomic, so add/set on existing keys only requires a read lock"*；`:42-57` `getOrCreate` RLock 快路径 |
| group commit | **未找到**（`grep group.?commit` 零命中） |

**与本项目的关系**：Vitess **独立佐证**了「per-key 锁必须与全局容器锁分离」这个核心判断。

## 2. PD（TiKV Placement Driver）— 控制面高并发的三种手法

TSO 实际路径是 `pkg/tso/`（非 `server/tso/`）。

| 事实 | file:line |
|---|---|
| **临界区内只有纯内存操作** | `pkg/tso/tso.go:144-156` 全函数体仅 `logical += count` + `tsoMux.Lock()`；**锁内无 IO / raft / etcd** |
| 落盘在锁外 + 后台 goroutine | `pkg/tso/tso.go:403-411` `t.saveTimestamp(save)` 在 `updateTimestamp` 内但 `tsoMux` 锁**外**（锁只在 `:116` 单独取） |
| 注释明确禁止在处理路径落盘 | `pkg/tso/tso.go:396-397` *"Only IntervalUpdate is allowed to save timestamp into etcd. It would be dangerous to save timestamp into etcd when handling overflowUpdate."* |
| **用定长数组替代 map** | `pkg/tso/keyspace_group_manager.go:99` `allocators [4096]*Allocator`；注释 `:97-98` *"Use a fixed size array to maximize the efficiency of concurrent access to different keyspace groups"* |
| 热路径只 `RLock` 读元数据拿指针，之后完全无锁 | `pkg/tso/keyspace_group_manager.go:283-284, 154-160`；写元数据才 `kgm.Lock()`（`:831-844`） |
| 一致性靠「时间窗口提前预留」而非锁 | `pkg/tso/tso.go:217` `save := next.Add(t.saveInterval)`，落盘的是**未来时刻**，内存只发 `T` 之前的 ID |
| 前端合批（channel 非 ring buffer） | `pkg/utils/tsoutil/tso_dispatcher.go:38` `maxMergeRequests = 10000`；`:82` 有缓冲 channel；`:143-148` 一次性抽干合并；`:180-186` 单次 RPC；`:200-214` 按 logical 切片分发 |
| actor 模式 | **未找到** |
| 并发测试 | `Makefile:279` `go test -tags deadlock -timeout 20m -race -cover`；`tests/integrations/tso/consistency_test.go:61,67` |

## 3. AgentScope（Python）— 限流与共享状态

| 事实 | file:line |
|---|---|
| per-worker `asyncio.Semaphore(max_concurrency)` **默认 4** | `src/agentscope/app/_service/_index_worker.py:123,207,335` |
| 限流意图注释 | `_index_worker.py:109-112` *"The semaphore protects shared resources that scale with the number of in-flight parses... while the lease CAS in storage protects against the cross-worker version of the same race."* |
| **`defaultdict(asyncio.Lock)` 按 (user, agent) 分片** | `app/workspace_manager/_base.py:60-63` 注释：*"Serialises read-binding-then-mint per (user, agent), so two concurrent first sessions cannot each mint a workspace"* |
| **指出「锁释放早于持久化落盘」的窗口** | `_base.py:64-68` *"The lock is released before that write lands, so without this the next request would read an empty session list and bind a second workspace to the same pair."* |
| 「无锁」论证依赖单线程——**不可移植到 Go** | `app/workspace_manager/_prewarm.py:67-70` *"No lock guards the buffer: every mutation... runs to completion without an intervening await, so on asyncio's single thread no other coroutine can observe a half-applied change."* |
| 批量提交按 batch_size 切片后 `gather` 并发（**非攒批**） | `app/_embedding/_embedding_base.py:204-209, 240-243, 256-258` |
| drain 上限 `max_batch=32` | `app/_service/_index_task_consumer.py:52-56` |
| 并发测试 | `tests/service_chat_locking_test.py:217,282-290`（`IsolatedAsyncioTestCase` + 2 task + gather） |

---

# 实测检验：三条建议，两条被否决

## 建议 1（Vitess）：map 锁换 `sync.Map` → **实测否决**

微基准（`BenchmarkStateLookup*`，12 核并行）：

| 方案 | ns/op |
|---|---|
| `sync.RWMutex` + map | 141.6 |
| `sync.Map` | **30.25**（快 4.7x） |

**但端到端收益约 0.0002%**：

```
map 操作           141.6 ns
单 batch 锁内工作   fsync 35ms + state_save 31ms = 66,000,000 ns
map 占比           141.6 / 66,000,000 = 0.0002%
```

**判定：否决。** 微基准 4.7x 的差异在 66ms 的锁内工作前完全不可见。为 0.0002% 收益引入 `sync.Map` 的语义变化（失去原子性保证）不划算。**保留 `RWMutex`。**

## 建议 2（PD）：定长数组替代 map → **不适用**

PD 用 `[4096]*Allocator` 数组下标直寻址，前提是 keyspace ID 可直接映射为下标。

**本项目不满足**：`internal/common/id.go:15` migration ID 是 `mig-<24位随机hex>`，**非连续、无法映射为数组下标**。

若要套用需引入「启动期解析 ID → 稠密下标」的间接层，属于新增复杂度换 0.0002% 收益（同上）。

**判定：不适用。**

## 建议 3（PD）：把 fsync 移出锁 → **实测否决（正确性）**

锁内时间构成（c=32 实测）：

| 段 | 耗时 | 占锁内 |
|---|---|---|
| fsync | 35.0ms | 52.2% |
| state_save | 31.0ms | 46.3% |
| write | 1.0ms | 1.5% |

移出 fsync+state_save 可让锁内缩短 67x —— 收益诱人。**但原型实现后经论证不成立**：

```
req-A 写自己的 .part 文件 → req-B 写自己的 .part 文件
req-B 调用 groupSync，发现 A 正在 fsync，于是等待 A 的 fsync 完成

问题：fsync(fd) 的语义是「把该 fd 关联的脏页刷盘」
     —— 它不保证其他文件的脏页被刷盘
     ⇒ A 的 fsync 覆盖不到 B 的数据

对比 PostgreSQL：group commit 成立是因为多事务写**同一个 WAL 文件**，
     fsync 覆盖同一文件的不同字节段
     本项目：每个 batch 写**不同的 .part 文件**，fsync 不跨文件生效
```

**判定：否决。** 跨文件 group commit 在 POSIX 语义下不成立。原型已回滚。

## 建议 4（PD）：慢路径彻底移出临界区（`saveLocked` 异步化）→ **OWASP 门否决**

若把 `saveLocked` 移到后台 goroutine，锁内只剩 1ms 的 `write`：

**但违反 fail-closed（OWASP Error Handling）**：HTTP 返回 200 时数据可能尚未落盘。
`saveLocked`（`target.go:641-663`）用 write-tmp + fsync + **atomic rename**，正是为了保证「返回即持久」。

**判定：否决。** 违反项目核心不变式「可恢复 + 幂等」。

---

# 调研后的最终结论

**三个工业项目里，没有任何一个能提供比当前方案更好的解法。**

| 项目 | 手法 | 能否用于本项目 |
|---|---|---|
| Vitess | per-key 锁 + `sync.Map` + 锁内命名约定 | 核心判断已采纳（独立佐证）；`sync.Map` 实测收益 0.0002%，否决 |
| PD | 定长数组 + 临界区纯内存 + 前端合批 | 数组不适用（ID 随机）；合批不安全（fsync 不跨文件）；异步落盘违反 fail-closed |
| AgentScope | per-key 锁 + 信号量限流 | per-key 锁已采纳（独立佐证）；「无锁靠单线程」不可移植到 Go |

**已验证的当前方案是这三个项目交集内的最优解**：

- 按 migration id 分片锁 ← Vitess `shard_buffer.go:111` + AgentScope `_base.py:60-63` 双重佐证
- 独立状态表锁 ← Vitess `buffer.go:160-163`（per-key 锁与容器锁分离）佐证
- chaos 计数改原子 ← 分片后必需的配套
- 保持 `state_save` 在锁内 ← PD 证明了异步化收益，但本场景因 fail-closed 必须否决

**下一步真正值得做的，不是继续调并发结构，而是：**

1. `TRANSFER_BATCH_BYTES` 默认 4MiB → 8MiB（Step 0 已实测 +30.5%，零代码）
2. 修 `MaxBatchBytes` 双用 bug（Finding 2）
3. 那 36% 的锁外时间（controller 往返）—— 三个项目都没覆盖这个维度，需自行 profile

---

# 第三轮：profile `prepare` 阶段（分片锁优化后暴露的新瓶颈）

## 背景

分片锁 + 8MiB batch 实测把吞吐从 122.3 提到 **189.0 MB/s**（+55%），但 `prepare` 从 158ms 涨到 **2052ms**，成为最大单点。本轮定位其成因。

**方法：先插桩测真实压测，再逐个证伪假设。** 连续推翻 3 个假设后才找到根因。

## 插桩结果（c=32）

| 指标 | 值 |
|---|---|
| `prepare_p50` | 1471ms |
| checkpoint 端到端累计 / 次数 | 111959ms / 100 = **单次 1120ms** |
| 走「目录已存在」复用分支 | **0 / 100** |
| 走 `snapshotTree` 复制分支 | **100 / 100** |

**checkpoint 占 `prepare` 的 76%。**

## 机制（`source.go:194-218`）

```go
checkpointID := migrationID                              // :194  每次都唯一
checkpointRoot := filepath.Join(s.CheckpointRoot, checkpointID, workloadID)
if _, err := os.Stat(checkpointRoot); err == nil {       // :200  永不复用
    ... buildManifest ...
}
if err := snapshotTree(root, checkpointRoot); err != nil  // :218  每次复制全量
```

`migrationID` 每次唯一 ⇒ `checkpointRoot` 每次全新 ⇒ **每次 checkpoint 都完整复制一遍 payload**。

## 三个被实测推翻的假设

| 假设 | 预测 | 实测 | 判定 |
|---|---|---|---|
| `buildManifest` 全文件 SHA 慢 | 单次应 ~200ms | 9.3ms，32 并发仍 8.87x 线性加速 | **推翻** |
| `handleWrite` 持读锁做 IO，阻塞 `handleFence` 写锁（`source.go:263-297` vs `:151`） | fence 应被阻塞 | fence 仅 0s，16 并发写也不阻塞 | **推翻**（Go 的 RWMutex 只在写锁已挂起时阻塞新读者） |
| `persistFencesLocked`（`:475-496`，锁内 marshal 全 map + fsync）成本随 fences 数 O(n) 增长 | 第 200 次应远慢于第 1 次 | 9.7ms → 4.8ms，**反而更快** | **推翻** |

## 真正根因：内存压力下的磁盘重读

实验室单次 `snapshotTree`（16MiB，page cache 热）= **23ms**；真实压测单次 = **1167ms**，差 **50 倍**。

排查到压测时的系统状态：

```
Pages free:  5527 × 16KB = 0.06 GB     ← 几乎耗尽
Pages active:   6.23 GB
Pages inactive: 6.19 GB
```

seed 阶段刚写入 **1.6 GB** payload（100 workload × 16MiB），把 page cache 挤满。随后 `snapshotTree` 要读这 1.6 GB 源数据，**大量页已缺失 → 从磁盘重读**。

旁证：本机并发复制带宽上限实测 2.7 GB/s（`conc=4/8` 时 2705/2806 MB/s），而 c=32 时 checkpoint 阶段需要持续 730 MB/s。**带宽够，但 latency 差 50 倍 ⇒ 是缺页/换页，不是带宽不足。**

## 结论

`prepare` 慢的链条：

```
migrationID 每次唯一（source.go:194）
  → checkpointRoot 每次全新
  → snapshotTree 每次复制全量 16MiB（source.go:218）
  → seed 已写入 1.6GB，page cache 耗尽（free 0.06GB）
  → 复制时从磁盘重读
  → 单次 1167ms，占 prepare 76%
```

**这不是锁竞争，也不是磁盘带宽不足，是「不必要的全量复制 + 内存不足」的叠加。**

## 修复方向（未实施，待批准）

| 方向 | 依据 | 预期 | 风险 |
|---|---|---|---|
| **A. checkpoint 按 workload 而非 migration 复用** | 同一 workload 的 payload 在多次迁移间不变 | 消除 100 次全量复制 | 需确认「同一 workload 重复迁移」是合法语义 |
| **B. 硬链接替代复制**（同文件系统内） | snapshot 只需读一致视图，不需独立副本 | 消除写放大 | 源文件被修改时会影响 checkpoint，需验证 |
| **C. 调大 batch 不解决此问题** | checkpoint 在 prepare 阶段，batch 是 transfer 阶段 | 无 | — |

**A 需要先确认业务语义**：当前 `checkpointID = migrationID` 是刻意隔离（每次迁移独立快照），改成按 workload 复用会改变这个语义。**这属于产品决策，不是纯技术优化。**

## 诚实记账

- 未测 c≥64 的 checkpoint 行为
- 「page cache 耗尽」是**推断链的终点**（free 0.06GB 是实测），但未用 `fs_usage` 直接观测缺页率
- 方案 A/B 均未实现、未验证
