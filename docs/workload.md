# Stateful Workload Contract

## 业务状态

这里使用一个最小但真实的业务序列：`task 1..N`。

```text
runtime/state.json
ledger/00000001.json
ledger/00000002.json
...
```

`runtime/state.json`：

- `last_completed_task`: 最后完成的业务 task。
- `business_counter`: 与 task 序列绑定的业务计数。
- `owner`: `source` 或 `target`。

`ledger/<task>.json`：每个已经完成的业务 task 的唯一记录。

## Source

Source Runner 不直接写 Source filesystem，而是通过 Source Agent：

```text
Workload Runner
    ↓ HTTP PUT
Source Agent /v1/workloads/{id}/write
    ↓
Source workload filesystem
```

因此 Source Agent 的 Fence 能真正阻断新的业务写入。

## Target

Target Runner 等待：

```text
workloads/{id}/.migration-activation.json
```

存在后读取 checkpoint 中的 `runtime/state.json`，并从：

```text
last_completed_task + 1
```

继续执行。

## 业务正确性

理想迁移满足：

```text
Source completed: 1..K
Target completed: K+1..N
Final:             1..N
```

因此最终验证的是业务序列，而不仅是文件 checksum。
