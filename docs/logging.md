# Logging Contract

## Required fields

| Field | Meaning |
|---|---|
| `service` | service identity: controller / source-agent / target-agent / stateful-workload-runner |
| `version` | runtime version; defaults to `sluicegate`, override with `SERVICE_VERSION` |
| `request_id` | request-level correlation ID where applicable |
| `trace_id` | cross-service correlation ID where applicable |
| `migration_id` | migration-level identity where applicable |
| `workload_id` | business workload identity |
| `role` | `source` / `target` for workload runner |
| `level` | INFO/WARN/ERROR/DEBUG |
| `msg` | stable event name, not a free-form sentence |
| `error_code` | machine-readable failure category |

## Event rules

- INFO: state transitions, durable business milestones, workload ownership changes.
- WARN: resumable interruption, deliberate fault injection, degraded behavior.
- ERROR: terminal failure or action failure requiring operator attention.
- DEBUG: per-chunk details and per-task details; do not use INFO for high-cardinality data-plane events.

## Important workload events

```text
workload_started
workload_task_completed
workload_fenced_stop
workload_fenced_after_task
workload_restored
workload_completed
```

The critical handoff evidence is:

```text
Source: workload_fenced_* last_completed_task=K
Target: workload_restored restored_from_task=K
Target: workload_completed last_completed_task=N
Controller: migration_committed
```

That sequence directly answers whether business execution continued rather than restarted.

Avoid putting secrets, fence tokens, credentials, or business payloads into logs.
