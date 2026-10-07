# PostgreSQL Control Plane

## Why PostgreSQL

The Migration Controller is managing business state, not only service discovery:

- migration lifecycle
- workload ownership
- idempotency
- checkpoint metadata
- progress metadata
- controller lease
- audit history

Therefore PostgreSQL is the primary control-plane system of record.

## Schema

```text
migrations
  ├── state
  ├── manifest
  ├── fence_epoch
  ├── stats
  └── errors

workloads
  ├── current_node
  ├── fence_epoch
  └── active_migration_id

idempotency_keys
  └── key → migration_id

controller_leases
  └── migration_id → controller holder + expiry

audit_events
  └── append-only state persistence history
```

## Important boundary

PostgreSQL stores control-plane facts. It does not store the checkpoint bytes or business files.

That data remains on Source/Target durable files and can later move to S3/MinIO/a dedicated transfer plane.
