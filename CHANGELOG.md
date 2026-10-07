# Changelog

## Performance diagnosis

- Added Source batch read/encode timing.
- Added Target batch queue-wait timing around `TargetServer.mu`.
- Added Target write, chunk fsync, and state-save timing.
- Added Target batch in-flight/peak instrumentation.
- Persisted Transfer telemetry into `Migration.Stats` so the controller response and LoadTest can report it.
- Added `scripts/bottleneck-matrix.sh` for 16MiB payload saturation at concurrency `1 2 4 8 16 32 64 100`.
- Added CSV summary and automatic 95%-of-peak throughput saturation point detection.

## Semantics

No change to Fence / Checkpoint / Transfer / Resume / Verify / Activate / Commit semantics. This work is diagnostic-first; concurrent Target transfer is intentionally left unchanged.
