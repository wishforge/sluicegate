#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "$ROOT/scripts/runtime-env.sh"
RUN="${RUN_DIR:-/tmp/sluicegate-load}"
BASE_PORT="${BASE_PORT:-22280}"
CONTROLLER_PORT=$BASE_PORT
SOURCE_PORT=$((BASE_PORT + 1))
TARGET_PORT=$((BASE_PORT + 2))
DATABASE_URL="${DATABASE_URL}"
PSQL_BIN="$ROOT/scripts/psql.sh"
RECONCILER_ENABLED="${RECONCILER_ENABLED:-false}"
KEEP_SERVICES="${KEEP_SERVICES:-0}"

WORKLOADS="${WORKLOADS:-100}"
CONCURRENCY="${CONCURRENCY:-20}"
SEED_CONCURRENCY="${SEED_CONCURRENCY:-20}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-4096}"
TASKS="${TASKS:-10}"
CHUNK_SIZE="${CHUNK_SIZE:-65536}"
DB_MAX_CONNS="${DB_MAX_CONNS:-32}"
DB_MIN_CONNS="${DB_MIN_CONNS:-4}"
LOAD_GLOBAL_TIMEOUT="${LOAD_GLOBAL_TIMEOUT:-90m}"
LOAD_PROGRESS_INTERVAL="${LOAD_PROGRESS_INTERVAL:-5s}"
TRANSFER_BATCH_BYTES="${TRANSFER_BATCH_BYTES:-8388608}"
LOAD_OUTPUT="${LOAD_OUTPUT:-$RUN/loadtest-result.json}"

"$ROOT/scripts/db-start.sh"
"$ROOT/scripts/db-reset.sh" >/dev/null
rm -rf "$RUN"
mkdir -p "$RUN"/{source,checkpoints,target,logs,metrics}

cleanup() {
  if [[ "$KEEP_SERVICES" == "1" ]]; then
    echo "[loadtest] KEEP_SERVICES=1; leaving services running"
    return
  fi
  local pids=()
  for f in source.pid target.pid controller.pid; do
    if [[ -f "$RUN/$f" ]]; then
      pid=$(cat "$RUN/$f" || true)
      if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
        pids+=("$pid")
        kill -TERM "$pid" 2>/dev/null || true
      fi
    fi
  done
  for pid in "${pids[@]:-}"; do wait "$pid" 2>/dev/null || true; done
}
print_logs_on_error() {
  rc=$?
  if [[ $rc -ne 0 ]]; then
    echo "[loadtest] FAILED rc=$rc"
    for f in "$RUN"/logs/*.log; do
      [[ -f "$f" ]] || continue
      echo "===== $f ====="
      tail -80 "$f" || true
    done
  fi
  cleanup
  exit "$rc"
}
trap print_logs_on_error EXIT

build() {
  (cd "$ROOT" && GOTOOLCHAIN=local go test ./...)
  (cd "$ROOT" && GOTOOLCHAIN=local go vet ./...)
  (cd "$ROOT" && GOTOOLCHAIN=local go build -o "$RUN/source-agent" ./cmd/source-agent)
  (cd "$ROOT" && GOTOOLCHAIN=local go build -o "$RUN/target-agent" ./cmd/target-agent)
  (cd "$ROOT" && GOTOOLCHAIN=local go build -o "$RUN/migration-controller" ./cmd/migration-controller)
  (cd "$ROOT" && GOTOOLCHAIN=local go build -o "$RUN/loadtest" ./cmd/loadtest)
}

wait_ready() {
  local port="$1"
  for _ in $(seq 1 200); do
    if curl -sf --max-time 1 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then return 0; fi
    sleep .1
  done
  return 1
}

snapshot_metrics() {
  local suffix="$1"
  for pair in \
    "controller $CONTROLLER_PORT" \
    "source $SOURCE_PORT" \
    "target $TARGET_PORT"; do
    set -- $pair
    curl -sf "http://127.0.0.1:$2/metrics" >"$RUN/metrics/${1}-${suffix}.prom"
  done
  "$PSQL_BIN" -d "$DATABASE_URL" -Atqc \
    "select datname,xact_commit,xact_rollback,blks_read,blks_hit,tup_inserted,tup_updated,tup_deleted from pg_stat_database where datname='migration';" \
    >"$RUN/metrics/postgres-${suffix}.txt"
}

build

DATABASE_URL="$DATABASE_URL" DB_MAX_CONNS="$DB_MAX_CONNS" DB_MIN_CONNS="$DB_MIN_CONNS" CONTROLLER_ID="load-controller" \
RECONCILER_ENABLED="$RECONCILER_ENABLED" \
HTTP_ADDR=:$CONTROLLER_PORT SERVICE_VERSION=sluicegate TRANSFER_BATCH_BYTES="$TRANSFER_BATCH_BYTES" \
"$RUN/migration-controller" >"$RUN/logs/controller.log" 2>&1 & echo $! >"$RUN/controller.pid"

WORKLOAD_ROOT="$RUN/source" CHECKPOINT_ROOT="$RUN/checkpoints" \
HTTP_ADDR=:$SOURCE_PORT SERVICE_VERSION=sluicegate \
"$RUN/source-agent" >"$RUN/logs/source.log" 2>&1 & echo $! >"$RUN/source.pid"

TARGET_ROOT="$RUN/target" HTTP_ADDR=:$TARGET_PORT SERVICE_VERSION=sluicegate \
"$RUN/target-agent" >"$RUN/logs/target.log" 2>&1 & echo $! >"$RUN/target.pid"

wait_ready "$CONTROLLER_PORT"
wait_ready "$SOURCE_PORT"
wait_ready "$TARGET_PORT"

snapshot_metrics before

printf '%s\n' "[loadtest] starting benchmark"
printf '%s\n' "  workloads=$WORKLOADS concurrency=$CONCURRENCY seed_concurrency=$SEED_CONCURRENCY payload_bytes=$PAYLOAD_BYTES tasks=$TASKS chunk_size=$CHUNK_SIZE reconciler=$RECONCILER_ENABLED"

"$RUN/loadtest" \
  -controller "http://127.0.0.1:$CONTROLLER_PORT" \
  -source "http://127.0.0.1:$SOURCE_PORT" \
  -target "http://127.0.0.1:$TARGET_PORT" \
  -workloads "$WORKLOADS" \
  -concurrency "$CONCURRENCY" \
  -seed-concurrency "$SEED_CONCURRENCY" \
  -payload-bytes "$PAYLOAD_BYTES" \
  -tasks "$TASKS" \
  -chunk-size "$CHUNK_SIZE" \
  -global-timeout "$LOAD_GLOBAL_TIMEOUT" \
  -progress-interval "$LOAD_PROGRESS_INTERVAL" \
  -output "$LOAD_OUTPUT"

snapshot_metrics after

python3 - "$RUN/metrics/controller-before.prom" "$RUN/metrics/controller-after.prom" "$RUN/metrics/source-before.prom" "$RUN/metrics/source-after.prom" "$RUN/metrics/target-before.prom" "$RUN/metrics/target-after.prom" "$RUN/metrics/postgres-before.txt" "$RUN/metrics/postgres-after.txt" <<'PY'
import json
import pathlib
import sys

def prom(path):
    out = {}
    for line in pathlib.Path(path).read_text().splitlines():
        if not line or line.startswith('#'):
            continue
        key, value = line.rsplit(' ', 1)
        out[key] = int(value)
    return out

def delta(before, after):
    a = prom(before)
    b = prom(after)
    return {k: b.get(k, 0) - a.get(k, 0) for k in sorted(set(a) | set(b))}

def pg(path):
    text = pathlib.Path(path).read_text().strip()
    if not text:
        return {}
    row = text.split('|')
    names = ['datname', 'xact_commit', 'xact_rollback', 'blks_read', 'blks_hit', 'tup_inserted', 'tup_updated', 'tup_deleted']
    return dict(zip(names, row))

print('[loadtest] controller_metric_delta', json.dumps(delta(sys.argv[1], sys.argv[2]), sort_keys=True))
print('[loadtest] source_metric_delta', json.dumps(delta(sys.argv[3], sys.argv[4]), sort_keys=True))
print('[loadtest] target_metric_delta', json.dumps(delta(sys.argv[5], sys.argv[6]), sort_keys=True))
print('[loadtest] postgres_before', json.dumps(pg(sys.argv[7]), sort_keys=True))
print('[loadtest] postgres_after', json.dumps(pg(sys.argv[8]), sort_keys=True))
PY

echo "[loadtest] result=$LOAD_OUTPUT"
cleanup
trap - EXIT
