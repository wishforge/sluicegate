#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "$ROOT/scripts/runtime-env.sh"
RUN="${RUN_DIR:-/tmp/sluicegate-e2e}"
BASE_PORT="${BASE_PORT:-21280}"
CONTROLLER_PORT=$BASE_PORT
SOURCE_PORT=$((BASE_PORT + 1))
TARGET_PORT=$((BASE_PORT + 2))
TOTAL_TASKS="${TOTAL_TASKS:-120}"
TASK_INTERVAL_MS="${TASK_INTERVAL_MS:-20}"
DATABASE_URL="${DATABASE_URL}"
PSQL_BIN="$ROOT/scripts/psql.sh"

"$ROOT/scripts/db-start.sh"
DATABASE_URL="$DATABASE_URL" "$ROOT/scripts/db-reset.sh" >/dev/null
rm -rf "$RUN"
mkdir -p "$RUN"/{source,checkpoints,target,logs}

cleanup() {
  local pids=()
  for f in source.pid target.pid controller.pid workload-source.pid workload-target.pid; do
    if [[ -f "$RUN/$f" ]]; then
      pid=$(cat "$RUN/$f" || true)
      if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then pids+=("$pid"); kill -TERM "$pid" 2>/dev/null || true; fi
    fi
  done
  for pid in "${pids[@]:-}"; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM

build() {
  (cd "$ROOT" && GOTOOLCHAIN=local go test ./...)
  (cd "$ROOT" && GOTOOLCHAIN=local go build -o "$RUN/source-agent" ./cmd/source-agent)
  (cd "$ROOT" && GOTOOLCHAIN=local go build -o "$RUN/target-agent" ./cmd/target-agent)
  (cd "$ROOT" && GOTOOLCHAIN=local go build -o "$RUN/migration-controller" ./cmd/migration-controller)
  (cd "$ROOT" && GOTOOLCHAIN=local go build -o "$RUN/workload-runner" ./cmd/workload-runner)
}
wait_ready(){ for _ in $(seq 1 120); do curl -sf "http://127.0.0.1:$1/healthz" >/dev/null 2>&1 && return 0; sleep .1; done; return 1; }
wait_for_line(){ local f="$1" p="$2" n="${3:-200}"; for _ in $(seq 1 "$n"); do grep -q "$p" "$f" 2>/dev/null && return 0; sleep .05; done; return 1; }

sha256_file() {
  local path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
  else
    echo "sha256 tool not found: need sha256sum or shasum" >&2
    return 1
  fi
}

build

WORKLOAD_ROOT="$RUN/source" CHECKPOINT_ROOT="$RUN/checkpoints" HTTP_ADDR=:$SOURCE_PORT SERVICE_VERSION=sluicegate "$RUN/source-agent" >"$RUN/logs/source-1.log" 2>&1 & echo $! >"$RUN/source.pid"
TARGET_ROOT="$RUN/target" HTTP_ADDR=:$TARGET_PORT SERVICE_VERSION=sluicegate "$RUN/target-agent" >"$RUN/logs/target-1.log" 2>&1 & echo $! >"$RUN/target.pid"
DATABASE_URL="$DATABASE_URL" DB_MAX_CONNS="${DB_MAX_CONNS:-32}" DB_MIN_CONNS="${DB_MIN_CONNS:-4}" CONTROLLER_ID="e2e-controller" RECONCILER_ENABLED=false FAIL_AFTER_CHUNKS=2 HTTP_ADDR=:$CONTROLLER_PORT SERVICE_VERSION=sluicegate "$RUN/migration-controller" >"$RUN/logs/controller-1.log" 2>&1 & echo $! >"$RUN/controller.pid"
wait_ready "$CONTROLLER_PORT"; wait_ready "$SOURCE_PORT"; wait_ready "$TARGET_PORT"

WORKLOAD_ROLE=source WORKLOAD_ID=agent-001 SOURCE_AGENT="http://127.0.0.1:$SOURCE_PORT" TOTAL_TASKS="$TOTAL_TASKS" TASK_INTERVAL_MS="$TASK_INTERVAL_MS" SERVICE_VERSION=sluicegate "$RUN/workload-runner" >"$RUN/logs/workload-source.log" 2>&1 & echo $! >"$RUN/workload-source.pid"
wait_for_line "$RUN/logs/workload-source.log" 'workload_task_completed' 200
sleep .1

resp=$(curl -sfS -X POST "http://127.0.0.1:$CONTROLLER_PORT/v1/migrations" -H 'Content-Type: application/json' -H 'Idempotency-Key: sluicegate-e2e-001' -d "{\"workload_id\":\"agent-001\",\"source_agent\":\"http://127.0.0.1:$SOURCE_PORT\",\"target_agent\":\"http://127.0.0.1:$TARGET_PORT\",\"chunk_size\":65536}")
id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$resp")

curl -sfS -X POST "http://127.0.0.1:$CONTROLLER_PORT/v1/migrations/$id/prepare" >/dev/null
wait_for_line "$RUN/logs/workload-source.log" 'workload_fenced_stop' 100
source_fenced_task=$(grep -E 'workload_fenced_(stop|after_task)' "$RUN/logs/workload-source.log" | tail -1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["last_completed_task"])')

kill "$(cat "$RUN/source.pid")" 2>/dev/null || true; sleep .3
WORKLOAD_ROOT="$RUN/source" CHECKPOINT_ROOT="$RUN/checkpoints" HTTP_ADDR=:$SOURCE_PORT SERVICE_VERSION=sluicegate "$RUN/source-agent" >"$RUN/logs/source-2.log" 2>&1 & echo $! >"$RUN/source.pid"
wait_ready "$SOURCE_PORT"
status=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:$SOURCE_PORT/v1/workloads/agent-001/write?path=blocked.txt" --data-binary 'must-not-write')
test "$status" = "423"

status=$(curl -sS -o "$RUN/transfer-1.json" -w '%{http_code}' -X POST "http://127.0.0.1:$CONTROLLER_PORT/v1/migrations/$id/transfer")
test "$status" = "502"

kill "$(cat "$RUN/target.pid")" 2>/dev/null || true
kill "$(cat "$RUN/controller.pid")" 2>/dev/null || true
sleep .4
TARGET_ROOT="$RUN/target" HTTP_ADDR=:$TARGET_PORT SERVICE_VERSION=sluicegate "$RUN/target-agent" >"$RUN/logs/target-2.log" 2>&1 & echo $! >"$RUN/target.pid"
DATABASE_URL="$DATABASE_URL" DB_MAX_CONNS="${DB_MAX_CONNS:-32}" DB_MIN_CONNS="${DB_MIN_CONNS:-4}" CONTROLLER_ID="e2e-controller" RECONCILER_ENABLED=false HTTP_ADDR=:$CONTROLLER_PORT SERVICE_VERSION=sluicegate "$RUN/migration-controller" >"$RUN/logs/controller-2.log" 2>&1 & echo $! >"$RUN/controller.pid"
wait_ready "$CONTROLLER_PORT"; wait_ready "$TARGET_PORT"

curl -sfS -X POST "http://127.0.0.1:$CONTROLLER_PORT/v1/migrations/$id/transfer" >/dev/null
curl -sfS -X POST "http://127.0.0.1:$CONTROLLER_PORT/v1/migrations/$id/activate" >/dev/null

checkpoint_sha=$(sha256_file "$RUN/checkpoints/$id/agent-001/runtime/state.json")
target_checkpoint_sha=$(sha256_file "$RUN/target/.migration-staging/$id/agent-001/runtime/state.json" 2>/dev/null || true)
if [[ -z "$target_checkpoint_sha" ]]; then target_checkpoint_sha=$(sha256_file "$RUN/target/workloads/agent-001/runtime/state.json"); fi
test "$checkpoint_sha" = "$target_checkpoint_sha"

WORKLOAD_ROLE=target WORKLOAD_ID=agent-001 TARGET_ROOT="$RUN/target" TOTAL_TASKS="$TOTAL_TASKS" TASK_INTERVAL_MS="$TASK_INTERVAL_MS" SERVICE_VERSION=sluicegate "$RUN/workload-runner" >"$RUN/logs/workload-target.log" 2>&1 & echo $! >"$RUN/workload-target.pid"
wait_for_line "$RUN/logs/workload-target.log" 'workload_restored' 200
wait_for_line "$RUN/logs/workload-target.log" 'workload_completed' 500
wait "$(cat "$RUN/workload-target.pid")"

curl -sfS -X POST "http://127.0.0.1:$CONTROLLER_PORT/v1/migrations/$id/commit" >/dev/null
state=$(curl -sfS "http://127.0.0.1:$CONTROLLER_PORT/v1/migrations/$id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])')
test "$state" = "COMMITTED"
final_state=$(python3 - "$RUN/target/workloads/agent-001/runtime/state.json" <<'PY'
import json,sys
print(json.load(open(sys.argv[1]))["last_completed_task"])
PY
)
test "$final_state" = "$TOTAL_TASKS"
python3 - "$RUN/target/workloads/agent-001" "$TOTAL_TASKS" <<'PY'
import json,pathlib,sys
root=pathlib.Path(sys.argv[1]); total=int(sys.argv[2])
ids=[json.loads(p.read_text())["task_id"] for p in sorted((root/"ledger").glob("*.json"))]
assert ids==list(range(1,total+1)), (ids[:5],ids[-5:],len(ids))
assert len(ids)==len(set(ids))==total
print(f"business_sequence=CONTIGUOUS 1..{total}")
PY

id2=$(curl -sfS -X POST "http://127.0.0.1:$CONTROLLER_PORT/v1/migrations" -H 'Content-Type: application/json' -H 'Idempotency-Key: sluicegate-e2e-001' -d "{\"workload_id\":\"agent-001\",\"source_agent\":\"http://127.0.0.1:$SOURCE_PORT\",\"target_agent\":\"http://127.0.0.1:$TARGET_PORT\",\"chunk_size\":65536}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
test "$id" = "$id2"
status=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT "http://127.0.0.1:$SOURCE_PORT/v1/workloads/agent-001/write?path=after-commit.txt" --data-binary 'new-write')
test "$status" = "200"
resumed=$(
  "$PSQL_BIN" -d "$DATABASE_URL" -Atqc "select coalesce((stats->>'chunks_resumed')::bigint,0) from migrations where id='$id'"
)
restored=$(grep 'workload_restored' "$RUN/logs/workload-target.log" | tail -1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["restored_from_task"])')

echo "PASS migration=$id state=$state checkpoint_sha256=$target_checkpoint_sha resumed_chunks=$resumed restored_from_task=$restored source_fenced_task=$source_fenced_task total_tasks=$TOTAL_TASKS final_task=$final_state store=postgres"
