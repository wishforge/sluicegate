#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "$ROOT/scripts/runtime-env.sh"
BASE_RUN="${RUN_DIR:-/tmp/sluicegate-chaos}"
DATABASE_URL="${DATABASE_URL}"
PSQL_BIN="$ROOT/scripts/psql.sh"
BASE_PORT="${BASE_PORT:-20180}"
CONTROLLER_LEASE_TTL_MS="${CONTROLLER_LEASE_TTL_MS:-3000}"
TOTAL_TASKS="${TOTAL_TASKS:-120}"
TASK_INTERVAL_MS="${TASK_INTERVAL_MS:-8}"
CHUNK_SIZE="${CHUNK_SIZE:-65536}"
"$ROOT/scripts/db-start.sh"
DATABASE_URL="$DATABASE_URL" "$ROOT/scripts/db-reset.sh" >/dev/null
mkdir -p "$BASE_RUN"

BIN="$BASE_RUN/bin"
mkdir -p "$BIN"

log(){ printf '[sluicegate] %s\n' "$*"; }
fail(){ printf '[sluicegate][FAIL] %s\n' "$*" >&2; exit 1; }

cleanup_all(){
  shopt -s nullglob
  for pidfile in "$BASE_RUN"/case-*/*.pid; do
    [[ -f "$pidfile" ]] || continue
    pid=$(cat "$pidfile" 2>/dev/null || true)
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    kill -TERM "$pid" 2>/dev/null || true
  done
  sleep 0.2
  for pidfile in "$BASE_RUN"/case-*/*.pid; do
    [[ -f "$pidfile" ]] || continue
    pid=$(cat "$pidfile" 2>/dev/null || true)
    [[ "$pid" =~ ^[0-9]+$ ]] || continue
    kill -KILL "$pid" 2>/dev/null || true
  done
}
trap cleanup_all EXIT INT TERM

free_port(){
  local port="$1"
  local pids
  pids=$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)
  for pid in $pids; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  sleep 0.1
  pids=$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)
  for pid in $pids; do
    kill -KILL "$pid" 2>/dev/null || true
  done
}

build() {
  log "building binaries"
  (cd "$ROOT" && GOTOOLCHAIN=local go build -o "$BIN/migration-controller" ./cmd/migration-controller)
  (cd "$ROOT" && GOTOOLCHAIN=local go build -o "$BIN/source-agent" ./cmd/source-agent)
  (cd "$ROOT" && GOTOOLCHAIN=local go build -o "$BIN/target-agent" ./cmd/target-agent)
  (cd "$ROOT" && GOTOOLCHAIN=local go build -o "$BIN/workload-runner" ./cmd/workload-runner)
}

wait_ready(){
  local port="$1"
  for _ in $(seq 1 80); do
    if curl -fsS --max-time 1 "http://127.0.0.1:$port/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  return 1
}


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

wait_dead(){
  local pid="$1"
  for _ in $(seq 1 60); do
    if ! kill -0 "$pid" 2>/dev/null; then return 0; fi
    sleep 0.05
  done
  return 1
}

reap(){
  local pid="$1"
  wait "$pid" 2>/dev/null || true
}

term_pid(){
  local pid="$1"
  kill -TERM "$pid" 2>/dev/null || true
  wait_dead "$pid" || { kill -KILL "$pid" 2>/dev/null || true; }
  wait "$pid" 2>/dev/null || true
}


post_retry(){
  local url="$1" max_attempts="${2:-80}"
  local body="" status=""
  for _ in $(seq 1 "$max_attempts"); do
    set +e
    body=$(curl -sS --max-time 10 -w $'\n%{http_code}' -X POST "$url" 2>/dev/null)
    set -e
    status="${body##*$'\n'}"
    if [[ "$status" == "200" ]]; then
      return 0
    fi
    if [[ "$status" != "409" ]]; then
      printf '%s\n' "$body" >&2
      return 1
    fi
    sleep 0.25
  done
  echo "timed out waiting for migration lease: $url" >&2
  return 1
}

wait_task_file(){
  local file="$1"; local timeout="$2"
  for _ in $(seq 1 "$timeout"); do
    [[ -f "$file" ]] && return 0
    sleep 0.05
  done
  return 1
}

source_write_status(){
  local port="$1" path="$2"
  curl -sS --max-time 3 -o /dev/null -w '%{http_code}' -X PUT \
    "http://127.0.0.1:$port/v1/workloads/agent-001/write?path=$path" \
    --data-binary 'chaos-write'
}

highest_task(){
  local root="$1"
  python3 "$ROOT/scripts/verify_sequence.py" --highest "$root"
}

verify_contiguous(){
  local root="$1" total="$2"
  python3 "$ROOT/scripts/verify_sequence.py" "$root" "$total"
}

start_stack(){
  local run="$1" base="$2" target_crash="${3:-0}" controller_crash="${4:-0}" commit_pause="${5:-0}"
  local cp=$((base+0)) sp=$((base+1)) tp=$((base+2))
  mkdir -p "$run"/{source,checkpoints,target,logs}
  free_port "$cp"; free_port "$sp"; free_port "$tp"

  WORKLOAD_ROOT="$run/source" CHECKPOINT_ROOT="$run/checkpoints" HTTP_ADDR=:$sp \
    "$BIN/source-agent" >"$run/logs/source.log" 2>&1 & echo $! >"$run/source.pid"

  if [[ "$target_crash" -gt 0 ]]; then
    CRASH_AFTER_CHUNKS="$target_crash" TARGET_ROOT="$run/target" HTTP_ADDR=:$tp \
      "$BIN/target-agent" >"$run/logs/target-1.log" 2>&1 & echo $! >"$run/target.pid"
  else
    TARGET_ROOT="$run/target" HTTP_ADDR=:$tp \
      "$BIN/target-agent" >"$run/logs/target-1.log" 2>&1 & echo $! >"$run/target.pid"
  fi

  if [[ "$controller_crash" -gt 0 ]]; then
    CRASH_AFTER_CHUNKS="$controller_crash" DATABASE_URL="$DATABASE_URL" DB_MAX_CONNS="${DB_MAX_CONNS:-32}" DB_MIN_CONNS="${DB_MIN_CONNS:-4}" CONTROLLER_ID="chaos-$(basename "$run")" CONTROLLER_LEASE_TTL_MS="$CONTROLLER_LEASE_TTL_MS" RECONCILER_ENABLED=false HTTP_ADDR=:$cp \
      "$BIN/migration-controller" >"$run/logs/controller-1.log" 2>&1 & echo $! >"$run/controller.pid"
  elif [[ "$commit_pause" -gt 0 ]]; then
    CHAOS_COMMIT_PAUSE_MS="$commit_pause" DATABASE_URL="$DATABASE_URL" DB_MAX_CONNS="${DB_MAX_CONNS:-32}" DB_MIN_CONNS="${DB_MIN_CONNS:-4}" CONTROLLER_ID="chaos-$(basename "$run")" CONTROLLER_LEASE_TTL_MS="$CONTROLLER_LEASE_TTL_MS" RECONCILER_ENABLED=false HTTP_ADDR=:$cp \
      "$BIN/migration-controller" >"$run/logs/controller-1.log" 2>&1 & echo $! >"$run/controller.pid"
  else
    DATABASE_URL="$DATABASE_URL" DB_MAX_CONNS="${DB_MAX_CONNS:-32}" DB_MIN_CONNS="${DB_MIN_CONNS:-4}" CONTROLLER_ID="chaos-$(basename "$run")" CONTROLLER_LEASE_TTL_MS="$CONTROLLER_LEASE_TTL_MS" RECONCILER_ENABLED=false HTTP_ADDR=:$cp \
      "$BIN/migration-controller" >"$run/logs/controller-1.log" 2>&1 & echo $! >"$run/controller.pid"
  fi

  wait_ready "$cp" || fail "controller not ready"
  wait_ready "$sp" || fail "source not ready"
  wait_ready "$tp" || fail "target not ready"
}

restart_controller(){
  local run="$1" base="$2"
  local port=$((base+0))
  sleep $((CONTROLLER_LEASE_TTL_MS / 1000 + 1))
  DATABASE_URL="$DATABASE_URL" DB_MAX_CONNS="${DB_MAX_CONNS:-32}" DB_MIN_CONNS="${DB_MIN_CONNS:-4}" CONTROLLER_ID="chaos-$(basename "$run")" CONTROLLER_LEASE_TTL_MS="$CONTROLLER_LEASE_TTL_MS" RECONCILER_ENABLED=false HTTP_ADDR=:$port \
    "$BIN/migration-controller" >"$run/logs/controller-restart.log" 2>&1 & echo $! >"$run/controller.pid"
  wait_ready "$port" || fail "controller restart not ready"
}

restart_target(){
  local run="$1" base="$2"
  local port=$((base+2))
  TARGET_ROOT="$run/target" HTTP_ADDR=:$port \
    "$BIN/target-agent" >"$run/logs/target-restart.log" 2>&1 & echo $! >"$run/target.pid"
  wait_ready "$port" || fail "target restart not ready"
}

prepare_workload(){
  local run="$1" base="$2"
  local sp=$((base+1))
  mkdir -p "$run/payload"
  printf 'sluicegate-payload-' > "$run/payload/payload.txt"
  python3 -c 'import sys; p=sys.argv[1]; open(p,"a",encoding="utf-8").write("x"*900000)' "$run/payload/payload.txt"
  curl -fsS -X PUT "http://127.0.0.1:$sp/v1/workloads/agent-001/write?path=data/payload.txt" --data-binary @"$run/payload/payload.txt" >/dev/null
  TOTAL_TASKS="$TOTAL_TASKS" TASK_INTERVAL_MS="$TASK_INTERVAL_MS" WORKLOAD_ROLE=source \
    WORKLOAD_ID=agent-001 SOURCE_AGENT="http://127.0.0.1:$sp" \
    "$BIN/workload-runner" >"$run/logs/workload-source.log" 2>&1 & echo $! >"$run/workload-source.pid"
  wait_task_file "$run/source/agent-001/ledger/00000020.json" 160 || fail "source workload did not reach task 20"
}

create_migration(){
  local run="$1" base="$2" case_key="$3"
  local cp=$((base+0)) sp=$((base+1)) tp=$((base+2))
  local resp id
  resp=$(curl -fsS --max-time 10 -X POST "http://127.0.0.1:$cp/v1/migrations" \
    -H 'Content-Type: application/json' -H "Idempotency-Key: $case_key" \
    -d "{\"workload_id\":\"agent-001\",\"source_agent\":\"http://127.0.0.1:$sp\",\"target_agent\":\"http://127.0.0.1:$tp\",\"chunk_size\":$CHUNK_SIZE}")
  id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$resp")
  echo "$id" > "$run/migration.id"
}

prepare_migration(){
  local run="$1" base="$2" id
  local cp=$((base+0))
  id=$(cat "$run/migration.id")
  curl -fsS --max-time 30 -X POST "http://127.0.0.1:$cp/v1/migrations/$id/prepare" >/dev/null
  local status
  status=$(source_write_status $((base+1)) "blocked-after-fence.txt")
  [[ "$status" == "423" ]] || fail "source was not fenced, status=$status"
  # Source may be running when fencing happens; it must stop on the next write.
  if kill -0 "$(cat "$run/workload-source.pid")" 2>/dev/null; then
    wait_dead "$(cat "$run/workload-source.pid")" || true
  fi
  local source_fenced_task
  source_fenced_task=$(highest_task "$run/source")
  verify_contiguous "$run/source" "$source_fenced_task"
}

start_target_runner(){
  local run="$1" base="$2"
  local tp=$((base+2))
  TOTAL_TASKS="$TOTAL_TASKS" TASK_INTERVAL_MS="$TASK_INTERVAL_MS" WORKLOAD_ROLE=target \
    WORKLOAD_ID=agent-001 TARGET_ROOT="$run/target" \
    "$BIN/workload-runner" >"$run/logs/workload-target.log" 2>&1 & echo $! >"$run/workload-target.pid"
}

finish_and_verify(){
  local run="$1" base="$2" case_name="$3"
  local cp=$((base+0)) sp=$((base+1)) tp=$((base+2)) id state sha_a sha_b resumed
  id=$(cat "$run/migration.id")
  start_target_runner "$run" "$base"
  wait_dead "$(cat "$run/workload-target.pid")" || fail "$case_name target workload did not finish"
  verify_contiguous "$run/target/workloads" "$TOTAL_TASKS"
  state=$(curl -fsS "http://127.0.0.1:$cp/v1/migrations/$id" | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])')
  [[ "$state" == "COMMITTED" ]] || fail "$case_name final state=$state"
  sha_a=$(sha256_file "$run/source/agent-001/data/payload.txt")
  sha_b=$(sha256_file "$run/target/workloads/agent-001/data/payload.txt")
  [[ "$sha_a" == "$sha_b" ]] || fail "$case_name payload checksum mismatch"
  # Idempotent replays must not create a second active workload.
  curl -fsS -X POST "http://127.0.0.1:$cp/v1/migrations/$id/activate" >/dev/null
  curl -fsS -X POST "http://127.0.0.1:$cp/v1/migrations/$id/commit" >/dev/null
  [[ -f "$run/target/workloads/agent-001/.migration-activation.json" ]] || fail "$case_name target activation marker missing"
  [[ "$(find "$run/target/workloads" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')" == "1" ]] || fail "$case_name expected exactly one active workload"
  status=$(source_write_status "$sp" "post-commit-$case_name.txt")
  [[ "$status" == "200" ]] || fail "$case_name source did not un-fence after commit: $status"
  resumed=$("$PSQL_BIN" -d "$DATABASE_URL" -Atqc "select coalesce((stats->>'chunks_resumed')::bigint,0) from migrations where id='$id'")
  echo "PASS case=$case_name migration=$id state=$state resumed_chunks=$resumed business_sequence=CONTIGUOUS_1..$TOTAL_TASKS source_released=true single_active_target=true"
}

run_case_controller_crash_transfer(){
  local n=1
  local run="$BASE_RUN/case-controller-crash-transfer"
  local base=$((BASE_PORT+n*10))
  local id
  log "CASE 1 controller crash during Transfer"
  rm -rf "$run"; start_stack "$run" "$base" 0 2 0; prepare_workload "$run" "$base"; create_migration "$run" "$base" "sluicegate-case1"
  prepare_migration "$run" "$base"
  local cp=$((base+0)); id=$(cat "$run/migration.id")
  set +e
  curl -sS --max-time 10 -X POST "http://127.0.0.1:$cp/v1/migrations/$id/transfer" >/dev/null 2>&1
  set -e
  wait_dead "$(cat "$run/controller.pid")" || { kill -KILL "$(cat "$run/controller.pid")" 2>/dev/null || true; }
  reap "$(cat "$run/controller.pid")"
  restart_controller "$run" "$base"
  post_retry "http://127.0.0.1:$cp/v1/migrations/$id/transfer"
  post_retry "http://127.0.0.1:$cp/v1/migrations/$id/activate"
  post_retry "http://127.0.0.1:$cp/v1/migrations/$id/commit"
  finish_and_verify "$run" "$base" "controller-transfer-crash"
  term_pid "$(cat "$run/workload-target.pid")" 2>/dev/null || true
  term_pid "$(cat "$run/controller.pid")" 2>/dev/null || true
  term_pid "$(cat "$run/target.pid")" 2>/dev/null || true
  term_pid "$(cat "$run/source.pid")" 2>/dev/null || true
}

run_case_target_crash_transfer(){
  local n=2
  local run="$BASE_RUN/case-target-crash-transfer"
  local base=$((BASE_PORT+n*10))
  local id
  log "CASE 2 target crash during Transfer"
  rm -rf "$run"; start_stack "$run" "$base" 2 0 0; prepare_workload "$run" "$base"; create_migration "$run" "$base" "sluicegate-case2"
  prepare_migration "$run" "$base"
  local cp=$((base+0)); local tp=$((base+2)); id=$(cat "$run/migration.id")
  set +e
  curl -sS --max-time 10 -X POST "http://127.0.0.1:$cp/v1/migrations/$id/transfer" >/dev/null 2>&1
  set -e
  wait_dead "$(cat "$run/target.pid")" || { kill -KILL "$(cat "$run/target.pid")" 2>/dev/null || true; }
  reap "$(cat "$run/target.pid")"
  restart_target "$run" "$base"
  post_retry "http://127.0.0.1:$cp/v1/migrations/$id/transfer"
  post_retry "http://127.0.0.1:$cp/v1/migrations/$id/activate"
  post_retry "http://127.0.0.1:$cp/v1/migrations/$id/commit"
  finish_and_verify "$run" "$base" "target-transfer-crash"
  term_pid "$(cat "$run/workload-target.pid")" 2>/dev/null || true
  term_pid "$(cat "$run/controller.pid")" 2>/dev/null || true
  term_pid "$(cat "$run/target.pid")" 2>/dev/null || true
  term_pid "$(cat "$run/source.pid")" 2>/dev/null || true
}

run_case_controller_after_activate(){
  local n=3
  local run="$BASE_RUN/case-controller-after-activate"
  local base=$((BASE_PORT+n*10))
  local id
  log "CASE 3 controller crash after Activate"
  rm -rf "$run"; start_stack "$run" "$base" 0 0 0; prepare_workload "$run" "$base"; create_migration "$run" "$base" "sluicegate-case3"
  prepare_migration "$run" "$base"
  local cp=$((base+0)); id=$(cat "$run/migration.id")
  curl -fsS --max-time 30 -X POST "http://127.0.0.1:$cp/v1/migrations/$id/transfer" >/dev/null
  curl -fsS --max-time 30 -X POST "http://127.0.0.1:$cp/v1/migrations/$id/activate" >/dev/null
  kill -KILL "$(cat "$run/controller.pid")" 2>/dev/null || true
  wait_dead "$(cat "$run/controller.pid")" || true
  reap "$(cat "$run/controller.pid")"
  restart_controller "$run" "$base"
  post_retry "http://127.0.0.1:$cp/v1/migrations/$id/commit"
  finish_and_verify "$run" "$base" "controller-after-activate"
  term_pid "$(cat "$run/workload-target.pid")" 2>/dev/null || true
  term_pid "$(cat "$run/controller.pid")" 2>/dev/null || true
  term_pid "$(cat "$run/target.pid")" 2>/dev/null || true
  term_pid "$(cat "$run/source.pid")" 2>/dev/null || true
}

run_case_controller_commit_crash(){
  local n=4
  local run="$BASE_RUN/case-controller-commit-crash"
  local base=$((BASE_PORT+n*10))
  local id
  log "CASE 4 controller crash between Target Commit and Source Release"
  rm -rf "$run"; start_stack "$run" "$base" 0 0 2500; prepare_workload "$run" "$base"; create_migration "$run" "$base" "sluicegate-case4"
  prepare_migration "$run" "$base"
  local cp=$((base+0)); local sp=$((base+1)); id=$(cat "$run/migration.id")
  curl -fsS --max-time 30 -X POST "http://127.0.0.1:$cp/v1/migrations/$id/transfer" >/dev/null
  curl -fsS --max-time 30 -X POST "http://127.0.0.1:$cp/v1/migrations/$id/activate" >/dev/null
  set +e
  (curl -sS --max-time 10 -X POST "http://127.0.0.1:$cp/v1/migrations/$id/commit" >/dev/null 2>&1) &
  commit_pid=$!
  sleep 0.35
  kill -KILL "$(cat "$run/controller.pid")" 2>/dev/null || true
  wait "$commit_pid" 2>/dev/null || true
  reap "$(cat "$run/controller.pid")"
  set -e
  status=$(source_write_status "$sp" "blocked-during-commit-chaos.txt")
  [[ "$status" == "423" ]] || fail "source was released too early during commit chaos: $status"
  restart_controller "$run" "$base"
  post_retry "http://127.0.0.1:$cp/v1/migrations/$id/commit"
  finish_and_verify "$run" "$base" "controller-commit-crash"
  term_pid "$(cat "$run/workload-target.pid")" 2>/dev/null || true
  term_pid "$(cat "$run/controller.pid")" 2>/dev/null || true
  term_pid "$(cat "$run/target.pid")" 2>/dev/null || true
  term_pid "$(cat "$run/source.pid")" 2>/dev/null || true
}

build
run_case_controller_crash_transfer
run_case_target_crash_transfer
run_case_controller_after_activate
run_case_controller_commit_crash
log "ALL CHAOS CASES PASS"
