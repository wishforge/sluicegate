#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "$ROOT/scripts/runtime-env.sh"
PAYLOAD_MATRIX="${PAYLOAD_MATRIX:-1024 65536 1048576 16777216}"
CONCURRENCY_MATRIX="${CONCURRENCY_MATRIX:-10 50 100}"
MATRIX_WORKLOADS="${MATRIX_WORKLOADS:-100}"
SEED_CONCURRENCY="${SEED_CONCURRENCY:-$CONCURRENCY_MATRIX}"
TASKS="${TASKS:-1}"
CHUNK_SIZE="${CHUNK_SIZE:-65536}"
TRANSFER_BATCH_BYTES="${TRANSFER_BATCH_BYTES:-8388608}"
DB_MAX_CONNS="${DB_MAX_CONNS:-32}"
DB_MIN_CONNS="${DB_MIN_CONNS:-4}"
LOAD_GLOBAL_TIMEOUT="${LOAD_GLOBAL_TIMEOUT:-30m}"
OUT_DIR="${OUT_DIR:-/tmp/sluicegate-matrix}"
mkdir -p "$OUT_DIR"
CSV="$OUT_DIR/summary.csv"
printf '%s\n' 'payload_bytes,concurrency,workloads,chunk_size,transfer_batch_bytes,duration_ms,completed,failed,migrations_per_sec,total_p50_ms,total_p95_ms,total_p99_ms,transfer_p50_ms,transfer_p95_ms,transfer_p99_ms,batch_count_p50,batch_count_p95,batch_count_p99' > "$CSV"

for payload in $PAYLOAD_MATRIX; do
  for concurrency in $CONCURRENCY_MATRIX; do
    echo
    echo "================ payload=${payload} concurrency=${concurrency} ================"
    OUT="$OUT_DIR/p${payload}-c${concurrency}.json"
    WORKLOADS="$MATRIX_WORKLOADS" \
    CONCURRENCY="$concurrency" \
    SEED_CONCURRENCY="$concurrency" \
    PAYLOAD_BYTES="$payload" \
    TASKS="$TASKS" \
    CHUNK_SIZE="$CHUNK_SIZE" \
    TRANSFER_BATCH_BYTES="$TRANSFER_BATCH_BYTES" \
    DB_MAX_CONNS="$DB_MAX_CONNS" \
    DB_MIN_CONNS="$DB_MIN_CONNS" \
    LOAD_GLOBAL_TIMEOUT="$LOAD_GLOBAL_TIMEOUT" \
    LOAD_OUTPUT="$OUT" \
    "$ROOT/scripts/load-test.sh"

    RESULT_PATH="$OUT"
    python3 - "$RESULT_PATH" "$CSV" <<'PY'
import json, pathlib, sys
r=json.loads(pathlib.Path(sys.argv[1]).read_text())
fields=['payload_bytes','concurrency','workloads','chunk_size','transfer_batch_bytes','duration_ms','completed','failed','migrations_per_sec','total_p50_ms','total_p95_ms','total_p99_ms','transfer_p50_ms','transfer_p95_ms','transfer_p99_ms','batch_count_p50','batch_count_p95','batch_count_p99']
# load-test result itself does not carry env knobs, so infer the two missing fields from the filename.
name=pathlib.Path(sys.argv[1]).name
import re
m=re.match(r'p(\d+)-c(\d+)\.json$', name)
payload=int(m.group(1)); concurrency=int(m.group(2))
row=[]
for f in fields:
    if f=='payload_bytes': row.append(payload)
    elif f=='concurrency': row.append(concurrency)
    elif f=='transfer_batch_bytes': row.append(r.get('transfer_batch_bytes',''))
    else: row.append(r.get(f,''))
with open(sys.argv[2],'a') as out:
    out.write(','.join(str(x) for x in row)+'\n')
PY
  done
done

echo "[perf-matrix] summary=$CSV"
