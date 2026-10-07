#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "$ROOT/scripts/runtime-env.sh"

BOTTLENECK_PAYLOAD_BYTES="${BOTTLENECK_PAYLOAD_BYTES:-16777216}"
BOTTLENECK_CONCURRENCY_MATRIX="${BOTTLENECK_CONCURRENCY_MATRIX:-1 2 4 8 16 32 64 100}"
BOTTLENECK_WORKLOADS="${BOTTLENECK_WORKLOADS:-100}"
BOTTLENECK_SEED_CONCURRENCY="${BOTTLENECK_SEED_CONCURRENCY:-100}"
BOTTLENECK_CHUNK_SIZE="${BOTTLENECK_CHUNK_SIZE:-65536}"
TRANSFER_BATCH_BYTES="${TRANSFER_BATCH_BYTES:-8388608}"
DB_MAX_CONNS="${DB_MAX_CONNS:-32}"
DB_MIN_CONNS="${DB_MIN_CONNS:-4}"
LOAD_GLOBAL_TIMEOUT="${LOAD_GLOBAL_TIMEOUT:-90m}"
OUT="${BOTTLENECK_OUT:-/tmp/sluicegate-bottleneck}"
CSV="$OUT/summary.csv"
mkdir -p "$OUT"
rm -f "$CSV"

printf '%s\n' "payload_bytes,concurrency,workloads,chunk_size,transfer_batch_bytes,duration_ms,completed,failed,migrations_per_sec,data_mb_per_sec,total_p50_ms,total_p95_ms,total_p99_ms,transfer_p50_ms,source_batch_http_p50_ms,source_read_p50_ms,source_encode_p50_ms,target_batch_http_p50_ms,target_queue_wait_p50_ms,target_write_p50_ms,target_fsync_p50_ms,target_state_save_p50_ms,target_peak_inflight_p50" >"$CSV"

for c in $BOTTLENECK_CONCURRENCY_MATRIX; do
  echo
  echo "================ payload=$BOTTLENECK_PAYLOAD_BYTES concurrency=$c ================"
  RESULT="$OUT/p${BOTTLENECK_PAYLOAD_BYTES}-c${c}.json"
  WORKLOADS="$BOTTLENECK_WORKLOADS" \
  CONCURRENCY="$c" \
  SEED_CONCURRENCY="$BOTTLENECK_SEED_CONCURRENCY" \
  PAYLOAD_BYTES="$BOTTLENECK_PAYLOAD_BYTES" \
  TASKS=1 \
  CHUNK_SIZE="$BOTTLENECK_CHUNK_SIZE" \
  TRANSFER_BATCH_BYTES="$TRANSFER_BATCH_BYTES" \
  DB_MAX_CONNS="$DB_MAX_CONNS" \
  DB_MIN_CONNS="$DB_MIN_CONNS" \
  LOAD_GLOBAL_TIMEOUT="$LOAD_GLOBAL_TIMEOUT" \
  LOAD_OUTPUT="$RESULT" \
  "$ROOT/scripts/load-test.sh"

  python3 - "$RESULT" "$CSV" <<'PY'
import json
import pathlib
import sys

result = json.loads(pathlib.Path(sys.argv[1]).read_text())
name = pathlib.Path(sys.argv[1]).name
payload, concurrency = [int(x) for x in name[1:-5].split('-c')]
fields = [
    'duration_ms','completed','failed','migrations_per_sec','data_mb_per_sec',
    'total_p50_ms','total_p95_ms','total_p99_ms','transfer_p50_ms',
    'source_batch_http_p50_ms','source_read_p50_ms','source_encode_p50_ms',
    'target_batch_http_p50_ms','target_queue_wait_p50_ms','target_write_p50_ms',
    'target_fsync_p50_ms','target_state_save_p50_ms','target_peak_inflight_p50',
]
row = [payload, concurrency, result.get('workloads',''), result.get('chunk_size',''), result.get('transfer_batch_bytes','')]
row.extend(result.get(k,'') for k in fields)
with open(sys.argv[2], 'a', encoding='utf-8') as f:
    f.write(','.join(str(x) for x in row) + '\n')
PY
done

python3 - "$CSV" <<'PY'
import csv
import pathlib
import sys

p = pathlib.Path(sys.argv[1])
rows = list(csv.DictReader(p.open()))
for r in rows:
    r['concurrency'] = int(r['concurrency'])
    r['migrations_per_sec'] = float(r['migrations_per_sec'])
    r['total_p50_ms'] = float(r['total_p50_ms'])
    r['target_queue_wait_p50_ms'] = float(r['target_queue_wait_p50_ms'])
    r['target_write_p50_ms'] = float(r['target_write_p50_ms'])
    r['target_fsync_p50_ms'] = float(r['target_fsync_p50_ms'])

best = max(rows, key=lambda r: r['migrations_per_sec'])
threshold = best['migrations_per_sec'] * 0.95
saturation = next((r for r in rows if r['migrations_per_sec'] >= threshold), best)
print(f"[bottleneck] peak_throughput={best['migrations_per_sec']:.2f}/s at concurrency={best['concurrency']}")
print(f"[bottleneck] 95pct_saturation_concurrency={saturation['concurrency']} threshold={threshold:.2f}/s")
print(f"[bottleneck] at_saturation p50={saturation['total_p50_ms']:.1f}ms transfer_p50={saturation['transfer_p50_ms'] if 'transfer_p50_ms' in saturation else 'n/a'}ms target_queue_p50={saturation['target_queue_wait_p50_ms']:.1f}ms target_write_p50={saturation['target_write_p50_ms']:.1f}ms target_fsync_p50={saturation['target_fsync_p50_ms']:.1f}ms")
print(f"[bottleneck] strongest_signal=target_queue_wait_p50_ms={saturation['target_queue_wait_p50_ms']:.1f}ms; target_peak_inflight_p50={saturation['target_peak_inflight_p50']}")
print(f"[bottleneck] summary={p}")
PY
