#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "$ROOT/scripts/runtime-env.sh"
if command -v psql >/dev/null 2>&1; then
  exec psql "$@"
fi
if ! command -v docker >/dev/null 2>&1; then
  echo "psql not found and docker is unavailable" >&2
  exit 127
fi
args=()
skip=0
for arg in "$@"; do
  if [[ "$skip" -eq 1 ]]; then skip=0; continue; fi
  if [[ "$arg" == "-d" ]]; then skip=1; continue; fi
  args+=("$arg")
done
exec docker exec -i "$SLUICEGATE_PG_CONTAINER" psql -U migration -d migration "${args[@]}"
