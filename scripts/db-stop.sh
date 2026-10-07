#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "$ROOT/scripts/runtime-env.sh"
if [[ "${STOP_EXISTING_PG:-false}" != "true" && "$SLUICEGATE_PG_CONTAINER" != "sluicegate-postgres" ]]; then
  echo "[db-stop] reusing existing postgres container=$SLUICEGATE_PG_CONTAINER; leaving it running"
  exit 0
fi
if docker compose version >/dev/null 2>&1; then docker compose -f "$ROOT/docker-compose.yml" down; else docker-compose -f "$ROOT/docker-compose.yml" down; fi
