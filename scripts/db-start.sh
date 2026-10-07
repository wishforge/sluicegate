#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "$ROOT/scripts/runtime-env.sh"

compose() {
  if docker compose version >/dev/null 2>&1; then
    docker compose -f "$ROOT/docker-compose.yml" "$@"
  else
    docker-compose -f "$ROOT/docker-compose.yml" "$@"
  fi
}

container_running() {
  local c="$1"
  [[ "$(docker inspect -f '{{.State.Running}}' "$c" 2>/dev/null || true)" == "true" ]]
}

container_owns_port() {
  local c="$1" port="$2"
  docker port "$c" 5432/tcp 2>/dev/null | grep -Eq "(^|:)${port}$"
}

# Prefer an already-running PostgreSQL container that owns the requested port,
# so an existing container is reused instead of starting a second one.
# Legacy container names are still checked so upgrades keep working.
if [[ "${REUSE_EXISTING_PG:-true}" == "true" ]]; then
  for existing in sluicegate-postgres migration-mvp52-postgres migration-mvp5-postgres; do
    if container_running "$existing" && container_owns_port "$existing" "$DB_PORT"; then
      export SLUICEGATE_PG_CONTAINER="$existing"
      export DATABASE_URL="postgres://migration:migration@${DB_HOST}:${DB_PORT}/migration?sslmode=disable"
      cat > "$ROOT/.runtime.env" <<ENV
export DB_HOST=$DB_HOST
export DB_PORT=$DB_PORT
export SLUICEGATE_PG_CONTAINER=$SLUICEGATE_PG_CONTAINER
export DATABASE_URL=$DATABASE_URL
ENV
      echo "[db-start] reusing existing postgres container=$SLUICEGATE_PG_CONTAINER port=$DB_PORT"
      cat "$ROOT/db/schema.sql" | "$ROOT/scripts/psql.sh" -d "$DATABASE_URL" -v ON_ERROR_STOP=1 >/dev/null
      exit 0
    fi
  done
fi

# If the configured host port is already occupied by something else, choose the next
# free port instead of failing docker compose with "port is already allocated".
if [[ "${AUTO_PORT:-true}" == "true" ]]; then
  is_port_open() {
    local host="$1" port="$2"
    if command -v nc >/dev/null 2>&1; then
      nc -z "$host" "$port" >/dev/null 2>&1
      return $?
    fi
    python3 - "$host" "$port" <<'PY' >/dev/null 2>&1
import socket
import sys
host, port = sys.argv[1], int(sys.argv[2])
s = socket.socket()
s.settimeout(0.25)
try:
    s.connect((host, port))
except OSError:
    raise SystemExit(1)
finally:
    s.close()
raise SystemExit(0)
PY
  }

  candidate="$DB_PORT"
  while is_port_open "$DB_HOST" "$candidate"; do
    candidate=$((candidate + 1))
  done
  DB_PORT="$candidate"
fi

export SLUICEGATE_PG_CONTAINER="sluicegate-postgres"
export DATABASE_URL="postgres://migration:migration@${DB_HOST}:${DB_PORT}/migration?sslmode=disable"
cat > "$ROOT/.runtime.env" <<ENV
export DB_HOST=$DB_HOST
export DB_PORT=$DB_PORT
export SLUICEGATE_PG_CONTAINER=$SLUICEGATE_PG_CONTAINER
export DATABASE_URL=$DATABASE_URL
ENV

echo "[db-start] starting postgres container=$SLUICEGATE_PG_CONTAINER port=$DB_PORT"
DB_PORT="$DB_PORT" compose up -d postgres

for _ in $(seq 1 60); do
  if "$ROOT/scripts/psql.sh" -d "$DATABASE_URL" -Atqc 'select 1' >/dev/null 2>&1; then
    cat "$ROOT/db/schema.sql" | "$ROOT/scripts/psql.sh" -d "$DATABASE_URL" -v ON_ERROR_STOP=1 >/dev/null
    echo "[db-start] postgres ready: $DATABASE_URL"
    exit 0
  fi
  sleep 1
done

echo "postgres did not become ready" >&2
exit 1
