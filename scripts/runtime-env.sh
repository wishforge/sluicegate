#!/usr/bin/env bash
# Shared runtime configuration. db-start.sh writes .runtime.env when needed.
ROOT="${ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
RUNTIME_ENV="$ROOT/.runtime.env"
if [[ -f "$RUNTIME_ENV" ]]; then
  # shellcheck disable=SC1090
  source "$RUNTIME_ENV"
fi

export DB_HOST="${DB_HOST:-127.0.0.1}"
export DB_PORT="${DB_PORT:-55432}"
export SLUICEGATE_PG_CONTAINER="${SLUICEGATE_PG_CONTAINER:-sluicegate-postgres}"
export DATABASE_URL="${DATABASE_URL:-postgres://migration:migration@${DB_HOST}:${DB_PORT}/migration?sslmode=disable}"
