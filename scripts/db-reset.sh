#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
source "$ROOT/scripts/runtime-env.sh"
cat "$ROOT/db/schema.sql" | "$ROOT/scripts/psql.sh" -d "${DATABASE_URL}" -v ON_ERROR_STOP=1 >/dev/null
cat "$ROOT/db/reset.sql" | "$ROOT/scripts/psql.sh" -d "${DATABASE_URL}" -v ON_ERROR_STOP=1 >/dev/null
