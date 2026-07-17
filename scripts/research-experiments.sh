#!/usr/bin/env bash
set -euo pipefail

# The Go grid command owns clean-tree enforcement, immutable-source build
# provenance, per-cell failure propagation, and atomic dataset replacement.
# This wrapper only makes the dedicated local Redis dependency available.

REDIS_DB="${TASKFORGE_EXPERIMENT_REDIS_DB:-14}"
REDIS_ADDR="${TASKFORGE_REDIS_ADDR:-localhost:6379}"

pilot=0
for argument in "$@"; do
  if [[ "$argument" == "-pilot" || "$argument" == "--pilot" || "$argument" == "-pilot=true" || "$argument" == "--pilot=true" ]]; then
    pilot=1
  fi
done
if (( ! pilot )) && [[ -n "$(git status --porcelain=v1 --untracked-files=all)" ]]; then
  echo "research experiments require a wholly clean source checkout; use -pilot for non-publishable output" >&2
  exit 2
fi

if ! redis-cli -h "${REDIS_ADDR%:*}" -p "${REDIS_ADDR##*:}" -n "$REDIS_DB" ping >/dev/null 2>&1; then
  docker compose up -d redis
  until redis-cli -h "${REDIS_ADDR%:*}" -p "${REDIS_ADDR##*:}" -n "$REDIS_DB" ping >/dev/null 2>&1; do sleep 1; done
fi

exec go run ./cmd/experiment-grid -redis-addr "$REDIS_ADDR" -redis-db "$REDIS_DB" "$@"
