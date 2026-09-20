#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# The experiment runner flushes its dedicated DB between variants. The default
# DB 14 is intentionally separate from the compose stack's normal data.
export TASKFORGE_EXPERIMENT_REDIS_DB="${TASKFORGE_EXPERIMENT_REDIS_DB:-14}"
export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"

if ! redis-cli -n "$TASKFORGE_EXPERIMENT_REDIS_DB" ping >/dev/null 2>&1; then
  docker compose up -d redis
  until redis-cli -n "$TASKFORGE_EXPERIMENT_REDIS_DB" ping >/dev/null 2>&1; do sleep 1; done
fi

# Smoke artifacts stay at the repository root: the certification manifest and
# .gitignore reference these exact paths.
mkdir -p artifacts/experiments/raw artifacts/experiments/reports
go -C research run ./cmd/experiment -smoke -output "$ROOT/artifacts/experiments/raw"
go -C research run ./cmd/experiment-report -input "$ROOT/artifacts/experiments/raw" -output "$ROOT/artifacts/experiments/reports"
