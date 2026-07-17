#!/usr/bin/env bash
set -euo pipefail

# The experiment runner flushes its dedicated DB between variants. The default
# DB 14 is intentionally separate from the compose stack's normal data.
export TASKFORGE_EXPERIMENT_REDIS_DB="${TASKFORGE_EXPERIMENT_REDIS_DB:-14}"
export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"

if ! redis-cli -n "$TASKFORGE_EXPERIMENT_REDIS_DB" ping >/dev/null 2>&1; then
  docker compose up -d redis
  until redis-cli -n "$TASKFORGE_EXPERIMENT_REDIS_DB" ping >/dev/null 2>&1; do sleep 1; done
fi

mkdir -p artifacts/experiments/raw artifacts/experiments/reports
go run ./cmd/experiment -smoke -output artifacts/experiments/raw
go run ./cmd/experiment-report -input artifacts/experiments/raw -output artifacts/experiments/reports
