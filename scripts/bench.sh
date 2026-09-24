#!/usr/bin/env bash
set -euo pipefail

if [[ "${TASKFORGE_RUN_BENCHMARKS:-}" != "1" ]]; then
  echo "set TASKFORGE_RUN_BENCHMARKS=1 to run Redis benchmarks"
  exit 1
fi

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"
: "${TASKFORGE_REDIS_ADDR:?set TASKFORGE_REDIS_ADDR to a dedicated Redis endpoint}"
: "${TASKFORGE_REDIS_DB:?set TASKFORGE_REDIS_DB to a dedicated non-zero Redis database}"
if [[ "${TASKFORGE_REDIS_DB}" == "0" ]]; then
  echo "TASKFORGE_REDIS_DB must not be 0 for benchmarks" >&2
  exit 2
fi

cmd=(
  go test
  -p 1
  -run '^$'
  -bench .
  -benchmem
  ./test/benchmark/... ./redis/...
)

echo "running: ${cmd[*]}"
"${cmd[@]}"
