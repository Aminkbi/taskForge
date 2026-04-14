#!/usr/bin/env bash
set -euo pipefail

if [[ "${TASKFORGE_RUN_BENCHMARKS:-}" != "1" ]]; then
  echo "set TASKFORGE_RUN_BENCHMARKS=1 to run Redis benchmarks"
  exit 1
fi

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"

cmd=(
  go test
  -run '^$'
  -bench .
  -benchmem
  ./test/benchmark/...
)

echo "running: ${cmd[*]}"
"${cmd[@]}"
