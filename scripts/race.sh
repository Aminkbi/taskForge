#!/usr/bin/env bash
set -euo pipefail

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"

cmd=(
  go test
  -race
  ./...
)

echo "running: ${cmd[*]}"
"${cmd[@]}"
