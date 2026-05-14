#!/usr/bin/env bash
set -euo pipefail

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"

cmd=(
  go test
  -race
  ./internal/...
  ./pkg/...
  ./cmd/...
)

echo "running: ${cmd[*]}"
"${cmd[@]}"
