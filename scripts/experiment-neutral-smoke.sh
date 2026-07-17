#!/usr/bin/env bash
set -euo pipefail

export TASKFORGE_EXPERIMENT_REDIS_DB="${TASKFORGE_EXPERIMENT_REDIS_DB:-14}"
export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"

if ! redis-cli -n "$TASKFORGE_EXPERIMENT_REDIS_DB" ping >/dev/null 2>&1; then
  docker compose up -d redis
  until redis-cli -n "$TASKFORGE_EXPERIMENT_REDIS_DB" ping >/dev/null 2>&1; do sleep 1; done
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
go run ./cmd/experiment-trace \
  -profile test/experiment/open-loop/smoke.json \
  -seed 20260718 \
  -output "$tmp/trace.json"
go run ./cmd/experiment-neutral \
  -trace "$tmp/trace.json" \
  -output "$tmp/results" \
  -snapshot-period 20ms \
  -drain-timeout 5s
test -s "$tmp/results/open-loop-smoke-20260718--taskforge-full--r0.json"
test -s "$tmp/results/open-loop-smoke-20260718--taskforge-no-admission--r0.json"
test -s "$tmp/results/open-loop-smoke-20260718--taskforge-no-dependency-budget--r0.json"
test -s "$tmp/results/open-loop-smoke-20260718--asynq--r0.json"
