#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAGE="$(mktemp -d)"
BIN="$STAGE/bin"
DATA="$STAGE/data"
PROXY_PID=""
cleanup() {
  if [[ -n "$PROXY_PID" ]]; then kill "$PROXY_PID" 2>/dev/null || true; fi
  rm -rf "$STAGE"
}
trap cleanup EXIT
cd "$ROOT"

if [[ ! -f research/second-wave/code-lock.json || ! -f research/second-wave/trace-lock.json ]]; then
  echo "freeze the registered code and trace corpus first" >&2
  exit 2
fi
if [[ -e research/second-wave/data ]]; then
  echo "registered second-wave data already exists and cannot be replaced" >&2
  exit 2
fi
mkdir -p "$BIN" "$DATA/raw"
export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"
CGO_ENABLED=0 go build -trimpath -buildvcs=false -o "$BIN/experiment-neutral" ./cmd/experiment-neutral
CGO_ENABLED=0 go build -trimpath -buildvcs=false -o "$BIN/experiment-redis-proxy" ./cmd/experiment-redis-proxy

if ! redis-cli -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1; then
  docker compose up -d redis
  for _ in $(seq 1 60); do
    redis-cli -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1 && break
    sleep 1
  done
fi
if ! redis-cli -h 127.0.0.1 -p 6379 ping >/dev/null 2>&1; then
  echo "Redis did not become ready" >&2
  exit 1
fi
"$BIN/experiment-redis-proxy" -listen 127.0.0.1:6380 -target 127.0.0.1:6379 -one-way-latency 500us >"$STAGE/proxy.log" 2>&1 &
PROXY_PID=$!
for _ in $(seq 1 40); do
  redis-cli -h 127.0.0.1 -p 6380 ping >/dev/null 2>&1 && break
  sleep 0.1
done
if ! redis-cli -h 127.0.0.1 -p 6380 ping >/dev/null 2>&1; then
  echo "latency proxy did not become ready" >&2
  exit 1
fi

while IFS=$'\t' read -r environment gomaxprocs topology; do
  addr=127.0.0.1:6379
  if [[ "$environment" == "constrained-emulated-network" ]]; then addr=127.0.0.1:6380; fi
  while IFS=$'\t' read -r profile seed repetitions systems duration_class; do
    for (( repetition=0; repetition<repetitions; repetition++ )); do
      output="$DATA/raw/$environment/$profile/$seed/r$repetition"
      mkdir -p "$output"
      echo "run $environment $profile $seed r$repetition systems=$systems"
      GOMAXPROCS="$gomaxprocs" "$BIN/experiment-neutral" \
        -trace "research/second-wave/traces/$profile-$seed.json" \
        -output "$output" -systems "$systems" -repetition "$repetition" \
        -redis-network tcp -redis-addr "$addr" -redis-db 14 \
        -concurrency 16 -taskforge-admission-pending 256 \
        -snapshot-period 250ms -drain-timeout 2m \
        -cost-cpu-second 0.000012 -cost-network-gb 0.08 -cost-memory-gb-second 0.000002 || true
    done
  done < <(jq -r '.profiles[] | . as $p | $p.seeds[] | [$p.name, ., $p.repetitions, ($p.systems|join(",")), $p.duration_class] | @tsv' research/second-wave/study-plan.json)
done < <(jq -r '.environments[] | [.name, .gomaxprocs, .redis_topology] | @tsv' research/second-wave/study-plan.json)

while IFS= read -r result; do gzip -n "$result"; done < <(find "$DATA/raw" -type f -name '*.json' | sort)
go run ./cmd/experiment-study-register -root research/second-wave -data "$DATA" -binary "$BIN/experiment-neutral" -source-commit "$(git rev-parse HEAD)"
mv "$DATA" research/second-wave/data
echo "registered second-wave data published atomically"
