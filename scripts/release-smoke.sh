#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"
export TASKFORGE_DIST_DIR="${TASKFORGE_DIST_DIR:-dist}"
export TASKFORGE_PLATFORMS="${TASKFORGE_PLATFORMS:-linux/amd64}"

started_pids=()
started_containers=()

cleanup() {
  for pid in "${started_pids[@]}"; do
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  done
  for container in "${started_containers[@]}"; do
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

wait_for_health() {
  local url="$1"
  for _ in {1..50}; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "health check failed: $url"
  return 1
}

smoke_binary() {
  local binary="$1"
  local port="$2"
  local log="/tmp/taskforge-release-smoke-$port.log"
  TASKFORGE_HTTP_ADDR="127.0.0.1:$port" \
    TASKFORGE_REDIS_ADDR="${TASKFORGE_REDIS_ADDR:-127.0.0.1:6379}" \
    TASKFORGE_SCHEDULES_JSON="[]" \
    "$binary" >"$log" 2>&1 &
  started_pids+=("$!")
  if ! wait_for_health "http://127.0.0.1:$port/healthz"; then
    if grep -q "socket: operation not permitted" "$log" && [[ "${TASKFORGE_REQUIRE_START_SMOKE:-}" != "1" ]]; then
      echo "sandbox denied listening on $port; skipped binary start smoke for $binary"
      return 0
    fi
    cat "$log"
    return 1
  fi
}

smoke_container() {
  local image="$1"
  local port="$2"
  local container
  container="$(docker run -d --network host \
    -e TASKFORGE_HTTP_ADDR="127.0.0.1:$port" \
    -e TASKFORGE_REDIS_ADDR="${TASKFORGE_REDIS_ADDR:-127.0.0.1:6379}" \
    -e TASKFORGE_SCHEDULES_JSON="[]" \
    "$image")"
  started_containers+=("$container")
  wait_for_health "http://127.0.0.1:$port/healthz"
}

./scripts/build-release.sh

for binary in \
  "$TASKFORGE_DIST_DIR/taskforge-scheduler-linux-amd64" \
  "$TASKFORGE_DIST_DIR/taskforge-api-linux-amd64"; do
  test -x "$binary"
  "$binary" version
done

if command -v curl >/dev/null 2>&1; then
  smoke_binary "$TASKFORGE_DIST_DIR/taskforge-scheduler-linux-amd64" 18082
  smoke_binary "$TASKFORGE_DIST_DIR/taskforge-api-linux-amd64" 18083
else
  echo "curl unavailable; skipped binary start smoke checks"
fi

if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  docker build -f deploy/docker/scheduler.Dockerfile -t taskforge/scheduler:smoke .
  docker build -f deploy/docker/api.Dockerfile -t taskforge/api:smoke .
  if command -v curl >/dev/null 2>&1; then
    smoke_container taskforge/scheduler:smoke 19082
    smoke_container taskforge/api:smoke 19083
  else
    echo "curl unavailable; skipped container start smoke checks"
  fi
else
  echo "docker daemon unavailable; skipped container image smoke build"
fi
