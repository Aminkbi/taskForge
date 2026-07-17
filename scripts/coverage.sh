#!/usr/bin/env bash
set -euo pipefail

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"

coverage_dir="${TASKFORGE_COVERAGE_DIR:-/tmp/taskforge-coverage}"
mkdir -p "$coverage_dir"

declare -A thresholds=(
  [overall]=45
  [core]=72
  [redis]=18
  [scheduler]=28
  [worker]=72
)

coverage_percent() {
  go tool cover -func="$1" | awk '/^total:/ {gsub("%", "", $3); print $3}'
}

require_threshold() {
  local name="$1"
  local actual="$2"
  local minimum="${thresholds[$name]}"
  if ! awk -v actual="$actual" -v minimum="$minimum" 'BEGIN { exit !(actual + 0 >= minimum + 0) }'; then
    echo "$name coverage ${actual}% is below required ${minimum}%" >&2
    exit 1
  fi
}

run_package() {
  local name="$1"
  local package="$2"
  local profile="$coverage_dir/$name.out"
  go test "$package" -coverprofile="$profile" >/dev/null
  local actual
  actual="$(coverage_percent "$profile")"
  require_threshold "$name" "$actual"
  printf '%-10s %6.1f%% (minimum %s%%)\n' "$name" "$actual" "${thresholds[$name]}"
}

all_profile="$coverage_dir/all.out"
go test ./... -coverprofile="$all_profile" >/dev/null
overall="$(coverage_percent "$all_profile")"
require_threshold overall "$overall"

echo "TaskForge coverage report (all production files in each package)"
printf '%-10s %6.1f%% (minimum %s%%)\n' overall "$overall" "${thresholds[overall]}"
run_package core .
run_package redis ./redis
run_package scheduler ./internal/scheduler
run_package worker ./worker

echo "profiles: $coverage_dir"
