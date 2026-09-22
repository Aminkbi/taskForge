#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

test -s research/third-wave/evidence/manifest.json
test -s research/third-wave/analysis-plan.md
test -s research/third-wave/paper/paper.md
test -s research/third-wave/blog/taskforge-wave3.md
test -s research/third-wave/evidence/optimization-map.md
test -x scripts/third-wave-identity.sh
test "$(scripts/third-wave-identity.sh | awk '/^treatment_sha256 / {print $2}')" = "b7045b8185cfbb98e7594ef6798d3eef4864094f35438975430d3e5ee648039b"

git diff --check

for path in redis/publish.go redis/redis.go redis/state_store.go redis/admission.go redis/fairness.go test/benchmark/cleanup_benchmark_test.go; do
  test -e "$path" || { echo "missing treatment surface: $path" >&2; exit 1; }
done

grep -q 'redis_round_trips/op' research/third-wave/analysis-plan.md
grep -q 'Bonferroni' research/third-wave/analysis-plan.md
grep -q 'not yet present' research/third-wave/paper/paper.md

GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}" go -C research test ./...
echo "wave 3 protocol and evidence package verified; treatment numbers remain pending"
