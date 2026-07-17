#!/usr/bin/env bash
set -euo pipefail

# Registered research grid from research/analysis-plan.md: every workload and
# variant, seeds 20260717 through 20260728, scale 8. One cell per invocation
# so a single failure loses one cell, is logged, and never aborts the grid.
# Raw results are committed research evidence: compact JSON, gzipped with a
# deterministic header, hostname replaced by a neutral label.

export TASKFORGE_EXPERIMENT_REDIS_DB="${TASKFORGE_EXPERIMENT_REDIS_DB:-14}"
OUT="${1:-research/data/raw}"
LOG="${OUT%/raw}/run-log.txt"
SEEDS="${TASKFORGE_RESEARCH_SEEDS:-$(seq 20260717 20260728)}"
SCALE="${TASKFORGE_RESEARCH_SCALE:-8}"

source_paths=('*.go' go.mod go.sum test/experiment/workloads scripts/research-experiments.sh)
if ! git diff --quiet -- "${source_paths[@]}" || ! git diff --cached --quiet -- "${source_paths[@]}"; then
  echo "research experiments require clean tracked experiment source" >&2
  exit 2
fi

if ! redis-cli -n "$TASKFORGE_EXPERIMENT_REDIS_DB" ping >/dev/null 2>&1; then
  docker compose up -d redis
  until redis-cli -n "$TASKFORGE_EXPERIMENT_REDIS_DB" ping >/dev/null 2>&1; do sleep 1; done
fi

BUILD_DIR="$(mktemp -d)"
CELL_ERROR="$BUILD_DIR/cell-error.log"
trap 'rm -rf "$BUILD_DIR"' EXIT
BIN="$BUILD_DIR/experiment"
go build -o "$BIN" ./cmd/experiment
export TASKFORGE_BUILD_SHA="$(git rev-parse HEAD)"

mkdir -p "$OUT"
: >"$LOG"
manifests="delayed-backlog hot-dependency noisy-neighbor retry-storm tenant-skew worker-crash"
variants="taskforge-fifo-static taskforge-no-fairness taskforge-no-admission taskforge-no-adaptive taskforge-no-dependency-budget taskforge-full asynq"
failed=0

for seed in $SEEDS; do
  for manifest in $manifests; do
    for variant in $variants; do
      started=$(date -u +%FT%TZ)
      rm -f "$OUT/$manifest--$variant--$seed.json" "$OUT/$manifest--$variant--$seed.json.gz"
      if "$BIN" -seed "$seed" -scale "$SCALE" -compact -hostname-label research-host \
        -manifest "$manifest" -variant "$variant" -output "$OUT" >/dev/null 2>"$CELL_ERROR"; then
        echo "ok $started $manifest $variant $seed" >>"$LOG"
      else
        echo "FAILED $started $manifest $variant $seed" >>"$LOG"
        sed 's/^/experiment error: /' "$CELL_ERROR" >&2
        failed=1
      fi
    done
  done
done

gzip -9nf "$OUT"/*.json
grep -c '^ok' "$LOG" || true
if (( failed != 0 )); then
  echo "failed cells recorded in $LOG" >&2
  exit 1
fi
