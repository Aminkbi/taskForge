#!/usr/bin/env bash
set -euo pipefail

test -s certification/benchmark-baseline.json
if [[ "$#" == "0" ]]; then
  if [[ "${TASKFORGE_BENCHMARK_METADATA_ONLY:-}" == "1" ]]; then
    echo "Benchmark baseline metadata is present; no performance comparison was run."
    echo "Use make bench-smoke for smoke validation."
    echo "Use make benchmark-regression BENCHMARK_ARGS='before.txt after.txt' to compare repeated Redis benchmark runs."
    exit 0
  fi
  echo "benchmark-regression requires BEFORE and AFTER benchmark logs" >&2
  echo "Use TASKFORGE_BENCHMARK_METADATA_ONLY=1 only for the explicit metadata-only check" >&2
  exit 2
fi
if [[ "$#" != "2" ]]; then
  echo "usage: benchmark-regression.sh BEFORE AFTER" >&2
  exit 2
fi
go run ./scripts/benchmarkcompare "$1" "$2"
