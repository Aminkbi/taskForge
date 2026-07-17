#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
cd "$ROOT"

go run ./cmd/experiment-analysis \
  -input research/data \
  -results "$TMP/results" \
  -figures "$TMP/figures" \
  -paper-template research/paper/paper.template.md \
  -paper "$TMP/paper.md" >/dev/null

diff -ru research/results "$TMP/results"
diff -ru research/figures "$TMP/figures"
cmp research/paper/paper.md "$TMP/paper.md"

if [[ $(wc -l <research/data/run-log.txt) -ne 504 ]]; then
  echo "research run log must contain exactly 504 cell records" >&2
  exit 1
fi
if grep -qvE '^(ok|not_measured) [a-z-]+ [a-z-]+ [0-9]+$' research/data/run-log.txt; then
  echo "research run log contains a failure, malformed status, or private detail" >&2
  exit 1
fi
if [[ $(grep -c '^not_measured worker-crash asynq ' research/data/run-log.txt) -ne 12 ]]; then
  echo "research run log must mark exactly 12 unsupported baseline fault cells not measured" >&2
  exit 1
fi
