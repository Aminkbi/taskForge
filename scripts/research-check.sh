#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
cd "$ROOT"

go run ./cmd/experiment-analysis \
  -input research/data/raw \
  -results "$TMP/results" \
  -figures "$TMP/figures" >/dev/null

diff -ru research/results "$TMP/results"
diff -ru research/figures "$TMP/figures"

if [[ $(wc -l <research/data/run-log.txt) -ne 504 ]]; then
  echo "research run log must contain exactly 504 cell records" >&2
  exit 1
fi
if grep -qvE '^(ok|FAILED) [0-9TZ:-]+ [a-z-]+ [a-z-]+ [0-9]+$' research/data/run-log.txt; then
  echo "research run log contains a non-status or potentially private line" >&2
  exit 1
fi
if grep -q '^FAILED ' research/data/run-log.txt; then
  echo "research run log contains failed cells" >&2
  exit 1
fi
