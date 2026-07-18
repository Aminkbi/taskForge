#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ -e research/second-wave/data || -e research/second-wave/results ]]; then
  echo "refusing to freeze inputs after result paths exist" >&2
  exit 2
fi
GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}" go run ./cmd/experiment-study-traces -root research/second-wave

