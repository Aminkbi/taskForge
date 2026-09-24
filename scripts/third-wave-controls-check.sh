#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

data="research/third-wave/control-comparison/data/final"
test -s "$data/metadata.json"
test -s "$data/analysis.json"
test -s "$data/analysis.md"
test -s "research/third-wave/control-comparison/plan.md"
test -s "research/third-wave/blog/taskforge-queue-controls.md"

analysis_tmp="$(mktemp -d)"
trap 'rm -rf "$analysis_tmp"' EXIT
python3 scripts/third-wave-controls-analysis.py --data "$data" --output "$analysis_tmp" >/dev/null
cmp "$data/analysis.json" "$analysis_tmp/analysis.json"
cmp "$data/analysis.md" "$analysis_tmp/analysis.md"
echo "latest-state overload-control comparison verified"
