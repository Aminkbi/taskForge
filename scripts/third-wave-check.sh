#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

test -s research/third-wave/evidence/manifest.json
test -s research/third-wave/analysis-plan.md
test -s research/third-wave/paper/paper.md
test -s research/third-wave/blog/taskforge-queue-controls.md
test -s research/third-wave/evidence/optimization-map.md
test -s research/third-wave/results/analysis.json
test -s research/third-wave/results/analysis.md
test -s research/third-wave/data/final/metadata.json
test -x scripts/third-wave-identity.sh
test "$(scripts/third-wave-identity.sh | awk '/^treatment_sha256 / {print $2}')" = "92d2107b03b01c378e0f7db70b76da4c6081b8eb1114132c1b69289b794054ee"

git diff --check

for path in redis/publish.go redis/redis.go redis/state_store.go redis/admission.go redis/fairness.go test/benchmark/cleanup_benchmark_test.go; do
  test -e "$path" || { echo "missing treatment surface: $path" >&2; exit 1; }
done

grep -q 'redis_round_trips/op' research/third-wave/analysis-plan.md
grep -q 'Bonferroni' research/third-wave/analysis-plan.md
grep -q 'completed run' research/third-wave/paper/paper.md
grep -q 'measured_numbers_present": true' research/third-wave/evidence/manifest.json

python3 - <<'PY'
import hashlib
import json
from pathlib import Path

root = Path('research/third-wave')
manifest = json.loads((root / 'evidence/manifest.json').read_text())
metadata = json.loads((root / 'data/final/metadata.json').read_text())
analysis = json.loads((root / 'results/analysis.json').read_text())
assert metadata['status'] == manifest['status']
assert metadata['sources']['baseline']['commit'].startswith(manifest['baseline_revision'])
assert metadata['sources']['treatment']['commit'].startswith(manifest['treatment_revision'])
assert metadata['treatment_diff_sha256'] == manifest['treatment_sha256']
assert metadata['protocol_sha256'] == hashlib.sha256((root / 'analysis-plan.md').read_bytes()).hexdigest()
assert metadata['amendment_sha256'] == hashlib.sha256((root / 'execution-notes.md').read_bytes()).hexdigest()
assert len(metadata['commands']) == 6
assert all(command['exit_code'] == 0 for command in metadata['commands'])
for name, expected in metadata['files'].items():
    assert hashlib.sha256((root / 'data/final' / name).read_bytes()).hexdigest() == expected, name
assert analysis['protocol']['families'] == {'publish': 27, 'setup-key': 9, 'snapshot': 72}
assert all('interval_familywise_bootstrap' in record for record in analysis['records'])
assert 'pending Redis run' not in (root / 'paper/paper.md').read_text()
PY

analysis_tmp="$(mktemp -d)"
trap 'rm -rf "$analysis_tmp"' EXIT
python3 scripts/third-wave-analysis.py \
  --baseline research/third-wave/data/final/baseline.txt \
  --treatment research/third-wave/data/final/treatment.txt \
  --setup-baseline research/third-wave/data/final/baseline-setup.txt \
  --setup-treatment research/third-wave/data/final/treatment-setup.txt \
  --output "$analysis_tmp" >/dev/null
cmp research/third-wave/results/analysis.json "$analysis_tmp/analysis.json"
cmp research/third-wave/results/analysis.md "$analysis_tmp/analysis.md"

GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}" go -C research test ./...
echo "third-wave protocol, completed measurements, and evidence package verified"
