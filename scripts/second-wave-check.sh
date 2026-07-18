#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
cd "$ROOT"

jq -r '.files[] | [.sha256, .path] | @tsv' research/second-wave/code-lock.json |
while IFS=$'\t' read -r expected path; do
  actual="$(sha256sum "$path" | cut -d' ' -f1)"
  [[ "$actual" == "$expected" ]] || { echo "code-lock mismatch: $path" >&2; exit 1; }
done

GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}" CGO_ENABLED=0 go build -trimpath -buildvcs=false -o "$TMP/experiment-neutral" ./cmd/experiment-neutral
expected_binary="$(jq -r '.binary_sha256' research/second-wave/data/dataset.json)"
actual_binary="$(sha256sum "$TMP/experiment-neutral" | cut -d' ' -f1)"
[[ "$actual_binary" == "$expected_binary" ]] || { echo "measured binary does not rebuild from locked source" >&2; exit 1; }

GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}" go run ./cmd/experiment-study-analysis -root research/second-wave -output "$TMP/derived"
diff -ru research/second-wave/results "$TMP/derived/results"
diff -ru research/second-wave/figures "$TMP/derived/figures"
cmp research/second-wave/paper/paper.md "$TMP/derived/paper/paper.md"

