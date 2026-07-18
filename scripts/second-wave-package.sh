#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-dist}"
TMP="$(mktemp -d)"
STAGE="$TMP/taskforge-paired-study"
trap 'rm -rf "$TMP"' EXIT
cd "$ROOT"
./scripts/second-wave-check.sh
mkdir -p "$STAGE" "$OUT"
jq -r '.files[].path' research/second-wave/code-lock.json | while read -r path; do
  mkdir -p "$STAGE/$(dirname "$path")"
  cp "$path" "$STAGE/$path"
done
cp -r research/second-wave/data research/second-wave/results research/second-wave/figures "$STAGE/research/second-wave/"
cp research/second-wave/code-lock.json research/second-wave/trace-lock.json "$STAGE/research/second-wave/"
cp research/second-wave/paper/paper.md "$STAGE/research/second-wave/paper/"
(
  cd "$STAGE"
  find . -type f ! -name MANIFEST.sha256 -print0 | sort -z | xargs -0 sha256sum >MANIFEST.sha256
)
epoch="$(git show -s --format=%ct HEAD)"
tar --sort=name --mtime="@$epoch" --owner=0 --group=0 --numeric-owner -cf - -C "$TMP" "$(basename "$STAGE")" |
  gzip -n >"$OUT/taskforge-paired-study.tar.gz"
sha256sum "$OUT/taskforge-paired-study.tar.gz"
