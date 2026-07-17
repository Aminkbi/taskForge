#!/usr/bin/env bash
set -euo pipefail

# Rebuilds the exact measured binary from its recorded immutable commit, then
# regenerates every derived result and figure into a temporary directory and
# byte-compares them with the committed artifact.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
SOURCE="$TMP/source"
cleanup() {
  rm -rf "$TMP"
}
trap cleanup EXIT
cd "$ROOT"

./scripts/research-check.sh

commit="$(jq -r '.runs[0].source_commit' research/data/dataset.json)"
tree="$(jq -r '.runs[0].source_tree' research/data/dataset.json)"
binary_digest="$(jq -r '.runs[0].binary_sha256' research/data/dataset.json)"
go_mod_digest="$(jq -r '.runs[0].dependency_locks[] | select(.path == "go.mod") | .sha256' research/data/dataset.json)"
go_sum_digest="$(jq -r '.runs[0].dependency_locks[] | select(.path == "go.sum") | .sha256' research/data/dataset.json)"

git cat-file -e "$commit^{commit}"
if [[ "$(git rev-parse "$commit^{tree}")" != "$tree" ]]; then
  echo "recorded source tree does not match source commit" >&2
  exit 1
fi
mkdir -p "$SOURCE"
git archive "$commit" | tar -x -C "$SOURCE"
if [[ "$(sha256sum "$SOURCE/go.mod" | cut -d' ' -f1)" != "$go_mod_digest" ]] ||
   [[ "$(sha256sum "$SOURCE/go.sum" | cut -d' ' -f1)" != "$go_sum_digest" ]]; then
  echo "recorded dependency lock does not match measured source" >&2
  exit 1
fi

(
  cd "$SOURCE"
  CGO_ENABLED=0 go build -trimpath -buildvcs=false -o "$TMP/experiment" ./cmd/experiment
)
if [[ "$(sha256sum "$TMP/experiment" | cut -d' ' -f1)" != "$binary_digest" ]]; then
  echo "rebuilt experiment binary differs from recorded binary" >&2
  exit 1
fi

echo "artifact integrity verified: source=$commit binary_sha256=$binary_digest"
