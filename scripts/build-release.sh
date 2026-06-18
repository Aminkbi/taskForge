#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"

version="${TASKFORGE_VERSION:-dev}"
commit="${TASKFORGE_COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
dist_dir="${TASKFORGE_DIST_DIR:-dist}"
platforms="${TASKFORGE_PLATFORMS:-linux/amd64}"

mkdir -p "$dist_dir"
rm -f "$dist_dir/SHA256SUMS"

for platform in $platforms; do
  os="${platform%/*}"
  arch="${platform#*/}"
  for cmd in worker scheduler api; do
    output="$dist_dir/taskforge-$cmd-$os-$arch"
    echo "building $output"
    GOOS="$os" GOARCH="$arch" CGO_ENABLED=0 go build \
      -trimpath \
      -ldflags "-s -w -X main.version=$version -X main.commit=$commit" \
      -o "$output" \
      "./cmd/$cmd"
  done
done

(
  cd "$dist_dir"
  sha256sum taskforge-* > SHA256SUMS
)
