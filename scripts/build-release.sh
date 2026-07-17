#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"

version="${TASKFORGE_VERSION:-dev}"
commit="${TASKFORGE_COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
dist_dir="${TASKFORGE_DIST_DIR:-dist}"
platforms="${TASKFORGE_PLATFORMS:-linux/amd64}"
release_binaries=()

mkdir -p "$dist_dir"
# A release directory is an output boundary, not an incremental cache. Remove
# supported and abandoned TaskForge artifacts so a deleted command can never be
# mistaken for part of the current release.
rm -f \
  "$dist_dir"/taskforge-* \
  "$dist_dir"/SHA256SUMS \
  "$dist_dir"/provenance.json \
  "$dist_dir"/*-image.oci.tar \
  "$dist_dir"/*-image-metadata.json

for platform in $platforms; do
  os="${platform%/*}"
  arch="${platform#*/}"
  for cmd in scheduler api; do
    output="$dist_dir/taskforge-$cmd-$os-$arch"
    echo "building $output"
    GOOS="$os" GOARCH="$arch" CGO_ENABLED=0 go build \
      -trimpath \
      -ldflags "-s -w -X main.version=$version -X main.commit=$commit" \
      -o "$output" \
      "./cmd/$cmd"
    release_binaries+=("$output")
  done
done

(
  cd "$dist_dir"
  sha256sum taskforge-scheduler-* taskforge-api-* > SHA256SUMS
)

go run ./scripts/generate-sbom.go \
  --output "$dist_dir/taskforge-binaries.spdx.json" \
  --version "$version" \
  --commit "$commit" \
  "${release_binaries[@]}"

cat >"$dist_dir/provenance.json" <<EOF
{"buildType":"https://taskforge.dev/release/v1","builder":"local-or-github-actions","commit":"$commit","version":"$version","subjects":"SHA256SUMS","sbom":"taskforge-binaries.spdx.json"}
EOF
