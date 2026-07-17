#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

export TASKFORGE_DIST_DIR="${TASKFORGE_DIST_DIR:-dist}"
export TASKFORGE_PLATFORMS="${TASKFORGE_PLATFORMS:-linux/amd64 linux/arm64 darwin/amd64 darwin/arm64}"
export TASKFORGE_VERSION="${TASKFORGE_VERSION:-dry-run}"
export TASKFORGE_COMMIT="${TASKFORGE_COMMIT:-$(git rev-parse HEAD 2>/dev/null || echo unknown)}"
export TASKFORGE_SBOM_GENERATOR="${TASKFORGE_SBOM_GENERATOR:-mirror.gcr.io/docker/buildkit-syft-scanner@sha256:79e7b013cbec16bbb436f312819a49a4a57752b2270c1a9332ae1a10fcc82a68}"

./scripts/build-release.sh
(
  cd "$TASKFORGE_DIST_DIR"
  sha256sum --check SHA256SUMS
)
test -s "$TASKFORGE_DIST_DIR/taskforge-binaries.spdx.json"
test -s "$TASKFORGE_DIST_DIR/provenance.json"
grep -q '"spdxVersion": "SPDX-2.3"' "$TASKFORGE_DIST_DIR/taskforge-binaries.spdx.json"
grep -Fq "\"commit\":\"$TASKFORGE_COMMIT\"" "$TASKFORGE_DIST_DIR/provenance.json"

# Rebuild in a fresh directory. This rejects a release artifact that varies
# between otherwise identical builds instead of merely validating a checksum it
# just generated.
repeat_dir="$(mktemp -d "${TMPDIR:-/tmp}/taskforge-release-repeat.XXXXXX")"
buildx_builder="${TASKFORGE_BUILDX_BUILDER:-}"
owned_buildx_builder=""
cleanup() {
  rm -rf "$repeat_dir"
  if [[ -n "$owned_buildx_builder" ]]; then
    docker buildx rm "$owned_buildx_builder" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
TASKFORGE_DIST_DIR="$repeat_dir" ./scripts/build-release.sh
cmp "$TASKFORGE_DIST_DIR/SHA256SUMS" "$repeat_dir/SHA256SUMS"
cmp "$TASKFORGE_DIST_DIR/taskforge-binaries.spdx.json" "$repeat_dir/taskforge-binaries.spdx.json"
cmp "$TASKFORGE_DIST_DIR/provenance.json" "$repeat_dir/provenance.json"

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "docker daemon is required to validate release images" >&2
  exit 1
fi

# The default Docker driver cannot export attestations. Use an isolated
# docker-container builder locally; CI may provide its configured builder with
# TASKFORGE_BUILDX_BUILDER.
if [[ -z "$buildx_builder" ]]; then
  buildx_builder="taskforge-release-$$"
  builder_args=(--name "$buildx_builder" --driver docker-container --driver-opt network=host)
  for proxy_name in HTTP_PROXY HTTPS_PROXY http_proxy https_proxy; do
    proxy_value="${!proxy_name:-}"
    if [[ -n "$proxy_value" ]]; then
      builder_args+=(--driver-opt "env.$proxy_name=$proxy_value")
    fi
  done
  docker buildx create "${builder_args[@]}" >/dev/null
  owned_buildx_builder="$buildx_builder"
fi
docker buildx inspect "$buildx_builder" --bootstrap >/dev/null

for role in scheduler api; do
  image="taskforge/${role}:dry-run"
  metadata="$TASKFORGE_DIST_DIR/${role}-image-metadata.json"
  archive="$TASKFORGE_DIST_DIR/${role}-image.oci.tar"
  # OCI output retains the Buildx SBOM and provenance attestations. A second
  # local load below is only for inspecting runtime configuration.
  docker buildx build --builder "$buildx_builder" \
    --sbom="generator=$TASKFORGE_SBOM_GENERATOR" \
    --provenance=mode=max \
    --metadata-file "$metadata" \
    --output "type=oci,dest=$archive" \
    -f "deploy/docker/${role}.Dockerfile" \
    --build-arg "VERSION=${TASKFORGE_VERSION}" \
    --build-arg "COMMIT=${TASKFORGE_COMMIT}" \
    .
  test -s "$archive"
  test -s "$metadata"
  tar -tf "$archive" | grep -qx 'index.json'
  grep -q 'provenance' "$metadata"
  docker buildx build --builder "$buildx_builder" --load \
    -f "deploy/docker/${role}.Dockerfile" \
    --build-arg "VERSION=${TASKFORGE_VERSION}" \
    --build-arg "COMMIT=${TASKFORGE_COMMIT}" \
    -t "$image" .
  test "$(docker image inspect --format '{{.Config.User}}' "$image")" = "65532:65532"
  test "$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}' "$image")" = "$TASKFORGE_COMMIT"
  test "$(docker image inspect --format '{{ index .Config.Labels "org.opencontainers.image.version" }}' "$image")" = "$TASKFORGE_VERSION"
  test "$(docker image inspect --format '{{json .Config.Healthcheck}}' "$image")" != "null"
done

echo "release dry-run validation passed; no release or registry publication occurred"
