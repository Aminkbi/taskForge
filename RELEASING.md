# Releasing

TaskForge releases are cut from Git tags.
Release artifacts should be reproducible from the tagged source and include binaries,
checksums, an SPDX SBOM, provenance metadata, and container images. Published images
carry Buildx SBOM and provenance attestations; GitHub also emits signed artifact
provenance for every binary and metadata file.

## Versioning

Use semantic version tags:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The release workflow uses the tag as `TASKFORGE_VERSION` and the commit SHA as `TASKFORGE_COMMIT`.
The release binaries expose this metadata through:

```bash
taskforge-scheduler version
taskforge-api version
```

## Local Release Build

```bash
TASKFORGE_VERSION=v0.1.0 \
TASKFORGE_COMMIT="$(git rev-parse --short HEAD)" \
TASKFORGE_PLATFORMS="linux/amd64 linux/arm64 darwin/amd64 darwin/arm64" \
./scripts/build-release.sh
```

The build writes binaries, `SHA256SUMS`, `taskforge-binaries.spdx.json`, and
`provenance.json` under `dist/`. Verify them with `sha256sum --check SHA256SUMS`.

## Dry-run validation

Run `make release-validate` before creating a tag. It builds all supported binary
targets and both local images, then verifies artifact checksums, binary SBOM and
provenance metadata, image labels, non-root execution, and Docker health checks.
It never calls a registry or GitHub release API.

## Image signing and verification policy

Only CI from a protected version tag may publish `ghcr.io` images. Buildx attaches
SBOM and provenance attestations (`--sbom=true --provenance=mode=max`) to each
multi-platform image. Before deployment, operators must verify the image digest and
its provenance/SBOM attestation with their registry's supported verifier (for example
`docker buildx imagetools inspect --raw`). If a signing identity is configured for
the release environment, it must sign the immutable digest after this verification;
tags alone are not an authorization boundary. CI must fail rather than publish if the
attestation step fails. The GitHub release is created only after both image
attestation builds succeed. No local dry-run signs or publishes an artifact.

## Release Checklist

- `make test`
- `make lint`
- `make race-test`
- `TASKFORGE_RUN_INTEGRATION=1 make integration-test`
- `make release-smoke`
- Review `CHANGELOG.md`
- Confirm release artifacts include `SHA256SUMS`
- Confirm the SBOM and provenance metadata are present and checksums verify
- Run `make vuln-check` and `make release-validate`
- Publish versioned container images for the scheduler and API sidecars

## Compatibility Notes

Release notes should call out any behavior change in:

- At-least-once delivery semantics.
- Lease ownership, ack, nack, or reclaim behavior.
- Retry, delayed, recurring, or DLQ flows.
- Public Go API.
- Redis key layout.
- Metrics, readiness, or admin API output.
- Redis storage/key schema, retention, or migration requirements.
- Reliability guarantees, including delivery, recovery, backpressure, and failure behavior.
