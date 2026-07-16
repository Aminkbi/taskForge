# Releasing

TaskForge releases are cut from Git tags.
Release artifacts should be reproducible from the tagged source and should include binaries, checksums, release notes, and container images.

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

The build writes binaries and `SHA256SUMS` under `dist/`.

## Release Checklist

- `make test`
- `make lint`
- `make race-test`
- `TASKFORGE_RUN_INTEGRATION=1 make integration-test`
- `make release-smoke`
- Review `CHANGELOG.md`
- Confirm release artifacts include `SHA256SUMS`
- Publish versioned container images for the scheduler and API sidecars

## Compatibility Notes

Release notes should call out any behavior change in:

- At-least-once delivery semantics.
- Lease ownership, ack, nack, or reclaim behavior.
- Retry, delayed, recurring, or DLQ flows.
- Public Go API.
- Redis key layout.
- Metrics, readiness, or admin API output.
