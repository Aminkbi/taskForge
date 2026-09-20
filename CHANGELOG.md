# Changelog

All notable changes to TaskForge are documented here.

This project has not published a stable release yet.

## Unreleased

### Added

- Public root, `redis`, and `worker` packages for canonical task models, Redis publishing, handler registration, worker embedding, task-state lookup, and DLQ operations.
- Short OSS contributor, security, code-of-conduct, license, and release process documentation.
- Release dry-run validation, checksums, SPDX binary SBOMs, provenance metadata,
  reachable dependency vulnerability scanning, and attestable image builds.

### Changed

- README reshaped into a shorter project entrypoint with detailed configuration and HTTP references moved under `docs/reference/`.
- Scheduler and API images now run as non-root, expose OCI source/version/revision
  labels, and include a Docker health check. No public Go API, Redis storage,
  metrics contract, or reliability guarantee changed.
- Container build context ignores the research, documentation, tests, examples,
  and certification trees, shrinking the context from about 56 MB to 1 MB.
- CI runs `make fuzz-smoke`, `make security-check`, and `make benchmark-regression`,
  and the release workflow re-runs the release checklist gates before publishing.
- Removed the unused, duplicated `IncRetried` and `IncDeadLettered` metric
  helpers; the retry and dead-letter counters are still incremented by
  `IncRetryScheduled` and `IncDeadLetterResult`.
- Toolchain refresh: Go 1.27.1, staticcheck v0.8.1, govulncheck (x/vuln) v1.8.0,
  current Prometheus, go-redis, OpenTelemetry, and Asynq dependencies, refreshed
  GitHub Actions pins, and `golang:1.27.1-alpine` plus refreshed distroless base
  images. The frozen research artifact still rebuilds to its recorded binary hash
  with its recorded `go1.26.5` toolchain.

### Fixed

- `.env.example` no longer ships the retired per-pool `lease_ttl` field that
  sidecar configuration decoding rejects, and now lists the global
  `TASKFORGE_LEASE_TTL` and `TASKFORGE_HTTP_AUTH_TOKEN` settings.
- The `experiment-smoke` CI job installs `redis-cli`, so the smoke script uses the
  job's Redis service instead of falling back to a `docker compose` Redis that
  collides with it on port 6379.
- Added Redis admission unit tests covering policy cloning, policy lookup,
  deferred-message annotation, and admission state recording, restoring the 18%
  `redis` package coverage floor.
