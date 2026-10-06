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

- Preserved the complete research repository at `research/archive-2026-10`;
  main now contains software, development checks, and concise operational docs.
- README reshaped into a shorter project entrypoint with detailed configuration and HTTP references moved under `docs/reference/`.
- Scheduler and API images now run as non-root, expose OCI source/version/revision
  labels, and include a Docker health check. No public Go API, Redis storage,
  metrics contract, or reliability guarantee changed.
- Container build context excludes documentation, tests, examples, and
  certification files.
- CI runs `make fuzz-smoke`, `make security-check`, and `make benchmark-regression`,
  and the release workflow re-runs the release checklist gates before publishing.
- Removed the unused, duplicated `IncRetried` and `IncDeadLettered` metric
  helpers; the retry and dead-letter counters are still incremented by
  `IncRetryScheduled` and `IncDeadLetterResult`.
- Toolchain refresh: Go 1.27.1, staticcheck v0.8.1, govulncheck (x/vuln) v1.8.0,
  current Prometheus, go-redis, and OpenTelemetry dependencies, refreshed
  GitHub Actions pins, and `golang:1.27.1-alpine` plus refreshed distroless base
  images.

### Fixed

- `.env.example` no longer ships the retired per-pool `lease_ttl` field that
  sidecar configuration decoding rejects, and now lists the global
  `TASKFORGE_LEASE_TTL` and `TASKFORGE_HTTP_AUTH_TOKEN` settings.
- Added Redis admission unit tests covering policy cloning, policy lookup,
  deferred-message annotation, and admission state recording, restoring the 18%
  `redis` package coverage floor.
