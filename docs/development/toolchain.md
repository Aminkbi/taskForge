# Toolchain and CI Policy

This document owns pinned tooling and CI coverage. For task-specific validation,
use the [architecture map](agent-context.md).

## Pinned inputs

- Go: `go.mod` declares `go 1.27.1`; CI uses `actions/setup-go` with it.
- Dockerfiles: `golang:1.27.1-alpine` and
  `gcr.io/distroless/static-debian12`, both pinned by multi-platform digest.
- CI Redis: `redis:7.4-alpine`.
- Staticcheck: `honnef.co/go/tools/cmd/staticcheck@v0.8.1` (Staticcheck 2026.2.1).
- Reachable dependency vulnerability scanning: `govulncheck@v1.8.0`, pinned
  by the `go.mod` tool directive.
- Image SBOM generator: Docker's BuildKit Syft scanner, pinned by
  multi-platform digest and fetched through `mirror.gcr.io` in release commands.
- GitHub Actions are pinned to immutable commit SHAs; comments name the
  reviewed release. Docker Buildx produces SPDX SBOM and SLSA provenance
  attestations for published images.

When upgrading Go, update `go.mod`, Dockerfiles, this page, and verify CI.

## Local lint parity

`make lint` runs `go vet`, verifies `gofmt`, and runs `staticcheck` when it is
installed. To match CI:

```bash
go install honnef.co/go/tools/cmd/staticcheck@v0.8.1
```

## CI tracks

- `lint`: formatting, vet, static analysis, and static security-adjacent checks.
- `unit`: `go test ./...` in the product module.
- `research-module`: `go -C research test ./...` plus a byte-reproducibility
  check that regenerating the derived research outputs leaves `research/`
  unchanged. Module-scoped commands do not cross module boundaries, so this job
  is what keeps the nested research module covered.
- `fuzz-smoke`: short mutation smoke for the configuration, delayed-entry, and
  leadership-fence fuzz targets.
- `deterministic-simulation`: seeded protocol fault schedules and invariants.
- `protocol-model-check`: bounded exhaustive delivery and scheduler state spaces.
- `coverage`: critical-package statement coverage floors.
- `integration`: Redis-backed tests.
- `race`: `go test -race ./...`.
- `benchmark-smoke`: each benchmark once.
- `benchmark-regression`: validates versioned baseline metadata in CI. Local
  measured comparisons require two repeated benchmark logs through
  `make benchmark-regression BENCHMARK_ARGS='before.txt after.txt'`.
- `experiment-smoke`: every comparative workload/variant with raw and derived evidence.
- `docs-and-examples`: active documentation, certification linkage, and the public demo contract.
- `release-smoke`: release binaries and images without publishing.
- `vulnerability-scan`: fails for a vulnerability reachable from TaskForge
  source. A false positive or accepted risk needs a time-bounded exception in
  `SECURITY.md`; ignored vulnerabilities are not permitted by CI.
- `release-validate`: builds every binary target and local container image,
  verifies checksums, SBOM/provenance metadata, OCI labels, and configured
  non-root user and healthcheck without creating a release or pushing an image.
  It rebuilds binaries in a fresh directory and fails if checksums or generated
  metadata drift. `release-smoke` supplies binary and image start/health evidence.

Release runs from version tags. The `release` workflow re-runs the release
checklist gates (`unit`, `lint`, `security`, `race`, `vulnerability`, and
`release-validate`) before publishing, so a tag cannot publish an artifact that
fails them. See [RELEASING.md](../../RELEASING.md).
