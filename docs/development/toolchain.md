# Toolchain and CI Policy

TaskForge keeps repository checks split by purpose so failures identify the broken contract quickly.

## Pinned inputs

- Go toolchain: `go.mod` declares `go 1.25.0`; CI reads that value through `actions/setup-go`.
- Container build image: each Dockerfile uses `golang:1.25.0-alpine`.
- Redis service image for CI: `redis:7.4-alpine`.
- Staticcheck: CI and local install guidance use `honnef.co/go/tools/cmd/staticcheck@2025.1.1`.

If Go is upgraded, update `go.mod`, all Dockerfiles, this document, and confirm the CI workflow still resolves the same toolchain.

## Local checks

Use the scripts or Makefile targets as the stable interface:

```bash
make test
make lint
make race-test
TASKFORGE_RUN_INTEGRATION=1 make integration-test
make bench-smoke
make release-smoke
```

`make lint` runs `go vet`, verifies `gofmt`, and runs `staticcheck` when it is installed locally. Staticcheck uses `XDG_CACHE_HOME=/tmp/taskforge-cache` by default when the caller has not set a cache path. Go commands default `GOCACHE` to `/tmp/taskforge-gocache` when the caller has not set it. To match CI:

```bash
go install honnef.co/go/tools/cmd/staticcheck@2025.1.1
```

GitHub Actions uses `actions/setup-go` module and build caching keyed from `go.sum`.

## CI tracks

- `lint`: formatting, vet, and pinned static analysis.
- `unit`: fast `go test ./...`.
- `integration`: Redis-backed tests with `TASKFORGE_RUN_INTEGRATION=1`.
- `race`: `go test -race ./...` for concurrency-sensitive packages.
- `benchmark-smoke`: compile and run each benchmark once without enabling the Redis benchmark harness.
- `release-smoke`: build release binaries and Docker images without publishing.

Release publishing is intentionally outside CI for now. A real release should be cut from a tag, attach the `dist/` outputs, build versioned container images, and include release notes that summarize behavior changes plus validation performed.
