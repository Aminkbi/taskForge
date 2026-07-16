# Toolchain and CI Policy

This document owns pinned tooling and CI coverage. For task-specific validation,
use the [architecture map](agent-context.md).

## Pinned inputs

- Go: `go.mod` declares `go 1.25.0`; CI uses `actions/setup-go` with it.
- Dockerfiles: `golang:1.25.0-alpine`.
- CI Redis: `redis:7.4-alpine`.
- Staticcheck: `honnef.co/go/tools/cmd/staticcheck@2025.1.1`.

When upgrading Go, update `go.mod`, Dockerfiles, this page, and verify CI.

## Local lint parity

`make lint` runs `go vet`, verifies `gofmt`, and runs `staticcheck` when it is
installed. To match CI:

```bash
go install honnef.co/go/tools/cmd/staticcheck@2025.1.1
```

## CI tracks

- `lint`: formatting, vet, static analysis.
- `unit`: `go test ./...`.
- `integration`: Redis-backed tests.
- `race`: `go test -race ./...`.
- `benchmark-smoke`: each benchmark once.
- `release-smoke`: release binaries and images without publishing.

Release runs from version tags; see [RELEASING.md](../../RELEASING.md).
