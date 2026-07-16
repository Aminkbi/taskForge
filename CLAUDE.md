# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

TaskForge is a Go (1.25.0) embeddable worker runtime with a Redis transport and optional scheduler/API sidecars. The defining design commitment is an **at-least-once delivery contract** (see README "Execution contract") — handlers must be idempotent, duplicate deliveries are expected, and exactly-once is out of scope. Preserve these semantics; call out any reliability or delivery-semantics change explicitly.

## Common commands

The `Makefile` / `scripts/` are the stable interface — prefer them over raw `go` invocations:

```bash
make test              # unit suite (scripts/test.sh -> go test ./...)
make lint              # go vet + gofmt check + staticcheck (when installed)
make fmt               # gofmt -w .
make race-test         # go test -race
make integration-test  # TASKFORGE_RUN_INTEGRATION=1 go test ./test/integration/...  (needs Redis on :6379)
make bench-smoke       # compile+run each benchmark once
make release-smoke     # build release binaries/images without publishing
make compose-up        # docker compose up --build -d (Redis + scheduler/API + Prometheus)
make compose-down
make run-scheduler | run-api | run-demo
```

Run a single test: `go test ./worker -run TestName` (add `-race` for concurrency code). `GOCACHE` defaults to `/tmp/taskforge-gocache`.

To match CI lint locally: `go install honnef.co/go/tools/cmd/staticcheck@2025.1.1`. CI tracks are separate jobs: `lint`, `unit`, `integration`, `race`, `benchmark-smoke`, `release-smoke` (see `.github/workflows/ci.yml`).

## Architecture

Entrypoints in `cmd/` are thin; sidecars are wired in matching `internal/app/<role>` packages. The core roles are:

- **worker** (`worker` package): embedded by applications that register Go handlers.
- **scheduler** (`cmd/scheduler`): releases delayed work when its ETA is reached, schedules retries, runs recurring schedules; uses leader election.
- **api** (`cmd/api`): health, readiness, metrics, and a small admin surface.

Key package boundaries:

- module root — canonical task, delivery, retry, state, DLQ, handler, and broker contracts.
- `redis` — Redis Streams transport, state/DLQ persistence, routing, fairness, admission, and dependency budgets.
- `worker` — embeddable execution engine. `Manager` supervises workers and orchestrates drain→force shutdown.
- `internal/scheduler` — delayed release, retry scheduling, recurring schedules, and leadership (Redis-backed lock with TTL renewal).
- `internal/config` — all configuration is environment-driven (`TASKFORGE_*` prefix), parsed into typed structs incl. JSON-encoded pool/budget/limit/schedule definitions. Add new settings here rather than hardcoding.
- `internal/observability`, `internal/httpserver`, `internal/logging`, `internal/healthcheck`, `internal/shutdown` — operational baseline (Prometheus metrics, OTEL tracing hooks, HTTP health/metrics, structured logging).
- `internal/clock` — injectable clock for deterministic tests.
- `cmd/example-*` + `internal/examples/*` — runnable handler examples using the root, `redis`, and `worker` APIs.

## Conventions

- Configuration: every new setting goes through `internal/config` with a `TASKFORGE_` env var; worker pools, budgets, limits, and schedules are passed as JSON env values (see `.env.example`).
- Tests: table-driven with `t.Parallel()` for independent cases; unit tests live beside their package as `*_test.go`. Integration tests are opt-in under `test/integration/` (require `TASKFORGE_RUN_INTEGRATION=1` and local Redis). Add/adjust tests alongside observable behavior changes.
- Reuse existing package patterns before adding abstractions; don't rewrite unrelated code for a focused task. Don't silently ignore failing tests, lint errors, or generated-file drift.
- Commit subjects: short, imperative, present tense, one change each.

## Roadmap docs (important)

When deferring follow-up work during a large task, append it to a file under `docs/roadmap/` — **only append, never edit existing roadmap files**. Do not commit roadmap docs unless the user explicitly asks to include them.
