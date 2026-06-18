# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

TaskForge is a Go (1.25.0) distributed job system inspired by Celery. It ships three runtime binaries plus example/demo binaries, all backed by Redis. The system is intentionally early-stage: some areas are incomplete on purpose rather than hidden. The defining design commitment is an **at-least-once delivery contract** (see README "Execution contract") — handlers must be idempotent, duplicate deliveries are expected, and exactly-once is out of scope. Preserve these semantics; call out any reliability or lease-semantics change explicitly.

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
make compose-up        # docker compose up --build -d (Redis + worker/scheduler/api + Prometheus)
make compose-down
make run-worker | run-scheduler | run-api | run-demo
```

Run a single test: `go test ./internal/runtime -run TestName` (add `-race` for concurrency code). `GOCACHE` defaults to `/tmp/taskforge-gocache`.

To match CI lint locally: `go install honnef.co/go/tools/cmd/staticcheck@2025.1.1`. CI tracks are separate jobs: `lint`, `unit`, `integration`, `race`, `benchmark-smoke`, `release-smoke` (see `.github/workflows/ci.yml`).

## Architecture

Entrypoints in `cmd/` are thin; each binary is wired together in a matching `internal/app/<role>` package (`api`, `scheduler`, `worker`). The three core roles:

- **worker** (`cmd/worker`): polls Redis, reserves deliveries, runs handlers, drives ack/retry/DLQ and lease renewal.
- **scheduler** (`cmd/scheduler`): releases delayed work when its ETA is reached, schedules retries, runs recurring schedules; uses leader election.
- **api** (`cmd/api`): health, readiness, metrics, and a small admin surface.

Key package boundaries:

- `internal/broker` — the central `Broker` interface (`Publish`/`Reserve`/`Ack`/`Nack`/`ExtendLease`) and message/result model. This is the queue contract; `internal/brokerredis` is the Redis Streams implementation (consumer groups, reclaim, leases) plus admission, budget, fairness, and adaptive-concurrency logic.
- `internal/runtime` — worker execution engine. `Manager` supervises a set of `Worker`s and orchestrates the explicit drain→force shutdown sequence (stop reserving, keep renewing owned leases, force-cancel only after `TASKFORGE_SHUTDOWN_TIMEOUT`). The runtime separates the logical `task_id` from a per-attempt `delivery_id` so stale acks are rejected deterministically.
- `internal/scheduler` — delayed release, retry scheduling, recurring schedules, and leadership (Redis-backed lock with TTL renewal).
- `internal/store` + `internal/storeredis` — durable task state and result/retention storage.
- `internal/tasks` — task model, state machine (`states.go`), and `RetryPolicy`.
- `internal/routing` + `internal/fairness` — routing policy and per-tenant/queue fairness policy.
- `internal/config` — all configuration is environment-driven (`TASKFORGE_*` prefix), parsed into typed structs incl. JSON-encoded pool/budget/limit/schedule definitions. Add new settings here rather than hardcoding.
- `internal/observability`, `internal/httpserver`, `internal/logging`, `internal/healthcheck`, `internal/shutdown` — operational baseline (Prometheus metrics, OTEL tracing hooks, HTTP health/metrics, structured logging).
- `internal/dlq` — dead-letter publishing. `internal/clock` — injectable clock for deterministic tests.
- `cmd/example-*` + `internal/examples/*` — runnable handler examples (email, media, external API). `pkg/taskforge` is the intended public surface.

## Conventions

- Configuration: every new setting goes through `internal/config` with a `TASKFORGE_` env var; worker pools, budgets, limits, and schedules are passed as JSON env values (see `.env.example`).
- Tests: table-driven with `t.Parallel()` for independent cases; unit tests live beside their package as `*_test.go`. Integration tests are opt-in under `test/integration/` (require `TASKFORGE_RUN_INTEGRATION=1` and local Redis). Add/adjust tests alongside observable behavior changes.
- Reuse existing package patterns before adding abstractions; don't rewrite unrelated code for a focused task. Don't silently ignore failing tests, lint errors, or generated-file drift.
- Commit subjects: short, imperative, present tense, one change each.

## Roadmap docs (important)

When deferring follow-up work during a large task, append it to a file under `docs/roadmap/` — **only append, never edit existing roadmap files**. Do not commit roadmap docs unless the user explicitly asks to include them.
