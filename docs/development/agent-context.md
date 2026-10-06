# Architecture map

Read this map first, then open the owning package and its nearby tests. TaskForge
is one Go module; `main` contains software and its engineering checks.

## Ownership

| Change | Start here |
| --- | --- |
| Public task, delivery, state, retry, configuration, handler, DLQ, broker contracts | module-root `*.go` |
| Redis transport, persistence, routing, fairness, admission, budgets | `redis/` |
| Embedded execution, leases, drain, concurrency | `worker/` |
| Delayed/retry release, recurrence, leadership | `internal/scheduler/` |
| Sidecar environment decoding | `internal/config/` |
| Scheduler/API wiring | `cmd/<role>/`, then `internal/app/<role>/` |
| Metrics, HTTP, health, logging, shutdown | matching `internal/` package |
| Redis behavior and performance | `test/integration/`, `test/benchmark/`, `redis/*bench*` |
| Protocol fault schedules and bounded models | `internal/sim/`, `internal/modelcheck/` |
| Reliability claims and release evidence | `certification/`, `cmd/certify/` |
| Validation, builds, CI | `Makefile`, `scripts/`, `.github/workflows/` |

The module-root `taskforge` package is dependency-free and must not import
`redis`, `worker`, or `internal`. Applications register handlers and embed
`worker`; scheduler and API are optional sidecars.

## Invariants

- Delivery is at least once; handlers must be idempotent.
- Task ID identifies logical work. Queue/fairness stream plus stream-local
  delivery ID identifies a broker entry; consumer ownership fences its lease.
  Stale or expired owners cannot ack, extend, retry, or dead-letter newer work.
- Retry preserves task identity and obeys delivery policy. DLQ publication
  succeeds before source acknowledgement.
- Scheduler writes require current leadership fencing. New publishes choose
  placement; retry, due release, recurrence, DLQ, and requeue preserve it.
- Embedded applications use module-root configuration. Sidecars decode
  `TASKFORGE_` settings through `internal/config`. Copy payloads and headers at
  API ownership boundaries.

## Validation

Use the narrowest check while iterating; run the relevant gate before finishing.

| Change | Command |
| --- | --- |
| One package/test | `go test ./path/to/package -run TestName` |
| General Go change | `make test` |
| Formatting/static analysis | `make lint` |
| Worker, lease, scheduler concurrency | `make race-test` |
| Protocol faults or model changes | `make simulation-test`, `make model-check` |
| Redis behavior | `make integration-test` with `TASKFORGE_INTEGRATION_REDIS_ADDR` and non-zero `TASKFORGE_INTEGRATION_REDIS_DB` |
| Performance | `make bench`; method and prerequisites in [benchmarks](../operations/benchmarks.md) |
| Reliability linkage | `make certification-check` |
| Documentation/examples | `make docs-check`, `make test-demo` for demo behavior |
| Build/release tooling | `make release-validate` |

## Documentation

Usage and public API: [README](../../README.md). Contracts: `docs/reference/`.
Operations and benchmarks: `docs/operations/`. Tooling and protocol models:
`docs/development/`. Release procedure: [RELEASING](../../RELEASING.md).

Roadmaps `01`–`30` are immutable history, not current architecture or a task list.
The full research repository is preserved at tag `research/archive-2026-10`;
use a separate checkout of that tag for research work.
