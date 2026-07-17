# Architecture Map

Use this file to route repository work. It is the canonical owner for agent and
contributor architecture guidance; product usage belongs in the
[README](../../README.md), deployment settings in the
[configuration reference](../reference/configuration.md), and recovery steps
in [runbooks](../operations/runbooks.md).

## Ownership

| Change concerns | Start here |
| --- | --- |
| Public task, delivery, state, retry, configuration, handler, DLQ, broker contracts | module-root `*.go` |
| Redis transport, persistence, routing, fairness, admission, budgets | `redis/` |
| Embedded execution, leases, drain, concurrency | `worker/` |
| Delayed/retry release, recurring work, leadership | `internal/scheduler/` |
| Sidecar environment decoding | `internal/config/` |
| Scheduler/API wiring | `cmd/<role>/`, then `internal/app/<role>/` |
| Metrics, HTTP, health, logging, shutdown | matching `internal/` package |
| Redis-backed end-to-end behavior | `test/integration/` |
| Comparative experiments, research artifact, paper | `cmd/experiment*`, `internal/experiment/`, `research/` |

`taskforge` is dependency-free: it must not import `redis`, `worker`, or
`internal`. Applications register handlers and embed `worker`; there is no
generic standalone worker binary.

## Invariants

- Delivery is at least once; duplicate execution is possible and handlers must
  be idempotent. Exactly-once is not offered.
- Task ID identifies logical work. Queue/fairness stream plus stream-local
  delivery ID identifies a broker entry; the consumer owner fences one lease.
  Stale or expired owners cannot acknowledge, extend, retry, or dead-letter
  newer work.
- A retry keeps task identity and is bounded by delivery policy. DLQ publish
  must succeed before its source delivery is acknowledged.
- Scheduler writes require current leadership fencing. Routing is chosen on a
  new publish; retry, due release, recurrence, DLQ, and requeue preserve the
  existing placement.
- Embedded applications use the validated module-root configuration model;
  sidecars decode `TASKFORGE_` environment settings into that same model through
  `internal/config`. Copy payloads and headers at API ownership boundaries.

## Narrow validation

| Change | Run first |
| --- | --- |
| One package | `go test ./path/to/package -run TestName` |
| General Go change | `make test` |
| Formatting or static analysis | `make lint` |
| Concurrent worker, lease, or scheduler behavior | `make race-test` |
| Deterministic protocol fault schedules | `make simulation-test` |
| Exhaustive bounded protocol state spaces | `make model-check` |
| Redis behavior | `make integration-test` (Redis on `localhost:6379`) |
| Reliability claim/check/artifact linkage | `make certification-check` |
| Registered research evidence or generated report | `make research-check` |
| Documentation links | `make docs-check` |

Use `make run-demo` for the public embedded-worker path and `make compose-up`
for the scheduler/API stack. See [toolchain policy](toolchain.md) for pinned
inputs and CI tracks.

## Documentation ownership

- README: product scope, quick start, public API.
- `docs/reference/`: configuration and HTTP endpoint contracts.
- `docs/reference/reliability.md`: reliability claims, evidence, and assumptions.
- `docs/operations/`: runbooks, benchmark method, and routing operation.
- `docs/development/`: this map, toolchain policy, and Redis development reset.
- `docs/roadmap/01`–`30`: immutable history, not active guidance.

## Context reduction record

The count covers README, agent/contributor guides, and `docs/development/`,
`docs/reference/`, and `docs/operations/`; it excludes `docs/roadmap/`.
Before: 1,453 lines / 9,382 words. After: 741 lines / 3,046 words.

Intentional duplication is limited to: the one-sentence at-least-once warning
in the README and agent guides; commands in the README for onboarding and this
map for change routing; and links to the canonical owner from topic-adjacent
documents.
