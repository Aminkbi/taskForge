# TaskForge

TaskForge is a Go codebase for building a small distributed job system.
It borrows a few ideas from Celery, but it is not trying to clone Celery feature-for-feature. The goal is narrower: keep the queue runtime easy to read, easy to extend, and honest about what is implemented versus what is still missing.

Right now the repo gives you the shape of the system:

- a worker process
- a scheduler for delayed and retryable jobs
- a small API/admin process
- Redis Streams-backed active queueing plus Redis delayed release paths
- logging, metrics, health checks, and optional OpenTelemetry wiring

It is a usable starting point for backend or infra work, but it is still an early-stage system rather than a finished queue product. Some areas are intentionally incomplete instead of being hidden behind vague promises.

## What is here today

- At-least-once delivery is the intended model.
- A task ID is the logical identity; each reserve creates a distinct delivery attempt.
- Redis is the first broker and result-store candidate.
- Delayed jobs and policy-driven retries flow through Redis plus a scheduler loop.
- Active delivery uses Redis Streams consumer groups with reclaim and durable lease renewal.
- Metrics, structured logging, and tracing hooks are already wired in.

## Execution contract

TaskForge's execution contract is explicit:

- Delivery is `at-least-once`.
- Duplicate deliveries are possible.
- Handlers must be idempotent.
- Successful completion means the handler returned success and the broker durably accepted the ack for that delivery owner.
- Exactly-once execution is out of scope.

Internally, the runtime distinguishes the logical `task_id` from a single `delivery_id` so stale acknowledgements can be rejected deterministically.

The repository builds and starts three binaries:

- `cmd/worker` polls Redis, runs a placeholder handler, and exercises the retry and DLQ paths.
- `cmd/scheduler` releases delayed work when its ETA is reached.
- `cmd/api` exposes health, readiness, metrics, and a small admin surface.
- `cmd/demo` runs a small end-to-end demo that lets the scheduler trigger file-appending tasks.

## What this is not

- It is not a drop-in Celery replacement.
- It is not trying to hide complexity behind a big framework.
- It does not ship RabbitMQ support yet.
- It does not claim production-complete retry, scheduling, scaling, or observability semantics in the current state.

## Project layout

```text
taskforge/
  cmd/
  deploy/docker/
  internal/
  pkg/taskforge/
  scripts/
  test/integration/
```

Most of the interesting logic lives under `internal/`:

- `internal/app/*` wires each binary together.
- `internal/config` loads typed config from environment variables.
- `internal/broker` defines the queue contract and message model.
- `internal/brokerredis` contains the Redis Streams broker and delayed-job release implementation.
- `internal/runtime` drives polling, execution, ack, retry, and lease extension.
- `internal/scheduler` handles delayed release and retry scheduling.
- `internal/store` and `internal/storeredis` sketch result storage.
- `internal/observability`, `internal/httpserver`, and `internal/logging` cover the operational baseline.

## Running locally

### Prerequisites

- Go 1.25+
- Docker with Compose support

### Quick start

```bash
cp .env.example .env
docker compose up --build
```

That brings up:

- Redis on `localhost:6379`
- worker admin endpoints on `localhost:8081`
- scheduler admin endpoints on `localhost:8082`
- API/admin endpoints on `localhost:8083`
- Prometheus on `localhost:9090`

If you prefer make targets:

```bash
make compose-up
make compose-down
make run-worker
make run-scheduler
make run-api
make run-demo
make run-example-email
make run-example-media
make run-example-external-api
make test
make bench
make lint
make fmt
```

You can also use the helper scripts:

```bash
./scripts/dev.sh
./scripts/test.sh
./scripts/lint.sh
./scripts/bench.sh
```

### Demo the scheduler

If you want to see delayed and recurring scheduling do real work on your machine, use the demo binary. It starts the worker manager plus scheduler, creates one immediate append task per configured worker pool, publishes one delayed task on the first pool queue, registers one recurring schedule on that same queue, and appends lines to a local file when tasks run.

```bash
make run-demo
```

By default it:

- uses Redis DB `15` unless `TASKFORGE_REDIS_DB` is explicitly set
- clears that demo DB before starting
- writes output to `/tmp/taskforge-demo.log`
- schedules one delayed run after `3s`
- schedules one recurring run every `2s`
- stops after `8s`

You can customize it:

```bash
TASKFORGE_DEMO_OUTPUT_FILE=/tmp/taskforge-demo.log \
TASKFORGE_DEMO_DELAYED_AFTER=2s \
TASKFORGE_DEMO_RECURRING_EVERY=1s \
TASKFORGE_DEMO_RUN_FOR=6s \
make run-demo
```

Then inspect the output file:

```bash
sed -n '1,20p' /tmp/taskforge-demo.log
```

### Runnable examples

TaskForge also includes three local example commands:

```bash
make run-example-email
make run-example-media
make run-example-external-api
```

They are documented in:

- [docs/operations/examples.md](./docs/operations/examples.md)
- [docs/operations/failure-matrix.md](./docs/operations/failure-matrix.md)
- [docs/operations/runbooks.md](./docs/operations/runbooks.md)
- [docs/operations/benchmarks.md](./docs/operations/benchmarks.md)

## Configuration

TaskForge reads configuration from environment variables:

```env
TASKFORGE_LOG_LEVEL=info
TASKFORGE_HTTP_ADDR=:8080
TASKFORGE_METRICS_ADDR=:8080
TASKFORGE_REDIS_ADDR=localhost:6379
TASKFORGE_REDIS_PASSWORD=
TASKFORGE_REDIS_DB=0
TASKFORGE_WORKER_POOLS_JSON=[
  {
    "name":"default",
    "queue":"default",
    "concurrency":4,
    "prefetch":4,
    "lease_ttl":"30s"
  }
]
TASKFORGE_TASK_TYPE_LIMITS_JSON=[]
TASKFORGE_POLL_INTERVAL=1s
TASKFORGE_SHUTDOWN_TIMEOUT=10s
TASKFORGE_SCHEDULER_LOCK_TTL=15s
TASKFORGE_SCHEDULER_RENEW_INTERVAL=5s
TASKFORGE_SCHEDULES_JSON=[]
TASKFORGE_OTEL_ENABLED=false
TASKFORGE_SERVICE_NAME=taskforge
```

`TASKFORGE_METRICS_ADDR` is already part of the config surface, but today `/metrics` is still served on the main HTTP listener.

Workers are configured as isolated queue pools through `TASKFORGE_WORKER_POOLS_JSON`.
Each pool can set `queue`, `concurrency`, `prefetch`, `lease_ttl`, retry defaults, and queue-local task caps.

Example:

```env
TASKFORGE_WORKER_POOLS_JSON=[
  {
    "name":"critical",
    "queue":"critical",
    "concurrency":2,
    "prefetch":2,
    "lease_ttl":"20s",
    "task_limits":[
      {"task_name":"reports.generate","max_concurrency":1}
    ]
  },
  {
    "name":"bulk",
    "queue":"bulk",
    "concurrency":6,
    "prefetch":12,
    "lease_ttl":"45s",
    "retry":{
      "max_deliveries":5,
      "initial_backoff":"1s",
      "max_backoff":"30s",
      "multiplier":2
    }
  }
]
TASKFORGE_TASK_TYPE_LIMITS_JSON=[
  {"task_name":"tenant.sync","max_concurrency":2}
]
```

Recurring schedules are configured statically through `TASKFORGE_SCHEDULES_JSON`. The first release is intentionally narrow:

- interval schedules only
- `coalesce` misfire policy only
- scheduler leadership enforced through a Redis lock
- durable recurring state plus a Redis due-time index keyed by `next_run_at`

Example:

```env
TASKFORGE_SCHEDULES_JSON=[
  {
    "id":"nightly-report",
    "interval":"15m",
    "queue":"default",
    "task_name":"reports.generate",
    "payload":{"kind":"nightly"},
    "headers":{"x-source":"scheduler"},
    "enabled":true,
    "misfire_policy":"coalesce",
    "start_at":"2026-04-14T10:00:00Z"
  }
]
```

## Scaling model

Phase 06 adds queue isolation as an explicit runtime model:

- Shared-queue horizontal scaling: run multiple worker processes with the same pool definition and queue name when you want throughput on one queue.
- Isolated critical queues: place critical work in its own queue and give it a dedicated worker pool so bulk backlogs do not consume that pool's leases or executor slots.
- Scheduler scaling: the scheduler remains leader-elected through Redis, so multiple scheduler instances are acceptable but only one should actively dispatch recurring work at a time.
- Recurring scheduler scaling: steady-state recurring dispatch work is proportional to schedules due in the current window, not the total configured schedule count. Inactive or far-future schedules stay in Redis durable state and the recurring due-time sorted set without forcing a full scan each tick.
- Redis considerations: each queue maps to its own stream and consumer group. Finalized entries are deleted on ack and nack so queue depth reflects live work instead of historical stream growth.
- Recurring Redis considerations: the main cost of large recurring fleets is Redis memory plus sorted-set maintenance for `next_run_at`, rather than scheduler CPU spent scanning every configured schedule.

## Health and metrics

Every process exposes:

- `/healthz`
- `/readyz`
- `/metrics`

Worker metrics now include queue-aware counters and gauges, including:

- `taskforge_queue_depth`
- `taskforge_queue_reserved`
- `taskforge_queue_consumers`
- per-queue success, failure, retry, reclaim, and active-task metrics

The API process also exposes:

- `/`
- `/v1/admin/ping`

## Testing

The unit test coverage is still small, but the basics are there:

- config parsing tests
- retry policy tests
- opt-in Redis integration tests under `test/integration`

Run the full unit suite with:

```bash
go test ./...
```

Run integration tests against local infrastructure with:

```bash
TASKFORGE_RUN_INTEGRATION=1 go test ./test/integration/...
```

Run the opt-in benchmark harness against local Redis with:

```bash
TASKFORGE_RUN_BENCHMARKS=1 make bench
```

## Notes for the next pass

- Add deeper admin and HTTP operations for dead-letter inspection and replay.
- Add real task registration and user-defined handlers.
- Persist task results and execution metadata properly.
- Add a second broker implementation without changing the runtime contract.
