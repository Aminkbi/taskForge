# TaskForge

TaskForge is a Go scaffold for building a small distributed job system.
It borrows a few ideas from Celery, but it is not trying to clone Celery feature-for-feature. The goal here is simpler: start with a codebase that is easy to read, easy to extend, and honest about what is finished and what is still a placeholder.

Right now the repo gives you the shape of the system:

- a worker process
- a scheduler for delayed and retryable jobs
- a small API/admin process
- Redis-backed queueing and delayed release paths
- logging, metrics, health checks, and optional OpenTelemetry wiring

It is a solid starting point for backend or infra work, but it is still a scaffold. Some reliability details are intentionally left as TODOs instead of being hidden behind vague promises.

## What is here today

- At-least-once delivery is the intended model.
- Redis is the first broker and result-store candidate.
- Delayed jobs and retries flow through Redis plus a scheduler loop.
- Worker leasing exists, but durable crash-safe visibility semantics are not finished yet.
- Metrics, structured logging, and tracing hooks are already wired in.

The repository builds and starts three binaries:

- `cmd/worker` polls Redis, runs a placeholder handler, and goes through retry and DLQ paths.
- `cmd/scheduler` releases delayed work when its ETA is reached.
- `cmd/api` exposes health, readiness, metrics, and a small admin surface.

## What this is not

- It is not a drop-in Celery replacement.
- It is not trying to hide complexity behind a big framework.
- It does not ship RabbitMQ support yet.
- It does not claim production-complete Redis lease semantics in the current state.

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
- `internal/brokerredis` contains the Redis implementation.
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
make test
make lint
make fmt
```

You can also use the helper scripts:

```bash
./scripts/dev.sh
./scripts/test.sh
./scripts/lint.sh
```

## Configuration

The scaffold reads configuration from environment variables:

```env
TASKFORGE_LOG_LEVEL=info
TASKFORGE_HTTP_ADDR=:8080
TASKFORGE_METRICS_ADDR=:8080
TASKFORGE_REDIS_ADDR=localhost:6379
TASKFORGE_REDIS_PASSWORD=
TASKFORGE_REDIS_DB=0
TASKFORGE_WORKER_CONCURRENCY=4
TASKFORGE_POLL_INTERVAL=1s
TASKFORGE_LEASE_TTL=30s
TASKFORGE_SHUTDOWN_TIMEOUT=10s
TASKFORGE_OTEL_ENABLED=false
TASKFORGE_SERVICE_NAME=taskforge
```

`TASKFORGE_METRICS_ADDR` is already part of the config surface, but today `/metrics` is still served on the main HTTP listener.

## Health and metrics

Every process exposes:

- `/healthz`
- `/readyz`
- `/metrics`

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

## Notes for the next pass

- Replace in-memory lease bookkeeping with durable Redis state.
- Add real task registration and user-defined handlers.
- Persist task results and execution metadata properly.
- Add deeper tracing around publish, reserve, execute, retry, and dead-letter flow.
- Add a second broker implementation without changing the runtime contract.
