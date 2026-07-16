# TaskForge

TaskForge is an early-stage Go runtime for building a small distributed job system on Redis Streams.
It is intentionally explicit about its reliability model: delivery is at least once, duplicate execution is possible, and task handlers must be idempotent.

The project is not a drop-in Celery replacement and does not claim production-complete semantics yet.
It is a readable foundation for queue runtime work, scheduler behavior, worker lifecycle handling, observability, and operational experiments.

## What Is Here

- Redis Streams-backed active queueing with durable delivery ownership.
- Delayed jobs, retry scheduling, recurring interval schedules, and DLQ replay paths.
- Worker drain behavior with lease renewal and forced shutdown after a grace window.
- Queue isolation, fairness policies, admission control, adaptive concurrency, and dependency budgets.
- Health checks, metrics, structured logging, and optional OpenTelemetry tracing.
- A public Go package for publishing tasks and embedding workers.

## Quick Start

Prerequisites:

- Go 1.25+
- Docker with Compose support

Run the local stack:

```bash
cp .env.example .env
docker compose up --build
```

That starts Redis, the optional scheduler and API sidecars, and Prometheus:

- scheduler admin: `http://localhost:8082`
- API/admin: `http://localhost:8083`
- Prometheus: `http://localhost:9090`

Useful local commands:

```bash
make test
make lint
make run-demo
make run-example-email
make run-example-media
make run-example-external-api
```

## Public Go API

Import the canonical model plus the Redis and worker implementations:

```go
import (
	"github.com/aminkbi/taskforge"
	taskforgeredis "github.com/aminkbi/taskforge/redis"
	"github.com/aminkbi/taskforge/worker"
)
```

Publish a task:

```go
broker := taskforgeredis.New(taskforgeredis.Options{
	Addr: "localhost:6379",
})
defer broker.Close()

task := taskforge.NewTask(
	"email.send",
	[]byte(`{"to":"user@example.com"}`),
	taskforge.WithQueue("default"),
	taskforge.WithIdempotencyKey("email:user@example.com:welcome"),
)

_, err := broker.Publish(ctx, task, taskforge.PublishOptions{})
```

Embed a worker in your own Go process:

```go
registry := taskforge.NewRegistry()
_ = registry.RegisterFunc("email.send", func(ctx context.Context, task taskforge.Task) error {
	// Decode task.Payload and perform an idempotent side effect.
	return nil
})

runtime, err := worker.New(worker.Options{
	Broker:      broker,
	Handler:     registry,
	Queue:       "default",
	Concurrency: 4,
})
if err != nil {
	return err
}
err = runtime.Run(ctx)
```

There is intentionally no generic worker binary. Applications embed the worker
package so their process owns task registration and handler code.

## Execution Contract

TaskForge's execution contract is deliberately narrow:

- Delivery is `at-least-once`.
- Duplicate deliveries are possible.
- Handlers must be idempotent.
- Handlers should respect `ctx.Done()`.
- A logical task ID is separate from a broker delivery attempt.
- Successful completion means the handler returned success and the broker durably accepted the ack for that delivery owner.
- Exactly-once execution is out of scope.

Worker shutdown is explicit: workers stop reserving new deliveries first, keep renewing owned leases for already-reserved work, and only force-cancel remaining execution when `TASKFORGE_SHUTDOWN_TIMEOUT` expires.
Lease loss means local execution ownership is no longer authoritative, so cancellation-insensitive handlers may still produce duplicate side effects.

## Project Layout

```text
cmd/                  optional sidecars and runnable examples
redis/                Redis broker, state, DLQ, and policy implementation
worker/               embeddable execution runtime
deploy/docker/        Dockerfiles for scheduler and API
docs/                 reference, operations, and development notes
internal/             scheduler, config, HTTP, and observability support
*.go                  canonical public models and contracts
scripts/              test, lint, benchmark, and release helpers
test/integration/     opt-in Redis integration tests
```

## Documentation

- [Configuration reference](./docs/reference/configuration.md)
- [HTTP and operations API reference](./docs/reference/http-api.md)
- [Runnable examples](./docs/operations/examples.md)
- [Operator runbooks](./docs/operations/runbooks.md)
- [Benchmark guide](./docs/operations/benchmarks.md)
- [Toolchain and CI policy](./docs/development/toolchain.md)
- [Redis v2 development reset](./docs/development/redis-v2-development-migration.md)

## Validation

Run the unit suite:

```bash
make test
```

Run Redis-backed integration tests against local Redis:

```bash
TASKFORGE_RUN_INTEGRATION=1 go test ./test/integration/...
```

Run the opt-in benchmark harness:

```bash
TASKFORGE_RUN_BENCHMARKS=1 make bench
```

## Current Gaps

- The public API is intentionally small and Redis-first.
- RabbitMQ and other broker backends are not implemented.
- Admin operations for DLQ inspection and replay are still narrow.
- Release publishing is being shaped around tagged binaries, checksums, and container images.
