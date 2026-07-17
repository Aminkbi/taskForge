# TaskForge

TaskForge is an early-stage Go runtime for Redis Streams-backed background work.
It delivers at least once: handlers must be idempotent because a task may run more than once.

## What Is Here

- Redis-backed publishing, leases, retries, delayed and recurring work, and DLQ handling.
- An embeddable worker, optional scheduler and read-only API sidecars, and operational metrics.
- Queue placement, fairness, admission, adaptive concurrency, and dependency budgets.

## Quick Start

Prerequisites:

- Go 1.26.5+
- Docker with Compose support

Run Redis and the adoption demo:

```bash
docker compose up -d redis
make run-demo
```

The demo embeds the worker and prints task, queue, and metrics results. It uses
only local Redis.

The scheduler and read-only API are optional operator sidecars. Run the full
operator stack when you need them:

```bash
docker compose up --build
```

That starts Redis, the optional scheduler and API sidecars, and Prometheus:

- scheduler admin: `http://localhost:8082`
- API/admin: `http://localhost:8083`
- Prometheus: `http://localhost:9090`

Validate the repository:

```bash
make test
make lint
make race-test       # concurrency changes
make integration-test # requires Redis on localhost:6379
```

## Public Go API

Import the canonical model plus the Redis and worker implementations:

```go
import (
	"time"

	"github.com/aminkbi/taskforge"
	taskforgeredis "github.com/aminkbi/taskforge/redis"
	"github.com/aminkbi/taskforge/worker"
)
```

Configure the overload controls once, then compile the same validated model for
the broker and worker:

```go
cfg := taskforge.Config{
	WorkerPools: []taskforge.WorkerPoolConfig{{
		Name: "default", Queue: "default", Concurrency: 4,
		TaskTimeout: 30 * time.Second,
	}},
}
broker, err := taskforgeredis.OpenFromConfig(ctx, cfg, taskforgeredis.Options{
	Addr: "localhost:6379",
})
if err != nil {
	return err
}
defer broker.Close()
```

Publish a task:

```go

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

runtime, err := worker.NewFromConfig(cfg, "default", worker.Options{
	Broker:  broker,
	Handler: registry,
})
if err != nil {
	return err
}
err = runtime.Run(ctx)
```

There is intentionally no generic worker binary. Applications embed the worker
package so their process owns task registration and handler code.

## Delivery Contract

TaskForge's execution contract is deliberately narrow:

- Delivery is `at-least-once`.
- Duplicate deliveries are possible.
- Handlers must be idempotent.
- Handlers should respect `ctx.Done()`.
- A logical task ID is separate from a broker delivery attempt.
- Successful completion means the handler returned success and the broker durably accepted the ack for that delivery owner.
- Exactly-once execution is out of scope.

For ownership, retry, scheduling, and retention invariants, see the
[architecture map](./docs/development/agent-context.md). Operator recovery
steps are in the [runbooks](./docs/operations/runbooks.md).

## Project Layout

```text
cmd/                  optional scheduler and API sidecars
examples/overload/    public-API-only adoption demo
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
- [Reliability contract and certification commands](./docs/reference/reliability.md)
- [HTTP and operations API reference](./docs/reference/http-api.md)
- [Operator runbooks](./docs/operations/runbooks.md)
- [Redis operating model](./docs/operations/redis.md)
- [Benchmark guide](./docs/operations/benchmarks.md)
- [Research artifact: pre-registered overload-control ablation study](./research/README.md)
- [Logical routing guide](./docs/operations/cluster-routing.md)
- [Toolchain and CI policy](./docs/development/toolchain.md)
- [Redis v2 development reset](./docs/development/redis-v2-development-migration.md)
- [Architecture map for contributors and agents](./docs/development/agent-context.md)

## Current Gaps

- Redis is the only broker backend.
- Redis Cluster and Sentinel are not supported; use a direct standalone Redis primary.
- The operator API is intentionally read-only and narrow.
