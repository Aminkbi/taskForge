# TaskForge

TaskForge is an early-stage Go runtime for Redis Streams-backed background work.
It delivers at least once: handlers must be idempotent because a task may run more than once.

## Features

- Redis-backed publishing, leases, retries, delayed and recurring work, and DLQ handling.
- An embeddable worker, optional scheduler and read-only API sidecars, and operational metrics.
- Queue placement, fairness, admission, adaptive concurrency, and dependency budgets.

## Quick Start

With Go 1.27.1+ and Docker Compose, run Redis and the adoption demo:

```bash
docker compose up -d redis
make run-demo
```

The demo embeds the worker. For the optional scheduler, read-only API, and
Prometheus stack:

```bash
docker compose up --build
```

Scheduler admin listens on `localhost:8082`, API/admin on `localhost:8083`, and
Prometheus on `localhost:9090`. Development checks are in the
[architecture map](./docs/development/agent-context.md).

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

Handlers should respect `ctx.Done()`. Completion requires both handler success
and a durable acknowledgement for the current delivery owner. Ownership,
retry, scheduling, and durability assumptions are defined in the
[reliability contract](./docs/reference/reliability.md); recovery steps are in
the [runbooks](./docs/operations/runbooks.md).

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
- [Logical routing guide](./docs/operations/cluster-routing.md)
- [Toolchain and CI policy](./docs/development/toolchain.md)
- [Redis v2 development reset](./docs/development/redis-v2-development-migration.md)
- [Architecture map for contributors and agents](./docs/development/agent-context.md)

The complete research repository, including experiments, papers, data, and
recorded software revisions, is preserved at tag `research/archive-2026-10`.
Open it separately with `git worktree add --detach ../taskforge-research research/archive-2026-10`.

## Current Gaps

- Redis is the only broker backend.
- Redis Cluster and Sentinel are not supported; use a direct standalone Redis primary.
- The operator API is intentionally read-only and narrow.
