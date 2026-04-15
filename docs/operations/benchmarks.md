# Benchmarks

TaskForge benchmarks are opt-in and Redis-backed. They are intended to give operators and maintainers a repeatable way to compare broker behavior across machines and configurations, not to publish universal SLA numbers.

## Preconditions

- Local Redis on `localhost:6379`, or `TASKFORGE_REDIS_ADDR` pointed at another reachable Redis instance
- A dedicated benchmark DB via `TASKFORGE_REDIS_DB` if you do not want to use the default benchmark DB
- `TASKFORGE_RUN_BENCHMARKS=1`
- Optional: `TASKFORGE_RUN_HEAVY_BENCHMARKS=1` to include the `100000` recurring-schedule scaling case

The benchmark harness lives under `test/benchmark/` and is exposed through:

```bash
TASKFORGE_RUN_BENCHMARKS=1 make bench
```

Equivalent direct command:

```bash
TASKFORGE_RUN_BENCHMARKS=1 GOCACHE=/tmp/taskforge-gocache \
go test -run '^$' -bench . -benchmem ./test/benchmark/...
```

## Covered scenarios

- publish throughput
- reserve and ack throughput
- end-to-end latency through a live worker
- reclaim latency after an unacked worker delivery expires
- scheduler release lag for delayed tasks
- recurring scheduler tick cost as configured schedule count grows
- retry-storm throughput until tasks land in DLQ

## Benchmark assumptions

- Broker: Redis Streams for active work, Redis sorted set for delayed work
- Default benchmark Redis DB: `14`
- Default reserve timeout in harness: `10ms`
- Reclaim benchmark lease TTL: `20ms`
- Scheduler benchmark poll interval: `5ms`
- Retry-storm benchmark retry delay: fixed `5ms`, max deliveries `2`

These settings bias toward short benchmark runtimes and observable reclaim/scheduler behavior. They are intentionally more aggressive than production defaults.

## Interpreting results

- Publish throughput mostly reflects Redis write latency and message encoding overhead.
- Reserve/ack throughput reflects the hot consumer-group path without handler work.
- End-to-end latency includes publish, reserve, handler dispatch, execution, and ack.
- Reclaim latency is measured from the start of the reclaiming reserve call after the original lease has already expired.
- Scheduler lag is measured from ETA to the point a worker can reserve the released task.
- Recurring scheduler tick scaling measures one steady-state recurring dispatch tick after index reconciliation, with a fixed small due subset and larger total schedule counts.
- The default recurring scaling benchmark covers `10` and `1000` schedules; set `TASKFORGE_RUN_HEAVY_BENCHMARKS=1` to include the `100000` schedule case.
- Retry-storm throughput measures how quickly initial tasks move through retry scheduling into DLQ under sustained transient failure.

## Sample results

Sample results below were collected on this machine with:

```bash
TASKFORGE_RUN_BENCHMARKS=1 GOCACHE=/tmp/taskforge-gocache \
./scripts/bench.sh
```

Hardware and runtime notes:

- CPU: `12th Gen Intel(R) Core(TM) i7-1255U`
- OS: `Linux 6.17.0-14-generic`
- Go: `go1.25.0 linux/amd64`
- Redis: local single-node `localhost:6379`

| Benchmark | Sample result |
| --- | --- |
| Publish throughput | `63488 ns/op`, `2982 B/op`, `36 allocs/op` |
| Reserve and ack throughput | `397673 ns/op`, `7683 B/op`, `135 allocs/op` |
| End-to-end latency | `787162 ns/op`, `777164 ns/e2e`, `13687 B/op`, `204 allocs/op` |
| Reclaim latency | `34612788 ns/op`, `2251000 ns/reclaim`, `17187 B/op`, `271 allocs/op` |
| Scheduler release lag | `29832747 ns/op`, `3747845 ns/scheduler_lag`, `21062 B/op`, `392 allocs/op` |
| Retry-storm throughput | `2463433 ns/op`, `405.9 final_tasks/s`, `193052 B/op`, `1927 allocs/op` |

If your results differ substantially, compare:

- Redis RTT and persistence settings
- CPU contention
- worker concurrency and prefetch
- benchmark DB isolation
- scheduler poll interval and lease TTL choices
