# Benchmarks

TaskForge benchmarks are opt-in and Redis-backed. Compare runs on the same
machine and Redis configuration; they are not SLA claims.

## Preconditions

- Redis at `localhost:6379`, or set `TASKFORGE_REDIS_ADDR`.
- A dedicated DB through `TASKFORGE_REDIS_DB` when needed.
- `TASKFORGE_RUN_BENCHMARKS=1`; add `TASKFORGE_RUN_HEAVY_BENCHMARKS=1` for the
  100,000-schedule case.

The harness is `test/benchmark/`:

```bash
TASKFORGE_RUN_BENCHMARKS=1 make bench
```

## Covered scenarios

- publish; reserve/ack; end-to-end latency; reclaim after lease expiry
- delayed release and scheduler catch-up
- multi-queue and skewed-fairness traffic
- recurring tick scaling and retry storms

## Interpretation

- Publish and reserve/ack measure Redis and encoding overhead without handler work.
- End-to-end includes publish, reserve, handler dispatch, execution, and ack.
- Reclaim starts after the original lease expires; scheduler lag is ETA to a
  reservable task.
- The harness uses short timings (10ms reserve timeout, 20ms reclaim lease,
  5ms scheduler poll) for fast, observable runs; do not treat them as defaults.

If results differ substantially, compare Redis RTT/persistence, CPU contention,
worker concurrency/prefetch, database isolation, and scheduler timing.
