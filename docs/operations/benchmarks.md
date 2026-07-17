# Benchmarks

TaskForge microbenchmarks are opt-in and Redis-backed. Compare runs on the
same machine and Redis configuration; they are not SLA claims.

## Comparative experiments

`make experiment-smoke` is the clean-checkout smoke command. It starts the
Redis compose service if necessary, uses the dedicated Redis DB 14, runs every
workload/variant combination with a fixed seed, and writes raw JSON under
`artifacts/experiments/raw/`. It then derives `summary.json` and a small SVG
plot under `artifacts/experiments/reports/`. The script clears its dedicated
database between runs; set `TASKFORGE_EXPERIMENT_REDIS_DB` to another empty,
non-zero database when DB 14 is not suitable.

Raw results are the evidence and are intentionally ignored by Git. Reports and
plots are derived artifacts; regenerate them from the raw directory rather
than editing either output. A result records raw per-task timestamps, seed,
build SHA (`TASKFORGE_BUILD_SHA` when supplied), host OS/architecture/CPU
count, Go version, Redis connection/configuration string, and Redis CPU,
memory, and command counters.

The manifests in `test/experiment/workloads/` cover tenant skew, noisy
neighbor, hot dependency, retry storm, delayed backlog, and worker crash.
Each run executes TaskForge FIFO/static, one TaskForge ablation for each of
fairness, admission, adaptive concurrency, and dependency budgets, full
TaskForge, and the separate `asynq` adapter. TaskForge controls are held
constant except for the named disabled control. Asynq is a Redis-backed Go
baseline but does not expose equivalent tenant-fairness, admission, adaptive,
or dependency-budget controls, so its results are marked non-comparable for
control-specific claims and may only be compared on common delivery metrics.

The runner records p50/p95/p99 enqueue-to-start and completion latency,
throughput, Jain fairness, SLO/starvation violations, retries, duplicates,
worker-crash recovery time (when observed), and Redis CPU/memory/ops. Throughput
uses the first observed enqueue through the final observed completion; worker
startup and shutdown are excluded. Jain fairness compares each active tenant's
SLO-compliant completion ratio, so offered tenant skew is not mistaken for
unfair service. Redis CPU and operation counts are per-run deltas; memory is
the end-of-run footprint. It does not print, publish, or imply a winner.
Larger repetitions should retain raw outputs outside the repository before a
result is interpreted.

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
