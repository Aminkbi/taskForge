# Performance development

Benchmarks live in `test/benchmark/` and `redis/*bench*`. They cover publish,
reserve/ack, reclaim, snapshots, worker batching, delayed/retry release,
recurrence, fairness, and control-plane costs.

## Run benchmarks

Use a dedicated standalone Redis endpoint and non-zero database. Benchmarks
clear their test keys; keep application data and concurrent workloads separate.

```bash
export TASKFORGE_RUN_BENCHMARKS=1
export TASKFORGE_REDIS_ADDR=127.0.0.1:6379
export TASKFORGE_REDIS_DB=14
make bench
```

Set `TASKFORGE_RUN_HEAVY_BENCHMARKS=1` for the 100,000-schedule case.
An enabled run fails if Redis is unavailable. `make bench-smoke` executes
available benchmarks once; Redis cases skip unless explicitly enabled, so it
does not establish performance.

## Regression comparison

Capture baseline and candidate on the same host, Go toolchain, Redis version,
configuration, and dedicated database. Use matching fixed iteration counts and
at least five samples. For example, run this on each revision with the same
Redis settings above, writing `before.txt` and `after.txt` respectively:

```bash
GOFLAGS='-benchtime=30x -count=5 -skip=Benchmark(Control|PublishThroughput|Reserve|Skewed|EndToEnd|Reclaim|Scheduler|Delayed|MultiQueue|Recurring|Retry|Short)' \
  make bench > before.txt

make benchmark-regression BENCHMARK_ARGS='before.txt after.txt'
```

The comparator requires matching OS, architecture, CPU, packages, benchmark
names, metrics, iteration counts, and sample counts. It reports median changes
and rejects increases above 15% in `ns/op`, `B/op`, `allocs/op`, Redis commands,
or Redis round trips. Server-wide network counters are reported without a gate.
The versioned policy is in [benchmark-baseline.json](../../certification/benchmark-baseline.json).

Without both logs the command fails. CI uses
`TASKFORGE_BENCHMARK_METADATA_ONLY=1` to check metadata presence; it performs no
measured comparison. Investigate regressions and repeat matched runs when host
noise affects the result.

## Optimize safely

1. Reproduce the slow path with a focused `go test` benchmark in one package
   and collect CPU, allocation, or mutex profiles using Go's profiling flags.
2. Change the measured bottleneck and compare latency, allocations, commands,
   and round trips. Keep raw logs with their revision and environment metadata.
3. Run the owning package's tests and the relevant Redis, race, simulation, or
   model checks from the [architecture map](../development/agent-context.md).

Publish/reserve benchmarks measure transport and encoding costs; end-to-end
benchmarks include handler execution. Harness timings are chosen for short
runs and are not deployment defaults. Results apply to their measured host and
workload. Historical comparative studies and measurement reports are preserved
in the complete repository at tag `research/archive-2026-10`.
