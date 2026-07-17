# Benchmarks

TaskForge microbenchmarks are opt-in and Redis-backed. Compare runs on the
same machine and Redis configuration; they are not SLA claims.

## Comparative experiments

### Neutral open-loop overload harness

The neutral harness is the preferred path for overload, dependency-budget,
admission, and cross-system delivery experiments. It has two deliberately
separate processes:

1. `cmd/experiment-trace` generates one deterministic trace and creates it
   read-only with exclusive-create semantics. The trace contains synthetic UTC
   arrival timestamps, tenant and service-time choices, delayed eligibility,
   per-attempt failure draws, and worker fault timestamps. Its SHA-256 covers
   all of those fields.
2. `cmd/experiment-neutral` only accepts an existing trace. It maps the same
   timestamp offsets onto each cell's run epoch and dispatches arrivals without
   waiting for prior enqueue calls. Enqueue blocking therefore appears as
   dispatch lag and enqueue duration instead of reducing the offered rate.

For example:

```bash
make experiment-trace \
  PROFILE=test/experiment/open-loop/sustained-and-burst.json \
  TRACE=/tmp/sustained-seed-20260718.json \
  TRACE_ARGS='-seed 20260718'

make experiment-neutral \
  TRACE=/tmp/sustained-seed-20260718.json \
  NEUTRAL_ARGS='-repetition 0 -output /tmp/neutral-results'
```

Generate a new path for every seed; trace generation refuses to replace an
existing file. Run repetitions 0 through 3 so the seeded counterbalance
rotates every arm through every position over one complete block. Redis DB 14
is flushed between cells. The
benchmark records the trace digest and position in every result, making an
accidental per-system workload or fixed-order comparison detectable.

The registered profiles under `test/experiment/open-loop/` have 30-second
warm-up and cooldown windows and a three-minute steady-state window with at
least 10,000 steady arrivals. Together they cover sustained and burst
overload, 4- and 16-tenant entitlement/load skews, service times at 1ms, 10ms,
100ms, and 1s, delayed-work pressure, retry amplification, and a dependency
whose latency and failure rate rise above declared capacity and collapse at a
declared overlap ratio. `make experiment-neutral-smoke` uses a shorter profile
only to check plumbing; its tails are not evidence.

Each raw result includes:

- accepted, deferred, and rejected enqueue observations, enqueue latency,
  external scheduler dispatch lag, and the trace timestamp;
- ready, deferred/scheduled, retry, and DLQ/archive backlog trajectories plus
  scheduler lag; every attempt also records eligibility-to-start lag, an
  explicit upper bound containing due-release and ready-queue wait;
- every downstream overlap, modeled latency, capacity, and failure;
- effective concurrency and controller action/reason;
- per-tenant offered, accepted, completed, SLO-compliant, entitlement share,
  service share, service deficit, and SLO attainment;
- Redis CPU, used memory, commands, network input bytes, and network output
  bytes; and
- cost per SLO-compliant completion when explicit CPU, network, and
  memory-time cost-unit rates are supplied. Zero rates intentionally produce
  zero cost rather than inventing infrastructure prices.

The dependency budget is configured to the modeled dependency's declared
capacity. It therefore gates a resource that can observably saturate and
collapse, rather than a constant handler sleep.

#### Adapter tuning and semantic boundaries

Adapters live in separate packages under `internal/experiment/adapters/`.
Their only shared code reads Redis server counters.

| Setting or semantic | TaskForge | Asynq |
| --- | --- | --- |
| Nominal worker concurrency | 16 by default; CLI-controlled | 16 by default; same CLI value |
| Poll/release period | 10ms scheduler, 1s blocking reserve | 10ms pending/delayed checks |
| Retry count/backoff | Trace maximum; fixed trace backoff | Same attempt count and fixed backoff |
| Delayed work | TaskForge delayed set and scheduler fence | Asynq scheduled set |
| Tenant entitlement | Weighted fairness from trace entitlements | Unsupported |
| Admission | Defer at 512 pending by default | No equivalent admission control |
| Dependency capacity | Budget tokens equal modeled capacity | No equivalent budget |
| Effective concurrency | Adaptive range 4-32 at default nominal 16 | Static 16 |
| DLQ column | TaskForge dead-letter contract | Asynq archive count; semantically different |
| Worker process crash | Unsupported by this in-process adapter | Unsupported by this in-process adapter |

The replay grid includes TaskForge full, no-admission, and
no-dependency-budget arms plus Asynq. The two TaskForge ablations change only
the named control, so admission and budget claims are evaluated against the
same dependency that the no-budget arm can drive beyond capacity.

Because graceful in-process shutdown is not equivalent to process death,
worker-fault traces are marked unsupported and excluded before either adapter
starts. They are not emitted as zero-valued measurements and must not enter a
delivery or recovery comparison. A future process-isolated adapter may opt in
only when it applies the trace's exact crash/recovery timestamps and preserves
the common delivery observation contract.

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
constant except for the named disabled control. The TaskForge arms run
through the public embedded `worker` package, so each control is the real
product control: fairness policies come from manifest tenants (with an
optional `fairness_weight` separating entitlement from offered load),
admission runs in defer mode with manifest-defined caps, dependency budgets
lease manifest-defined downstream capacity, and adaptive concurrency operates
within bounds [2, 8] around the static concurrency of 4. Arrival is a
concurrent multi-publisher stream against the running worker for TaskForge
and Asynq alike. Correction (2026-07-17): before the research-artifact wave,
the runner used a bespoke reserve loop that only engaged fairness, so earlier
raw runs for the admission, adaptive, and dependency-budget ablations
differed from `taskforge-no-fairness` in name only; regenerate any local raw
data rather than comparing it across that boundary. Asynq is a Redis-backed
Go baseline but does not expose equivalent tenant-fairness, admission,
adaptive, or dependency-budget controls, so its results are marked
non-comparable for control-specific claims and may only be compared on common
delivery metrics.

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

The registered multi-seed research grid, its pre-registered analysis plan,
committed raw evidence, and the derived statistical report live under
[`research/`](../../research/README.md); run it with
`make research-experiments` and regenerate every table and figure with
`make research-analysis`. `make research-check` validates the complete grid,
per-cell provenance, privacy-safe run log, and byte-reproducibility of every
derived result, paper table, and figure without modifying committed outputs.
`make artifact-integrity` additionally extracts the recorded source commit,
rebuilds the measured binary, and compares its digest.

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

## Measured fairness optimization (2026-07-17)

This profiling pass optimized one demonstrated bottleneck: fairness reservation
repeatedly read each active tenant stream through independent Redis round trips. The CPU profile
for 1,000 skewed fairness reserve/ack operations attributed 50.0% of samples to
syscalls and 58.1% cumulatively to Redis client processing. The allocation
profile attributed 25.2% cumulatively to `loadFairnessSnapshots`. The mutex
profile contained only 1.50ms of delay over a 2.13s run, so no lock change was
made.

The implementation now pipelines the read-only pending preflight and fairness
snapshot commands. The actual expired-delivery load and `XCLAIM`, pending-owner
validation, `XACK`, and entry deletion are unchanged. The pipelined snapshots
remain intentionally non-atomic, as they were before; candidate reservation is
still a nonblocking `XREADGROUP` and retries selection after a race. Exact
oldest-ready age is skipped only during reservation, where it is not a selection
input, and remains enabled for admission and metrics.

The matched benchmark used baseline `f995c2c` and the optimized candidate, 200
fixed operations per run, 12 runs per side. The interval below is a seeded,
nonparametric bootstrap of the difference between independently resampled run
medians; it describes run-to-run noise on this host, not other environments.

| Metric | Before | After | Change or uncertainty |
| --- | ---: | ---: | ---: |
| reserve/ack median | 1.528ms | 0.964ms | -36.9%; bootstrap 95% interval -46.7% to -30.7% |
| reserve/ack range | 1.050-2.165ms | 0.447-1.116ms | 12 runs per side |
| Redis round trips/op | 18.51 | 10.01 | -45.9% |
| Redis commands/op | 18.51 | 12.51 | -32.4% |
| bytes/op | about 18,140 | about 14,735 | -18.8% |
| allocations/op | 346 | 276 | -20.2% |

Twelve matched non-smoke `tenant-skew/taskforge-full` experiment runs used
seeds 20260717 through 20260728. Median p99 completion fell from 46.53ms
(33.10-56.41ms range) to 29.29ms (20.50-35.00ms), median throughput rose from
442.0 to 621.0 tasks/s, and median Redis commands fell from 946.5 to 693. Every
run on both sides recorded zero retries, duplicates, and SLO violations. The
complete comparative smoke harness also passed after the change. Its
worker-crash p99 was neutral (1.027s before, 1.029s after), as expected because
that path waits for lease expiry before reclaim.

Reproduce the benchmark and raw profiles with Redis on the configured benchmark
database:

```bash
TASKFORGE_RUN_BENCHMARKS=1 go test -run '^$' \
  -bench '^BenchmarkSkewedFairnessReserveAck$' \
  -benchtime=200x -count=12 -benchmem ./test/benchmark

mkdir -p /tmp/taskforge-profile
TASKFORGE_RUN_BENCHMARKS=1 go test -run '^$' \
  -bench '^BenchmarkSkewedFairnessReserveAck$' -benchtime=1000x \
  -cpuprofile=/tmp/taskforge-profile/fairness.cpu.pprof \
  -memprofile=/tmp/taskforge-profile/fairness.heap.pprof \
  -blockprofile=/tmp/taskforge-profile/fairness.block.pprof \
  -mutexprofile=/tmp/taskforge-profile/fairness.mutex.pprof \
  ./test/benchmark

go tool pprof -top /tmp/taskforge-profile/fairness.cpu.pprof
go tool pprof -top -sample_index=alloc_space \
  /tmp/taskforge-profile/fairness.heap.pprof
make experiment-smoke
```

Environment: Linux 7.0.0-27-generic x86-64; 12 logical CPUs on an Intel
i7-1255U; Go 1.26.5; Redis 7.4.9 standalone on localhost with AOF enabled,
`appendfsync everysec`, `save 60 1`, and `maxmemory-policy noeviction`. CPU
frequency scaling was enabled, Redis and the benchmark shared the host, and the
runs were not isolated from normal workstation activity. Raw experiment JSON
and profiles remain local because they contain host metadata and absolute paths.

### Neutral results

- Mutex contention was negligible before and after; lock tuning was rejected.
- With five recurring schedules due, the median tick was 5.77ms for 10
  configured schedules and 6.92ms for 1,000, with overlapping five-run ranges.
  The due index avoided growth proportional to configured schedules, so no
  scheduler query change was justified.
- Releasing one due task with 10,000 unrelated delayed entries took a median
  0.321ms for `MoveDue` (0.187-0.536ms range). The delayed queue index already
  avoided the unrelated backlog, so it was left unchanged.
- Remaining formatting, hashing, and tracing allocations were not optimized:
  network round trips remained the dominant measured cost and no independent
  benchmark demonstrated material benefit from those micro-optimizations.
