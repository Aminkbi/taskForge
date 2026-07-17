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

## Control-plane frontier pass (2026-07-18)

This pass defines a host-local target for the common successful-delivery workload:
TaskForge FIFO/static median throughput must be no more than 15% below the
tuned Asynq adapter. The target is enforced from a complete counterbalanced
result directory, not from one favorable pair:

```bash
make experiment-trace \
  PROFILE=test/experiment/open-loop/frontier-common-contract.json \
  TRACE=/tmp/taskforge-frontier.json \
  TRACE_ARGS='-seed 20260718'

for repetition in 0 1 2 3; do
  go run ./cmd/experiment-neutral \
    -trace /tmp/taskforge-frontier.json \
    -output /tmp/taskforge-frontier-results \
    -systems taskforge-fifo-static,asynq \
    -repetition "$repetition" \
    -concurrency 16 \
    -snapshot-period 500ms \
    -drain-timeout 30s
done

make frontier-check FRONTIER_RESULTS=/tmp/taskforge-frontier-results
```

The checker rejects excluded, incomplete, rejected-enqueue, mixed-trace,
unpaired, and non-counterbalanced inputs. It computes each arm's median over
completion throughput measured from the first through last completion. The
profile offers 6,000 one-millisecond tasks/s for six seconds, with no retry,
delay, failure, admission, fairness, adaptive, or dependency-budget behavior.
It is a deliberately narrow common-workload throughput target, not a claim
that the adapters have identical state, DLQ, or crash semantics.

### Result and variability

Four counterbalanced repetitions on the host described below completed all
36,003 arrivals per arm with zero enqueue rejections, retries, duplicates, or
unfinished tasks.

| Metric | TaskForge FIFO/static | Asynq |
| --- | ---: | ---: |
| median completion throughput | 4,498 tasks/s | 4,610 tasks/s |
| throughput range | 4,278-4,761 tasks/s | 4,113-5,280 tasks/s |
| median p99 eligibility-to-start lag | 2.01s | 1.87s |
| median peak ready backlog | 9,007 | 8,154 |
| median Redis commands/completion | 13.97 | 17.25 |
| median Redis network bytes/completion | 3,288 | 1,672 |

The observed median throughput loss is 2.43%, so this block meets the 15%
target. Individual paired relative results ranged from TaskForge 13.7% faster
to 19.0% slower. The result is therefore host- and block-specific, and the
large order/run variation must remain visible in later multi-environment work.
TaskForge's roughly 2x network-byte cost is not hidden: unlike this Asynq
adapter, TaskForge writes its public queued, leased, running, and terminal task
state contract.

### Matched microbenchmarks

The baseline binary was built from `e804820`; the candidate used the same Go,
Redis, database, payload, and benchmark counts. Values are medians of the
listed repeated runs. `B/op` is Go allocated bytes, distinct from Redis network
bytes.

| Path | Before | After | Change |
| --- | ---: | ---: | ---: |
| FIFO publish, 6 runs | 221us, 4,952 B/op, 76 allocs | 149us, 4,250 B/op, 64 allocs | -32.7% time, -15.8% allocs |
| FIFO reserve+ack, 8 runs | 469us, 7,804 B/op, 134 allocs | 207us, 6,938 B/op, 102 allocs | -55.9% time, -23.9% allocs |
| skewed fairness reserve+ack, 8 runs | 1.097ms, 14,721 B/op, 276 allocs | 0.449ms, 12,357 B/op, 208 allocs | -59.1% time, -24.6% allocs |
| fairness Redis commands/task | 12.50 | 7.01 | -43.9% |
| fairness client round trips/task | 10.00 | 5.01 | -49.9% |

The final short-task feeder reaches all 16 handlers transiently but averages
6.4 active handlers and a median 5.6k tasks/s with prefetch 32. Nominal
concurrency is therefore reachable, but the remaining sustained limiter is
quantified: per-delivery running-state and ownership/state-finalization work,
plus Go scheduling and tracing, leave average utilization at about 40% for
one-millisecond handlers. Bounded FIFO batching improves the matched median by
about 5% over the sequential feeder; it is not described as a full solution.

### Command, byte, and allocation accounting

Run the accounting directly with:

```bash
TASKFORGE_RUN_BENCHMARKS=1 go test -run '^$' \
  -bench '^BenchmarkControlPlaneCategories$' \
  -benchtime=500x -count=5 -benchmem ./test/benchmark
```

| Category | Client commands / round trips | Redis network bytes/op | Go B/op / allocs |
| --- | ---: | ---: | ---: |
| standalone state transition | 1 / 1 | 517 | 2,952 / 43 |
| dependency-budget acquire+release | 2 / 2 | 474 | 1,412 / 46 |
| adaptive snapshot persistence | 1 / 1 | 398 | 894 / 14 |
| configured idle scheduler poll | 1 / 1 | 123 | 480 / 14 |

The FIFO/static arm constructs no fairness policy, adaptive writer, admission
policy, or dependency budget, and detects that its immutable trace contains no
delayed/retry work, so those controls issue zero Redis commands. A configured
production scheduler still polls: one indexed command per interval is the
measured cost, rather than an unmeasured “near zero” claim.

### Retained and rejected changes

Retained changes are intentionally limited to measured wins:

- Consumer-group creation is positively cached and invalidated/recreated on
  `NOGROUP`; it is no longer attempted for every reservation.
- FIFO reservation can return a bounded batch. Fairness remains one candidate
  at a time so a tenant stream cannot bypass weighted tier selection.
- Pending-owner/idle validation, `XACK`, `XDEL`, and terminal Redis state are
  one atomic script when the broker owns the state store. Custom state stores
  retain the previous ack-then-record behavior.
- FIFO publish and queued state are atomic for the built-in Redis state store;
  delayed, deduplicated, admission, routing, retry, and DLQ paths retain their
  existing placement and receipt rules.
- Reclaim scans run at one quarter of the shortest observed lease, capped at
  100ms and floored at 1ms, instead of once per reservation. Expired work is
  delayed by a bounded control interval, never acknowledged or reassigned
  without the existing owner and idle checks.
- The neutral FIFO/static arm disables genuinely absent controls and suppresses
  its scheduler only when the immutable trace proves there can be no delayed,
  deferred, or retry release.
- Budget-blocked adaptive windows reset healthy history and scale down; a
  regression test prevents blocked tokens from being interpreted as healthy
  backlog for scale-up.

Rejected or deferred approaches remain part of the record:

- Four concurrent reservation feeders regressed median throughput slightly
  (about 2.90k to 2.80k tasks/s before reclaim suppression) and did not improve
  average concurrency, so the prototype was removed.
- A process-local fairness-key cache was rejected because external publishers
  can introduce keys; bounded staleness would change service selection. The
  existing pipelined snapshot reconstruction remains.
- Atomic multi-tenant fairness selection was not retained: dynamically keyed
  tenant streams and quota snapshots would require a larger storage-layout
  change without matched evidence.
- Budget acquire/release remains two immediate atomic lease operations. Async
  or batched release could strand shared capacity after task completion.
- Adaptive state remains one durable write per enabled control period so
  operator snapshots do not become silently stale. Disabled adaptive control
  installs no writer in the FIFO/static arm.
- General production scheduler backoff was rejected because API and scheduler
  commonly run in separate processes; without a durable cross-process wakeup,
  local no-work caching could miss newly earlier work. The indexed poll cost is
  reported above instead.

Environment: Linux 7.0.0-27-generic x86-64; 12 logical CPUs on an Intel
i7-1255U; Go 1.26.5; Redis 7.4.9 standalone on localhost. CPU scaling and
normal host activity were not isolated. Raw neutral JSON, profiles, and the
rejected prototype captures remain private because they are large and contain
host timing metadata.

The committed `research/data` grid remains immutable evidence for its recorded
source revision; it was not partially regenerated with the optimized code. A
later full replacement study will start from the optimized revision.
