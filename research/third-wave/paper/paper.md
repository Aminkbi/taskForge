# Wave 3: Measuring TaskForge's optimized control plane without trading away semantics

## Abstract

TaskForge's earlier paired workload study established the operational context;
this study measures whether a committed Redis control-plane optimization lowers
its own cost while preserving delivery and state guarantees. The treatment is
revision `b2947f3`, compared with clean parent `4446ab3`, using identical
benchmark harnesses, a dedicated standalone Redis process, 30-iteration samples,
and ten repetitions. The study measures publish, queue snapshot, and setup/key
families. All correctness gates pass. Results are host-local engineering
measurements, not universal throughput claims.

## Context from wave 2

The frozen paired study measured one workstation in a native loopback class and
the same host with four Go processors and a declared 1 ms round trip. In the
common-delivery sweep, TaskForge FIFO/static was close to Asynq on native
throughput (median difference 2.1 tasks/s, 97.5% interval [0.59, 7.95]) but
was faster in the emulated-latency class (704.7 tasks/s, [159.3, 1048.4]).
Control-specific effects were workload- and environment-dependent: the
fairness contrast was strongly different under the emulated path, while the
long-duration admission contrast reversed sign between classes. Those results
motivate measuring control-plane cost directly instead of treating a workload
throughput result as a mechanism explanation.

## Method

The treatment is committed revision `b2947f3`, with clean parent `4446ab3` as
baseline; the binary diff digest identifies the treatment.
Each benchmark uses Redis DB 14, a dedicated Redis process, fixed 30-iteration
samples, ten repetitions, and the same Go toolchain. Samples are paired by
benchmark name and repetition. Primary outcomes are Redis round trips,
commands, nanoseconds, bytes, and allocations per operation. Ten thousand
paired bootstrap resamples with a fixed seed produce Bonferroni-adjusted
family-wise intervals.

Correctness is a gate: duplicate publish, queued-state visibility, stale lease
fencing, key compatibility, and bounded dead-letter handling must pass before a
performance result can be called an optimization.

## Results

The completed run contains 27 publish comparisons, 72 snapshot comparisons,
and nine supplementary setup/key comparisons. Negative effects mean lower
treatment cost. Ten-thousand-resample paired bootstrap intervals use the fixed
seed and family adjustment from the analysis plan; raw observations and the
derived table are in `data/final/` and `results/`.

Representative medians are:

| Case | Baseline | Treatment | Paired change |
| --- | ---: | ---: | ---: |
| Fair publish, no receipt | 153.6 µs | 100.6 µs | −34.8% |
| Publish throughput | 99.7 µs | 98.5 µs | +0.5%, inconclusive |
| Metrics snapshot, 64 tenants, 64 KiB payload | 43.3 ms | 0.510 ms | −98.8% |
| Key construction microbenchmark | 698.5 ns | 286.2 ns | −57.8% |

Every snapshot comparison improved. Publish results improved for the
fairness and deduplication factors; the plain publish path was within the
15% regression guard but was slightly slower in this run. Cached group setup
was effectively unchanged, while the uncached check remained dominated by its
Redis round trip. No comparison exceeded the 15% regression guard.

| Family | Primary outcome | Paired samples | Median change | Family-wise interval | Status |
| --- | --- | ---: | ---: | --- | --- |
| Publish | Redis round trips/op | — | — | — | pending Redis run |
| Publish | Redis commands/op | — | — | — | pending Redis run |
| Snapshot | p95 ns/op | — | — | — | pending Redis run |
| Setup/key | allocations/op | — | — | — | pending Redis run |

## Reproducibility and limits

The package is now a measured artifact with raw logs, source identities, Redis
metadata, and correctness output. It measures host-local Redis control-plane
cost. It does not establish remote Redis performance, multi-host contention,
handler throughput, or crash recovery. The wave 2 recovery cells remain
explicitly unsupported.

The central reporting rule is simple: a lower benchmark number without passing
invariants is a regression, and an interval crossing zero is inconclusive.
