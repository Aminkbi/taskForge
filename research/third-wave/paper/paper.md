# Wave 3: Measuring TaskForge's optimized control plane without trading away semantics

## Abstract

TaskForge's first two research waves studied overload behavior with paired open-loop
traces. Wave 3 asks a narrower engineering question: can the control plane
become cheaper while its delivery and state guarantees remain intact? We
pre-register a paired treatment of committed revision `b2947f3`: atomic publish-plus-state
recording, pipelined queue metrics, cached consumer-group setup, cheaper key
construction, and bounded unprocessable-delivery handling. The shared package
contains the protocol, source-to-evidence map, and wave 2 context. The Redis
service was unavailable while this package was prepared, so treatment numbers
are intentionally absent. This is a protocol-ready research artifact, not a
performance claim.

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

Treatment measurements are **not yet present**. The Redis service was
unavailable in the preparation environment, so no number is imputed from the
wave 2 workload corpus or from source-code inspection. Once both logs and the
correctness output are captured, this section is generated from the fixed
analysis plan; the estimand and decision rule cannot be changed to fit the
observed direction.

The pending benchmark documentation records provisional host-local medians of
510 to 278 microseconds for fair publish without a receipt, 707 to 502
microseconds with a receipt, and 8.80 to 0.89 milliseconds for 64-tenant,
64-KiB metrics. Those observations are useful leads, but their raw paired logs
are not present in this package, so they remain outside the evidence table and
cannot support the wave 3 claim.

| Family | Primary outcome | Paired samples | Median change | Family-wise interval | Status |
| --- | --- | ---: | ---: | --- | --- |
| Publish | Redis round trips/op | — | — | — | pending Redis run |
| Publish | Redis commands/op | — | — | — | pending Redis run |
| Snapshot | p95 ns/op | — | — | — | pending Redis run |
| Setup/key | allocations/op | — | — | — | pending Redis run |

## Reproducibility and limits

The wave 3 package is reproducible as a protocol now and becomes a measured
artifact when the two logs, source identities, Redis metadata, and correctness
output are added. It measures host-local Redis control-plane cost. It does not
establish remote Redis performance, multi-host contention, handler throughput,
or crash recovery. The wave 2 recovery cells remain explicitly unsupported.

The central reporting rule is simple: a lower benchmark number without passing
invariants is a regression, and an interval crossing zero is inconclusive.
