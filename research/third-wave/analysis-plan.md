# Registered analysis plan: control-plane cost without semantic regression

## Question

Does committed revision `b2947f3` reduce Redis work and host-side
overhead while preserving delivery, state, fairness, and dead-letter
invariants?

The unit of analysis is one matched benchmark sample on one host, Redis
configuration, Go toolchain, benchmark name, and fixed iteration count. A
candidate sample is paired with a baseline sample at the same repetition index;
the baseline is a clean checkout built with the same toolchain. The committed
revision is the treatment. The wave 2 workload corpus is context, not a control arm for
this microbenchmark question.

## Pre-declared estimands

Primary estimands are the median paired percentage change (treatment minus
baseline) in:

1. `redis_round_trips/op` and `redis_commands/op` for publish and queue-metrics
   benchmarks;
2. `ns/op` for `BenchmarkPublishThroughput` and
   `BenchmarkSnapshotCosts`; and
3. `B/op` and `allocs/op` for the same benchmark names.

The sign convention is fixed: negative is an improvement. A paired percentile
bootstrap interval uses 10,000 resamples and seed `20260922`. Each benchmark
family (publish, snapshot, setup/key hot path) receives one Bonferroni-adjusted
95% family-wise interval. The raw per-sample values remain the evidence.

Secondary outcomes are p50/p95/p99 latency from the repeated benchmark samples,
and the ratio of the treatment's Redis work to baseline work. They are
descriptive when the primary interval crosses zero.

## Treatment matrix

The benchmark names and factors are fixed before measurement:

* publish: fairness on/off × deduplication on/off;
* snapshots: 1, 16, and 64 tenants × 256 B and 64 KiB payloads × metrics and
  admission-age operations; and
* correctness: concurrent duplicate publish, stale lease fencing, queued-state
  visibility, and bounded unprocessable-delivery handling.

The benchmark harness uses Redis DB 14, no concurrent workload, a dedicated
Redis process, and `-benchtime=30x -count=10`. Any failed or skipped sample is
retained with its reason and cannot be converted to zero. A Redis outage makes
the performance portion incomplete; it does not justify substituting a
different store or an in-memory fake.

## Decision rules

The patch supports an optimization claim only when:

* the family-wise interval for the relevant primary metric is below zero;
* no invariant test fails; and
* no paired benchmark in the same family regresses by more than 15% in
  `ns/op`, allocations, Redis commands, or round trips.

An interval containing zero is reported as inconclusive. A faster result with a
failed invariant is a semantic regression, not a performance win. Results are
stratified by benchmark factor; pooled language is prohibited when factor
effects reverse sign.

## Validity and limits

These measurements identify host-local control-plane cost. They do not measure
remote Redis, multi-host contention, network failures, or application handler
time. The latency-proxy class from wave 2 remains contextual evidence unless
the treatment is rerun through the same proxy. Redis server counters include
the dedicated server's work and should not be presented as worker CPU.

The small-sample benchmark design is appropriate for a regression decision,
not a universal throughput claim. Any blog headline must state the host,
toolchain, Redis topology, sample count, and whether the result is measured or
inconclusive.
