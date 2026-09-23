# Wave 3 latest-state rerun: Redis control-plane cost and correctness

## Abstract

This amended run compares TaskForge commit `5d1d882` with the pre-optimization
baseline `4446ab3`. The treatment includes the Redis control-plane optimization
and subsequent worker, metrics, and recurring-scheduler changes. Identical
benchmark harnesses ran against one dedicated standalone Redis 7.4 container
on a single AMD Ryzen 9 5900HS host with Go 1.27.1-X. Both revisions passed
Redis, worker, and integration correctness suites. The results describe the
cumulative latest state, not the effect of a single source change or the
benefit of overload controls over a FIFO queue.

## Method and amendment

The [original analysis plan](../analysis-plan.md) registered revision `b2947f3`
as treatment. The [execution notes](../execution-notes.md) disclose the later
treatment change to `5d1d882` and a failed provisional correctness run that
was excluded before measurement. The baseline remains `4446ab3`. Both exported
source revisions receive the same benchmark harness. The run uses Redis DB 14,
persistence disabled, 30 operations per benchmark sample, and ten repetitions.
The complete baseline arm ran before the treatment arm; pairing by repetition
index does not control temporal drift.

The primary publish and snapshot families contain 27 and 72 metric-cell
comparisons. The nine setup/key comparisons are supplementary because that
family's benchmark definitions were added after diagnostic runs. Reported
changes are medians of paired percentage changes, which need not equal the
percentage change between the two displayed arm medians. The derived
[analysis](../results/analysis.md) reports descriptive 95% paired bootstrap
intervals and Bonferroni-adjusted family-wise intervals from 10,000 resamples.

## Results

Representative measurements from the completed run are:

| Case | Baseline median | Latest median | Median paired change | Family-wise interval |
| --- | ---: | ---: | ---: | ---: |
| Fair publish without deduplication | 167.8 µs/op | 104.2 µs/op | −38.4% | [−45.3%, −32.8%] |
| Plain publish without deduplication | 98.7 µs/op | 97.5 µs/op | +0.6% | [−12.3%, +24.9%] |
| Publish-throughput benchmark | 100.0 µs/op | 93.6 µs/op | −6.7% | [−18.7%, +15.2%] |
| Metrics snapshot, 64 tenants and 64 KiB payload | 43.1 ms/op | 0.530 ms/op | −98.8% | [−98.9%, −98.7%] |
| Three-key construction microbenchmark | 652 ns/op | 295 ns/op | −56.2% | [−65.3%, −46.8%] |

All 72 snapshot comparisons had lower treatment cost. Twenty-one of 27 publish
comparisons and three of nine supplementary setup/key comparisons were lower.
The median regression gate found no comparison beyond its 15% threshold. The
plain publish and publish-throughput timing intervals cross zero; neither is a
reliable speedup claim from this run.

The large metrics-snapshot reduction is consistent with the Redis code reading
stream depth, pending counts, and consumer information through a pipeline.
Fair publish records the ready entry and built-in queued state in one script.
These mechanisms are visible in the source and measured Redis work counters,
but the cumulative treatment prevents attributing every observed timing
difference exclusively to one commit.

## Correctness and limits

The baseline and latest revisions passed the recorded Redis, worker, and
integration suites before benchmarks. The latest revision includes a fix for
a concurrent recurring-schedule reconciliation race exposed by the excluded
provisional run. The [raw benchmark logs and metadata](../data/final/) and
[regression gate](../data/final/regression-gate.txt) identify both source
revisions, the common harness, Redis configuration, and command outcomes.

These measurements are host-local control-plane costs. They do not measure
application handler time, remote Redis, independent hosts, workload SLOs,
tenant protection, or whether admission, fairness, budgets, and adaptive
concurrency outperform FIFO/static execution. The small sample count and
baseline-then-treatment order further limit causal interpretation. The
separate blog describes the latest product controls and uses this run only for
the implementation-cost question.
