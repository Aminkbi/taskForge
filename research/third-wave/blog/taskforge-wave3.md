# A fast queue can still fail under overload: what TaskForge changes

Imagine one queue serving several customers. A large import fills it with low-priority work. A smaller customer's urgent tasks wait behind the import. Retries add more work, while every active handler calls an external API that can safely handle only a few requests at a time. Enqueue latency can look good throughout this incident. The question is how the queue decides what to admit, whose work to serve, and how much to run.

[TaskForge](https://github.com/Aminkbi/taskForge) is an early-stage Go runtime built around that question. Its simplest worker pool uses FIFO selection and fixed concurrency over a Redis Streams broker. The same runtime can enable admission control, weighted tenant fairness, dependency budgets, and adaptive concurrency. Those controls are useful only when their effect on the workload is worth their cost.

## Start with the ordinary queue

The clean baseline is TaskForge with FIFO selection and a fixed worker limit. It already has leased, **at-least-once** delivery, bounded retries, delayed work, and a dead-letter path. A task can run more than once, so handlers need an idempotent boundary for external effects. Those delivery rules stay in place when overload controls are enabled.

Keeping the broker and worker path the same gives each comparison a clear question. If a control changes the result, is it protecting a tenant or dependency, or is it simply adding queue time and Redis work? The [configuration model](https://github.com/Aminkbi/taskForge/blob/main/docs/reference/configuration.md) exposes each control independently, so an application can answer that question one change at a time.

| Decision | FIFO/static behavior | Optional TaskForge control |
| --- | --- | --- |
| Should more work enter? | Publish the next accepted task | Admission can defer or reject when configured queue, tenant, retry, or age limits are reached |
| Who runs next? | Take ready work in arrival order | Weighted fairness gives tenant keys configured service weights, reservations, and quotas |
| How much work may call a dependency? | Worker concurrency is the only broad limit | A named budget holds tokens for the full handler run, across tasks sharing that dependency |
| How many handlers should run? | Keep a fixed worker limit | Adaptive concurrency adjusts its window using latency, errors, backlog, and starvation signals |

These choices have different failure modes. Deferral lengthens completion time. A dependency budget can reduce throughput while protecting a capacity limit. A feedback controller can react too late or choose a poor window. Fairness weights need to express entitlement rather than simply mirror whichever tenant sends the most tasks. Choose each control based on its measured tradeoffs.

## What the latest committed run tells us

After the latest worker, metrics, and recurring-scheduler changes were committed, I reran the [third-wave control-plane protocol](https://github.com/Aminkbi/taskForge/tree/main/research/third-wave). It compares the current committed revision `5d1d882` with pre-optimization revision `4446ab3`, using the same benchmark harness for both, a dedicated standalone Redis 7.4 process over loopback, Go 1.27.1-X, 30 operations per sample, and ten repetitions. Both revisions passed the recorded Redis, worker, and integration correctness suites. The first attempt at the latest-state run exposed a concurrent recurring-schedule reconciliation race; the committed revision includes its fix.

| Measured path | Earlier revision | Latest revision | Median paired change |
| --- | ---: | ---: | ---: |
| Fair publish, without deduplication | 167.8 µs/op | 104.2 µs/op | −38.4% |
| Plain publish, without deduplication | 98.7 µs/op | 97.5 µs/op | +0.6%, inconclusive |
| Queue metrics snapshot, 64 tenants and 64 KiB payload | 43.1 ms/op | 0.530 ms/op | −98.8% |

The percentage is calculated from paired samples, so it can differ from the change between the displayed medians. The fair-publish and snapshot reductions have family-adjusted intervals below zero; the plain-publish interval crosses zero. The [raw logs](https://github.com/Aminkbi/taskForge/tree/main/research/third-wave/data/final) and [derived analysis](https://github.com/Aminkbi/taskForge/blob/main/research/third-wave/results/analysis.md) show the other factors and the 15% regression gate.

This run measures implementation cost across two source revisions. It does **not** compare FIFO/static with admission, adaptive concurrency, or the full set of controls under an overloaded workload. It does not establish a tenant SLO improvement, a remote Redis speedup, or a universal throughput ranking. The latest revision also includes changes beyond the Redis optimization, so the comparison is cumulative.

## The comparison that matters for an application

Start with the same task arrivals and handler behavior, then compare FIFO/static with one control enabled. For admission, record accepted and deferred work, backlog age, and completion deadlines. For fairness, inspect each tenant's share of timely completions against its configured entitlement. For a dependency budget, observe concurrent calls and failures at that dependency as well as queue time. For adaptive concurrency, follow the effective worker window together with latency, errors, and throughput.

TaskForge gives Go applications a shared runtime for those experiments and for the resulting operating policy. The [README](https://github.com/Aminkbi/taskForge#readme) has an embedded-worker demo, and the [reliability contract](https://github.com/Aminkbi/taskForge/blob/main/docs/reference/reliability.md) states the delivery and Redis boundaries. The useful question is which policy keeps the work your application values moving when load and failures arrive together.
