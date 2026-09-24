# What should a task queue control beyond FIFO?

A simple queue answers one question: which ready task runs next? A production
task runtime also needs to decide whether work should enter the ready queue,
how many handlers should run, and how much concurrency a shared dependency can
accept.

[TaskForge](https://github.com/Aminkbi/taskForge) is an embeddable Go runtime
for Redis-backed background work. It provides publishing, leases, retries,
delayed and recurring work, dead-letter handling, and optional overload
controls:

| Queue decision | Simple FIFO queue | TaskForge control |
| --- | --- | --- |
| Which task runs next? | Ready work in arrival order | Weighted tenant fairness |
| Should more work enter the ready queue? | Accept it | Defer work at a configured limit |
| How many handlers run? | A fixed worker count | Adaptive concurrency from feedback |
| How many calls share a dependency? | No separate limit | A named dependency budget |

TaskForge still delivers at least once, so handlers must be idempotent. The
controls are policies around that delivery model, and each one has a cost. To
understand those tradeoffs, I compared each control with a simple FIFO/static
configuration under fixed offered load.

## The comparison

The experiment uses immutable open-loop traces generated before measurement.
Every arm receives the same scheduled arrivals, so a control cannot change the
offered load. The dependency model has capacity, latency growth, and failures
above capacity. Results use all offered tasks as the denominator for on-time
completion; rejected, late, failed, and unresolved tasks remain visible.

The experiment uses six profiles, two seeds per profile, and 38 cells. Each
cell runs on the same TaskForge source with a dedicated Redis 7.4 container on
loopback. Pressure profiles offer a fixed arrival stream for six seconds after
warmup, followed by a bounded drain. The pressure arms use eight workers; a
four-worker static-capacity arm tests whether a reactive control beats a safe
static limit.

The numbers below are medians of the two seeds. “SLO success” is the fraction
of all offered measurement-window tasks completed within 500 ms of their
scheduled arrival. p99 is conditional on successful completion and includes
queue and retry wait. Backlog values are sampled every 100 ms.

| Profile | Arm | SLO success | Conditional p99 | Downstream failures | Ready backlog max | Deferred backlog max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Fairness pressure | FIFO/static | 14.2% | 3,550 ms | 0% | 1,325 | 0 |
| Fairness pressure | Fairness only | 20.9% | 3,556 ms | 0% | 1,318 | 0 |
| Admission pressure | FIFO/static | 14.2% | 3,507 ms | 0% | 1,311 | 0 |
| Admission pressure | Admission only | 31.0% | 6,442 ms | 0% | 21 | 1,228 |
| Dependency pressure | FIFO/static | 1.3% | 20,136 ms | 85.3% | 1,397 | 7 |
| Dependency pressure | Budget only | 20.9% | 2,367 ms | 0% | 430 | 0 |
| Dependency pressure | Static capacity (4) | 22.1% | 2,268 ms | 0% | 412 | 0 |
| Adaptive pressure | FIFO/static | 1.2% | 19,994 ms | 85.1% | 1,396 | 6 |
| Adaptive pressure | Adaptive only | 14.4% | 3,779 ms | 9.7% | 441 | 5 |
| Adaptive pressure | Static capacity (4) | 21.7% | 2,235 ms | 0% | 410 | 0 |

## What the controls actually bought

**Fairness protected the smaller tenant.** The aggregate SLO number hides the
main result because the noisy tenant supplied nine times as many arrivals. The
protected tenant completed 100% of its offered work within the SLO with
fairness enabled, versus about 12% under FIFO/static. This is an isolation
result, not a claim that fairness makes every task faster.

**Admission contained the ready queue.** The ready backlog fell from about
1,311 to 21 tasks. The same offered work accumulated in the deferred backlog,
which peaked around 1,228 tasks. On-time completion improved from 14% to 31%,
but conditional p99 nearly doubled because deferral moves waiting time into the
task's completion path. Admission protects the active queue and selected
deadlines; it does not create more processing capacity.

**The dependency budget prevented the modeled failure mode.** With eight
workers and no budget, about 85% of downstream attempts failed, and about 97%
ran while the dependency was above its modeled capacity. Holding four
dependency tokens reduced both rates to zero and raised on-time completion
from 1% to 21%. A four-worker static cap produced nearly the same result. The
experiment therefore supports dependency isolation, while showing that a
simpler static limit can be competitive for this workload.

**Adaptive concurrency helped, but reacted too late to match the static cap.**
The adaptive arm reduced downstream failures to about 10% and raised on-time
completion from 1% to 14%. Its controller moved between four and nine workers.
The static four-worker arm avoided the modeled failures entirely and reached
22% on-time completion. Adaptive control is useful when the safe capacity is
unknown or changes, but this short fixed-capacity scenario favors the known
static limit.

At the stable light load, every arm completed 100% of offered work with p99
around 23–24 ms. The controls therefore had small overhead when no overload
was present in these measurements.

## What I would configure

I would start with FIFO/static for a uniform workload and add one control for a
specific failure mode. Fairness is worth evaluating when one tenant can delay
another. Admission is useful when ready-queue growth threatens important work,
provided the deferred backlog and end-to-end deadline are part of the SLO.
A dependency budget is useful when downstream capacity is a real constraint;
compare it with a static worker cap. Adaptive concurrency is useful when that
capacity changes or cannot be configured safely in advance.

The [experiment protocol and profiles](https://github.com/Aminkbi/taskForge/tree/main/research/queue-controls),
[raw results and generated analysis](https://github.com/Aminkbi/taskForge/tree/main/research/queue-controls/data),
and [reproduction script](https://github.com/Aminkbi/taskForge/blob/main/scripts/queue-controls-run.py)
contain the details behind these numbers. The project README has the complete
embedded-worker example.

These 38 single-host cells are descriptive evidence. They are not production
SLO guarantees, significance tests, or proof that the full set of controls
dominates FIFO. They do test the controls against fixed offered load and an
explicit downstream failure model, which makes the direction and cost of each
result interpretable. The prose plan was edited after measurement and no
longer matches its recorded hash; the original copy is unavailable. The runner
and executable profiles still match their recorded hashes.
