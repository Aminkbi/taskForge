# When should a queue do more than FIFO? A TaskForge control comparison

A queue can accept work quickly while one tenant waits behind another, retries pile up, or too many handlers hit the same dependency. [TaskForge](https://github.com/Aminkbi/taskForge) is a Go task runtime that gives an application four separate decisions: whether to admit work, whose work runs next, how many handlers run, and how many may use a shared dependency.

I wanted to compare those decisions with an ordinary queue, using the current TaskForge runtime on both sides. The baseline here is **TaskForge with FIFO selection and four fixed workers**. It uses the same Redis broker, embedded worker, at-least-once delivery, retries, and delayed-work path as the controlled arms. The full arm enables tenant fairness and adaptive concurrency, with admission and dependency budgets configured where the workload calls for them. For each workload, a third arm removes the most relevant control from the full configuration.

| Decision | FIFO/static baseline | Optional control |
| --- | --- | --- |
| Which task runs next? | Ready work in arrival order | Weighted tenant fairness |
| Should more work enter the ready queue? | Accept it | Defer work when a configured limit is reached |
| How many handlers run? | Four workers | Adjust the concurrency window from feedback |
| How many calls share a dependency? | No separate limit | Hold a named budget token while the handler runs |

These controls have costs. Deferring a task increases its completion time. A dependency budget can hold workers back. Adaptive concurrency needs time and a useful signal before its window changes. The experiment asks where those costs buy a better workload outcome.

## Four workloads, one source revision

All arms ran from TaskForge commit [`0cc8a56`](https://github.com/Aminkbi/taskForge/commit/0cc8a5699df839946f9aa4dcbc5cfbe05653ca57), after the worker, metrics, and recurring-scheduler changes were committed. Each workload ran at scale 8 with two input seeds and three repetitions per seed: six paired blocks per workload, 72 cells total. A dedicated Redis 7.4 container ran over loopback on one host; its database was cleared between cells. The workloads are short, closed-loop simulations with real TaskForge broker and worker controls, rather than production traffic.

The table shows the **median of six runs** for each arm. Completion p99 includes time spent waiting in the queue. An SLO violation means a task completed after its workload's deadline. Fractional violation medians come from an even number of runs.

| Workload | FIFO/static: p99 / violations | Full controls: p99 / violations | Relevant control disabled: p99 / violations |
| --- | ---: | ---: | ---: |
| Noisy neighbor (without fairness) | 148 ms / 63 | 106 ms / 6 | 111 ms / 21.5 |
| Delayed backlog (without admission) | 82 ms / 0 | 90 ms / 0 | 76 ms / 0 |
| Hot dependency (without budget) | 168 ms / 20 | 340 ms / 90.5 | 128 ms / 0 |
| Retry storm (without adaptive concurrency) | 92 ms / 0 | 120 ms / 0 | 118 ms / 0 |

The [raw observations and analysis](https://github.com/Aminkbi/taskForge/tree/main/research/third-wave/control-comparison/data/final) also report throughput, fairness, and differences paired by seed and repetition. The table's arm medians should not be subtracted to reconstruct those paired differences.

## What changed when a control was removed?

**Tenant fairness helped in the noisy-neighbor case.** One tenant offered much more work than the protected tenant. Full controls had a median of 6 late tasks, versus 63 for FIFO/static and 21.5 when fairness alone was removed. Median completed throughput was 1,650 tasks/s with full controls, 1,209 with FIFO/static, and 1,498 without fairness. This is the clearest benefit in this run; the particular fairness weights and arrival mix matter.

**Admission did not rescue the delayed-backlog case.** No arm missed the workload's deadline. Full controls took 90 ms at p99 and completed 1,678 tasks/s; removing admission brought p99 to 76 ms and throughput to 1,983 tasks/s. Here, the deferral cost was visible without an observed SLO benefit. That says something about this load and deadline, not about every overload scenario.

**The dependency budget was expensive in this harness.** Full controls limited a shared dependency to two tokens. They reached 340 ms p99, 458 tasks/s, and 90.5 late tasks, compared with 128 ms, 1,160 tasks/s, and no late tasks when only that budget was removed. The simulated dependency always takes the same service time. It does **not** slow down or fail when too many calls arrive, so this experiment measures the budget's cost but cannot show whether it prevents a real downstream outage.

**Adaptive concurrency had little visible effect in the retry storm.** Removing it changed median p99 from 120 to 118 ms and throughput from 1,206 to 1,208 tasks/s; both arms had zero late tasks. FIFO/static completed faster at 92 ms p99. This short run does not show an adaptive-concurrency win.

## How I would use this result

I would start with FIFO/static and add a control for a specific failure mode. Tenant fairness earns a closer look when one tenant can delay another. An admission limit needs a backlog and deadline where deferral protects useful work. A dependency budget needs measurements from the actual dependency, including errors and latency as concurrency rises. Adaptive concurrency needs a long enough load change to reveal its response.

These six-block, single-host comparisons are descriptive, not significance tests or production SLO guarantees. They show both a useful fairness outcome and costs that a queue owner should expect to measure. The [experiment plan](https://github.com/Aminkbi/taskForge/blob/main/research/third-wave/control-comparison/plan.md), [reproduction instructions](https://github.com/Aminkbi/taskForge/blob/main/research/third-wave/README.md), and [delivery contract](https://github.com/Aminkbi/taskForge/blob/main/docs/reference/reliability.md) provide the details behind the numbers. TaskForge still delivers at least once, so handlers must ensure external effects are idempotent regardless of which controls are enabled.
