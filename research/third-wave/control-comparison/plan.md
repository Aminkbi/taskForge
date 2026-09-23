# Latest-state overload-control comparison

Status: exploratory protocol fixed before this run. Earlier TaskForge workload
studies used related scenarios, so this is not independent confirmation or a
new preregistration. The measured product source is commit `0cc8a56`.

## Question and comparison

How does the latest TaskForge runtime behave with FIFO selection and static
concurrency, with all four overload controls, and with one relevant control
removed? Every arm uses the same TaskForge Redis broker and embedded worker.
Asynq is excluded because it cannot serve as a one-control ablation.

| Workload | Arms | Intended control contrast |
| --- | --- | --- |
| `noisy-neighbor` | FIFO/static, full, no-fairness | tenant fairness |
| `delayed-backlog` | FIFO/static, full, no-admission | deferred admission |
| `hot-dependency` | FIFO/static, full, no-dependency-budget | dependency budget |
| `retry-storm` | FIFO/static, full, no-adaptive | adaptive concurrency |

Each arm receives scale 8 of the checked-in workload manifest and seeds
`20260923` and `20260924`, repeated three times. The six seed/repetition blocks
per workload are paired by input seed. Variant order is deterministically
shuffled within each block. Each cell runs alone against database 14 of a
dedicated, persistence-disabled Redis 7.4 container. The runner flushes that
database between cells. No failed or missing cell is silently replaced.

## Outcomes and reporting

Record p99 completion time, completed throughput, Jain equality of
SLO-compliant completion ratios, and SLO violation count for every arm.
Report arm medians and median within-block differences for full minus
FIFO/static and full minus the named ablation. Lower p99 and violations are
favorable; higher throughput and Jain equality are favorable. Show unfavorable
and inconclusive-looking results alongside favorable ones. Six blocks on one
host are descriptive evidence, not a significance test or a production SLA.

This short, closed-loop harness does not model a downstream service whose
latency collapses under excessive concurrency. The dependency-budget contrast
therefore measures the workload cost of the budget, not prevention of an
external outage. Retries and delayed releases are exercised where a workload
requests them. The experiment does not re-prove TaskForge's delivery contract.
