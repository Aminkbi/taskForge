# Overload Controls for Multi-Tenant Background Task Execution: A Pre-Registered Ablation Study of TaskForge

Mohammadamin Khanbabaei — draft for internal review. Not submitted or
published.

## Abstract

Background task queues commonly share workers and a broker across tenants.
TaskForge combines weighted tenant fairness, deferred admission, dependency
budgets, and feedback-controlled concurrency on top of leased at-least-once
delivery. This paper reports a pre-registered single-host ablation artifact
containing {{RUNS}} registered run records across {{WORKLOADS}} workloads and
{{VARIANTS}} variants. Of those records, {{MEASURED_RUNS}} are measurements
and {{NOT_MEASURED_RUNS}} are explicitly unsupported baseline fault cells
reported as not measured. The evidence shows targeted protection and
substantial control-plane tradeoffs, but it does not establish general
superiority over another queue. All quantitative evidence below is generated
from the checked raw dataset rather than transcribed into the paper.

## 1. Introduction

A multi-tenant background queue can fail when one tenant monopolizes ready
work, retries amplify load, a shared dependency is driven beyond capacity, or
a crashed consumer holds a delivery until lease recovery. Established systems
research supplies fairness, admission, and feedback-control mechanisms for
these problems [WFQ, DRF, SEDA, DAGOR, Breakwater]. TaskForge applies related
mechanisms inside an embeddable Redis-backed Go task runtime.

This paper claims composition and measurement, not mechanism novelty. Its
contributions are a registered workload and ablation grid, execution through
the public TaskForge worker path, per-cell raw evidence and provenance, and a
deterministic analysis that reports favorable, unfavorable, and inconclusive
contrasts together.

## 2. System under test

TaskForge offers at-least-once delivery. Duplicate execution is possible,
handlers must be idempotent, and exactly-once execution is not claimed.
Consumer leases fence acknowledgement and retry ownership; dead-letter
publication precedes source acknowledgement; scheduler writes require a
leadership fence. The experiment observes retries and duplicates but relies on
the repository's simulation, model, race, and integration checks for those
protocol invariants.

The evaluated controls are:

- weighted tenant selection, with offered load separable from entitlement;
- defer-mode admission caps at publish and due-release time;
- named dependency-token budgets held during handler execution; and
- a bounded concurrency controller driven by latency, error, backlog, and
  starvation signals.

## 3. Related work and positioning

Asynq supplies a Redis-backed Go task queue with static worker concurrency and
queue priority, but no equivalent per-tenant fairness, deferred admission,
dependency-capacity, or feedback-controller surface [Asynq]. River,
Machinery, and Celery expose different priority, queue, rate, and persistence
choices [River, Machinery, Celery]. Temporal is a durable workflow platform
with namespace, task-queue, rate, and worker-slot controls at a much larger
architectural scope [Temporal]. These systems are context rather than evidence
that TaskForge is superior. The Asynq arm is descriptive and excluded from all
TaskForge control contrasts.

## 4. Experimental design

The frozen hypotheses, control-engagement map, exclusion rules, and metric
definitions are in [`../analysis-plan.md`](../analysis-plan.md). Exact
workload descriptions and scaled parameters are copied from the raw manifests
into the generated
[`../results/analysis.md`](../results/analysis.md); the paper does not keep a
second hand-maintained description.

The unit of analysis is one workload/variant/seed run. Cell summaries are
medians of measured runs with a seeded {{RESAMPLES}}-resample percentile
bootstrap interval. Registered contrasts subtract each TaskForge ablation
from the full TaskForge arm. A contrast is called detected only when its
interval excludes zero. Throughput materiality uses the frozen relative-change
rule. The unsupported Asynq worker-crash cells contain no equivalent injected
fault, are marked not measured, and contribute neither zeroes nor baseline
comparisons.

## 5. Generated results

The following is the paper's complete numeric result table. It is generated
from `analysis.json` by `cmd/experiment-analysis`; editing this paper or its
template cannot change a value. The more detailed generated report lists every
metric and every pre-registered contrast.

{{GENERATED_EVIDENCE}}

### 5.1 Interpretation boundaries

Admission, fairness, dependency budgets, and adaptive concurrency are
interpreted only through their registered TaskForge contrasts. A control can
improve its target metric while worsening latency, throughput, or command
cost, and those outcomes are not collapsed into a single rank. The baseline
is reported only on common measured delivery metrics. Its unsupported fault
cells are not treated as successful zero-recovery observations.

The first-wave harness is closed loop, uses short fixed handler service times,
does not model a dependency whose latency degrades under excess concurrency,
and executes on one co-located host. Consequently, the table characterizes
this artifact and motivates the next open-loop study; it does not support a
cross-environment performance or production-SLO claim.

## 6. Artifact provenance and reproducibility

The measured source is commit `{{SOURCE_COMMIT}}`; the measured experiment
binary has SHA-256 `{{BINARY_SHA256}}`. Each cell repeats that provenance,
the source tree, build and runner arguments, dependency-lock digests, Redis
configuration, a sanitized environment allowlist, and the compressed result
digest. The registered runner rejects any tracked or untracked source change.
An explicit pilot mode permits exploratory dirty-tree runs but marks their
ledger non-publishable, and the publication analysis rejects that ledger.

`make artifact-integrity` extracts the recorded commit, verifies its tree
and dependency locks, rebuilds the measured binary, compares its digest, then
regenerates the analysis, paper, and figures in a temporary directory and
byte-compares them with the committed outputs.

## 7. Threats to validity

- The experiment uses one co-located host and Redis instance; host-level
  intervals do not establish cross-machine generality.
- Closed-loop publishers do not impose an identical external arrival trace on
  systems with different enqueue costs.
- Short fixed service times cause control-plane round trips to be unusually visible.
- The dependency workload proves an execution cap, not prevention of collapse
  in a downstream service whose latency changes with load.
- Variant order is fixed, so thermal or background activity can correlate
  with an arm.
- Peak logical-task overlap includes retry intervals and is an execution proxy
  only for retry-free workloads.
- Jain fairness over outcome ratios measures equality of outcomes, not
  entitlement satisfaction; per-tenant SLO outcomes remain necessary.
- The Asynq adapter is not expert-tuned and cannot express the evaluated
  controls or the registered crash schedule.

## 8. Conclusion

This repaired first-wave artifact provides traceable evidence about the
targeted behavior and overhead of TaskForge's overload controls. It is a basis
for replication and for a neutral open-loop study, not a final performance
claim. Future work should replay fixed arrival traces, model dependency
collapse, test realistic service-time ranges and multiple environments, and
measure controller trajectories and per-tenant entitlement deficit directly.

## Availability

The raw grid, provenance ledger, generated analysis, generated paper, figures,
and artifact guide are in `research/`. Citation metadata is at the repository
root. Packaging is local and deterministic; uploading, DOI minting, and
submission remain separate human-approved actions.

## References

- [WFQ] A. Demers, S. Keshav, S. Shenker. "Analysis and Simulation of a Fair
  Queueing Algorithm." SIGCOMM 1989.
- [DRF] A. Ghodsi et al. "Dominant Resource Fairness: Fair Allocation of
  Multiple Resource Types." NSDI 2011.
- [SEDA] M. Welsh, D. Culler, E. Brewer. "SEDA: An Architecture for
  Well-Conditioned, Scalable Internet Services." SOSP 2001.
- [DAGOR] H. Zhou et al. "Overload Control for Scaling WeChat
  Microservices." SoCC 2018.
- [Breakwater] I. Cho et al. "Overload Control for microsecond-scale RPCs
  with Breakwater." OSDI 2020.
- [Asynq] https://github.com/hibiken/asynq
- [River] https://github.com/riverqueue/river
- [Machinery] https://github.com/RichardKnop/machinery
- [Celery] https://docs.celeryq.dev
- [Temporal] https://github.com/temporalio/temporal
