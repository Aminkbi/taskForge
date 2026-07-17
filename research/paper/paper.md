# Overload Controls for Multi-Tenant Background Task Execution: A Pre-Registered Ablation Study of TaskForge

Mohammadamin Khanbabaei — draft for internal review. Not submitted or
published.

## Abstract

Background task queues commonly share workers and a broker across tenants.
TaskForge combines weighted tenant fairness, deferred admission, dependency
budgets, and feedback-controlled concurrency on top of leased at-least-once
delivery. This paper reports a pre-registered single-host ablation artifact
containing 504 registered run records across 6 workloads and
7 variants. Of those records, 492 are measurements
and 12 are explicitly unsupported baseline fault cells
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
medians of measured runs with a seeded 10000-resample percentile
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

<!-- Generated from analysis.json; do not edit. -->
| Workload | Variant | Status | p99 completion (ms) | Throughput (tasks/s) | Jain fairness | SLO violations | Peak concurrency | Recovery (ms) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| delayed-backlog | taskforge-fifo-static | measured | 201.1 [190.9, 226.5] | 704 [654, 749] | 1.000 [1.000, 1.000] | 0 [0, 0] | 2 [2, 2] | 0 [0, 0] |
| delayed-backlog | taskforge-no-fairness | measured | 229.7 [215.6, 250.3] | 609 [585, 663] | 1.000 [1.000, 1.000] | 0 [0, 2] | 2 [2, 2] | 0 [0, 0] |
| delayed-backlog | taskforge-no-admission | measured | 341.5 [287.9, 355.0] | 436 [419, 513] | 0.998 [0.987, 0.999] | 40 [26, 46] | 2 [2, 2] | 0 [0, 0] |
| delayed-backlog | taskforge-no-adaptive | measured | 228.8 [212.1, 244.0] | 480 [447, 517] | 1.000 [1.000, 1.000] | 0 [0, 0] | 2 [2, 2] | 0 [0, 0] |
| delayed-backlog | taskforge-no-dependency-budget | measured | 249.9 [241.4, 258.1] | 484 [456, 508] | 1.000 [1.000, 1.000] | 2 [0, 8] | 2 [2, 2] | 0 [0, 0] |
| delayed-backlog | taskforge-full | measured | 250.8 [226.8, 295.0] | 470 [440, 517] | 1.000 [0.999, 1.000] | 2 [0, 8] | 2 [1, 2] | 0 [0, 0] |
| delayed-backlog | asynq | measured | 81.4 [78.6, 84.9] | 1561 [1541, 1644] | 1.000 [1.000, 1.000] | 0 [0, 0] | 4 [4, 4] | 0 [0, 0] |
| hot-dependency | taskforge-fifo-static | measured | 252.4 [239.2, 256.2] | 541 [533, 560] | 0.989 [0.983, 0.996] | 73 [68, 76] | 4 [3, 4] | 0 [0, 0] |
| hot-dependency | taskforge-no-fairness | measured | 487.7 [483.1, 500.5] | 299 [294, 314] | 0.978 [0.971, 0.988] | 112 [108, 114] | 2 [2, 2] | 0 [0, 0] |
| hot-dependency | taskforge-no-admission | measured | 491.2 [489.1, 495.5] | 298 [295, 300] | 0.986 [0.976, 0.990] | 114 [111, 116] | 2 [2, 2] | 0 [0, 0] |
| hot-dependency | taskforge-no-adaptive | measured | 391.7 [372.0, 398.6] | 375 [362, 382] | 0.985 [0.978, 0.990] | 103 [98, 106] | 2 [2, 2] | 0 [0, 0] |
| hot-dependency | taskforge-no-dependency-budget | measured | 324.1 [276.1, 347.2] | 432 [408, 488] | 0.984 [0.979, 0.990] | 96 [80, 104] | 3 [3, 3] | 0 [0, 0] |
| hot-dependency | taskforge-full | measured | 477.8 [460.7, 503.5] | 304 [290, 315] | 0.985 [0.979, 0.989] | 112 [109, 115] | 2 [2, 2] | 0 [0, 0] |
| hot-dependency | asynq | measured | 167.1 [162.6, 169.2] | 862 [857, 876] | 1.000 [0.999, 1.000] | 18 [15, 20] | 4 [4, 4] | 0 [0, 0] |
| noisy-neighbor | taskforge-fifo-static | measured | 273.7 [240.0, 286.2] | 592 [565, 661] | 0.966 [0.922, 0.979] | 127 [121, 141] | 3 [3, 3] | 0 [0, 0] |
| noisy-neighbor | taskforge-no-fairness | measured | 211.1 [197.4, 220.5] | 648 [617, 674] | 0.962 [0.931, 0.984] | 116 [110, 122] | 3 [3, 3] | 0 [0, 0] |
| noisy-neighbor | taskforge-no-admission | measured | 380.9 [343.4, 405.7] | 440 [420, 491] | 0.639 [0.615, 0.680] | 152 [146, 158] | 2 [2, 2] | 0 [0, 0] |
| noisy-neighbor | taskforge-no-adaptive | measured | 267.7 [253.4, 306.5] | 438 [416, 469] | 0.838 [0.807, 0.846] | 111 [107, 116] | 2 [2, 2] | 0 [0, 0] |
| noisy-neighbor | taskforge-no-dependency-budget | measured | 303.1 [263.5, 324.0] | 419 [405, 440] | 0.836 [0.808, 0.844] | 110 [108, 115] | 2 [2, 2] | 0 [0, 0] |
| noisy-neighbor | taskforge-full | measured | 283.1 [246.9, 313.8] | 430 [412, 442] | 0.831 [0.785, 0.843] | 113 [108, 121] | 2 [2, 2] | 0 [0, 0] |
| noisy-neighbor | asynq | measured | 142.6 [139.4, 146.2] | 1167 [1152, 1175] | 0.996 [0.988, 0.998] | 61 [58, 64] | 4 [4, 4] | 0 [0, 0] |
| retry-storm | taskforge-fifo-static | measured | 240.1 [228.9, 274.5] | 575 [512, 610] | 0.998 [0.996, 0.999] | 40 [38, 51] | 42 [40, 42] | 0 [0, 0] |
| retry-storm | taskforge-no-fairness | measured | 311.4 [282.2, 374.4] | 453 [389, 490] | 0.999 [0.996, 1.000] | 68 [54, 84] | 28 [26, 32] | 0 [0, 0] |
| retry-storm | taskforge-no-admission | measured | 416.5 [379.3, 445.9] | 354 [332, 386] | 0.989 [0.981, 0.997] | 90 [86, 96] | 45 [45, 46] | 0 [0, 0] |
| retry-storm | taskforge-no-adaptive | measured | 323.4 [310.7, 373.8] | 365 [335, 383] | 0.985 [0.964, 0.994] | 70 [62, 77] | 26 [26, 26] | 0 [0, 0] |
| retry-storm | taskforge-no-dependency-budget | measured | 342.7 [334.6, 367.3] | 341 [333, 357] | 0.988 [0.957, 1.000] | 79 [68, 86] | 27 [26, 28] | 0 [0, 0] |
| retry-storm | taskforge-full | measured | 346.7 [332.5, 357.6] | 344 [325, 355] | 0.990 [0.960, 0.997] | 84 [78, 86] | 27 [26, 28] | 0 [0, 0] |
| retry-storm | asynq | measured | 98.7 [94.4, 100.7] | 1385 [1332, 1449] | 1.000 [1.000, 1.000] | 0 [0, 0] | 36 [34, 40] | 0 [0, 0] |
| tenant-skew | taskforge-fifo-static | measured | 234.2 [215.0, 240.6] | 667 [650, 730] | 0.982 [0.953, 0.989] | 126 [114, 130] | 2 [2, 2] | 0 [0, 0] |
| tenant-skew | taskforge-no-fairness | measured | 209.3 [200.5, 221.3] | 733 [706, 753] | 0.970 [0.966, 0.984] | 112 [106, 121] | 2 [2, 2] | 0 [0, 0] |
| tenant-skew | taskforge-no-admission | measured | 363.1 [341.4, 398.4] | 458 [422, 488] | 0.977 [0.961, 0.988] | 148 [142, 156] | 2 [2, 2] | 0 [0, 0] |
| tenant-skew | taskforge-no-adaptive | measured | 354.3 [339.7, 375.4] | 472 [456, 493] | 0.982 [0.972, 0.994] | 148 [144, 150] | 2 [2, 2] | 0 [0, 0] |
| tenant-skew | taskforge-no-dependency-budget | measured | 352.4 [294.6, 382.1] | 481 [441, 562] | 0.981 [0.961, 0.997] | 154 [148, 158] | 2 [2, 2] | 0 [0, 0] |
| tenant-skew | taskforge-full | measured | 336.6 [324.5, 358.6] | 499 [474, 506] | 0.979 [0.959, 0.988] | 148 [141, 152] | 2 [2, 2] | 0 [0, 0] |
| tenant-skew | asynq | measured | 91.7 [89.2, 97.7] | 1625 [1570, 1702] | 1.000 [1.000, 1.000] | 0 [0, 0] | 4 [4, 4] | 0 [0, 0] |
| worker-crash | taskforge-fifo-static | measured | 163.9 [151.0, 190.4] | 316 [102, 563] | 1.000 [1.000, 1.000] | 0 [0, 1] | 3 [3, 3] | 703 [201, 1255] |
| worker-crash | taskforge-no-fairness | measured | 173.8 [165.8, 187.6] | 563 [106, 582] | 1.000 [1.000, 1.000] | 0 [0, 1] | 3 [3, 3] | 201 [201, 1199] |
| worker-crash | taskforge-no-admission | measured | 257.5 [239.0, 269.5] | 429 [410, 457] | 1.000 [0.992, 1.000] | 6 [0, 12] | 2 [2, 2] | 202 [201, 202] |
| worker-crash | taskforge-no-adaptive | measured | 246.3 [235.6, 271.4] | 457 [417, 473] | 1.000 [1.000, 1.000] | 0 [0, 15] | 2 [2, 3] | 202 [201, 202] |
| worker-crash | taskforge-no-dependency-budget | measured | 259.1 [242.3, 283.8] | 419 [397, 468] | 0.998 [0.994, 1.000] | 7 [2, 18] | 2 [2, 2] | 201 [201, 202] |
| worker-crash | taskforge-full | measured | 271.0 [261.0, 284.2] | 412 [390, 432] | 0.997 [0.994, 0.999] | 14 [6, 22] | 2 [2, 2] | 201 [201, 202] |
| worker-crash | asynq | not measured | not measured | not measured | not measured | not measured | not measured | not measured |

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

The measured source is commit `65ff0428821518eb75c42b4d7fa9a82b54bf408d`; the measured experiment
binary has SHA-256 `fa831a92eff3f1f32e84e94f8a2adc7ccd5cf0cde226c66a43a31fea0f7c556a`. Each cell repeats that provenance,
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
