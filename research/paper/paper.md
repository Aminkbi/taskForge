# Overload Controls for Multi-Tenant Background Task Execution: A Pre-Registered Ablation Study of TaskForge

Mohammadamin Khanbabaei — draft v0.1, 2026-07-17. Not submitted or published;
prepared for preprint-quality internal review.

## Abstract

Background task queues routinely share one worker fleet and one broker across
many tenants, yet most open-source Go queues offer only FIFO ordering and
static concurrency. TaskForge is a Redis-backed Go task runtime whose thesis
is predictable multi-tenant execution under overload through four controls:
weighted tenant fairness, deferred admission control, dependency budgets, and
feedback-controlled worker concurrency, on top of leased at-least-once
delivery. We evaluate that thesis with a pre-registered, fully reproducible
ablation study: six seeded workloads, seven system variants (an all-controls-
off anchor, four single-control ablations, the full system, and Asynq as a
non-comparable baseline), twelve seeds per cell, and seeded bootstrap
intervals over per-run medians. Admission deferral cuts p99 completion latency
by 68-86 ms on all three parameterized workloads, although its SLO benefit is
distinguished from zero on only two. Fairness eliminates SLO violations for
the protected tenant under a 12:1 offered-load skew (0 versus 7-8 per run),
and dependency budgets hold peak concurrent executions at the configured
capacity (2 versus 3). The costs exceed our pre-registered materiality bound:
with millisecond-scale tasks on one host, the full control stack reduces
throughput by 25-47% against the FIFO/static anchor on five workloads, while
adaptive concurrency demonstrates no pre-registered latency benefit. We
report these and a lease-recovery tail beyond the nominal TTL alongside the
positive results. All raw data, analysis code, and figures regenerate from
the repository with two commands.

## 1. Introduction

A multi-tenant background queue fails in characteristic ways: one tenant's
burst starves everyone else; retry storms amplify load exactly when capacity
is scarce; a shared downstream dependency is driven past its capacity by
well-meaning worker parallelism; and recovery after a worker crash is bounded
by lease bookkeeping that few systems state explicitly. Production systems at scale
use admission control and load shedding for this class of problem
[DAGOR, Breakwater, SEDA], and cluster schedulers have long used weighted or
dominant-resource fairness [WFQ, DRF]; mainstream open-source task queues in
Go largely have not.

TaskForge implements those controls in a small embeddable Go runtime over
Redis Streams. This paper does not claim novelty for any individual
mechanism; each is an application of established ideas. The contribution is
an engineering-scale, honestly reported evaluation:

1. a pre-registered experimental design and analysis plan, frozen before the
   registered data existed ([`../analysis-plan.md`](../analysis-plan.md));
2. a single-command ablation harness in which every arm runs the public
   product path, so an ablation disables the real control;
3. committed raw evidence (504 runs) with a deterministic bootstrap analysis
   whose every table and figure regenerates byte-identically;
4. results that include the unfavorable outcomes: material throughput cost,
   a workload where the controls only add overhead, and a baseline that is
   substantially faster on raw delivery overhead.

## 2. System under test

TaskForge's delivery contract is explicitly at-least-once: a task may execute
more than once, handlers must be idempotent, and exactly-once is not offered.
Ownership is fenced by consumer leases; stale owners cannot acknowledge,
retry, or dead-letter newer work; dead-letter publication precedes source
acknowledgement; scheduler writes require leadership fencing. Those
invariants are covered by deterministic simulation, bounded model checking,
and Redis integration tests in the repository and are assumed, not re-proven,
here (observed retries and duplicates are still reported per run so a
regression would surface).

The four evaluated controls:

- **Tenant fairness.** Weighted deficit-style selection across per-tenant
  streams inside a queue, in the tradition of weighted fair queueing [WFQ].
  A tenant's entitlement (fairness weight) may differ from its offered load.
- **Admission control.** At publish and due-release time, a queue policy
  compares pending backlog, per-tenant backlog, oldest-ready age, retry
  backlog, and dead-letter size against caps; in defer mode an over-cap task
  is delayed and re-evaluated rather than rejected, akin to queue-level
  overload deferral in DAGOR-style systems [DAGOR].
- **Dependency budgets.** A named downstream dependency has a token capacity;
  a worker must hold a budget lease to execute a mapped task, capping global
  concurrent pressure on the dependency regardless of worker parallelism.
- **Adaptive concurrency.** A per-pool feedback loop adjusts effective worker
  concurrency between bounds using measured latency, error rate, backlog, and
  budget starvation, in the spirit of feedback-controlled service stages
  [SEDA] and adaptive limit controllers [NetflixLimits, CoDel].

## 3. Related work

| System / work | Backend | Tenant fairness | Overload admission | Dependency caps | Feedback concurrency | Delivery |
| --- | --- | --- | --- | --- | --- | --- |
| TaskForge | Redis Streams | weighted per-tenant streams | defer-mode caps at publish/release | token budgets | bounded feedback loop | at-least-once, leased |
| Asynq [Asynq] | Redis | weighted/strict *queue* priority, not per-tenant keys | none documented | none | static | at-least-once |
| River [River] | Postgres | priority + per-queue workers | none documented | unique jobs, not capacity caps | static per queue | at-least-once, transactional enqueue |
| Machinery [Machinery] | Redis/AMQP | none | none | none | static | at-least-once |
| Celery [Celery] | AMQP/Redis | manual queue separation | static rate limits | none | worker autoscale (pool size), not latency-feedback admission | at-least-once |
| Temporal [Temporal] | service + DB | namespace/task-queue isolation | server rate limits, dynamic config | per-worker slot limits, resource-based tuner | worker slot tuners | workflow engine; effectively-once workflow semantics over at-least-once activities |
| WFQ [WFQ], DRF [DRF] | — | foundational fairness definitions | — | — | — | — |
| SEDA [SEDA], DAGOR [DAGOR], Breakwater [Breakwater], CoDel [CoDel] | — | — | foundational overload/admission control | — | SEDA stage controllers | — |

Positioning: TaskForge composes queue-level fairness, admission, and capacity
controls inside one small embeddable runtime, where prior open-source Go
queues expose at most static priorities, and platform systems (Temporal)
provide related controls at a much larger architectural footprint. We claim
composition and evaluation, not mechanism novelty. Feature rows describe
documented behavior of the cited versions at the time of writing and coarse
categories, not exhaustive audits.

## 4. Experimental design

The full pre-registered design, hypotheses H1-H4, metric definitions,
statistical procedure, and pilot-disclosed amendments are in
[`../analysis-plan.md`](../analysis-plan.md); this section summarizes.

**Workloads.** Six seeded manifests exercise one failure narrative each:
tenant skew (8:2:1 offered load), noisy neighbor (12:1 offered load, equal
entitlement), hot dependency (shared downstream, budget capacity 2), retry
storm (25% of tasks fail once), delayed backlog (50% future-dated), and
worker crash (one reservation abandoned without ack). Task counts are scaled
to 128-192 per run; service times are 1-3 ms so that a single host reaches
genuine overload; arrival is a concurrent four-publisher stream against a
running worker.

**Variants.** `taskforge-fifo-static` (all controls off), four single-control
ablations, `taskforge-full`, and `asynq` through an isolated adapter. All
TaskForge arms run the public embedded `worker` path with lease TTL 200 ms,
base concurrency 4, and adaptive bounds [2, 8]. Asynq runs the same manifests
at concurrency 4 but exposes none of the controls, so it is descriptive only.
Where a workload defines no admission cap or budget, the corresponding
ablation is pre-declared out of scope rather than reported as a null effect.

**Measurement and statistics.** Per run: p50/p95/p99 enqueue-to-start and
completion latency, throughput, Jain's index [Jain] over per-tenant
SLO-compliant completion ratios, SLO violations (total and for non-dominant
tenants), peak concurrent executions, retries, duplicates, crash recovery
time, and Redis CPU/memory/command deltas. The unit of analysis is the run;
12 seeds per cell; cell summaries are medians with seeded 10,000-resample
bootstrap 95% intervals; pre-registered contrasts are differences of medians
(full minus each other TaskForge arm) with independent-resample intervals.
Throughput contrasts also report relative-change intervals and apply the
pre-registered material-reduction rule (the interval lies below -10%). A
contrast is "detected" only when its absolute interval excludes zero. Every
pre-registered contrast is reported in
[`../results/analysis.md`](../results/analysis.md).

## 5. Results

All numbers below are medians across 12 seeds with bootstrap 95% intervals,
from [`../results/analysis.md`](../results/analysis.md); figures are in
[`../figures/`](../figures/) and regenerate from committed raw data via
`make research-analysis`.

### 5.1 Admission reduces tail latency, but not every overload outcome (H2: partially supported)

On all three admission-parameterized workloads, removing admission increases
p99 completion latency. Full minus `no-admission`:

- delayed backlog: p99 completion -86 ms [-105, -60], SLO violations -36.5
  [-42.5, -25], non-dominant violations -13.5 [-21, -7];
- noisy neighbor: p99 -68 ms [-88, -28], violations -37.5 [-46, -29.5], Jain
  +0.17 [+0.13, +0.21], with throughput -60 tasks/s [-87, -38];
- retry storm: p99 -70 ms [-102, -4] and in-flight logical-task peak -18.5
  [-19, -17], while the SLO-violation interval includes zero
  (-11.5 [-22.5, +0.5]).

Deferral spreads over-cap arrivals across time instead of letting the ready
backlog swamp the fairness and reservation machinery. It does not pay for
itself uniformly: the delayed-backlog throughput difference is inconclusive,
and it reduces throughput under the noisy neighbor. The experiment also did
not record a backlog time series, so H2's literal backlog-bound prediction is
not directly tested; only its latency, SLO, and logical in-flight consequences
are observed.

### 5.2 Fairness protects entitled tenants and lowers an equality index (H1: protection supported; the fairness-index prediction was wrong in an instructive way)

Under the 12:1 noisy neighbor with equal entitlements, the protected tenant
records a median 0 [0, 0] SLO violations per run in every fairness-enabled
arm, versus 8 [6, 10] without fairness and 7 [6, 11] under FIFO. The
non-dominant-tenant contrast is -7.5 [-10.5, -6.5], while the total-violation
interval includes zero (-7 [-21, +8]). However,
Jain's index over SLO-compliance ratios *falls* in the protecting arms
(-0.12 [-0.16, -0.09]): protecting the entitled tenant necessarily widens
the outcome gap to the over-quota tenant, and an equality-of-outcome index
reads that as unfairness. We pre-registered Jain as a primary fairness
metric; we report its direction faithfully and conclude that
entitlement-conditioned measures (per-tenant violations) are the right
protection metric, while Jain over outcomes measures equality, not justice.
On tenant skew, where entitlements mirror offered load, fairness is
not beneficial: no protected-tenant advantage is detected, while full fairness
adds 137 ms p99 [106, 174] and a material 31% throughput reduction against the
no-fairness arm. Entitlement-aware scheduling is useful only when the offered
load violates the intended entitlement.

### 5.3 Dependency budgets cap downstream pressure at a declared latency price (H3: supported)

On the hot-dependency workload the budgeted system holds peak concurrent
executions at the configured capacity: 2 versus 3 for `no-dependency-budget`
(-1.00 [-1.00, -1.00]) and versus 3-4 for FIFO/static. The pre-declared costs
materialize: +157 ms p99 [+125, +184], -128 tasks/s [-149, -96], and +15.5 SLO
violations [+10, +27] against the no-budget ablation, from the
budget-lease round trips and deliberately capped downstream access. A dependency budget is
the correct control when the downstream's capacity, not the queue's SLO, is
the binding constraint; it cannot be spun as free.

### 5.4 Adaptive concurrency has no demonstrated benefit (H4: not supported)

On the two pre-registered H4 workloads, adaptive concurrency is
indistinguishable from the static arm on p99 completion latency: delayed
backlog -13 ms [-29, +21] and retry storm +5 ms [-30, +48]. It hurts under the
noisy neighbor (+25 ms [+2, +63], -17 tasks/s [-38, -3]) and especially the
budget-capped hot dependency (+122 ms [+110, +140], -95 tasks/s [-106, -84]),
where scaling reservation pressure cannot add downstream capacity. The larger
adaptive envelope (up to 8 versus static 4) produces no general throughput
win. The controller needs dependency-budget awareness and direct trajectory
measurement before it can be recommended.

### 5.5 The controls' overhead is material at this scale (negative result)

Our research question required "no material throughput reduction",
pre-registered as a bootstrap interval entirely below -10% relative change.
That bar is failed on five of six workloads: full-stack throughput versus
FIFO/static falls 25% (delayed backlog) to 47% (hot dependency), and only the
worker-crash interval is too wide to determine. Detected p99 increases appear
on hot dependency, noisy neighbor, retry storm, tenant skew, and worker crash
(tenant skew: +133 ms [+106, +170]; worker crash: +72 ms [+52, +97]). The
mechanism is visible in the Redis command counts: the full stack multiplies
per-run commands by roughly 1.9x (tenant skew, 3,583 to 6,792) to 3.5x (noisy
neighbor, 3,733 to 13,135) because fairness snapshots,
admission signals, budget leases, and state bookkeeping are all extra round
trips, and at 1-3 ms service times those round trips are the same order as
the work itself. Whether the overhead amortizes at realistic service times
(tens of milliseconds to seconds) is untested here and is the most important
open question this study leaves.

### 5.6 Baseline comparison (descriptive only)

Asynq's p99 completion beats the best TaskForge arm by 1.5x (hot dependency)
to 2.7x (delayed backlog and retry storm) on the five common non-crash
workloads. Its lower per-task overhead also yields no more—and usually fewer—
total SLO violations than the TaskForge arms, although TaskForge full still enforces the protected
tenant and dependency-cap policies that Asynq cannot express. This is an
honest statement of TaskForge's current
constant-factor cost, not a controls comparison: Asynq exposes none of the
evaluated controls, the closed-loop publishers produced different arrival
timings against each enqueue path, and its worker-crash cell injects no crash
and is therefore reported as not measured.
Where the controls matter — the protected tenant under fairness, capped
downstream pressure under budgets — Asynq has no equivalent lever.

### 5.7 Failure recovery and delivery semantics

Full TaskForge and three single ablations reclaim the abandoned reservation at
201-202 ms median, close to the 200 ms lease TTL. FIFO/static has a 201 ms
median but an interval extending to 1.255 s, and `no-fairness` reaches a 679 ms
median [201 ms, 1.205 s]. An idle reserve can therefore delay the next expiry
check by up to the configured one-second reserve timeout; recovery is bounded
by lease expiry plus polling behavior, not the lease TTL alone. Retries and
duplicates match the injected schedule exactly (50 intentional retries per
retry-storm run; the single abandoned delivery re-executed once), and no lost
task appears in any of the 504 runs.

## 6. Threats to validity

- **Single host, single Redis, co-located load.** A 12-CPU workstation with
  frequency scaling and background activity; intervals capture run-to-run
  noise on this host only. No claim generalizes across hardware or network
  topologies.
- **Millisecond service times.** Chosen so one host reaches overload, they
  maximize the visibility of control-plane overhead. The 25-47% throughput
  cost is a property of this regime; it neither proves nor excuses the cost
  at production service times.
- **Closed-loop arrival.** Publishers enqueue as fast as the system under
  test accepts, so slower publish paths receive gentler arrival rates; the
  offered load is the same task set, not an identical arrival process. This
  favors slower systems and still produced material overhead findings.
- **No degrading downstream model.** Handler service time is fixed even when
  concurrency exceeds the declared dependency capacity. The budget experiment
  proves a concurrency cap, not prevention of latency collapse or retry
  amplification in a real overloaded dependency.
- **Fixed execution order.** Variants run in a fixed order inside each
  workload/seed block, so warming, thermal, or background-load effects can
  correlate with an arm despite flushing the Redis database.
- **Small per-run samples.** With 128-192 tasks per run, within-run p99 is a
  high-variance order statistic; we mitigate with 12 seeds and run-level
  inference, not by pooling correlated samples.
- **Metric semantics.** Peak concurrency measures execution overlap only in
  retry-free workloads; Jain over outcomes penalizes entitlement protection
  (Section 5.2); duplicates include intentional retries; the recorded value
  0 for Jain when no tenant meets its SLO is a degenerate-case convention.
- **Baseline fairness.** Asynq was run through a minimal adapter with tuned
  check intervals but without expert-level tuning, no crash injection, and
  none of the controls; it is reported descriptively and excluded from every
  contrast.
- **Post-registration correction.** An initial complete grid was discarded
  after review found irrelevant idle scheduler polling, a stale crash-trigger
  field name, omitted relative-throughput intervals, dirty-tree provenance,
  and privacy-unsafe logs. The source was committed, all 504 cells were rerun,
  and no old cell was retained; the full disclosure is in the analysis plan.

## 7. Conclusion

Composing weighted fairness, deferred admission, dependency budgets, and
feedback concurrency in an embeddable Redis-backed runtime delivers targeted
tenant and dependency protections, with admission producing the most
consistent tail-latency benefit. It also imposes a material constant-factor
cost at millisecond task scales; adaptive concurrency has no demonstrated
benefit; and reclaim latency can include reserve polling beyond the lease TTL.
Future work must test realistic service and downstream-collapse regimes,
reduce reservation overhead, add budget awareness to the adaptive controller, use
entitlement-conditioned fairness metrics, and replay fixed-rate open-loop
arrival traces.

## Availability

Raw data (504 runs, gzipped JSON with full environment metadata), the
pre-registered plan, analysis code, and figure generation are in the
TaskForge repository under `research/`; `make research-experiments`
re-executes the grid and `make research-analysis` regenerates every number
and figure in this paper from the committed raw data. See the artifact guide
([`../README.md`](../README.md)) and citation metadata (`CITATION.cff`,
`.zenodo.json`) at the repository root.

## References

- [WFQ] A. Demers, S. Keshav, S. Shenker. "Analysis and Simulation of a Fair
  Queueing Algorithm." SIGCOMM 1989.
- [DRF] A. Ghodsi, M. Zaharia, B. Hindman, A. Konwinski, S. Shenker,
  I. Stoica. "Dominant Resource Fairness: Fair Allocation of Multiple
  Resource Types." NSDI 2011.
- [Jain] R. Jain, D.-M. Chiu, W. Hawe. "A Quantitative Measure of Fairness
  and Discrimination for Resource Allocation in Shared Computer Systems."
  DEC Technical Report TR-301, 1984.
- [SEDA] M. Welsh, D. Culler, E. Brewer. "SEDA: An Architecture for
  Well-Conditioned, Scalable Internet Services." SOSP 2001.
- [DAGOR] H. Zhou et al. "Overload Control for Scaling WeChat
  Microservices." SoCC 2018.
- [Breakwater] I. Cho, A. Saeed, J. Fried, S. J. Park, M. Alizadeh,
  A. Belay. "Overload Control for µs-scale RPCs with Breakwater." OSDI 2020.
- [CoDel] K. Nichols, V. Jacobson. "Controlling Queue Delay." ACM Queue,
  2012.
- [TailAtScale] J. Dean, L. A. Barroso. "The Tail at Scale." CACM 56(2),
  2013.
- [NetflixLimits] Netflix. "concurrency-limits" (adaptive concurrency-limit
  library). https://github.com/Netflix/concurrency-limits, 2018.
- [Asynq] Asynq: simple, reliable Go task queue backed by Redis.
  https://github.com/hibiken/asynq.
- [River] River: fast, robust job queue for Go + Postgres.
  https://github.com/riverqueue/river.
- [Machinery] Machinery: asynchronous task queue for Go.
  https://github.com/RichardKnop/machinery.
- [Celery] Celery: distributed task queue for Python.
  https://docs.celeryq.dev.
- [Temporal] Temporal: durable execution platform.
  https://temporal.io / https://github.com/temporalio/temporal.
- [Redis] Redis Streams. https://redis.io/docs/latest/develop/data-types/streams/.
