# Pre-Registered Analysis Plan

Status: frozen before the registered experiment grid was executed. The
registered raw data in [`data/raw/`](data/raw/) was produced after this file
was written. Deviations, if any, are listed in the paper's threats-to-validity section;
they are never silently folded into this plan.

## Research question

Can dependency-aware admission control, tenant fairness, and
feedback-controlled concurrency reduce tail latency, starvation, and
downstream overload without materially reducing throughput or weakening
explicit at-least-once failure semantics?

"Materially reducing throughput" is fixed in advance as: the 95% bootstrap
confidence interval for the relative change in median per-run throughput
(control-enabled versus the contrast arm) lies entirely below -10%.

## Hypotheses

- **H1 (fairness).** On `tenant-skew` and `noisy-neighbor`, variants with
  tenant fairness enabled (`taskforge-full` and all ablations except
  `taskforge-no-fairness`) achieve higher Jain fairness over per-tenant
  SLO-compliant completion ratios and fewer SLO violations for non-dominant
  tenants than `taskforge-no-fairness` and `taskforge-fifo-static`, without
  materially reducing throughput.
- **H2 (admission).** On the admission-parameterized workloads
  (`noisy-neighbor`, `delayed-backlog`, `retry-storm`), deferred admission
  bounds the pending backlog and reduces SLO violations for tasks outside the
  overloading source, at the pre-declared cost of added completion latency for
  deferred tasks. A neutral or negative result is plausible in this closed
  workload (total work is conserved) and will be reported as such.
- **H3 (dependency budgets).** On `hot-dependency`, a dependency budget of
  capacity 2 keeps the peak number of concurrently executing budgeted tasks at
  or below capacity (measured from raw sample intervals), where non-budgeted
  variants exceed it, at the pre-declared cost of longer completion latency.
- **H4 (adaptive concurrency).** On `retry-storm` and `delayed-backlog`,
  feedback-controlled concurrency (bounds [2, 8] around the static value 4)
  exploits backlog headroom without increasing tail completion latency
  relative to `taskforge-no-adaptive`. The adaptive arm has a larger
  admissible concurrency envelope than the static arm; this confound is
  declared here and repeated in the threats-to-validity section rather than
  hidden.

At-least-once semantics are not re-proven by this experiment; they are covered
by the deterministic simulator, protocol models, and integration tests cited
in the paper. The experiment only reports observed retries and duplicates per
run so that a semantics-weakening regression would be visible.

## Pilot runs and pre-execution amendments

Single-seed pilot runs (seed 20260717, not part of the registered data) were
used to validate harness mechanics before this plan was frozen. They exposed
three defects that were fixed, and are disclosed here, before the registered
grid was executed:

1. The T12 harness never wired admission, adaptive-concurrency, or
   dependency-budget controls, so four of the seven variants differed only by
   label. The runner now executes the public embedded worker path with each
   control genuinely engaged.
2. The manifest tenant weight served as both offered-load skew and fairness
   entitlement, so the noisy neighbor was entitled to dominate. Manifests now
   separate `fairness_weight` from offered weight; only `noisy-neighbor` uses
   the distinction.
3. Workers previously started only after the entire burst was published, so
   at registered scale every latency included a service-does-not-exist-yet
   offset that no control could influence. Arrival is now a concurrent
   multi-publisher stream against a running worker for both TaskForge and the
   baseline.

No registered results existed when these amendments were made; the registered
grid was executed once, after this section was written.

## Experimental units and grid

- Six workload manifests (`test/experiment/workloads/`): `tenant-skew`,
  `noisy-neighbor`, `hot-dependency`, `retry-storm`, `delayed-backlog`,
  `worker-crash`, each scaled by a factor of 8 (task counts 128-192).
- Seven variants: `taskforge-fifo-static`, four single-control ablations,
  `taskforge-full`, and the non-comparable `asynq` baseline.
- Twelve seeds: 20260717 through 20260728, chosen to match the T14
  optimization-report precedent and fixed before execution.
- The unit of analysis is one run (one seed of one workload/variant cell).
  Samples within a run are correlated and are never pooled across runs for
  inference; per-run summary statistics are the observations.

## Control engagement map

Controls are engaged through the public embedded worker path, not a bespoke
loop. Where a workload gives a control nothing to act on, the corresponding
contrast is pre-declared out of scope rather than reported as a null effect:

| Control | Engaged on | Mechanism |
| --- | --- | --- |
| Tenant fairness | all workloads | weighted fairness policy from manifest tenants |
| Admission control | `noisy-neighbor` (per-key cap), `delayed-backlog`, `retry-storm` (queue caps) | defer mode, 10ms defer interval |
| Dependency budget | `hot-dependency` (capacity 2) | budget lease per executing task |
| Adaptive concurrency | all workloads | bounds [2, 8], static baseline 4 |

Two declared interactions: the per-fairness-key admission cap requires
fairness metadata, so it is inert in the `taskforge-no-fairness` arm; and
`taskforge-fifo-static` disables all four controls, making it the
all-controls-off anchor rather than a single ablation.

## Metrics

Primary, computed per run by `internal/experiment`:

- p50/p95/p99 enqueue-to-start and enqueue-to-completion latency;
- throughput (first observed enqueue to last observed completion);
- Jain fairness index over per-tenant SLO-compliant completion ratios;
- SLO violation count (starvation proxy);
- retries and duplicate executions;
- worker-crash recovery time (`worker-crash` only);
- Redis CPU seconds, end-of-run memory, and command-count delta.

Secondary, computed by the analysis tool from raw samples:

- peak concurrent executions (overlap of [start, completion] intervals),
  the downstream-overload proxy for H3;
- per-tenant SLO violation counts, the non-dominant-tenant measure for H1/H2.

## Statistical analysis

- Point estimate per cell and metric: median across the 12 per-run values.
- Uncertainty: 95% percentile bootstrap interval of the median, 10,000
  resamples, seeded deterministic generator (seed 20260717) so every
  reported interval is exactly reproducible.
- Contrasts (per workload, per metric): `taskforge-full` minus
  `taskforge-fifo-static`; `taskforge-full` minus each single ablation.
  Estimate: difference of medians with a 95% bootstrap interval from
  independently resampled arms. A contrast is reported as "detected" only if
  its interval excludes zero; otherwise it is reported as "not distinguished
  from zero", never dropped.
- All pre-declared contrasts are reported, including unfavorable and neutral
  ones. No additional post-hoc contrasts are promoted to claims.
- `asynq` is reported descriptively on common delivery metrics only and is
  excluded from control contrasts, because it exposes no equivalent controls.
- No null-hypothesis significance tests or p-values are reported; intervals
  carry the uncertainty statement.

## Exclusion and failure rules

- No run is excluded post hoc. A run that fails or times out is recorded in
  the run log with its seed and error, the cell is analyzed with reduced n,
  and the failure is reported.
- Reruns are permitted only for infrastructure failure external to the system
  under test (Redis unavailable, host out of memory) and every rerun is
  logged. Results are never selected by outcome.
- The registered grid is executed once. If a code defect is found after
  execution, the fix and a full re-execution replace the entire grid; partial
  replacement of cells is prohibited.

## Environment

One shared workstation (12-CPU Intel i7-1255U, Linux, Go 1.26.5, Redis 7.4.9
standalone, AOF `everysec`, `maxmemory-policy noeviction`), Redis and workers
co-located, CPU frequency scaling enabled, not isolated from normal
workstation activity. Every raw result embeds build SHA, OS, architecture,
CPU count, Go version, and the Redis configuration string; the hostname is
replaced by a neutral label before results are committed. These conditions
bound the claims: results characterize control behavior under contention on
one host, not absolute performance or cross-environment superiority.
