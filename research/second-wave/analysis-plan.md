# Registered Paired Multi-Environment Study

Status: frozen before any registered replay result was created. The JSON plan,
analysis code, workload profiles, generated trace bytes, and trace digest lock
are immutable inputs to the registered run. Pilot output is never accepted by
the release validator.

## Pre-result full-grid replacement

The first replay pass was discarded in full before any result was published.
Review of its generated report found that achieved throughput had been computed
as eventual completions divided by the fixed steady window, which collapses to
offered rate when drain eventually succeeds, and that service deficit used
eventual completions rather than SLO-compliant service. It also exposed
asymmetric TaskForge error-log I/O during modeled dependency failures. Before
the replacement lock, achieved throughput was defined as steady-arrival
completions divided by time from steady start through the later of steady end
or last completion; service share was changed to SLO-compliant service; and
adapter logging was discarded because failures are already raw observations.
No cell from the discarded pass is retained in the registered dataset.

## Scope and contrast families

The common-delivery family compares TaskForge FIFO/static with Asynq only for
successful Redis-backed enqueue, at-least-once processing, and completion
outcomes shared by both adapters. It does not compare TaskForge task-state,
DLQ, fairness, admission, adaptive-concurrency, dependency-budget, or crash
contracts. Asynq uses concurrency 16, 10ms task and delayed-task polling, a
fixed retry delay from the trace, task IDs, queue `default`, and one-second
retention. These settings are recorded in every raw result.

The capability family compares TaskForge full controls with one-control
ablations. Those contrasts answer whether a TaskForge feature changes overload
behavior; Asynq is not treated as a missing-feature arm. River is not included:
its PostgreSQL persistence path cannot be isolated from the persistence and
delivery-contract change in this Redis experiment.

## Blocking and experimental units

One run is one system replay of one immutable seed trace in one declared
environment. The analysis block is environment, profile/load level, seed, and
repetition. Systems in a block receive identical arrival timestamps, tenant,
service-time, payload-size, delay, and failure draws. System order is
deterministically randomized from the seed. Only within-block differences are
analyzed; task samples inside a run are not independent observations.

The measured environments are (1) the native 12-logical-CPU host with
co-resident Redis over direct loopback TCP, and (2) the same physical host constrained
to four Go processors with Redis reached through a co-resident TCP proxy adding
a declared 1ms round trip. The second is an emulated network/resource class,
not independent hardware or evidence about a remote cloud. Conclusions must
name these two measured classes and may not generalize to unmeasured hardware.

## Grid

The common-contract sweep uses one tenant, 1ms service, payloads from 256B to
4KiB, downstream capacity 64, and fixed rates 500, 1,500, and 3,000 tasks/s.
The capability sweep uses 16 tenants with unequal offered load and entitlement,
a 1ms/10ms/100ms/1s service mix, 256B/4KiB/64KiB payload mix, dependency
capacity 8, and rates 20, 60, and 140 tasks/s. The repeated 60-second overload
profile uses four tenants, capacity 6, and payloads through 256KiB. Each profile
has seeds 20260718 and 20260719. Exact durations and parameters live in the
immutable profiles and traces; labels below/knee/overload are design labels,
and observed backlog/attainment is reported even when an empirical knee shifts
by environment.

## Outcomes and multiplicity

Primary common-contract outcomes are completed throughput and normalized
Redis CPU/network/memory cost per SLO-compliant completion. Primary capability
outcomes are protected-tenant SLO attainment, maximum
entitlement-normalized service deficit, downstream over-capacity/failure
rates, throughput, and normalized cost. Jain equality is secondary because it
can look favorable while every tenant misses its SLO. Harness dispatch p99,
backlog, controller trajectories, and long-duration contrasts are exploratory.

Every estimate is the median paired difference, accompanied by a paired
bootstrap interval, standardized paired effect, relative percentage when the
denominator is nonzero, and fraction of blocks in which the left arm is higher.
Primary intervals use Bonferroni family-wise 95% coverage within each declared
endpoint set (the exact per-contrast confidence is in `study-plan.json`).
Exploratory intervals are unadjusted 95%. There are no detection counts and no
post-hoc promotion of an exploratory metric. A throughput loss is practically
material at more than 10%; protected-attainment and downstream-safety changes
are material at five percentage points; normalized-deficit changes are
material at 0.05.

## Failure, recovery, and reporting rules

All scheduled cells remain in the dataset ledger. Infrastructure or system
failures are `failed`; semantically unsupported cells are `not_measured`; both
retain reasons and are printed in machine-readable and narrative results. A
failed cell is never silently rerun or replaced. Registered crash recovery is
not claimed: neither in-process adapter implements a process-kill-equivalent
fault, so recovery remains explicitly not measured until an external process
harness can apply the same ownership/lease fault to both systems.

Environment-specific sign reversals are generated automatically. Pooled
language is prohibited when strata reverse or when a cell is missing. The
release archive contains the plan, code manifest, traces, raw data, dataset
ledger, results, figures, and paper, with SHA-256 manifests and a command that
byte-compares regenerated derived outputs.
