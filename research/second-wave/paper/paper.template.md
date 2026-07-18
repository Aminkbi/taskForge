# Overload controls under paired load: a registered two-class TaskForge study

## Abstract

We evaluate Redis-backed background-task execution using immutable open-loop
traces and paired system contrasts. The design separates a common successful
delivery comparison with tuned Asynq from TaskForge-only capability ablations.
It measures throughput together with protected-tenant SLO attainment,
entitlement-normalized service deficit, downstream overload, normalized Redis
cost, and explicitly unsupported recovery cells. The scope is one physical
workstation under two measured execution/network classes, not a multi-host or
remote-cloud claim.

## Method

The frozen plan, workload profiles, trace corpus, and digest lock precede the
registered result corpus. Each block fixes environment, profile, seed, and
repetition. Every arm in a block receives identical arrivals and failure draws,
and system order is deterministically counterbalanced. Primary intervals use
the registered Bonferroni coverage; exploratory intervals use 95% coverage.
Asynq is tuned at concurrency 16 with 10ms task and delayed-task polling. River
is excluded because changing from Redis to PostgreSQL would confound persistence
and delivery contract with queue implementation.

{{GENERATED_RESULTS}}

## Threats to validity

Both classes share one physical host. The networked class is a declared
latency-injected loopback proxy, not remote infrastructure. Redis telemetry
cost excludes worker-process CPU. Two seeds limit interval resolution, and
profile labels describe intended below-knee-overload regimes even if an
environment shifts the empirical knee. Process-kill-equivalent recovery is
not implemented by both adapters, so those registered cells are retained as
not measured rather than treated as zero or omitted.
