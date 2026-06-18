# Phase 24: Tail-Latency Scheduling, Queue Reordering, and Work Stealing

## Commit goal

Reduce queueing delay and tail latency under high utilization by adding configurable queue disciplines, starvation-safe reordering, and bounded work stealing.

## Why this phase exists

First-come-first-served delivery is understandable but vulnerable to head-of-line blocking. Research such as Murmuration shows that queue reordering and scheduler/node cooperation can substantially improve job completion time under high utilization.

TaskForge already has queue isolation, fairness policies, adaptive concurrency, task limits, and benchmarks. This phase adds smarter scheduling while preserving predictable operator controls.

## Changes

### Add queue discipline policies

Support queue-local policies such as:

- FIFO
- priority with aging
- shortest expected runtime first
- shortest remaining retries first
- deadline or slack-aware ordering
- fairness-bucket weighted ordering

Policies should be configured per queue and surfaced in admin state.

### Capture runtime estimates

Track per task name and fairness bucket:

- execution duration histograms
- retry probability
- deadline miss rate
- queue wait time
- observed payload-size or metadata classes when safe

Use these estimates for scheduling hints, not hard correctness assumptions.

### Implement starvation safeguards

Every non-FIFO discipline must include:

- maximum wait age before priority boost
- per-fairness-key minimum service guarantees where configured
- operator metrics for skipped, boosted, and reordered tasks
- a safe fallback to FIFO

### Add bounded work stealing

Allow idle workers or pools to steal from compatible queues when policy permits. Stealing should respect:

- task resource requirements
- tenant and fairness boundaries
- dependency budgets
- placement labels
- queue priority and isolation settings

Stealing should be opt-in and visible, because accidental cross-queue execution can violate isolation assumptions.

### Add hedged execution only for safe tasks

For tasks explicitly marked idempotent and hedge-safe, allow a second delivery attempt when the first attempt is a severe tail outlier. Hedging must use delivery ownership and effect records so only one result is accepted and unsafe side effects are blocked.

## Tests

- Unit test: each queue discipline produces expected ordering with aging and fairness safeguards
- Simulation test: skewed long and short tasks show reduced wait time without starvation
- Integration test: work stealing respects queue isolation and task resource constraints
- Integration test: hedged execution accepts one winner and rejects stale completion
- Benchmark: tail-latency scenarios report p50, p95, p99, and starvation counters

## Acceptance criteria

- Queue ordering becomes a declared policy surface
- Tail-latency improvements are measurable in benchmarks and simulations
- Starvation prevention is built into every reordering mode
- Work stealing and hedging are opt-in, observable, and constrained by safety metadata
