# Phase 06: Queue Isolation and Scaling Model

## Commit goal

Add queue-level isolation and a clear scaling model so the system can be operated under uneven workloads without accidental noisy-neighbor failures.

## Why this phase exists

The current worker runtime has only one queue and one concurrency value. That is not enough for production workloads with different priorities, SLAs, or tenant behavior.

## Changes

### Add per-queue runtime config

Extend config and runtime wiring to support:

- multiple owned queues per worker process
- per-queue concurrency
- per-queue lease TTL
- per-queue reserve batch or prefetch settings
- per-queue retry defaults

### Add task-type concurrency controls

Introduce optional per-task-type limits so one handler cannot saturate all worker capacity.

This should support:

- global cap per task type
- queue-local cap per task type
- optional future tenant-aware limits

### Define scaling modes

Document and support:

- horizontal worker scaling on a shared queue
- isolated worker pools for critical queues
- scheduler singleton or leader-elected scheduler scaling
- Redis resource considerations for many queues and many pending deliveries

### Add operator metrics for queue isolation

Expose enough metrics to see whether one queue or task type is starving others:

- queue depth
- per-queue reserved count
- per-queue success and failure rates
- per-queue reclaim count

## Tests

- Integration test: multiple queues with isolated worker assignment
- Integration test: per-task-type cap prevents one task family from saturating workers
- Benchmark: noisy queue does not fully starve an isolated critical queue under the configured model

## Acceptance criteria

- The worker is no longer implicitly single-queue and single-policy
- Scaling guidance is written and reflected in config and runtime wiring
- Queue isolation is measurable, not just described
