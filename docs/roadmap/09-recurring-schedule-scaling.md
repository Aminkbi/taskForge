# Phase 09: Recurring Schedule Scaling

## Commit goal

Remove the recurring scheduler's full-scan behavior so dispatch cost scales with due schedules, not total configured schedules.

## Why this phase exists

The first recurring implementation is intentionally narrow and simple: the scheduler can scan every configured schedule on each tick and decide what is due. That works for small schedule counts, but it becomes wasteful as the number of schedules grows.

With enough schedules, the current approach makes scheduler work grow roughly linearly with the total number of schedules, even when only a small fraction are actually due.

## Changes

### Replace full scans with a due-time index

Move recurring lookup to a Redis sorted set keyed by `next_run_at`.

The scheduler leader should:

- query only due schedules with `ZRANGEBYSCORE`
- load the state for the due schedule IDs
- dispatch the due runs
- compute each schedule's next run time
- update the sorted-set score after dispatch

### Keep durable recurring state

Retain a durable state record per schedule so the scheduler can still reason about:

- schedule ID
- next run time
- last dispatched time
- definition hash
- misfire policy

The sorted set is the dispatch index, not the only source of truth.

### Preserve singleton semantics

Keep the existing leader-owned scheduler model. This phase is about reducing lookup cost, not changing duplicate or failover semantics.

### Document expected scaling behavior

Document the new operational expectation clearly:

- recurring dispatch cost is proportional to schedules due in the current window
- large numbers of inactive or infrequent schedules should not materially slow each tick
- Redis memory and sorted-set update cost become the main scaling consideration

## Tests

- Unit test: only schedules whose `next_run_at` is due are selected
- Integration test: many future schedules do not block dispatch of a small due set
- Integration test: rescheduling updates the due-time index correctly after dispatch
- Benchmark: recurring scheduler tick cost under 10, 1,000, and 100,000 schedules

## Acceptance criteria

- The recurring scheduler no longer performs a full schedule scan on every tick
- Due-schedule lookup is driven by a durable Redis time index
- Scaling behavior is explicit and measurable
