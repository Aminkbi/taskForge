# Phase 11: Admission Control and Overload Backpressure

## Commit goal

Make overload behavior explicit by adding bounded queueing, pressure signals, and admission decisions for new and retried work.

## Why this phase exists

Without admission control, overload shows up as deeper queues, longer delays, reclaim noise, and retry amplification. That is not a policy. It is just saturation becoming visible too late.

Modern distributed systems increasingly rely on explicit overload control to preserve useful work, bound tail latency, and avoid failure cascades. TaskForge should make those choices operator-visible and configurable instead of treating backlog growth as the default response to every spike.

## Changes

### Define overload signals

Base overload decisions on broker-visible signals such as:

- queue depth
- oldest pending age
- reserve latency
- retry pressure
- dead-letter growth

Avoid depending on process-local heuristics alone.

### Add admission decisions

Support a policy that can:

- accept work immediately
- defer work for later release
- reject work with an explicit overload reason

This should apply to both new publishes and retry scheduling so failed tasks do not automatically amplify overload.

### Bound pending work

Document and support bounded pending behavior:

- per-queue pending caps
- optional per-fairness-key caps
- queue-delay thresholds that trigger degraded admission

The system should prefer controlled shedding or deferral over unbounded backlog growth when configured to do so.

### Surface backpressure operationally

Expose enough information for operators and callers to understand:

- when admission is degrading
- which policy is active
- which work was deferred or rejected
- whether retries are being throttled

Readiness should remain about process health, but overload state should be visible through metrics, logs, and API/admin responses where appropriate.

## Tests

- Integration test: sustained overload triggers configured defer or reject behavior
- Integration test: retry scheduling respects admission policy instead of causing runaway amplification
- Integration test: bounded pending caps prevent unbounded queue growth in the protected mode
- Metric and API test: overload transitions are observable with stable labels and reasons

## Acceptance criteria

- Overload behavior is a documented policy rather than an accidental byproduct of backlog growth
- Operators can choose between defer and reject semantics for saturated queues
- Retry storms can be contained by the same admission model as new work
