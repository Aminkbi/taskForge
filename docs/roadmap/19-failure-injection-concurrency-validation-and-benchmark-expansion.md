# Phase 19: Failure Injection, Concurrency Validation, and Benchmark Expansion

## Commit goal

Expand validation so TaskForge is tested against realistic failure modes, concurrency hazards, and capacity scenarios rather than only happy-path correctness.

## Why this phase exists

Queue systems rarely fail in obvious single-threaded ways. They fail under timing races, lease-renewal loss, overload, partial shutdown, and storage instability.

The current repository already has meaningful tests, but a stronger production posture requires explicit validation of adverse conditions and representative capacity shapes. Benchmarks and race coverage should answer operational questions, not only measure toy cases.

TaskForge should prove its behavior under stress instead of relying only on design intent.

## Changes

### Add targeted failure injection

Introduce integration scenarios for failures such as:

- Redis timeout or transient unavailability
- lease-renewal failure during execution
- scheduler leadership loss mid-loop
- delayed-index corruption or malformed deferred entries
- publish failure during retry or dead-letter transition

The point is to validate deterministic behavior under adverse but expected conditions.

### Expand concurrency validation

Add explicit coverage for:

- race-sensitive runtime paths
- worker drain plus in-flight execution
- duplicate delivery windows
- cancellation-insensitive handlers
- concurrent admin and control-plane observation during state change

This should complement `go test -race`, not replace it.

### Make benchmarks operationally meaningful

Add benchmark scenarios that model:

- multiple queues
- many fairness keys
- large delayed and retry backlogs
- skewed tenant traffic
- scheduler catch-up after downtime

The benchmark suite should help size the system and detect regressions in the hot paths that matter.

### Define success signals for validation

Document what benchmark and failure-injection outputs should demonstrate, such as:

- bounded publish latency under backlog
- stable reserve latency under contention
- safe duplicate behavior after lease loss
- predictable control-plane recovery after leader turnover

This keeps validation tied to product claims.

## Tests

- Integration test: lease-renew failure produces deterministic abandonment and redelivery behavior
- Integration test: scheduler leadership loss prevents unsafe continued control-plane mutation
- Race CI test: concurrency-sensitive packages run under `go test -race`
- Benchmark suite: multi-queue and backlog-heavy scenarios produce tracked baseline results

## Acceptance criteria

- TaskForge validates meaningful failure modes instead of only normal operation
- Concurrency-sensitive packages receive explicit race and timing coverage
- Benchmarks answer capacity and regression questions relevant to production use
