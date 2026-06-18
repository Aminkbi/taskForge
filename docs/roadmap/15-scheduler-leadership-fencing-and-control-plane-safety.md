# Phase 15: Scheduler Leadership Fencing and Control-Plane Safety

## Commit goal

Strengthen scheduler leadership so delayed and recurring control-plane writes remain safe under lease ambiguity, failover, and stale-leader execution.

## Why this phase exists

The current scheduler uses Redis-backed leadership with a renewable lock. That is a good first step, but a lock alone does not fully answer the control-plane safety problem.

In distributed systems, stale leaders are dangerous when they can keep making writes after the cluster has logically moved on. A production scheduler should not rely only on best-effort lock ownership when delayed release and recurring dispatch mutate durable state.

TaskForge should add fencing-style semantics so the scheduler can prove whether a control-plane writer is still authoritative.

## Changes

### Add fenced leadership epochs

Extend scheduler leadership with a monotonically advancing ownership token or epoch that changes on every successful leadership acquisition.

Control-plane operations that mutate scheduler state should carry that epoch so stale writers can be rejected deterministically after leadership changes.

### Define protected scheduler writes

Document which state transitions require current leadership authority, including:

- delayed task release
- recurring schedule dispatch
- recurring schedule state advancement
- cleanup or reconciliation operations that remove scheduler-owned state

This phase is about protecting control-plane mutation, not changing worker-side delivery semantics.

### Clarify failover and lease-loss behavior

Define expected behavior for:

- scheduler lock renewal failure
- Redis restart or failover
- process pause longer than leadership TTL
- concurrent leaders during ambiguous network conditions

The contract should prefer duplicate prevention and deterministic rejection over optimistic local assumptions.

### Surface leadership safety operationally

Expose enough control-plane visibility for operators to reason about:

- current leader identity
- active leadership epoch
- last successful renewal
- stale-writer rejection events
- control-plane safety failures

Leadership should be inspectable, not a hidden internal detail.

## Tests

- Integration test: old leader cannot mutate recurring or delayed state after a new leader acquires authority
- Integration test: leadership renewal failure causes safe demotion instead of continued blind writes
- Integration test: recurring state advancement rejects stale leadership epochs
- Admin and metric test: leadership identity, epoch, and loss events are observable

## Acceptance criteria

- Scheduler leadership is strong enough to protect control-plane mutations under failover ambiguity
- Stale leaders cannot silently continue mutating delayed or recurring state
- Operators can reason about scheduler authority and failover from logs, metrics, and admin state
