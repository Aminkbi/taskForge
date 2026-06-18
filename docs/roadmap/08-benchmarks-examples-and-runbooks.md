# Phase 08: Benchmarks, Examples, and Runbooks

## Commit goal

Finish the production hardening track by documenting performance, failure modes, and expected operations through real examples and repeatable benchmark scenarios.

## Why this phase exists

A queue is not production-ready because it compiles or because its semantics are well designed. It is production-ready when operators and future maintainers can predict its behavior under stress and failure.

## Changes

### Add benchmark coverage

Create a reproducible benchmark suite that measures:

- publish throughput
- reserve and ack throughput
- end-to-end latency
- reclaim latency after worker death
- scheduler release lag
- throughput degradation under retry storms

Benchmarks should document hardware assumptions and broker settings so results are interpretable.

### Add failure-mode documentation

Write a failure matrix covering:

- worker crash mid-task
- worker crash after side effect but before ack
- Redis restart during active deliveries
- network partition between worker and Redis
- scheduler leader loss
- dead-letter replay failure

For each scenario, document:

- what the system guarantees
- what duplicates may occur
- what operators should look at first

### Add real examples

Add runnable or near-runnable examples for:

- idempotent email dispatch
- long-running media processing with lease renewal
- retryable external API task with DLQ and replay path

Each example should explain:

- idempotency strategy
- retry policy
- timeout policy
- expected duplicate-delivery behavior

### Add operator runbooks

Document operational responses for:

- stuck pending entries
- reclaim storms
- growing DLQ
- high scheduler lag
- queue starvation

## Tests and verification

- Benchmark harness can be run locally and in a larger environment
- Example code stays in sync with current interfaces
- Docs are consistent with actual broker and runtime semantics

## Acceptance criteria

- The repository includes real evidence of queue behavior under load and failure
- Operators have concrete runbooks instead of relying on intuition
- The production story is documented honestly, including caveats and duplicate-delivery expectations
