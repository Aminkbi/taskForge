# Phase 12: Adaptive Concurrency and Dependency Budgets

## Commit goal

Move beyond static worker concurrency by letting TaskForge react to downstream limits and shared dependency pressure.

## Why this phase exists

Static concurrency is easy to configure, but it is often the wrong control surface once a queue is healthy and the real bottleneck is a downstream API, database, or storage system.

Many task systems expose fixed worker counts and leave operators to guess a safe value. That works until latency, rate limits, or error bursts change faster than config can. TaskForge should make downstream capacity a first-class operational concept.

## Changes

### Add dependency budgets

Introduce named budgets or tokens for shared dependencies such as:

- external APIs
- databases
- storage backends

Tasks that consume the same downstream should compete for a shared budget even if they run on different queues or worker pools.

### Add adaptive concurrency policy

Allow worker pools to adjust concurrency within configured bounds based on signals such as:

- downstream latency
- error rate
- budget exhaustion
- reserve backlog

The policy should be explicit, bounded, and observable rather than a hidden feedback loop.

### Define interaction with fairness and overload control

Document how adaptive concurrency composes with earlier phases:

- fairness still decides who gets capacity
- admission control still decides whether work enters the system
- dependency budgets decide how much safe execution capacity is actually available

### Guard against oscillation

Require damping and safety rules so the runtime does not thrash:

- bounded step sizes
- cooldown windows
- conservative recovery after sustained errors
- clear fallback to static concurrency when adaptive control is disabled

## Tests

- Integration test: worker concurrency reduces when a downstream dependency becomes slow or starts failing
- Integration test: shared dependency budgets prevent one task family from exhausting external capacity
- Integration test: concurrency recovers gradually after downstream health improves
- Benchmark or simulation: adaptive control avoids obvious oscillation under changing latency

## Acceptance criteria

- TaskForge can express downstream-aware execution limits instead of only queue-local concurrency
- Adaptive concurrency changes are bounded, observable, and reversible
- Shared dependencies can be protected without manually fragmenting queues or worker pools
