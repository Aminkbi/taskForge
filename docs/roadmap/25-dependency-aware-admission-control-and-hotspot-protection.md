# Phase 25: Dependency-Aware Admission Control and Hotspot Protection

## Commit goal

Evolve admission control from queue-backlog thresholds into dependency-aware overload protection that can defend downstream hot spots.

## Why this phase exists

The current admission policy already watches queue pending counts, fairness-key pending counts, oldest ready age, retry backlog, and DLQ size. That is a strong baseline, but it still treats overload mostly as a queue-local symptom.

DRACO's core lesson is useful here: in large systems, overload is often tied to a hot downstream resource, not total system capacity. TaskForge should protect database shards, object-store prefixes, APIs, tenants, and regional dependencies as first-class admission dimensions.

## Changes

### Extend dependency budgets into dependency health

Track dependency state such as:

- configured capacity
- current leased tokens
- observed latency and error rate
- circuit state
- hot key or hot shard indicators
- freshness of the health signal

Health should be visible to admission, adaptive concurrency, and operator endpoints.

### Route admission by dependency mapping

Allow tasks to declare or derive dependency keys:

- database cluster and shard
- external API and tenant account
- object-store bucket or prefix
- payment provider region
- model-serving endpoint

Admission should be able to defer or reject only the work mapped to unhealthy dependencies while allowing unrelated work to continue.

### Add load shedding policy tiers

Support policy decisions such as:

- accept
- defer with jitter
- reject new work
- reject retries before new work, or the reverse
- route to degraded queue
- require manual replay for dangerous retry storms

Policy should be explainable in `PublishResult.Reason` and in metrics.

### Close the loop with adaptive concurrency

Adaptive worker concurrency should consider dependency health, not only queue backlog, latency, and errors. A queue with healthy local workers but an unhealthy downstream dependency should scale down or hold steady instead of amplifying the hot spot.

## Tests

- Unit test: dependency mapping selects the right admission policy and reason
- Integration test: one hot dependency is throttled while unrelated dependencies keep accepting work
- Integration test: retry storm against an unhealthy dependency is damped instead of amplified
- Integration test: adaptive concurrency reacts to dependency health signals
- Benchmark: hotspot scenarios show preserved throughput for unaffected traffic

## Acceptance criteria

- Admission control can protect downstream hot spots selectively
- Operators can see which dependency caused throttling and which work was affected
- Adaptive concurrency and admission share a coherent dependency-health model
