# Phase 20: Cluster Routing, Sharding, and Operating Model

## Commit goal

Define how TaskForge scales beyond a single-broker, single-control-plane deployment by making routing, sharding, and cluster-wide versus node-local policy boundaries explicit.

## Why this phase exists

Earlier phases make the single-cluster runtime stronger, but large projects eventually need a clearer answer to a higher-level architecture question: how does TaskForge scale when one Redis-backed control plane or one routing model is no longer enough?

Without an explicit operating model, cluster growth tends to happen accidentally through ad hoc queue partitioning, undocumented tenant placement, and inconsistent policy enforcement.

TaskForge should document a deliberate scaling model before operators are forced to invent one under pressure.

## Changes

### Define routing and placement policy

Document how work is assigned across queues, shards, or clusters based on dimensions such as:

- queue purpose
- tenant or traffic class
- regional or data-boundary requirements
- dependency isolation needs

Routing should be a policy surface, not only a publish-time convention.

### Clarify cluster-wide versus node-local controls

Make it explicit which controls are:

- local to a worker process or node
- shared within one Redis-backed cluster
- intended to be global only through future coordination or external routing

This is especially important for task-type limits, adaptive concurrency, fairness, and admission behavior.

### Define shard and control-plane responsibilities

Specify the intended responsibilities of each layer:

- workers execute and renew leases
- schedulers own due-release and recurring control-plane work within a shard
- routing chooses the destination queue or shard
- operators observe cluster health and rebalance intentionally

That keeps cluster growth from blurring control boundaries.

### Add operational guidance for large deployments

Document expectations for:

- adding new shards or clusters
- moving tenants or queues between them
- handling shard-local overload
- reasoning about fairness and retries across routing boundaries

The goal is a coherent operating model, not immediate multi-backend implementation.

## Tests

- Design and integration test: routing rules produce stable queue or shard placement for representative traffic classes
- Integration test: shard-local overload does not require undocumented cross-shard behavior
- Simulation or benchmark: skewed tenant load can be redistributed according to the documented placement model
- Documentation test: cluster operating guidance matches the implemented routing and policy boundaries

## Acceptance criteria

- TaskForge has an explicit scaling and routing model for larger deployments
- Operators can tell which controls are local, shard-scoped, or cluster-scoped
- Queue placement and shard growth become deliberate operational choices instead of accidental conventions

## Implementation notes

This phase is implemented as a policy-driven routing layer for new publishes plus an explicit operating document in `docs/operations/cluster-routing.md`.

The implemented boundary is intentionally conservative:

- `TASKFORGE_ROUTING_POLICY_JSON` defines ordered routing rules.
- Rules can match task name, source queue, fairness key, traffic class, and exact headers.
- Destinations can set the executable queue and fixed or hash-selected logical shard metadata.
- Queue placement is applied before publish-time admission control.
- Retries, due releases, recurring dispatches, DLQ publishes, DLQ replays, and broker requeues preserve existing queue placement.
- `taskforge_shard` is logical operator metadata in this phase; Redis keys remain queue-scoped under the existing single-control-plane runtime.

Future multi-Redis or multi-control-plane dispatch should consume the same placement model instead of changing task execution semantics.
