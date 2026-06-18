# Phase 16: Delayed and Retry Index Scalability

## Commit goal

Replace the current globally scanned delayed and retry indexing path with a scaling model whose cost grows with relevant due work, not total deferred backlog.

## Why this phase exists

TaskForge already uses Redis sorted sets for delayed release, but some operational decisions still depend on work that scales with the full deferred set.

That is acceptable for small backlogs, but it becomes an avoidable bottleneck as queues, tenants, and retry volume grow. Publish-time overload checks and scheduler loops should not degrade because unrelated queues have accumulated a large delayed backlog.

TaskForge should make delayed and retry indexing a first-class scaling concern instead of letting it remain an incidental implementation detail.

## Changes

### Move from one global delayed view to targeted indexes

Adopt a model where delayed and retry lookup is partitioned by queue or shard so:

- due-release scans touch only relevant work
- admission checks avoid global delayed backlog inspection
- high-volume retry traffic on one queue does not penalize another queue's control path

The exact key layout can vary, but the scaling requirement should be explicit.

### Separate scheduling indexes from task payload durability

Preserve the distinction between:

- the durable deferred task payload
- the index used to find due work quickly

That makes future migration, sharding, and compaction decisions easier without tying all semantics to one Redis structure.

### Define retry backlog visibility cheaply

Overload and admission control should be able to answer questions such as:

- how much retry work exists for this queue
- how old the deferred backlog is
- whether the queue is accumulating delayed pressure faster than it drains

These signals should be cheap enough to use operationally under load.

### Document migration and coexistence expectations

If the delayed layout changes, define how the system handles:

- migration from the existing global structure
- mixed old and new deferred state during rollout
- rollback safety

This phase should not assume an offline data rewrite.

## Tests

- Integration test: due release on one queue does not require scanning unrelated queues' delayed backlog
- Integration test: retry backlog metrics and admission signals remain accurate after index partitioning
- Integration test: rollout from the previous delayed layout preserves deferred-task correctness
- Benchmark: publish and due-release latency under large multi-queue delayed backlog

## Acceptance criteria

- Delayed and retry control-path cost scales with relevant queue or shard state rather than one global deferred set
- Admission and overload signals remain available without expensive full-backlog scans
- Migration expectations are explicit and operationally safe

## Implementation note

The Redis delayed layout now uses queue-scoped delayed sorted sets plus a small queue index keyed by each queue's oldest ETA. Retry backlog visibility is maintained in queue-scoped retry indexes, so admission reads a queue-local cardinality instead of decoding every deferred payload.

Because TaskForge is not released yet, rollout does not preserve the previous single `taskforge:delayed` key as a compatibility read path. Existing local Redis state from earlier development builds can be flushed or republished into the new layout.
