# Phase 21: Native Workflow Graphs, Canvas Primitives, and Recovery Semantics

## Commit goal

Add a first-class workflow layer on top of the existing task execution contract so TaskForge can express chains, groups, joins, maps, and DAGs without hiding the at-least-once delivery model.

## Why this phase exists

Celery's canvas primitives are one of the reasons it remains useful beyond simple background jobs. TaskForge should exceed that surface by making graph execution observable, replayable, and recovery-aware from the start instead of treating workflows as client-side conventions.

The current code already has durable task state, delivery metadata, retries, delayed release, and result retention. Those are the right building blocks for graph execution, but there is no durable graph record, dependency state machine, join barrier, or workflow-level recovery policy yet.

Research systems such as ExoFlow are a useful north star because they separate execution from recovery and ask users to describe determinism and output visibility. TaskForge should not claim exactly-once execution globally, but it can use workflow metadata to make replay safer and less wasteful.

## Changes
policy
### Define workflow graph records

Introduce durable records for:

- workflow ID and version
- node IDs, task names, queues, payload references, and dependency edges
- graph state, node state, and terminal reason
- parent-child relationship between workflow nodes and logical task IDs
- workflow-level created, started, completed, canceled, and updated timestamps

The graph record should be independent of the broker stream so workflows can survive process restarts, scheduler turnover, and worker crashes.

### Add canvas-style primitives

Support a small set of composable primitives:

- `chain`: run nodes sequentially
- `group`: fan out independent nodes
- `join`: wait for a declared set of upstream nodes
- `map`: create bounded fan-out over an input collection
- `race`: complete when the first successful branch finishes, with explicit cancellation semantics for losers

Each primitive should compile into the same graph model instead of requiring separate scheduler code paths.

### Make recovery annotations explicit

Allow nodes to declare metadata such as:

- deterministic or nondeterministic output
- external side effects or internal-only output
- checkpoint required before downstream visibility
- replay allowed, replay discouraged, or manual intervention required
- cancellation behavior and compensation hook name

These annotations should guide scheduling and operator warnings. They should not silently upgrade the system to exactly-once execution.

### Add workflow scheduling and barrier release

Extend the scheduler or add a workflow controller that can:

- release root nodes
- observe node completion through durable state transitions
- release downstream nodes when dependencies satisfy their barrier
- mark failed, canceled, partially completed, and blocked workflows
- handle duplicate node completion idempotently

Barrier release must be fenced or otherwise concurrency-safe so multiple controllers cannot publish the same downstream node unsafely.

### Preserve queue semantics inside workflows

Workflow nodes should still use normal queues, fairness keys, dependency budgets, task limits, admission control, retries, and leases. The workflow layer coordinates dependency readiness; it should not create a separate execution engine that bypasses runtime safety.

## Tests

- Unit test: graph compilation produces stable node and edge records for chain, group, join, map, and race primitives
- Integration test: successful chain and fan-out/fan-in workflows reach terminal success with queryable node states
- Integration test: duplicate upstream completion does not double-release a downstream node
- Integration test: failed node applies the declared workflow failure policy
- Integration test: controller restart continues from durable graph state

## Acceptance criteria

- TaskForge can express workflow DAGs as durable, queryable product state
- Canvas primitives compile to one coherent graph model
- Workflow recovery semantics are explicit, documented, and tested
- Existing task-level at-least-once semantics remain visible instead of being papered over
