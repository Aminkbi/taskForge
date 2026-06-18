# Phase 29: Autoscaling, Kubernetes Operator, and Capacity Intelligence

## Commit goal

Add production deployment automation that can scale workers and schedulers from queue demand, resource placement, dependency health, and service objectives.

## Why this phase exists

TaskForge already has metrics and Docker assets, but running a serious task platform needs more than static deployments. Operators need autoscaling, safe rollouts, shard-aware configuration, capacity recommendations, and clear status for infeasible or backlogged work.

Ray's autoscaling and placement observability are useful references, but TaskForge should optimize for queue and workflow operations rather than general distributed compute.

## Changes

### Define scaling signals

Use signals such as:

- ready depth and reserved count
- oldest ready age
- publish rate and completion rate
- retry and DLQ growth
- workflow barrier backlog
- placement pending and infeasible counts
- dependency health and budget saturation
- worker startup latency and drain duration

Scaling should consider service objectives, not only CPU utilization.

### Build scaling policy

Support policy modes:

- queue lag target
- throughput target
- deadline miss prevention
- dependency-capped scale-down
- resource-placement demand
- manual floor and ceiling

The policy should avoid amplifying dependency overload.

### Add a Kubernetes operator

Provide CRDs for:

- TaskForge cluster
- worker pool
- scheduler deployment
- queue and routing policy
- dependency budget
- autoscaling policy
- shard or cluster routing policy

The operator should reconcile config, roll out safely, expose status, and preserve drain semantics during upgrades.

### Add capacity intelligence

Generate recommendations from benchmark and production telemetry:

- required workers for a lag target
- queues needing isolation
- fairness keys causing skew
- dependency budgets that are too tight or too loose
- scheduler release capacity versus delayed backlog

Recommendations should be explainable and never apply destructive changes automatically.

## Tests

- Unit test: scaling policy reacts correctly to lag, throughput, placement, and dependency signals
- Integration test: scale-up reduces oldest ready age in a Redis-backed environment
- Operator envtest: CRDs reconcile expected deployments and config maps
- Upgrade test: rolling worker update drains before termination
- Simulation test: dependency overload prevents runaway scale-up

## Acceptance criteria

- TaskForge can be operated as a Kubernetes-native system
- Autoscaling decisions are tied to queue and workflow objectives
- Scaling is dependency-aware and respects drain semantics
- Operators receive useful capacity recommendations with supporting evidence
