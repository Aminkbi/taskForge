# Phase 23: Resource-Aware Placement, Locality, and Gang Scheduling

## Commit goal

Add a resource model and placement planner so tasks can request CPU, memory, GPU, dependency, locality, and all-or-nothing execution constraints.

## Why this phase exists

The current worker-pool model is queue-centric: route to a queue, then let workers reserve work. That is simple and strong for many workloads, but modern task systems also need resource-aware placement for ML jobs, media pipelines, data locality, hardware affinity, and multi-task reservations.

Ray placement groups are a useful product reference: they make resource bundles explicit, support PACK and SPREAD placement, and expose pending or infeasible placement state. TaskForge should adapt that idea to queues and workers while keeping broker durability and operator clarity.

## Changes

### Define worker capabilities

Extend worker lifecycle records to publish capabilities such as:

- CPU slots and memory class
- GPU count, accelerator type, or custom resource labels
- region, zone, node, rack, and data locality labels
- dependency access labels such as database shard or object-store region
- supported task names or runtime images if needed later

Capabilities should expire with worker lifecycle TTLs so stale placement data does not outlive a dead worker.

### Add task resource requests

Allow tasks and workflow nodes to request:

- scalar resources such as CPU slots or GPU units
- labels and anti-labels
- PACK, SPREAD, STRICT_PACK, and STRICT_SPREAD policies
- locality preferences for input objects or tenant placement
- gang bundles for all-or-nothing multi-node work

Requests should be validated before publish when possible and marked infeasible when they cannot ever be satisfied by known capacity.

### Implement placement reservations

Add a durable reservation layer for work that cannot be represented by simple queue depth. A placement reservation should:

- atomically claim the required resource bundles
- have a TTL and renewal path
- release on task completion, cancellation, or worker death
- expose pending, created, partially lost, and infeasible states

This layer should integrate with dependency budgets rather than duplicate them.

### Keep queues as execution lanes

Placement should decide eligible lanes or worker pools; Redis Streams should still carry executable task deliveries. This prevents the placement planner from becoming an unbounded in-memory scheduler.

## Tests

- Unit test: placement planner classifies feasible, pending, and infeasible requests
- Integration test: STRICT_PACK only releases work when all bundles can be reserved
- Integration test: worker death releases or reconstructs affected reservations
- Integration test: locality preference changes placement without violating hard constraints
- API test: placement state is inspectable by task, workflow, and operator endpoints

## Acceptance criteria

- TaskForge can schedule work by resources and locality, not only by queue name
- Gang-style reservations are explicit and observable
- Resource placement composes with leases, fairness, admission, and worker lifecycle
