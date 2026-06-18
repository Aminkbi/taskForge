# Production Queue Roadmap

This directory breaks the next major evolution of TaskForge into commit-sized plans.
Each phase is intended to leave the repository in a coherent, reviewable state.

The goal is a strongly engineered production queue with:

- a durable broker abstraction
- explicit lease and recovery semantics
- strong observability
- a clear scaling model
- operational safety and failure-mode documentation

## Current baseline

The current scaffold is a solid starting point, but it is still a scaffold:

- [internal/broker/broker.go](../../internal/broker/broker.go) exposes a minimal broker contract.
- [internal/broker/message.go](../../internal/broker/message.go) contains delivery metadata and lease-oriented runtime fields.
- [internal/brokerredis/redis.go](../../internal/brokerredis/redis.go) uses Redis Streams and consumer groups for active delivery, while delayed jobs still flow through sorted sets.
- [internal/runtime/worker.go](../../internal/runtime/worker.go) assumes a simpler reserve/ack lifecycle than a production queue needs.

That means the repository is production-shaped, but phase 03 is still required to finish reclaim, crash recovery, and durable lease renewal.

## Principles for every phase

- Keep the execution model explicitly `at-least-once`.
- Assume handlers must be idempotent.
- Prefer durable broker state over process-local bookkeeping.
- Leave the repo passing tests after every phase.
- Document semantics before hiding them behind implementation details.

## Commit sequence

1. [Phase 01: Execution Contract and Broker State Model](./01-execution-contract-and-broker-state.md)
2. [Phase 02: Redis Streams Broker Migration](./02-redis-streams-broker.md)
3. [Phase 03: Lease Recovery and Worker Crash Semantics](./03-lease-recovery-and-worker-crash-semantics.md)
4. [Phase 04: Retry Engine and Dead-Letter Envelope](./04-retries-backoff-and-dead-letter.md)
5. [Phase 05: Delayed and Recurring Jobs](./05-delayed-and-recurring-jobs.md)
6. [Phase 06: Queue Isolation and Scaling Model](./06-queue-isolation-and-scaling.md)
7. [Phase 07: Observability and Operational Safety](./07-observability-and-operational-safety.md)
8. [Phase 08: Benchmarks, Examples, and Runbooks](./08-benchmarks-examples-and-runbooks.md)
9. [Phase 09: Recurring Schedule Scaling](./09-recurring-schedule-scaling.md)
10. [Phase 10: Tenant-Aware Fairness and Quotas](./10-tenant-aware-fairness-and-quotas.md)
11. [Phase 11: Admission Control and Overload Backpressure](./11-admission-control-and-overload-backpressure.md)
12. [Phase 12: Adaptive Concurrency and Dependency Budgets](./12-adaptive-concurrency-and-dependency-budgets.md)
13. [Phase 13: Fairness Follow-Ups](./13-fairness-follow-ups.md)
14. [Phase 14: Worker Drain, Shutdown, and Execution Lifecycle](./14-worker-drain-shutdown-and-execution-lifecycle.md)
15. [Phase 15: Scheduler Leadership Fencing and Control-Plane Safety](./15-scheduler-leadership-fencing-and-control-plane-safety.md)
16. [Phase 16: Delayed and Retry Index Scalability](./16-delayed-and-retry-index-scalability.md)
17. [Phase 17: Queryable Task State and Result Retention](./17-queryable-task-state-and-result-retention.md)
18. [Phase 18: Production Repo, CI, and Release Hardening](./18-production-repo-ci-and-release-hardening.md)
19. [Phase 19: Failure Injection, Concurrency Validation, and Benchmark Expansion](./19-failure-injection-concurrency-validation-and-benchmark-expansion.md)
20. [Phase 20: Cluster Routing, Sharding, and Operating Model](./20-cluster-routing-sharding-and-operating-model.md)

## Hardening wave

Phases 14 and later form a new hardening wave for production-readiness at larger scale.
They build on the earlier queue, scheduling, fairness, and overload work by tightening lifecycle semantics, control-plane safety, operational maturity, and cluster-scale assumptions.

## Recommended order of implementation

Implement these phases in order. The earlier phases intentionally reshape the contract and backend assumptions that later phases build on. In particular:

- Phase 01 should land before any durable broker work.
- Phase 02 should replace the current list-based Redis path instead of trying to patch it up further.
- Phase 03 should complete crash recovery before adding more scheduling and scaling features.
- Phases 07 and 08 should not be treated as optional polish. They are part of production-readiness.
- Phases 14 and later should start only after the current fairness, admission, and adaptive-control work is in place.
- The hardening wave should be implemented in numeric order because worker lifecycle semantics, scheduler control-plane safety, and backlog indexing decisions constrain the later repo, benchmark, and cluster-shaping phases.

## Post-20 ambition wave

The next roadmap wave raises TaskForge from a strong Redis-backed queue into a distributed execution platform. These phases are intentionally more ambitious than the initial production-readiness work: they cover workflow semantics, external-effect correctness, resource-aware placement, tail-latency scheduling, overload control, backend portability, operator control planes, SDKs, autoscaling, and formal validation.

21. [Phase 21: Native Workflow Graphs, Canvas Primitives, and Recovery Semantics](./21-native-workflow-graphs-canvas-and-recovery-semantics.md)
22. [Phase 22: Idempotency, External Effects, and Transactional Side-Effect Safety](./22-idempotency-external-effects-and-transactional-safety.md)
23. [Phase 23: Resource-Aware Placement, Locality, and Gang Scheduling](./23-resource-aware-placement-locality-and-gang-scheduling.md)
24. [Phase 24: Tail-Latency Scheduling, Queue Reordering, and Work Stealing](./24-tail-latency-scheduling-queue-reordering-and-work-stealing.md)
25. [Phase 25: Dependency-Aware Admission Control and Hotspot Protection](./25-dependency-aware-admission-control-and-hotspot-protection.md)
26. [Phase 26: Multi-Backend Broker and State-Store Portability](./26-multi-backend-broker-and-state-store-portability.md)
27. [Phase 27: Operator Control Plane, Event Stream, CLI, and Admin UI](./27-operator-control-plane-event-stream-cli-and-admin-ui.md)
28. [Phase 28: Stable Protocols and Multi-Language SDKs](./28-stable-protocols-and-multi-language-sdks.md)
29. [Phase 29: Autoscaling, Kubernetes Operator, and Capacity Intelligence](./29-autoscaling-kubernetes-operator-and-capacity-intelligence.md)
30. [Phase 30: Formal Models, Deterministic Simulation, and Reliability Certification](./30-formal-models-deterministic-simulation-and-reliability-certification.md)

## Research and product references for the ambition wave

- Celery's current stable routing and monitoring documentation describes mature task routing, worker inspection, events, and monitoring surfaces. TaskForge should match those operational affordances while keeping stronger lease, fairness, and admission semantics. See <https://docs.celeryq.dev/en/latest/userguide/routing.html> and <https://docs.celeryq.dev/en/stable/userguide/monitoring.html>.
- Ray's placement groups provide a useful model for atomic resource reservation, PACK/SPREAD placement strategies, and inspectable scheduling state. TaskForge should adapt those ideas for task queues without becoming a general compute cluster. See <https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html>.
- Ray's actor fault-tolerance documentation makes the semantic distinction between at-most-once and at-least-once actor task retries explicit. TaskForge should keep this same level of honesty for every runtime mode. See <https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html>.
- ExoFlow argues for decoupling execution from recovery and using task annotations about determinism and external visibility. TaskForge should borrow the idea of recovery-aware workflow metadata while preserving its explicit at-least-once base contract. See <https://www.usenix.org/conference/osdi23/presentation/zhuang>.
- Murmuration shows that queue reordering and scheduler/node cooperation can materially reduce job completion time under high utilization. TaskForge should turn that into practical queue disciplines, aging, and starvation safeguards. See <https://anil.recoil.org/papers/2024-socc-murmuration.pdf>.
- DRACO's dependency-aware admission control shows why overload decisions should consider the downstream resource that a request will hit, not only aggregate queue depth. TaskForge's existing dependency budgets are a strong foundation for this. See <https://doi.org/10.1016/j.jpdc.2024.104935>.
