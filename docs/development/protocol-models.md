# Executable protocol models

`internal/modelcheck` contains finite explicit-state models for TaskForge's
delivery-ownership and scheduler-fencing protocols. They complement the seeded
failure simulator: simulation samples larger scenarios, while model checking
exhausts every enabled action ordering inside smaller declared domains.

The checker is implemented in Go instead of TLA+/TLC so the normal pinned Go
toolchain is sufficient for local and CI runs. This is justified only because
the modeled domains are finite and small enough to exhaust: the checker fails
if either its depth or state cap truncates exploration, checks invariants after
every transition, and emits a shortest counterexample using breadth-first
search. It also rejects a completed state graph containing a nonterminal
deadlock or cycle.

## Reproducible commands and bounds

The CI smoke command is:

```sh
make model-check
```

It runs the checker tests, including deliberately defective protocols, then
checks both correct models with `max-depth=32` and `max-states=100000`. A
typical run explores about 37,000 delivery states and 200 scheduler states in
well under a second; the exact totals are printed so unexpected state-space
changes are visible.

Run a model or reproduce a deliberately injected defect directly:

```sh
go run ./internal/modelcheck/cmd/modelcheck -model delivery
go run ./internal/modelcheck/cmd/modelcheck -model scheduler
go run ./internal/modelcheck/cmd/modelcheck -model delivery -mutation delivery-id-only
go run ./internal/modelcheck/cmd/modelcheck -model delivery -mutation retry-without-receipt
go run ./internal/modelcheck/cmd/modelcheck -model scheduler -mutation scheduler-owner-only
```

The mutation commands must exit nonzero with a counterexample. The tests pin
the required actions and violated invariant rather than brittle full traces.
`delivery-id-only` accepts an old owner after reclaim, `retry-without-receipt`
duplicates a retry whose first publish committed but lost its reply, and
`scheduler-owner-only` accepts an old epoch after the same scheduler identity
reacquires leadership. These mutations are confined to the model checker and
are not production switches.

## Delivery ownership model

The finite domain has one logical task, two worker identities, at most two
delivery attempts, one nack/requeue, three lease generations, one extension
per generation, and logical time `0..4`. A delivery generation is a ghost
variable used to identify old handles in traces; production does not persist
that field. The model explores:

- `Reserve`, current-owner `ExtendLease`, logical time, lease expiry, and
  `Reclaim` by another worker;
- current and stale `Ack`, `Nack`, lease extension, and retry publication;
- retry publish success and commit-with-lost-reply, replay with a stable
  receipt, then acknowledgement of the source delivery;
- succeeded and dead-lettered logical terminal states.

Checked safety invariants are:

- `delivery/stale-owner-mutated-current-lease`: an old handle cannot mutate or
  finalize the reclaimed delivery;
- `delivery/retry-publication-not-idempotent`: one delivery-scoped retry
  receipt creates at most one replacement;
- queued work has no owner and leased work has one bounded fenced owner;
- terminal state has exactly one terminal write and no active owner;
- retry attempts stay within the modeled delivery-policy bound.

The liveness check requires the completely explored nonterminal graph to have
no cycle and no deadlock. Within the encoded bounds, every maximal execution
therefore reaches succeeded or dead-lettered. This is a bounded progress
claim, not an unbounded production guarantee.

## Scheduler fencing model

The finite domain has two scheduler identities, two monotonically issued
epochs, one renewal per epoch, one leadership turnover, logical time `0..3`,
and one idempotent due release. Each scheduler retains local and previous
fences so writes can race expiry and reacquisition, including the subtle case
where the same owner reacquires with a newer epoch.

Checked safety invariants are:

- `scheduler/stale-epoch-write-accepted`: an accepted write must carry the
  exact live owner, epoch, and token;
- an absent leader has no live epoch, and a live epoch is the latest issued;
- no write can use an unissued epoch;
- the modeled terminal release occurs at most once.

The same complete-graph deadlock/cycle check proves bounded progress to a
release under the assumptions below.

## Safety and liveness assumptions

These model assumptions are explicit:

- Redis commands represented by one transition are atomic. In particular,
  publish plus receipt, fenced due release, leadership acquisition, and each
  compare-and-mutate operation do not expose intermediate state.
- Worker consumer identities differ across the lease generations that may
  execute concurrently. The implementation's delivery fence is stream
  delivery ID plus consumer owner; the model's ghost generation is not an
  additional production fence.
- A retry receipt remains present throughout the ambiguity/recovery window and
  its key is unique to the source delivery.
- Logical clocks are monotonic, lease/leadership expiry is eventually
  observed, Redis becomes available, and an enabled reserve or leadership
  acquisition eventually runs.
- Bounded progress allows at most two lease losses, one extension per lease,
  one requeue, one retry, one leadership turnover, and one leadership renewal.
  On the final lease or epoch, the environment eventually supplies a handler
  terminal outcome or a scheduler write. The transition system encodes these
  fairness bounds by not permitting another loss at the final bound.
- Succeeded and dead-lettered are the only logical terminal states in the
  current canonical state model. `retry_scheduled` terminates a delivery, not
  the logical task.

Weakening or removing these assumptions requires enlarging the model before
drawing a corresponding conclusion. In particular, same-consumer reclaim and
receipt expiry are documented gaps, not silently covered cases.

## Model-to-code and test map

| Model action or invariant | Implementation boundary | Observable implementation tests |
| --- | --- | --- |
| `Reserve` | `redis.Broker.Reserve`, Redis consumer group pending entry | `TestRedisPublishReserveAndAck`, `TestRedisConsumersDoNotDuplicateGroupDelivery` |
| `ExtendLease` | `redis.Broker.ExtendLease`, pending idle reset | `TestRedisExtendLeasePreventsReclaim`, `TestWorkerKeepsPendingDeliveryLeasedWhileLocallyBlocked` |
| expiry and `Reclaim` | pending idle threshold and `XCLAIM` ownership transfer | `TestRedisReclaimsExpiredDelivery`, `TestWorkerLeaseRenewFailureAbandonsAndAllowsRedisRedelivery` |
| stale `Ack`/`Nack`/extend rejection | `redis.Broker.validatePendingDelivery` owner and expiry checks | `TestRedisRejectsStaleOwnerOperationsAfterReclaim`, `TestRedisExpiresCurrentOwnerAck` |
| retry publish before source ack | `worker.processTask`, delivery-scoped `retry:<delivery-id>` key | `TestWorkerProcessTaskRetriesFailedTask`, `TestWorkerDoesNotAcknowledgeBeforeReplacementPublishSucceeds` |
| ambiguous retry deduplication | atomic publish-receipt scripts and replay using the same key | `TestRedisPublishDeduplicationKeyPublishesOnce`; T10 seed `ambiguous_publish_deduplicates` covers the lost-reply schedule |
| terminal monotonicity | canonical state transition table and worker terminal writes | `TestAcceptedTransitionsNeverLeaveATerminalState`, `TestWorkerProcessTaskRecordsRunningAndTerminalState` |
| epoch `Acquire`, renewal, expiry, turnover | `scheduler.RedisLeaderElector` token/epoch scripts | `TestSchedulerLeaderElectionDispatchesRecurringOnce`, `TestSchedulerFastFailoverDoesNotDuplicateRecurringRun` |
| stale fenced due write | fenced delayed-release Lua scripts | `TestRedisMoveDueRejectsStaleFenceWithoutMutatingDelayedTask` |
| stale recurring mutation | `RedisScheduleStateStore` leadership-key `WATCH` and token comparison | `TestRecurringRemoveFromDueIndexRejectsStaleLeadershipFence`, `TestSchedulerStaleMoveDueDemotesAndSkipsRecurringMutation` |
| idempotent terminal release | receipt inside fenced release transition | `TestRedisMoveDueConcurrentReleasePublishesOnce`, `TestRecurringSyncDueConcurrentDispatchPublishesOneNominalRun` |

The mapping is traceability, not refinement proof: tests execute selected code
paths and the models exhaust abstract paths, but there is no machine-checked
translation between Go/Lua and model transitions.

## Unmodeled Redis and runtime behavior

The checker does **not** prove Redis server behavior, Lua atomicity, `WATCH`
retry semantics, stream/PENDING/XCLAIM details, TTL precision, cluster failover,
replication durability, script cancellation, key construction, serialization,
fairness queues, dependency budgets, goroutine cancellation, or state-store
writes that occur after broker acknowledgement. It also does not cover receipt
expiry, a process reusing the same consumer identity for a concurrent stale
lease, more than two retries/epochs, clock skew, or Redis unavailability that
outlasts the bounded liveness assumptions.

Use `make integration-test` for real Redis semantics, `make race-test` for Go
concurrency, and `make simulation-test` for longer deterministic fault
schedules. None of those checks, alone or together, establishes exactly-once
execution; TaskForge remains explicitly at least once.
