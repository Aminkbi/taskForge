# Optimization-to-evidence map

This map binds each narrative claim to a source surface, a measurement, and an
invariant. It is intentionally concrete enough for a reviewer to reproduce or
reject a claim.

| Claim under test | Source surface in candidate | Measurement | Safety evidence |
| --- | --- | --- | --- |
| Publish performs fewer Redis exchanges | `redis/publish.go`, publish scripts, `redis/redis.go` | `BenchmarkPublishStateCosts`, `redis_round_trips/op`, `redis_commands/op` | dedup receipt and queued state are both visible after a successful publish |
| Queue snapshots scale with one Redis pipeline | `redis/redis.go`, `loadQueueDepth` | `BenchmarkSnapshotCosts`, p95 `ns/op` by tenant count | missing stream/group behavior remains zero-valued and error-safe |
| Group setup avoids repeated checks | `redis/redis.go`, `consumerGroups` cache | reserve/setup benchmark first-use versus steady-state | group creation remains serialized and errors are not cached |
| Hot key construction reduces allocations | `redis/redis.go`, `redis/state_store.go` | `B/op`, `allocs/op`, `ns/op` | key bytes remain byte-for-byte compatible with prior format |
| Unprocessable deliveries cannot grow without bound | pending delivery and DLQ paths | bounded-delivery integration/property checks | at-least-once acknowledgement and DLQ publish-before-ack invariant |

The treatment is immutable revision `b2947f3` against baseline `4446ab3`. Its
reproducibility identity is the pair `(baseline, treatment, sha256(git diff
--binary baseline treatment))`, computed by `scripts/third-wave-identity.sh`.
A benchmark log without all three values is not publication evidence.
