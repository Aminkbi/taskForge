# Phase 02: Redis Streams Broker Migration

## Commit goal

Replace the list-based Redis delivery path with Redis Streams and consumer groups so broker ownership and pending-delivery state become durable.

## Why this phase exists

The current implementation in [internal/brokerredis/redis.go](../../internal/brokerredis/redis.go) uses:

- `RPush` for enqueue
- `BLPop` for reserve
- in-process maps for leases

That means lease state disappears on worker crash and cannot be recovered by another worker.

## Changes

### Replace active delivery primitives

Migrate the Redis broker implementation to use:

- `XADD` to publish runnable tasks
- `XGROUP CREATE` for queue consumer groups
- `XREADGROUP` to reserve tasks
- `XACK` to acknowledge completion

One stream per queue is the recommended default for this phase.

### Define stream and consumer naming

Use stable, operator-readable names:

- stream key: `taskforge:stream:<queue>`
- consumer group: `taskforge:<queue>`
- consumer name: `<service-name>:<hostname>:<pid-or-instance-id>`

Keep naming centralized in the broker package so it is not duplicated across runtime code.

### Preserve current external behavior

The worker should still consume from a configured queue and process one task per lease.
This commit is about replacing storage and reserve semantics, not introducing retries, reclaim loops, or recurring jobs yet.

### Delayed jobs remain separate

Keep delayed-job storage in the existing sorted-set path for now.
This avoids mixing two large changes in one commit.

## Tests

- Integration test: publish then reserve from a stream-backed queue
- Integration test: ack removes the message from pending ownership
- Integration test: two consumers on the same group do not both receive the same delivery
- Unit tests for stream key and group name generation

## Acceptance criteria

- The Redis broker no longer uses process-local lease maps for normal reserve/ack behavior
- Active delivery uses Streams and consumer groups end-to-end
- Existing worker and scheduler binaries still compile
- Integration tests cover the new reserve/ack path
