# Redis Operating Model

TaskForge stores its queue streams, pending-delivery ownership, delayed/retry
indexes, scheduler leadership, task state, and control-plane data in one Redis
deployment. That deployment is a durability dependency, not a cache.

## Supported topology

Connect every TaskForge process directly to the writable primary of one
**standalone Redis** deployment. Sidecars validate `redis_mode=standalone` and
the writable `role=master` at startup. TLS is supported, including an optional
private CA and mutual TLS client certificate.

Redis Sentinel and Redis Cluster are explicitly unsupported. Sentinel has no
tested discovery or failover behavior here. Redis Cluster is unsafe because
TaskForge uses Lua operations across multiple keys without a proven common
hash-slot invariant. The logical shard labels in routing policy are application
placement metadata, not Redis Cluster shards.

## Persistence and capacity

Use durable storage and enable both AOF and RDB snapshots. The Compose setup is
a development baseline: AOF with `appendfsync everysec`, a `save 60 1`
snapshot, and a named `redis-data` volume. Production must place `/data` on
durable storage and monitor AOF rewrite and persistence errors.

Set `maxmemory-policy noeviction`. Any eviction policy can discard streams,
pending ownership, scheduler fences, delayed indexes, or task state and breaks
the delivery contract. Capacity alerts must leave headroom for AOF rewrite and
recovery; do not rely on Redis eviction as queue backpressure.

## Restart, failover, and recovery

A Redis restart interrupts publishers, workers, and schedulers. With durable
persistence, streams and pending entries recover. A worker may have completed
an external side effect before its Redis acknowledgement is durably recorded;
after restart or a lease expiry, that delivery can run again. At-least-once
therefore still applies across Redis restart and recovery. Handlers must use
idempotency keys or equivalent external-effect protection.

There is no automatic TaskForge failover. If a primary is replaced manually,
the replacement must be restored from the same durable Redis data and exposed
as a writable standalone primary before processes reconnect. A promoted or
replacement Redis instance that lacks acknowledged writes can also cause
duplicate execution; lost queue data cannot be repaired by TaskForge.

Back up Redis using a Redis-consistent method that includes the AOF and RDB
files, and test restoration into an isolated standalone instance. Stop writers
or use your platform's documented consistent backup mechanism before claiming a
recoverable point. Restore first, verify `redis_mode:standalone` and
`role:master`, then start schedulers and workers. Never use `FLUSHDB` or an
eviction-based reset on a production TaskForge database.

For local development, `make compose-down` preserves the named Redis volume;
`make compose-reset` removes it for a disposable clean slate.
