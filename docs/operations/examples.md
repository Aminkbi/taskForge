# Runnable Demo

TaskForge has one supported runnable adoption path. It uses only the public
`taskforge`, `redis`, and `worker` packages; application code owns its handler
registration and embeds the worker.

```bash
docker compose up -d redis
make run-demo
```

The deterministic demo finishes in a few seconds. It publishes two tasks for a
noisy tenant and one protected task, runs two embedded worker slots, and holds
the first pair while it reads queue and fairness snapshots. The noisy tenant's
hard quota keeps its second task queued, while the protected tenant receives
its reserved slot. The JSON result includes all final task states, the metrics
snapshots captured during that hold, the maximum noisy-tenant concurrency, and
confirmation that the Prometheus handler returned metrics.

It defaults to Redis DB `15` so it does not mix with the Compose sidecars. Set
`TASKFORGE_DEMO_REDIS_ADDR`, `TASKFORGE_DEMO_REDIS_PASSWORD`, or
`TASKFORGE_DEMO_REDIS_DB` to use another local Redis target. The handler has no
external network dependency.

The Redis-backed executable contract runs with:

```bash
make test-demo
```
