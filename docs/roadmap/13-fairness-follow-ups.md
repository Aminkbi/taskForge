# Phase 13: Fairness Follow-Ups

## Why this exists

Phase 10 introduced tenant-aware fairness and quota enforcement for shared queues.
The first implementation is intentionally conservative and leaves a few targeted follow-ups for later work.

## Deferred improvements

### Tighten durable fairness accounting

- replace queue-level wake lists with a more compact signaling primitive so long bursts do not leave excess wake tokens behind
- consider moving from per-reserve snapshot inspection toward a broker-maintained ready index when fairness-key counts become large

### Improve starvation indicators

- compute oldest-ready age from true ready work rather than approximating from the oldest stream entry when pending deliveries exist ahead of ready work
- expose a stronger starvation metric that compares observed service against configured weight

### Strengthen quota semantics

- add explicit publish-time defer or reject behavior once admission control is implemented
- support time-window or rate-based quotas in addition to concurrent leased-delivery caps where operators need that model

### Expand policy expressiveness

- support named reusable fairness policies if multiple queues need the same rule set
- evaluate classifier plugins or header fallback for environments that cannot populate `fairness_key` directly
