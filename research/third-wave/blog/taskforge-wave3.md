# Making a queue cheaper without making it less safe

TaskForge's earlier experiments answered a systems question: what happens when
background work overloads a shared Redis queue? The next question is smaller and
more practical. After tightening the hot paths, did we actually remove work,
and did we keep the guarantees that keep the queue useful?

Wave 3 treats committed revision `b2947f3` as an experiment. It combines five changes:
publish and queued state are recorded together; queue metrics share a Redis
pipeline; consumer-group setup is cached after success; hot key construction
avoids formatting overhead; and unprocessable deliveries are bounded. Each
change has a named benchmark and a named safety check in the evidence map.

The design avoids a common performance-story trap. The wave 2 study showed
that a result can change with the environment: fairness effects were visible
under an emulated-latency path, and the long admission result reversed between
the two measured classes. That is why wave 3 does not call a faster end-to-end
run proof of a faster publish path. It measures Redis round trips, commands,
allocations, and operation latency directly, with a clean checkout paired to
the committed revision and its clean parent on the same machine.

There is no invented chart here. Redis was unavailable when the package was
prepared, so the treatment table is marked pending. That is a useful result of
the process: the protocol, source identity, fixed iteration count, bootstrap
seed, regression limit, and scope are all decided before numbers arrive. When
the run is possible, `make third-wave-check` verifies the package and
`make benchmark-regression` compares matched logs.

The benchmark notes do contain promising leads: fair publish is listed
as 510 to 278 microseconds without a receipt and 707 to 502 microseconds with
one; 64 tenants with 64-KiB payloads are listed as 8.80 to 0.89 milliseconds
for metrics. Those are provisional engineering observations, not wave 3
results, because the paired raw logs are missing. The evidence package keeps
that distinction visible.

The eventual headline has a strict shape: “On [host] with [Go] and [Redis],
the pending patch changed [metric] by [estimate] with [interval], while all
invariant checks passed.” If the interval crosses zero, the honest headline is
that the optimization was not resolved by this experiment. If a safety check
fails, the speedup does not count.

That standard is the point of wave 3. A queue is optimized only when its
control plane uses fewer resources and its delivery contract still holds.
