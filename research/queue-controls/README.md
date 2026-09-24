# Corrected queue-control study

This study supersedes the closed-loop comparison in
[`research/third-wave/control-comparison`](../third-wave/control-comparison/).
Its arrivals are frozen before measurement, so a policy that defers work cannot
also change the offered-load trace. The dependency profile models latency growth
and failures above capacity.

The retained run contains 38 cells from two seeds across six profiles. Raw
compressed results, frozen traces, provenance, and generated analysis are in
[`data/`](data/). Regenerate the derived report with:

```bash
make queue-controls-check
```

To run a new study into an unused directory, start with a dedicated Redis
container and use:

```bash
make queue-controls-run QUEUE_CONTROLS_OUTPUT=/tmp/taskforge-queue-controls-run
```

The run script records the source commit, exact Go source hashes, trace and
binary digests, arm order, Redis image, cell arguments, and raw result hashes.
The checked-in `source-overlays.patch` records the harness changes relative to
the parent source revision used for the retained run.
The prose plan was edited after measurement and no longer matches its recorded
hash. Its original copy is unavailable; the runner and executable profiles
match the metadata. Treat the plan as an explanation of the retained study,
not an exact snapshot of the pre-run text.

Results are descriptive and host-specific. They support control-specific
claims about this modeled load; they do not establish production SLOs.
