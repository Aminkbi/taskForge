# TaskForge third-wave research

The [Dev.to draft](blog/taskforge-wave3.md) reports the latest-state overload-control comparison. Every arm uses product source commit `0cc8a56`. The baseline is TaskForge with FIFO selection and static concurrency; the other arms use the full control set or remove one relevant control. The [fixed comparison plan](control-comparison/plan.md) describes the workloads and limits.

## Control-comparison evidence

The completed run is in [`control-comparison/data/final/`](control-comparison/data/final/): metadata, 72 compressed raw observations, and generated analysis in JSON and Markdown. The run used scale 8, two seeds, three repetitions per seed, and a dedicated persistence-disabled Redis 7.4 container over loopback. All cells used the same TaskForge source and the same host. The analysis reports arm medians and paired differences; these are descriptive results, not a significance test.

Verify the retained evidence and regenerate its analysis from the raw cells:

```bash
make third-wave-controls-check
```

To run the fixed protocol again, provide a dedicated Redis container reachable at the supplied address. The runner clears database 14 between cells and writes to a new output directory:

```bash
python3 scripts/third-wave-controls.py \
  --redis-container <dedicated-redis-container> \
  --redis-addr <mapped-redis-address> \
  --output <new-output-directory>
python3 scripts/third-wave-controls-analysis.py \
  --data <new-output-directory> \
  --output <new-analysis-directory>
```

## Control-plane implementation study

The separate [paper](paper/paper.md) and its [raw logs](data/final/) compare the Redis control-plane implementation across source revisions. That study has a different estimand and does not support the blog's workload claims. Its original protocol, amendment, and validation remain in [the analysis plan](analysis-plan.md), [execution notes](execution-notes.md), and `make third-wave-check`.
