# Latest-state overload-control comparison

All arms use TaskForge commit `0cc8a56`. Values are medians of six runs on one host; they are descriptive, not significance tests.

## Arm medians

| Workload | Arm | p99 completion (ms) | Throughput (tasks/s) | Jain equality | SLO violations |
| --- | --- | ---: | ---: | ---: | ---: |
| noisy-neighbor | taskforge-fifo-static | 148.2 | 1209.3 | 0.992 | 63.0 |
| noisy-neighbor | taskforge-full | 106.0 | 1650.3 | 1.000 | 6.0 |
| noisy-neighbor | taskforge-no-fairness | 110.8 | 1497.8 | 0.998 | 21.5 |
| delayed-backlog | taskforge-fifo-static | 81.9 | 1821.5 | 1.000 | 0.0 |
| delayed-backlog | taskforge-full | 90.2 | 1677.9 | 1.000 | 0.0 |
| delayed-backlog | taskforge-no-admission | 75.8 | 1982.9 | 1.000 | 0.0 |
| hot-dependency | taskforge-fifo-static | 167.9 | 906.5 | 1.000 | 20.0 |
| hot-dependency | taskforge-full | 339.6 | 457.7 | 0.970 | 90.5 |
| hot-dependency | taskforge-no-dependency-budget | 128.0 | 1160.1 | 1.000 | 0.0 |
| retry-storm | taskforge-fifo-static | 91.7 | 1565.9 | 1.000 | 0.0 |
| retry-storm | taskforge-full | 120.2 | 1205.9 | 1.000 | 0.0 |
| retry-storm | taskforge-no-adaptive | 117.9 | 1207.5 | 1.000 | 0.0 |

## Paired contrasts

Differences are full controls minus the named arm within the same seed and repetition. Negative p99 and violation differences are favorable; positive throughput and Jain differences are favorable.

| Workload | Contrast | p99 difference (ms) | Throughput difference (tasks/s) | Jain difference | Violation difference |
| --- | --- | ---: | ---: | ---: | ---: |
| noisy-neighbor | full minus taskforge-fifo-static | -41.0 | +429.9 | +0.008 | -56.0 |
| noisy-neighbor | full minus taskforge-no-fairness | -1.5 | +161.1 | +0.002 | -11.5 |
| delayed-backlog | full minus taskforge-fifo-static | +7.8 | -155.5 | +0.000 | +0.0 |
| delayed-backlog | full minus taskforge-no-admission | +13.1 | -294.4 | +0.000 | +0.0 |
| hot-dependency | full minus taskforge-fifo-static | +169.6 | -449.8 | -0.029 | +71.0 |
| hot-dependency | full minus taskforge-no-dependency-budget | +210.7 | -699.3 | -0.030 | +90.5 |
| retry-storm | full minus taskforge-fifo-static | +29.7 | -371.3 | +0.000 | +0.0 |
| retry-storm | full minus taskforge-no-adaptive | +2.6 | -5.8 | +0.000 | +0.0 |

Observed paired ranges are in `analysis.json`. This short closed-loop harness does not model dependency collapse or support a production SLA claim.
