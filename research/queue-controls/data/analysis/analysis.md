# Corrected queue-control comparison

All cells use immutable open-loop arrivals. Metrics use all offered measurement-window tasks as the success denominator; conditional p99 includes only successful completions.

| Profile | System | Offered | Accepted | Completed | SLO success | p99 ms | downstream failures | over-capacity | ready max | deferred max | concurrency range |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| fairness-pressure | taskforge-fifo-static | 3601 | 3601 | 3601 | 0.142 | 3539.373056 | 0.000 | 0.000 | 1322 | 0 | 8–8 |
| fairness-pressure | taskforge-fairness-only | 3601 | 3601 | 3601 | 0.211 | 3571.504128 | 0.000 | 0.000 | 1330 | 0 | 8–8 |
| fairness-pressure | taskforge-fairness-only | 3601 | 3601 | 3601 | 0.207 | 3541.166848 | 0.000 | 0.000 | 1306 | 0 | 8–8 |
| fairness-pressure | taskforge-fifo-static | 3601 | 3601 | 3601 | 0.141 | 3559.635968 | 0.000 | 0.000 | 1328 | 0 | 8–8 |
| admission-pressure | taskforge-fifo-static | 3601 | 3601 | 3601 | 0.144 | 3509.531904 | 0.000 | 0.000 | 1312 | 0 | 8–8 |
| admission-pressure | taskforge-admission-only | 3601 | 3601 | 3601 | 0.308 | 6427.34208 | 0.000 | 0.000 | 22 | 1226 | 8–8 |
| admission-pressure | taskforge-admission-only | 3601 | 3601 | 3601 | 0.311 | 6455.710976 | 0.000 | 0.000 | 20 | 1229 | 8–8 |
| admission-pressure | taskforge-fifo-static | 3601 | 3601 | 3601 | 0.141 | 3505.086208 | 0.000 | 0.000 | 1309 | 0 | 8–8 |
| dependency-pressure | taskforge-static-capacity | 1561 | 1561 | 1561 | 0.218 | 2319.373056 | 0.000 | 0.000 | 420 | 0 | 4–4 |
| dependency-pressure | taskforge-fifo-static | 1561 | 1561 | 443 | 0.013 | 20132.146944 | 0.849 | 0.972 | 1398 | 6 | 8–8 |
| dependency-pressure | taskforge-budget-only | 1561 | 1561 | 1561 | 0.210 | 2361.40032 | 0.000 | 0.000 | 429 | 0 | 8–8 |
| dependency-pressure | taskforge-budget-only | 1561 | 1561 | 1561 | 0.207 | 2371.730944 | 0.000 | 0.000 | 431 | 0 | 8–8 |
| dependency-pressure | taskforge-static-capacity | 1561 | 1561 | 1561 | 0.224 | 2216.851712 | 0.000 | 0.000 | 404 | 0 | 4–4 |
| dependency-pressure | taskforge-fifo-static | 1561 | 1561 | 414 | 0.014 | 20140.450048 | 0.856 | 0.972 | 1395 | 7 | 8–8 |
| adaptive-pressure | taskforge-static-capacity | 1561 | 1561 | 1561 | 0.222 | 2228.51072 | 0.000 | 0.000 | 408 | 0 | 4–4 |
| adaptive-pressure | taskforge-fifo-static | 1561 | 1561 | 459 | 0.012 | 20053.974272 | 0.845 | 0.972 | 1400 | 6 | 8–8 |
| adaptive-pressure | taskforge-adaptive-only | 1561 | 1561 | 1556 | 0.094 | 3909.679104 | 0.103 | 0.624 | 456 | 6 | 4–9 |
| adaptive-pressure | taskforge-adaptive-only | 1561 | 1561 | 1560 | 0.194 | 3648.317952 | 0.091 | 0.625 | 426 | 4 | 4–8 |
| adaptive-pressure | taskforge-static-capacity | 1561 | 1561 | 1561 | 0.212 | 2241.798144 | 0.000 | 0.000 | 412 | 0 | 4–4 |
| adaptive-pressure | taskforge-fifo-static | 1561 | 1561 | 414 | 0.012 | 19933.89952 | 0.857 | 0.972 | 1392 | 6 | 8–8 |
| stable-light | taskforge-budget-only | 300 | 300 | 300 | 1.000 | 23.129088 | 0.000 | 0.000 | 0 | 0 | 8–8 |
| stable-light | taskforge-fifo-static | 300 | 300 | 300 | 1.000 | 23.028224 | 0.000 | 0.000 | 0 | 0 | 8–8 |
| stable-light | taskforge-adaptive-only | 300 | 300 | 300 | 1.000 | 22.913024 | 0.000 | 0.000 | 0 | 0 | 8–8 |
| stable-light | taskforge-fairness-only | 300 | 300 | 300 | 1.000 | 23.54176 | 0.000 | 0.000 | 1 | 0 | 8–8 |
| stable-light | taskforge-admission-only | 300 | 300 | 300 | 1.000 | 23.494912 | 0.000 | 0.000 | 0 | 0 | 8–8 |
| stable-light | taskforge-fairness-only | 300 | 300 | 300 | 1.000 | 23.929088 | 0.000 | 0.000 | 1 | 0 | 8–8 |
| stable-light | taskforge-admission-only | 300 | 300 | 300 | 1.000 | 23.371776 | 0.000 | 0.000 | 0 | 0 | 8–8 |
| stable-light | taskforge-budget-only | 300 | 300 | 300 | 1.000 | 23.065088 | 0.000 | 0.000 | 0 | 0 | 8–8 |
| stable-light | taskforge-adaptive-only | 300 | 300 | 300 | 1.000 | 22.85312 | 0.000 | 0.000 | 0 | 0 | 8–8 |
| stable-light | taskforge-fifo-static | 300 | 300 | 300 | 1.000 | 22.957056 | 0.000 | 0.000 | 0 | 0 | 8–8 |
| fragile-light | taskforge-budget-only | 300 | 300 | 300 | 1.000 | 23.136768 | 0.000 | 0.000 | 0 | 0 | 8–8 |
| fragile-light | taskforge-static-capacity | 300 | 300 | 300 | 1.000 | 22.861824 | 0.000 | 0.000 | 0 | 0 | 4–4 |
| fragile-light | taskforge-fifo-static | 300 | 300 | 300 | 1.000 | 22.928128 | 0.000 | 0.000 | 0 | 0 | 8–8 |
| fragile-light | taskforge-adaptive-only | 300 | 300 | 300 | 1.000 | 22.908672 | 0.000 | 0.000 | 0 | 0 | 8–8 |
| fragile-light | taskforge-budget-only | 300 | 300 | 300 | 1.000 | 23.022848 | 0.000 | 0.000 | 0 | 0 | 8–8 |
| fragile-light | taskforge-adaptive-only | 300 | 300 | 300 | 1.000 | 22.991104 | 0.000 | 0.000 | 0 | 0 | 8–8 |
| fragile-light | taskforge-static-capacity | 300 | 300 | 300 | 1.000 | 22.93888 | 0.000 | 0.000 | 0 | 0 | 4–4 |
| fragile-light | taskforge-fifo-static | 300 | 300 | 300 | 1.000 | 22.917888 | 0.000 | 0.000 | 0 | 0 | 8–8 |

## Paired contrasts

| Profile | Contrast | Metric | Median difference | Per-seed values |
| --- | --- | --- | ---: | --- |
| fairness-pressure | taskforge-fairness-only minus taskforge-fifo-static | slo_success | 0.06706 | 0.06831, 0.06582 |
| fairness-pressure | taskforge-fairness-only minus taskforge-fifo-static | accepted_fraction | 0 | 0, 0 |
| fairness-pressure | taskforge-fairness-only minus taskforge-fifo-static | conditional_p99_ms | 6.831 | 32.13, -18.47 |
| fairness-pressure | taskforge-fairness-only minus taskforge-fifo-static | downstream_failure_rate | 0 | 0, 0 |
| fairness-pressure | taskforge-fairness-only minus taskforge-fifo-static | downstream_over_capacity_rate | 0 | 0, 0 |
| fairness-pressure | taskforge-fairness-only minus taskforge-fifo-static | ready_backlog_max | -7 | 8, -22 |
| fairness-pressure | taskforge-fairness-only minus taskforge-fifo-static | deferred_backlog_max | 0 | 0, 0 |
| fairness-pressure | taskforge-fairness-only minus taskforge-fifo-static | total_backlog_max | -7 | 8, -22 |
| fairness-pressure | taskforge-fairness-only minus taskforge-fifo-static | controller_min | 0 | 0, 0 |
| fairness-pressure | taskforge-fairness-only minus taskforge-fifo-static | controller_max | 0 | 0, 0 |
| fairness-pressure | taskforge-fairness-only minus taskforge-fifo-static | redis_cpu_seconds | 0.2943 | 0.2526, 0.3361 |
| fairness-pressure | taskforge-fairness-only minus taskforge-fifo-static | dispatch_p99_ms | -0.004853 | -0.003883, -0.005822 |
| admission-pressure | taskforge-admission-only minus taskforge-fifo-static | slo_success | 0.1672 | 0.1641, 0.1702 |
| admission-pressure | taskforge-admission-only minus taskforge-fifo-static | accepted_fraction | 0 | 0, 0 |
| admission-pressure | taskforge-admission-only minus taskforge-fifo-static | conditional_p99_ms | 2934 | 2918, 2951 |
| admission-pressure | taskforge-admission-only minus taskforge-fifo-static | downstream_failure_rate | 0 | 0, 0 |
| admission-pressure | taskforge-admission-only minus taskforge-fifo-static | downstream_over_capacity_rate | 0 | 0, 0 |
| admission-pressure | taskforge-admission-only minus taskforge-fifo-static | ready_backlog_max | -1290 | -1290, -1289 |
| admission-pressure | taskforge-admission-only minus taskforge-fifo-static | deferred_backlog_max | 1228 | 1226, 1229 |
| admission-pressure | taskforge-admission-only minus taskforge-fifo-static | total_backlog_max | -67 | -70, -64 |
| admission-pressure | taskforge-admission-only minus taskforge-fifo-static | controller_min | 0 | 0, 0 |
| admission-pressure | taskforge-admission-only minus taskforge-fifo-static | controller_max | 0 | 0, 0 |
| admission-pressure | taskforge-admission-only minus taskforge-fifo-static | redis_cpu_seconds | 2.455 | 2.45, 2.46 |
| admission-pressure | taskforge-admission-only minus taskforge-fifo-static | dispatch_p99_ms | -0.1819 | -0.1349, -0.2288 |
| dependency-pressure | taskforge-budget-only minus taskforge-fifo-static | slo_success | 0.1951 | 0.1973, 0.1928 |
| dependency-pressure | taskforge-budget-only minus taskforge-fifo-static | accepted_fraction | 0 | 0, 0 |
| dependency-pressure | taskforge-budget-only minus taskforge-fifo-static | conditional_p99_ms | -1.777e+04 | -1.777e+04, -1.777e+04 |
| dependency-pressure | taskforge-budget-only minus taskforge-fifo-static | downstream_failure_rate | -0.8525 | -0.8487, -0.8564 |
| dependency-pressure | taskforge-budget-only minus taskforge-fifo-static | downstream_over_capacity_rate | -0.9721 | -0.9722, -0.9721 |
| dependency-pressure | taskforge-budget-only minus taskforge-fifo-static | ready_backlog_max | -966.5 | -969, -964 |
| dependency-pressure | taskforge-budget-only minus taskforge-fifo-static | deferred_backlog_max | -6.5 | -6, -7 |
| dependency-pressure | taskforge-budget-only minus taskforge-fifo-static | total_backlog_max | -1061 | -1064, -1058 |
| dependency-pressure | taskforge-budget-only minus taskforge-fifo-static | controller_min | 0 | 0, 0 |
| dependency-pressure | taskforge-budget-only minus taskforge-fifo-static | controller_max | 0 | 0, 0 |
| dependency-pressure | taskforge-budget-only minus taskforge-fifo-static | redis_cpu_seconds | -0.759 | -0.7633, -0.7547 |
| dependency-pressure | taskforge-budget-only minus taskforge-fifo-static | dispatch_p99_ms | -0.0002825 | -0.004698, 0.004133 |
| dependency-pressure | taskforge-static-capacity minus taskforge-fifo-static | slo_success | 0.2076 | 0.2056, 0.2095 |
| dependency-pressure | taskforge-static-capacity minus taskforge-fifo-static | accepted_fraction | 0 | 0, 0 |
| dependency-pressure | taskforge-static-capacity minus taskforge-fifo-static | conditional_p99_ms | -1.787e+04 | -1.781e+04, -1.792e+04 |
| dependency-pressure | taskforge-static-capacity minus taskforge-fifo-static | downstream_failure_rate | -0.8525 | -0.8487, -0.8564 |
| dependency-pressure | taskforge-static-capacity minus taskforge-fifo-static | downstream_over_capacity_rate | -0.9721 | -0.9722, -0.9721 |
| dependency-pressure | taskforge-static-capacity minus taskforge-fifo-static | ready_backlog_max | -984.5 | -978, -991 |
| dependency-pressure | taskforge-static-capacity minus taskforge-fifo-static | deferred_backlog_max | -6.5 | -6, -7 |
| dependency-pressure | taskforge-static-capacity minus taskforge-fifo-static | total_backlog_max | -1079 | -1073, -1085 |
| dependency-pressure | taskforge-static-capacity minus taskforge-fifo-static | controller_min | -4 | -4, -4 |
| dependency-pressure | taskforge-static-capacity minus taskforge-fifo-static | controller_max | -4 | -4, -4 |
| dependency-pressure | taskforge-static-capacity minus taskforge-fifo-static | redis_cpu_seconds | -1.071 | -0.97, -1.173 |
| dependency-pressure | taskforge-static-capacity minus taskforge-fifo-static | dispatch_p99_ms | 0.003577 | 0.001188, 0.005965 |
| adaptive-pressure | taskforge-adaptive-only minus taskforge-fifo-static | slo_success | 0.132 | 0.082, 0.1819 |
| adaptive-pressure | taskforge-adaptive-only minus taskforge-fifo-static | accepted_fraction | 0 | 0, 0 |
| adaptive-pressure | taskforge-adaptive-only minus taskforge-fifo-static | conditional_p99_ms | -1.621e+04 | -1.614e+04, -1.629e+04 |
| adaptive-pressure | taskforge-adaptive-only minus taskforge-fifo-static | downstream_failure_rate | -0.7542 | -0.7418, -0.7665 |
| adaptive-pressure | taskforge-adaptive-only minus taskforge-fifo-static | downstream_over_capacity_rate | -0.3479 | -0.3486, -0.3471 |
| adaptive-pressure | taskforge-adaptive-only minus taskforge-fifo-static | ready_backlog_max | -955 | -944, -966 |
| adaptive-pressure | taskforge-adaptive-only minus taskforge-fifo-static | deferred_backlog_max | -1 | 0, -2 |
| adaptive-pressure | taskforge-adaptive-only minus taskforge-fifo-static | total_backlog_max | -1048 | -1036, -1059 |
| adaptive-pressure | taskforge-adaptive-only minus taskforge-fifo-static | controller_min | -4 | -4, -4 |
| adaptive-pressure | taskforge-adaptive-only minus taskforge-fifo-static | controller_max | 0.5 | 1, 0 |
| adaptive-pressure | taskforge-adaptive-only minus taskforge-fifo-static | redis_cpu_seconds | -1.082 | -1.039, -1.125 |
| adaptive-pressure | taskforge-adaptive-only minus taskforge-fifo-static | dispatch_p99_ms | 0.002414 | 0.003432, 0.001396 |
| adaptive-pressure | taskforge-static-capacity minus taskforge-fifo-static | slo_success | 0.205 | 0.2101, 0.1999 |
| adaptive-pressure | taskforge-static-capacity minus taskforge-fifo-static | accepted_fraction | 0 | 0, 0 |
| adaptive-pressure | taskforge-static-capacity minus taskforge-fifo-static | conditional_p99_ms | -1.776e+04 | -1.783e+04, -1.769e+04 |
| adaptive-pressure | taskforge-static-capacity minus taskforge-fifo-static | downstream_failure_rate | -0.8512 | -0.845, -0.8574 |
| adaptive-pressure | taskforge-static-capacity minus taskforge-fifo-static | downstream_over_capacity_rate | -0.9722 | -0.9722, -0.9721 |
| adaptive-pressure | taskforge-static-capacity minus taskforge-fifo-static | ready_backlog_max | -986 | -992, -980 |
| adaptive-pressure | taskforge-static-capacity minus taskforge-fifo-static | deferred_backlog_max | -6 | -6, -6 |
| adaptive-pressure | taskforge-static-capacity minus taskforge-fifo-static | total_backlog_max | -1082 | -1088, -1075 |
| adaptive-pressure | taskforge-static-capacity minus taskforge-fifo-static | controller_min | -4 | -4, -4 |
| adaptive-pressure | taskforge-static-capacity minus taskforge-fifo-static | controller_max | -4 | -4, -4 |
| adaptive-pressure | taskforge-static-capacity minus taskforge-fifo-static | redis_cpu_seconds | -1.156 | -1.151, -1.16 |
| adaptive-pressure | taskforge-static-capacity minus taskforge-fifo-static | dispatch_p99_ms | 0.008212 | 0.00343, 0.01299 |
| stable-light | taskforge-fairness-only minus taskforge-fifo-static | slo_success | 0 | 0, 0 |
| stable-light | taskforge-fairness-only minus taskforge-fifo-static | accepted_fraction | 0 | 0, 0 |
| stable-light | taskforge-fairness-only minus taskforge-fifo-static | conditional_p99_ms | 0.7428 | 0.5135, 0.972 |
| stable-light | taskforge-fairness-only minus taskforge-fifo-static | downstream_failure_rate | 0 | 0, 0 |
| stable-light | taskforge-fairness-only minus taskforge-fifo-static | downstream_over_capacity_rate | 0 | 0, 0 |
| stable-light | taskforge-fairness-only minus taskforge-fifo-static | ready_backlog_max | 1 | 1, 1 |
| stable-light | taskforge-fairness-only minus taskforge-fifo-static | deferred_backlog_max | 0 | 0, 0 |
| stable-light | taskforge-fairness-only minus taskforge-fifo-static | total_backlog_max | 1 | 1, 1 |
| stable-light | taskforge-fairness-only minus taskforge-fifo-static | controller_min | 0 | 0, 0 |
| stable-light | taskforge-fairness-only minus taskforge-fifo-static | controller_max | 0 | 0, 0 |
| stable-light | taskforge-fairness-only minus taskforge-fifo-static | redis_cpu_seconds | 0.0684 | 0.06172, 0.07507 |
| stable-light | taskforge-fairness-only minus taskforge-fifo-static | dispatch_p99_ms | -0.001211 | -0.004641, 0.002219 |
| stable-light | taskforge-admission-only minus taskforge-fifo-static | slo_success | 0 | 0, 0 |
| stable-light | taskforge-admission-only minus taskforge-fifo-static | accepted_fraction | 0 | 0, 0 |
| stable-light | taskforge-admission-only minus taskforge-fifo-static | conditional_p99_ms | 0.4407 | 0.4667, 0.4147 |
| stable-light | taskforge-admission-only minus taskforge-fifo-static | downstream_failure_rate | 0 | 0, 0 |
| stable-light | taskforge-admission-only minus taskforge-fifo-static | downstream_over_capacity_rate | 0 | 0, 0 |
| stable-light | taskforge-admission-only minus taskforge-fifo-static | ready_backlog_max | 0 | 0, 0 |
| stable-light | taskforge-admission-only minus taskforge-fifo-static | deferred_backlog_max | 0 | 0, 0 |
| stable-light | taskforge-admission-only minus taskforge-fifo-static | total_backlog_max | 0 | 0, 0 |
| stable-light | taskforge-admission-only minus taskforge-fifo-static | controller_min | 0 | 0, 0 |
| stable-light | taskforge-admission-only minus taskforge-fifo-static | controller_max | 0 | 0, 0 |
| stable-light | taskforge-admission-only minus taskforge-fifo-static | redis_cpu_seconds | 0.04434 | 0.04221, 0.04648 |
| stable-light | taskforge-admission-only minus taskforge-fifo-static | dispatch_p99_ms | 0.01477 | 0.008204, 0.02134 |
| stable-light | taskforge-budget-only minus taskforge-fifo-static | slo_success | 0 | 0, 0 |
| stable-light | taskforge-budget-only minus taskforge-fifo-static | accepted_fraction | 0 | 0, 0 |
| stable-light | taskforge-budget-only minus taskforge-fifo-static | conditional_p99_ms | 0.1044 | 0.1009, 0.108 |
| stable-light | taskforge-budget-only minus taskforge-fifo-static | downstream_failure_rate | 0 | 0, 0 |
| stable-light | taskforge-budget-only minus taskforge-fifo-static | downstream_over_capacity_rate | 0 | 0, 0 |
| stable-light | taskforge-budget-only minus taskforge-fifo-static | ready_backlog_max | 0 | 0, 0 |
| stable-light | taskforge-budget-only minus taskforge-fifo-static | deferred_backlog_max | 0 | 0, 0 |
| stable-light | taskforge-budget-only minus taskforge-fifo-static | total_backlog_max | 0 | 0, 0 |
| stable-light | taskforge-budget-only minus taskforge-fifo-static | controller_min | 0 | 0, 0 |
| stable-light | taskforge-budget-only minus taskforge-fifo-static | controller_max | 0 | 0, 0 |
| stable-light | taskforge-budget-only minus taskforge-fifo-static | redis_cpu_seconds | 0.04382 | 0.05081, 0.03683 |
| stable-light | taskforge-budget-only minus taskforge-fifo-static | dispatch_p99_ms | 0.00476 | 0.003074, 0.006446 |
| stable-light | taskforge-adaptive-only minus taskforge-fifo-static | slo_success | 0 | 0, 0 |
| stable-light | taskforge-adaptive-only minus taskforge-fifo-static | accepted_fraction | 0 | 0, 0 |
| stable-light | taskforge-adaptive-only minus taskforge-fifo-static | conditional_p99_ms | -0.1096 | -0.1152, -0.1039 |
| stable-light | taskforge-adaptive-only minus taskforge-fifo-static | downstream_failure_rate | 0 | 0, 0 |
| stable-light | taskforge-adaptive-only minus taskforge-fifo-static | downstream_over_capacity_rate | 0 | 0, 0 |
| stable-light | taskforge-adaptive-only minus taskforge-fifo-static | ready_backlog_max | 0 | 0, 0 |
| stable-light | taskforge-adaptive-only minus taskforge-fifo-static | deferred_backlog_max | 0 | 0, 0 |
| stable-light | taskforge-adaptive-only minus taskforge-fifo-static | total_backlog_max | 0 | 0, 0 |
| stable-light | taskforge-adaptive-only minus taskforge-fifo-static | controller_min | 0 | 0, 0 |
| stable-light | taskforge-adaptive-only minus taskforge-fifo-static | controller_max | 0 | 0, 0 |
| stable-light | taskforge-adaptive-only minus taskforge-fifo-static | redis_cpu_seconds | 0.001526 | 0.003291, -0.000239 |
| stable-light | taskforge-adaptive-only minus taskforge-fifo-static | dispatch_p99_ms | -0.001296 | -0.004771, 0.00218 |
| fragile-light | taskforge-budget-only minus taskforge-fifo-static | slo_success | 0 | 0, 0 |
| fragile-light | taskforge-budget-only minus taskforge-fifo-static | accepted_fraction | 0 | 0, 0 |
| fragile-light | taskforge-budget-only minus taskforge-fifo-static | conditional_p99_ms | 0.1568 | 0.2086, 0.105 |
| fragile-light | taskforge-budget-only minus taskforge-fifo-static | downstream_failure_rate | 0 | 0, 0 |
| fragile-light | taskforge-budget-only minus taskforge-fifo-static | downstream_over_capacity_rate | 0 | 0, 0 |
| fragile-light | taskforge-budget-only minus taskforge-fifo-static | ready_backlog_max | 0 | 0, 0 |
| fragile-light | taskforge-budget-only minus taskforge-fifo-static | deferred_backlog_max | 0 | 0, 0 |
| fragile-light | taskforge-budget-only minus taskforge-fifo-static | total_backlog_max | 0 | 0, 0 |
| fragile-light | taskforge-budget-only minus taskforge-fifo-static | controller_min | 0 | 0, 0 |
| fragile-light | taskforge-budget-only minus taskforge-fifo-static | controller_max | 0 | 0, 0 |
| fragile-light | taskforge-budget-only minus taskforge-fifo-static | redis_cpu_seconds | 0.03966 | 0.04141, 0.03791 |
| fragile-light | taskforge-budget-only minus taskforge-fifo-static | dispatch_p99_ms | 0.0025 | 0.003791, 0.00121 |
| fragile-light | taskforge-adaptive-only minus taskforge-fifo-static | slo_success | 0 | 0, 0 |
| fragile-light | taskforge-adaptive-only minus taskforge-fifo-static | accepted_fraction | 0 | 0, 0 |
| fragile-light | taskforge-adaptive-only minus taskforge-fifo-static | conditional_p99_ms | 0.02688 | -0.01946, 0.07322 |
| fragile-light | taskforge-adaptive-only minus taskforge-fifo-static | downstream_failure_rate | 0 | 0, 0 |
| fragile-light | taskforge-adaptive-only minus taskforge-fifo-static | downstream_over_capacity_rate | 0 | 0, 0 |
| fragile-light | taskforge-adaptive-only minus taskforge-fifo-static | ready_backlog_max | 0 | 0, 0 |
| fragile-light | taskforge-adaptive-only minus taskforge-fifo-static | deferred_backlog_max | 0 | 0, 0 |
| fragile-light | taskforge-adaptive-only minus taskforge-fifo-static | total_backlog_max | 0 | 0, 0 |
| fragile-light | taskforge-adaptive-only minus taskforge-fifo-static | controller_min | 0 | 0, 0 |
| fragile-light | taskforge-adaptive-only minus taskforge-fifo-static | controller_max | 0 | 0, 0 |
| fragile-light | taskforge-adaptive-only minus taskforge-fifo-static | redis_cpu_seconds | 0.008518 | 0.009007, 0.00803 |
| fragile-light | taskforge-adaptive-only minus taskforge-fifo-static | dispatch_p99_ms | 0.006617 | -0.005664, 0.0189 |
| fragile-light | taskforge-static-capacity minus taskforge-fifo-static | slo_success | 0 | 0, 0 |
| fragile-light | taskforge-static-capacity minus taskforge-fifo-static | accepted_fraction | 0 | 0, 0 |
| fragile-light | taskforge-static-capacity minus taskforge-fifo-static | conditional_p99_ms | -0.02266 | -0.0663, 0.02099 |
| fragile-light | taskforge-static-capacity minus taskforge-fifo-static | downstream_failure_rate | 0 | 0, 0 |
| fragile-light | taskforge-static-capacity minus taskforge-fifo-static | downstream_over_capacity_rate | 0 | 0, 0 |
| fragile-light | taskforge-static-capacity minus taskforge-fifo-static | ready_backlog_max | 0 | 0, 0 |
| fragile-light | taskforge-static-capacity minus taskforge-fifo-static | deferred_backlog_max | 0 | 0, 0 |
| fragile-light | taskforge-static-capacity minus taskforge-fifo-static | total_backlog_max | 0 | 0, 0 |
| fragile-light | taskforge-static-capacity minus taskforge-fifo-static | controller_min | -4 | -4, -4 |
| fragile-light | taskforge-static-capacity minus taskforge-fifo-static | controller_max | -4 | -4, -4 |
| fragile-light | taskforge-static-capacity minus taskforge-fifo-static | redis_cpu_seconds | 0.000833 | 0.006023, -0.004357 |
| fragile-light | taskforge-static-capacity minus taskforge-fifo-static | dispatch_p99_ms | 0.000829 | -0.001432, 0.00309 |

These are descriptive runs on one host. They test the controls under fixed offered load and modeled downstream behavior; they do not establish production SLOs or general effects.
