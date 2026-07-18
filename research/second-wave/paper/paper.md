# Overload controls under paired load: a registered two-class TaskForge study

## Abstract

We evaluate Redis-backed background-task execution using immutable open-loop
traces and paired system contrasts. The design separates a common successful
delivery comparison with tuned Asynq from TaskForge-only capability ablations.
It measures throughput together with protected-tenant SLO attainment,
entitlement-normalized service deficit, downstream overload, normalized Redis
cost, and explicitly unsupported recovery cells. The scope is one physical
workstation under two measured execution/network classes, not a multi-host or
remote-cloud claim.

## Method

The frozen plan, workload profiles, trace corpus, and digest lock precede the
registered result corpus. Each block fixes environment, profile, seed, and
repetition. Every arm in a block receives identical arrivals and failure draws,
and system order is deterministically counterbalanced. Primary intervals use
the registered Bonferroni coverage; exploratory intervals use 95% coverage.
Asynq is tuned at concurrency 16 with 10ms task and delayed-task polling. River
is excluded because changing from Redis to PostgreSQL would confound persistence
and delivery contract with queue implementation.

## Paired second-wave results

Effects are left minus right within the registered environment/profile/seed/repetition blocks. Intervals use the pre-declared confidence shown; they are not detection counts.

### common-contract — native-local-direct

taskforge-fifo-static versus asynq; profiles: common-below, common-knee, common-overload.

| Metric | Pairs | Paired median effect | Interval | Relative | Standardized | Left higher |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| throughput_per_second | 6 | 2.11468 tasks_per_second | [0.587616, 7.94917] (0.975) | 0.15% | 0.780 | 83.3% |
| cost_per_slo_completion | 6 | 1.87015e-07 normalized_cost_units | [4.87586e-08, 2.14486e-07] (0.975) | 32.53% | 1.880 | 100.0% |

### common-contract — constrained-emulated-network

taskforge-fifo-static versus asynq; profiles: common-below, common-knee, common-overload.

| Metric | Pairs | Paired median effect | Interval | Relative | Standardized | Left higher |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| throughput_per_second | 6 | 704.729 tasks_per_second | [159.324, 1048.35] (0.975) | 248.00% | 1.589 | 100.0% |
| cost_per_slo_completion | 6 | 0 normalized_cost_units | [-3.98815e-06, 7.38542e-07] (0.975) | not defined | -0.471 | 33.3% |

### admission-capability — native-local-direct

taskforge-full versus taskforge-no-admission; profiles: feature-below, feature-knee, feature-overload.

| Metric | Pairs | Paired median effect | Interval | Relative | Standardized | Left higher |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| protected_slo_attainment | 6 | 0 proportion | [0, 0.153846] (0.99) | 0.00% | 0.408 | 16.7% |
| max_normalized_service_deficit | 6 | 0 proportion | [0, 0] (0.99) | 0.00% | 0.000 | 0.0% |
| downstream_over_capacity_rate | 6 | 0 proportion | [0, 0] (0.99) | not defined | 0.000 | 0.0% |
| throughput_per_second | 6 | -0.00423966 tasks_per_second | [-1.60264, 3.93347] (0.99) | -0.02% | 0.211 | 33.3% |
| cost_per_slo_completion | 6 | 3.45523e-05 normalized_cost_units | [1.39679e-05, 0.000451565] (0.99) | 87.70% | 0.768 | 100.0% |

### admission-capability — constrained-emulated-network

taskforge-full versus taskforge-no-admission; profiles: feature-below, feature-knee, feature-overload.

| Metric | Pairs | Paired median effect | Interval | Relative | Standardized | Left higher |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| protected_slo_attainment | 6 | 0 proportion | [0, 0.428571] (0.99) | not defined | 0.408 | 16.7% |
| max_normalized_service_deficit | 6 | 0 proportion | [0, 0] (0.99) | 0.00% | 0.000 | 0.0% |
| downstream_over_capacity_rate | 6 | 0 proportion | [0, 0] (0.99) | not defined | 0.000 | 0.0% |
| throughput_per_second | 6 | -0.127138 tasks_per_second | [-3.56255, 4.47065] (0.99) | -0.78% | 0.024 | 33.3% |
| cost_per_slo_completion | 6 | 2.14124e-05 normalized_cost_units | [-0.0050381, 9.1962e-05] (0.99) | 30.51% | -0.574 | 66.7% |

### budget-capability — native-local-direct

taskforge-full versus taskforge-no-dependency-budget; profiles: feature-below, feature-knee, feature-overload.

| Metric | Pairs | Paired median effect | Interval | Relative | Standardized | Left higher |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| downstream_over_capacity_rate | 6 | -0.0302654 proportion | [-0.875369, 0] (0.983) | not defined | -0.680 | 0.0% |
| downstream_failure_rate | 6 | 0 proportion | [-0.0645472, 0] (0.983) | 0.00% | -0.639 | 0.0% |
| throughput_per_second | 6 | -0.00764869 tasks_per_second | [-42.3888, 0.0227715] (0.983) | -0.03% | -0.611 | 16.7% |

### budget-capability — constrained-emulated-network

taskforge-full versus taskforge-no-dependency-budget; profiles: feature-below, feature-knee, feature-overload.

| Metric | Pairs | Paired median effect | Interval | Relative | Standardized | Left higher |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| downstream_over_capacity_rate | 6 | 0 proportion | [0, 0] (0.983) | not defined | 0.000 | 0.0% |
| downstream_failure_rate | 6 | 0 proportion | [0, 0] (0.983) | 0.00% | 0.000 | 0.0% |
| throughput_per_second | 6 | -0.0457723 tasks_per_second | [-1.13424, 0.0192727] (0.983) | -0.16% | -0.590 | 50.0% |

### fairness-capability — native-local-direct

taskforge-full versus taskforge-no-fairness; profiles: feature-below, feature-knee, feature-overload.

| Metric | Pairs | Paired median effect | Interval | Relative | Standardized | Left higher |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| protected_slo_attainment | 6 | 0 proportion | [-0.307692, 0] (0.983) | 0.00% | -0.619 | 0.0% |
| max_normalized_service_deficit | 6 | 0 proportion | [0, 0.0382775] (0.983) | 0.00% | 0.645 | 33.3% |
| jain_slo_equality | 6 | 0 proportion | [-0.852385, 0] (0.983) | 0.00% | -0.645 | 0.0% |

### fairness-capability — constrained-emulated-network

taskforge-full versus taskforge-no-fairness; profiles: feature-below, feature-knee, feature-overload.

| Metric | Pairs | Paired median effect | Interval | Relative | Standardized | Left higher |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| protected_slo_attainment | 6 | -0.247596 proportion | [-1, 0] (0.983) | -78.57% | -0.893 | 0.0% |
| max_normalized_service_deficit | 6 | 0.0198276 proportion | [0, 0.0518323] (0.983) | 2.06% | 0.896 | 50.0% |
| jain_slo_equality | 6 | -0.825561 proportion | [-0.906405, 0] (0.983) | -90.09% | -1.284 | 0.0% |

### long-duration-admission — native-local-direct

taskforge-full versus taskforge-no-admission; profiles: feature-long-overload.

| Metric | Pairs | Paired median effect | Interval | Relative | Standardized | Left higher |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| protected_slo_attainment | 2 | -0.509286 proportion | [-0.559242, -0.45933] (0.95) | -56.53% | -7.209 | 0.0% |
| max_normalized_service_deficit | 2 | 0.224711 proportion | [0.168948, 0.280474] (0.95) | 35.08% | 2.849 | 100.0% |
| downstream_over_capacity_rate | 2 | 0 proportion | [0, 0] (0.95) | not defined | 0.000 | 0.0% |
| throughput_per_second | 2 | -2.23417 tasks_per_second | [-2.50879, -1.95955] (0.95) | -3.81% | -5.753 | 0.0% |
| cost_per_slo_completion | 2 | 0.00331682 normalized_cost_units | [0.00303536, 0.00359828] (0.95) | 389.68% | 8.333 | 100.0% |

### long-duration-admission — constrained-emulated-network

taskforge-full versus taskforge-no-admission; profiles: feature-long-overload.

| Metric | Pairs | Paired median effect | Interval | Relative | Standardized | Left higher |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| protected_slo_attainment | 2 | -0.0554094 proportion | [-0.191388, 0.0805687] (0.95) | 16.75% | -0.288 | 50.0% |
| max_normalized_service_deficit | 2 | -0.133969 proportion | [-0.150291, -0.117647] (0.95) | -13.40% | -5.804 | 0.0% |
| downstream_over_capacity_rate | 2 | 0 proportion | [0, 0] (0.95) | not defined | 0.000 | 0.0% |
| throughput_per_second | 2 | -8.39491 tasks_per_second | [-8.80922, -7.9806] (0.95) | -28.90% | -14.328 | 0.0% |
| cost_per_slo_completion | 2 | -0.0195661 normalized_cost_units | [-0.0344282, -0.00470401] (0.95) | -72.96% | -0.931 | 0.0% |

### Failures and unsupported cells

| Status | Environment | Profile | Seed | Repetition | System | Reason |
| --- | --- | --- | ---: | ---: | --- | --- |
| not_measured | native-local-direct | recovery-contract | 20260718 | 0 | taskforge-fifo-static | trace contains worker faults without equivalent crash/recovery and delivery semantics |
| not_measured | native-local-direct | recovery-contract | 20260718 | 0 | asynq | trace contains worker faults without equivalent crash/recovery and delivery semantics |
| not_measured | native-local-direct | recovery-contract | 20260719 | 0 | taskforge-fifo-static | trace contains worker faults without equivalent crash/recovery and delivery semantics |
| not_measured | native-local-direct | recovery-contract | 20260719 | 0 | asynq | trace contains worker faults without equivalent crash/recovery and delivery semantics |
| not_measured | constrained-emulated-network | recovery-contract | 20260718 | 0 | taskforge-fifo-static | trace contains worker faults without equivalent crash/recovery and delivery semantics |
| not_measured | constrained-emulated-network | recovery-contract | 20260718 | 0 | asynq | trace contains worker faults without equivalent crash/recovery and delivery semantics |
| not_measured | constrained-emulated-network | recovery-contract | 20260719 | 0 | taskforge-fifo-static | trace contains worker faults without equivalent crash/recovery and delivery semantics |
| not_measured | constrained-emulated-network | recovery-contract | 20260719 | 0 | asynq | trace contains worker faults without equivalent crash/recovery and delivery semantics |

### Environment-specific reversals

| Contrast | Metric | First environment/effect | Second environment/effect |
| --- | --- | ---: | ---: |
| long-duration-admission | max_normalized_service_deficit | native-local-direct: 0.224711 | constrained-emulated-network: -0.133969 |
| long-duration-admission | cost_per_slo_completion | native-local-direct: 0.00331682 | constrained-emulated-network: -0.0195661 |

### Scope boundary

The measurements cover one 12-logical-CPU workstation in native/direct-loopback and four-Go-processor/latency-injected-loopback classes. The network class is emulated and the physical host is shared; these results do not establish behavior on remote Redis, other hardware, or independent hosts. Recovery is reported as not measured where the adapters cannot apply an equivalent process-kill delivery fault.


## Threats to validity

Both classes share one physical host. The networked class is a declared
latency-injected loopback proxy, not remote infrastructure. Redis telemetry
cost excludes worker-process CPU. Two seeds limit interval resolution, and
profile labels describe intended below-knee-overload regimes even if an
environment shifts the empirical knee. Process-kill-equivalent recovery is
not implemented by both adapters, so those registered cells are retained as
not measured rather than treated as zero or omitted.
