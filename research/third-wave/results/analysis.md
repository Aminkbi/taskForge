# Completed control-plane benchmark

Negative values indicate lower treatment cost. Intervals are paired bootstrap descriptive intervals; family-wise alpha is recorded per comparison.

| Family | Benchmark | Metric | Baseline median | Treatment median | Change | 95% interval |
| --- | --- | --- | ---: | ---: | ---: | --- |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_false-16` | `ns/op` | 9.93e+04 | 1.12e+05 | +13.5% | [+1.5%, +23.6%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_false-16` | `redis_commands/op` | 1 | 1 | +0.0% | [+0.0%, +0.0%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_false-16` | `redis_network_bytes/op` | 664 | 724 | +9.0% | [+9.0%, +9.1%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_false-16` | `redis_round_trips/op` | 1 | 1 | +0.0% | [+0.0%, +0.0%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_false-16` | `B/op` | 4.38e+03 | 4.48e+03 | +2.1% | [+1.0%, +2.5%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_false-16` | `allocs/op` | 61 | 56 | -8.2% | [-8.2%, -8.2%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_true-16` | `ns/op` | 2.21e+05 | 1.92e+05 | -17.8% | [-22.9%, -12.3%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_true-16` | `redis_commands/op` | 3 | 2 | -33.3% | [-33.3%, -33.3%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_true-16` | `redis_network_bytes/op` | 974 | 946 | -2.9% | [-2.9%, -2.8%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_true-16` | `redis_round_trips/op` | 3 | 2 | -33.3% | [-33.3%, -33.3%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_true-16` | `B/op` | 5.15e+03 | 5.15e+03 | -0.0% | [-0.9%, +0.8%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_true-16` | `allocs/op` | 83 | 70 | -15.7% | [-15.7%, -15.7%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_false-16` | `ns/op` | 1.54e+05 | 1.01e+05 | -34.8% | [-39.3%, -32.3%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_false-16` | `redis_commands/op` | 7 | 1 | -85.7% | [-85.7%, -85.7%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_false-16` | `redis_network_bytes/op` | 1.11e+03 | 887 | -20.2% | [-20.2%, -20.2%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_false-16` | `redis_round_trips/op` | 2 | 1 | -50.0% | [-50.0%, -50.0%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_false-16` | `B/op` | 6.62e+03 | 4.83e+03 | -27.2% | [-27.8%, -26.9%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_false-16` | `allocs/op` | 110 | 63 | -42.7% | [-42.7%, -42.7%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_true-16` | `ns/op` | 2.25e+05 | 1.71e+05 | -24.8% | [-29.8%, -17.1%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_true-16` | `redis_commands/op` | 3 | 2 | -33.3% | [-33.3%, -33.3%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_true-16` | `redis_network_bytes/op` | 1.19e+03 | 1.11e+03 | -7.0% | [-7.0%, -7.0%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_true-16` | `redis_round_trips/op` | 3 | 2 | -33.3% | [-33.3%, -33.3%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_true-16` | `B/op` | 5.69e+03 | 5.5e+03 | -3.3% | [-3.6%, -2.8%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_true-16` | `allocs/op` | 96 | 77 | -19.8% | [-19.8%, -19.8%] |
| publish | `BenchmarkPublishThroughput-16` | `ns/op` | 9.97e+04 | 9.85e+04 | +0.5% | [-5.0%, +5.9%] |
| publish | `BenchmarkPublishThroughput-16` | `B/op` | 4.35e+03 | 4.43e+03 | +1.7% | [+0.3%, +3.2%] |
| publish | `BenchmarkPublishThroughput-16` | `allocs/op` | 61 | 56 | -8.2% | [-8.2%, -8.2%] |
| setup-key | `BenchmarkSetupKeyCosts/cached_group-16` | `ns/op` | 797 | 795 | -0.8% | [-4.8%, +4.5%] |
| setup-key | `BenchmarkSetupKeyCosts/cached_group-16` | `B/op` | 70 | 70 | +0.0% | [+0.0%, +0.0%] |
| setup-key | `BenchmarkSetupKeyCosts/cached_group-16` | `allocs/op` | 1 | 1 | +0.0% | [+0.0%, +0.0%] |
| setup-key | `BenchmarkSetupKeyCosts/three_keys-16` | `ns/op` | 698 | 286 | -57.8% | [-70.3%, -52.2%] |
| setup-key | `BenchmarkSetupKeyCosts/three_keys-16` | `B/op` | 286 | 104 | -63.6% | [-63.6%, -63.6%] |
| setup-key | `BenchmarkSetupKeyCosts/three_keys-16` | `allocs/op` | 9 | 3 | -66.7% | [-66.7%, -66.7%] |
| setup-key | `BenchmarkSetupKeyCosts/uncached_group-16` | `ns/op` | 6e+04 | 5.57e+04 | -2.5% | [-16.3%, +1.8%] |
| setup-key | `BenchmarkSetupKeyCosts/uncached_group-16` | `B/op` | 750 | 750 | +0.0% | [-0.3%, +0.0%] |
| setup-key | `BenchmarkSetupKeyCosts/uncached_group-16` | `allocs/op` | 32 | 32 | +0.0% | [+0.0%, +0.0%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_256-16` | `ns/op` | 6.95e+05 | 2.43e+05 | -64.0% | [-66.4%, -62.8%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_256-16` | `redis_commands/op` | 11 | 5 | -54.5% | [-54.5%, -54.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_256-16` | `redis_network_bytes/op` | 6.5e+03 | 1.64e+03 | -74.9% | [-74.9%, -74.9%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_256-16` | `redis_round_trips/op` | 10 | 4 | -60.0% | [-60.0%, -60.0%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_256-16` | `B/op` | 1.51e+04 | 4.77e+03 | -68.4% | [-69.2%, -68.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_256-16` | `allocs/op` | 282 | 83 | -70.6% | [-70.6%, -70.6%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_65536-16` | `ns/op` | 1.43e+06 | 5.35e+05 | -62.8% | [-63.8%, -61.6%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_65536-16` | `redis_commands/op` | 11 | 5 | -54.5% | [-54.5%, -54.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_65536-16` | `redis_network_bytes/op` | 5.29e+05 | 8.87e+04 | -83.2% | [-83.2%, -83.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_65536-16` | `redis_round_trips/op` | 10 | 4 | -60.0% | [-60.0%, -60.0%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_65536-16` | `B/op` | 8.64e+05 | 2.49e+05 | -71.1% | [-71.1%, -71.1%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_65536-16` | `allocs/op` | 286 | 83 | -71.0% | [-71.1%, -70.6%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_256-16` | `ns/op` | 7.87e+06 | 2.55e+06 | -68.4% | [-69.7%, -67.4%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_256-16` | `redis_commands/op` | 146 | 65 | -55.5% | [-55.5%, -55.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_256-16` | `redis_network_bytes/op` | 1.01e+05 | 2.43e+04 | -76.0% | [-76.0%, -76.0%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_256-16` | `redis_round_trips/op` | 115 | 34 | -70.4% | [-70.4%, -70.4%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_256-16` | `B/op` | 2.28e+05 | 6.65e+04 | -70.8% | [-70.8%, -70.7%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_256-16` | `allocs/op` | 4.09e+03 | 1.05e+03 | -74.3% | [-74.3%, -74.3%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_65536-16` | `ns/op` | 2.01e+07 | 6.33e+06 | -68.6% | [-69.0%, -68.1%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_65536-16` | `redis_commands/op` | 146 | 65 | -55.5% | [-55.5%, -55.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_65536-16` | `redis_network_bytes/op` | 8.46e+06 | 1.42e+06 | -83.2% | [-83.2%, -83.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_65536-16` | `redis_round_trips/op` | 115 | 34 | -70.4% | [-70.4%, -70.4%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_65536-16` | `B/op` | 1.38e+07 | 3.99e+06 | -71.2% | [-71.2%, -71.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_65536-16` | `allocs/op` | 4.15e+03 | 1.06e+03 | -74.5% | [-74.5%, -74.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_256-16` | `ns/op` | 3.02e+07 | 8.64e+06 | -71.4% | [-71.6%, -71.0%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_256-16` | `redis_commands/op` | 578 | 257 | -55.5% | [-55.5%, -55.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_256-16` | `redis_network_bytes/op` | 4.05e+05 | 9.7e+04 | -76.1% | [-76.1%, -76.1%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_256-16` | `redis_round_trips/op` | 451 | 130 | -71.2% | [-71.2%, -71.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_256-16` | `B/op` | 9.08e+05 | 2.67e+05 | -70.6% | [-70.6%, -70.6%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_256-16` | `allocs/op` | 1.62e+04 | 4.12e+03 | -74.6% | [-74.6%, -74.6%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_65536-16` | `ns/op` | 7.67e+07 | 2.52e+07 | -67.2% | [-67.8%, -65.3%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_65536-16` | `redis_commands/op` | 578 | 257 | -55.5% | [-55.5%, -55.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_65536-16` | `redis_network_bytes/op` | 3.38e+07 | 5.67e+06 | -83.2% | [-83.2%, -83.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_65536-16` | `redis_round_trips/op` | 451 | 130 | -71.2% | [-71.2%, -71.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_65536-16` | `B/op` | 5.52e+07 | 1.59e+07 | -71.1% | [-71.1%, -71.1%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_65536-16` | `allocs/op` | 1.64e+04 | 4.16e+03 | -74.6% | [-74.6%, -74.6%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_256-16` | `ns/op` | 4.07e+05 | 1.17e+05 | -70.1% | [-73.3%, -69.5%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_256-16` | `redis_commands/op` | 6 | 4 | -33.3% | [-33.3%, -33.3%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_256-16` | `redis_network_bytes/op` | 3.45e+03 | 902 | -73.8% | [-73.8%, -73.8%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_256-16` | `redis_round_trips/op` | 6 | 2 | -66.7% | [-66.7%, -66.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_256-16` | `B/op` | 8.08e+03 | 2.05e+03 | -74.6% | [-74.7%, -74.1%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_256-16` | `allocs/op` | 165 | 45 | -72.7% | [-72.7%, -72.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_65536-16` | `ns/op` | 7.86e+05 | 1.22e+05 | -84.5% | [-85.5%, -83.2%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_65536-16` | `redis_commands/op` | 6 | 4 | -33.3% | [-33.3%, -33.3%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_65536-16` | `redis_network_bytes/op` | 2.65e+05 | 902 | -99.7% | [-99.7%, -99.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_65536-16` | `redis_round_trips/op` | 6 | 2 | -66.7% | [-66.7%, -66.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_65536-16` | `B/op` | 4.33e+05 | 2.05e+03 | -99.5% | [-99.5%, -99.5%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_65536-16` | `allocs/op` | 167 | 45 | -73.1% | [-73.1%, -72.9%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_256-16` | `ns/op` | 5.34e+06 | 2.37e+05 | -95.6% | [-95.6%, -95.3%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_256-16` | `redis_commands/op` | 81 | 49 | -39.5% | [-39.5%, -39.5%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_256-16` | `redis_network_bytes/op` | 5.33e+04 | 1.26e+04 | -76.4% | [-76.4%, -76.4%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_256-16` | `redis_round_trips/op` | 81 | 2 | -97.5% | [-97.5%, -97.5%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_256-16` | `B/op` | 1.22e+05 | 2.67e+04 | -78.1% | [-78.1%, -78.1%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_256-16` | `allocs/op` | 2.52e+03 | 394 | -84.4% | [-84.4%, -84.4%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_65536-16` | `ns/op` | 1.08e+07 | 2.3e+05 | -97.9% | [-98.0%, -97.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_65536-16` | `redis_commands/op` | 81 | 49 | -39.5% | [-39.5%, -39.5%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_65536-16` | `redis_network_bytes/op` | 4.23e+06 | 1.26e+04 | -99.7% | [-99.7%, -99.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_65536-16` | `redis_round_trips/op` | 81 | 2 | -97.5% | [-97.5%, -97.5%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_65536-16` | `B/op` | 6.92e+06 | 2.67e+04 | -99.6% | [-99.6%, -99.6%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_65536-16` | `allocs/op` | 2.56e+03 | 394 | -84.6% | [-84.6%, -84.6%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_256-16` | `ns/op` | 2.12e+07 | 4.79e+05 | -97.7% | [-97.8%, -97.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_256-16` | `redis_commands/op` | 321 | 193 | -39.9% | [-39.9%, -39.9%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_256-16` | `redis_network_bytes/op` | 2.13e+05 | 5e+04 | -76.6% | [-76.6%, -76.6%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_256-16` | `redis_round_trips/op` | 321 | 2 | -99.4% | [-99.4%, -99.4%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_256-16` | `B/op` | 4.86e+05 | 1.06e+05 | -78.1% | [-78.1%, -78.1%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_256-16` | `allocs/op` | 1.01e+04 | 1.5e+03 | -85.1% | [-85.1%, -85.1%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_65536-16` | `ns/op` | 4.33e+07 | 5.1e+05 | -98.8% | [-98.9%, -98.8%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_65536-16` | `redis_commands/op` | 321 | 193 | -39.9% | [-39.9%, -39.9%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_65536-16` | `redis_network_bytes/op` | 1.69e+07 | 5e+04 | -99.7% | [-99.7%, -99.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_65536-16` | `redis_round_trips/op` | 321 | 2 | -99.4% | [-99.4%, -99.4%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_65536-16` | `B/op` | 2.77e+07 | 1.06e+05 | -99.6% | [-99.6%, -99.6%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_65536-16` | `allocs/op` | 1.02e+04 | 1.5e+03 | -85.3% | [-85.3%, -85.3%] |

## Decision summary

- **publish**: 20/27 comparisons lower; 0 exceeded the 15% regression guard.
- **setup-key**: 5/9 comparisons lower; 0 exceeded the 15% regression guard.
- **snapshot**: 72/72 comparisons lower; 0 exceeded the 15% regression guard.
