# Completed control-plane benchmark

Negative values indicate lower treatment cost. Changes are medians of within-index paired percentage changes. The 95% intervals are descriptive; family-wise intervals use the Bonferroni tail for each comparison family.

| Family | Benchmark | Metric | Baseline median | Treatment median | Change | 95% interval | Family-wise interval |
| --- | --- | --- | ---: | ---: | ---: | --- | --- |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_false-16` | `ns/op` | 9.87e+04 | 9.75e+04 | +0.6% | [-7.8%, +16.3%] | [-12.3%, +24.9%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_false-16` | `redis_commands/op` | 1 | 1 | +0.0% | [+0.0%, +0.0%] | [+0.0%, +0.0%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_false-16` | `redis_network_bytes/op` | 664 | 724 | +9.0% | [+9.0%, +9.1%] | [+9.0%, +9.1%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_false-16` | `redis_round_trips/op` | 1 | 1 | +0.0% | [+0.0%, +0.0%] | [+0.0%, +0.0%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_false-16` | `B/op` | 4.4e+03 | 4.47e+03 | +1.5% | [+0.7%, +1.8%] | [+0.3%, +2.2%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_false-16` | `allocs/op` | 61 | 56 | -8.2% | [-8.2%, -8.2%] | [-8.2%, -8.2%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_true-16` | `ns/op` | 2.24e+05 | 1.56e+05 | -26.8% | [-34.9%, -21.7%] | [-39.7%, -20.6%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_true-16` | `redis_commands/op` | 3 | 2 | -33.3% | [-33.3%, -33.3%] | [-33.3%, -33.3%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_true-16` | `redis_network_bytes/op` | 974 | 946 | -2.9% | [-2.9%, -2.8%] | [-2.9%, -2.8%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_true-16` | `redis_round_trips/op` | 3 | 2 | -33.3% | [-33.3%, -33.3%] | [-33.3%, -33.3%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_true-16` | `B/op` | 5.15e+03 | 5.15e+03 | -0.5% | [-1.3%, +0.1%] | [-1.4%, +0.9%] |
| publish | `BenchmarkPublishStateCosts/fair_false/dedup_true-16` | `allocs/op` | 83 | 70 | -15.7% | [-15.7%, -15.7%] | [-15.7%, -15.7%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_false-16` | `ns/op` | 1.68e+05 | 1.04e+05 | -38.4% | [-42.4%, -34.7%] | [-45.3%, -32.8%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_false-16` | `redis_commands/op` | 7 | 1 | -85.7% | [-85.7%, -85.7%] | [-85.7%, -85.7%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_false-16` | `redis_network_bytes/op` | 1.11e+03 | 887 | -20.2% | [-20.2%, -20.2%] | [-20.2%, -20.2%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_false-16` | `redis_round_trips/op` | 2 | 1 | -50.0% | [-50.0%, -50.0%] | [-50.0%, -50.0%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_false-16` | `B/op` | 6.61e+03 | 4.81e+03 | -27.3% | [-27.8%, -26.8%] | [-27.9%, -26.6%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_false-16` | `allocs/op` | 110 | 63 | -42.7% | [-42.7%, -42.7%] | [-42.7%, -42.7%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_true-16` | `ns/op` | 2.43e+05 | 1.72e+05 | -26.3% | [-32.0%, -23.6%] | [-37.1%, -18.7%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_true-16` | `redis_commands/op` | 3 | 2 | -33.3% | [-33.3%, -33.3%] | [-33.3%, -33.3%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_true-16` | `redis_network_bytes/op` | 1.19e+03 | 1.11e+03 | -7.0% | [-7.0%, -7.0%] | [-7.0%, -7.0%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_true-16` | `redis_round_trips/op` | 3 | 2 | -33.3% | [-33.3%, -33.3%] | [-33.3%, -33.3%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_true-16` | `B/op` | 5.68e+03 | 5.5e+03 | -3.1% | [-3.9%, -2.6%] | [-4.0%, -2.4%] |
| publish | `BenchmarkPublishStateCosts/fair_true/dedup_true-16` | `allocs/op` | 96 | 77 | -19.8% | [-19.8%, -19.8%] | [-19.8%, -19.8%] |
| publish | `BenchmarkPublishThroughput-16` | `ns/op` | 1e+05 | 9.36e+04 | -6.7% | [-16.9%, +11.2%] | [-18.7%, +15.2%] |
| publish | `BenchmarkPublishThroughput-16` | `B/op` | 4.29e+03 | 4.43e+03 | +2.4% | [+1.7%, +3.2%] | [+1.0%, +3.2%] |
| publish | `BenchmarkPublishThroughput-16` | `allocs/op` | 61 | 56 | -8.2% | [-8.2%, -8.2%] | [-8.2%, -8.2%] |
| setup-key | `BenchmarkSetupKeyCosts/cached_group-16` | `ns/op` | 781 | 791 | +0.5% | [-4.5%, +10.6%] | [-4.6%, +14.5%] |
| setup-key | `BenchmarkSetupKeyCosts/cached_group-16` | `B/op` | 70 | 70 | +0.0% | [+0.0%, +0.0%] | [+0.0%, +0.0%] |
| setup-key | `BenchmarkSetupKeyCosts/cached_group-16` | `allocs/op` | 1 | 1 | +0.0% | [+0.0%, +0.0%] | [+0.0%, +0.0%] |
| setup-key | `BenchmarkSetupKeyCosts/three_keys-16` | `ns/op` | 652 | 295 | -56.2% | [-62.0%, -51.1%] | [-65.3%, -46.8%] |
| setup-key | `BenchmarkSetupKeyCosts/three_keys-16` | `B/op` | 286 | 104 | -63.6% | [-63.6%, -63.0%] | [-63.6%, -62.5%] |
| setup-key | `BenchmarkSetupKeyCosts/three_keys-16` | `allocs/op` | 9 | 3 | -66.7% | [-66.7%, -66.7%] | [-66.7%, -66.7%] |
| setup-key | `BenchmarkSetupKeyCosts/uncached_group-16` | `ns/op` | 6.01e+04 | 6.2e+04 | +1.0% | [-11.4%, +15.4%] | [-14.5%, +21.2%] |
| setup-key | `BenchmarkSetupKeyCosts/uncached_group-16` | `B/op` | 750 | 750 | +0.0% | [-0.1%, +0.1%] | [-0.3%, +0.3%] |
| setup-key | `BenchmarkSetupKeyCosts/uncached_group-16` | `allocs/op` | 32 | 32 | +0.0% | [+0.0%, +0.0%] | [+0.0%, +0.0%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_256-16` | `ns/op` | 6.72e+05 | 2.38e+05 | -65.6% | [-65.9%, -63.0%] | [-66.7%, -59.6%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_256-16` | `redis_commands/op` | 11 | 5 | -54.5% | [-54.5%, -54.5%] | [-54.5%, -54.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_256-16` | `redis_network_bytes/op` | 6.5e+03 | 1.63e+03 | -74.9% | [-74.9%, -74.9%] | [-74.9%, -74.9%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_256-16` | `redis_round_trips/op` | 10 | 4 | -60.0% | [-60.0%, -60.0%] | [-60.0%, -60.0%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_256-16` | `B/op` | 1.52e+04 | 4.77e+03 | -68.6% | [-69.4%, -68.6%] | [-69.7%, -68.4%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_256-16` | `allocs/op` | 282 | 83 | -70.6% | [-70.6%, -70.6%] | [-70.6%, -70.6%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_65536-16` | `ns/op` | 1.36e+06 | 5.2e+05 | -61.8% | [-62.9%, -60.9%] | [-63.3%, -59.9%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_65536-16` | `redis_commands/op` | 11 | 5 | -54.5% | [-54.5%, -54.5%] | [-54.5%, -54.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_65536-16` | `redis_network_bytes/op` | 5.29e+05 | 8.87e+04 | -83.2% | [-83.2%, -83.2%] | [-83.2%, -83.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_65536-16` | `redis_round_trips/op` | 10 | 4 | -60.0% | [-60.0%, -60.0%] | [-60.0%, -60.0%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_65536-16` | `B/op` | 8.65e+05 | 2.49e+05 | -71.1% | [-71.2%, -71.1%] | [-71.2%, -71.1%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_1/payload_65536-16` | `allocs/op` | 287 | 83 | -70.9% | [-71.1%, -70.7%] | [-71.1%, -70.7%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_256-16` | `ns/op` | 7.7e+06 | 2.32e+06 | -70.0% | [-70.2%, -69.1%] | [-70.5%, -68.8%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_256-16` | `redis_commands/op` | 146 | 65 | -55.5% | [-55.5%, -55.5%] | [-55.5%, -55.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_256-16` | `redis_network_bytes/op` | 1.01e+05 | 2.43e+04 | -76.0% | [-76.0%, -76.0%] | [-76.0%, -76.0%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_256-16` | `redis_round_trips/op` | 115 | 34 | -70.4% | [-70.4%, -70.4%] | [-70.4%, -70.4%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_256-16` | `B/op` | 2.28e+05 | 6.65e+04 | -70.8% | [-70.8%, -70.8%] | [-70.8%, -70.7%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_256-16` | `allocs/op` | 4.09e+03 | 1.05e+03 | -74.3% | [-74.3%, -74.3%] | [-74.3%, -74.3%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_65536-16` | `ns/op` | 2e+07 | 6.42e+06 | -67.8% | [-68.8%, -65.5%] | [-69.7%, -65.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_65536-16` | `redis_commands/op` | 146 | 65 | -55.5% | [-55.5%, -55.5%] | [-55.5%, -55.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_65536-16` | `redis_network_bytes/op` | 8.46e+06 | 1.42e+06 | -83.2% | [-83.2%, -83.2%] | [-83.2%, -83.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_65536-16` | `redis_round_trips/op` | 115 | 34 | -70.4% | [-70.4%, -70.4%] | [-70.4%, -70.4%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_65536-16` | `B/op` | 1.38e+07 | 3.99e+06 | -71.2% | [-71.2%, -71.2%] | [-71.2%, -71.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_16/payload_65536-16` | `allocs/op` | 4.15e+03 | 1.06e+03 | -74.5% | [-74.5%, -74.5%] | [-74.5%, -74.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_256-16` | `ns/op` | 3.02e+07 | 8.73e+06 | -71.0% | [-71.8%, -70.1%] | [-72.3%, -69.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_256-16` | `redis_commands/op` | 578 | 257 | -55.5% | [-55.5%, -55.5%] | [-55.5%, -55.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_256-16` | `redis_network_bytes/op` | 4.05e+05 | 9.7e+04 | -76.1% | [-76.1%, -76.1%] | [-76.1%, -76.1%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_256-16` | `redis_round_trips/op` | 451 | 130 | -71.2% | [-71.2%, -71.2%] | [-71.2%, -71.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_256-16` | `B/op` | 9.08e+05 | 2.67e+05 | -70.6% | [-70.6%, -70.6%] | [-70.7%, -70.6%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_256-16` | `allocs/op` | 1.62e+04 | 4.12e+03 | -74.6% | [-74.6%, -74.6%] | [-74.6%, -74.6%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_65536-16` | `ns/op` | 7.6e+07 | 2.5e+07 | -67.2% | [-68.1%, -67.0%] | [-68.9%, -66.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_65536-16` | `redis_commands/op` | 578 | 257 | -55.5% | [-55.5%, -55.5%] | [-55.5%, -55.5%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_65536-16` | `redis_network_bytes/op` | 3.38e+07 | 5.67e+06 | -83.2% | [-83.2%, -83.2%] | [-83.2%, -83.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_65536-16` | `redis_round_trips/op` | 451 | 130 | -71.2% | [-71.2%, -71.2%] | [-71.2%, -71.2%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_65536-16` | `B/op` | 5.52e+07 | 1.59e+07 | -71.1% | [-71.1%, -71.1%] | [-71.1%, -71.1%] |
| snapshot | `BenchmarkSnapshotCosts/admission_age/tenants_64/payload_65536-16` | `allocs/op` | 1.64e+04 | 4.16e+03 | -74.6% | [-74.6%, -74.6%] | [-74.6%, -74.6%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_256-16` | `ns/op` | 3.97e+05 | 1.27e+05 | -67.7% | [-69.7%, -65.5%] | [-70.2%, -64.5%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_256-16` | `redis_commands/op` | 6 | 4 | -33.3% | [-33.3%, -33.3%] | [-33.3%, -33.3%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_256-16` | `redis_network_bytes/op` | 3.44e+03 | 902 | -73.8% | [-73.8%, -73.8%] | [-73.8%, -73.8%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_256-16` | `redis_round_trips/op` | 6 | 2 | -66.7% | [-66.7%, -66.7%] | [-66.7%, -66.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_256-16` | `B/op` | 8.08e+03 | 2.05e+03 | -74.6% | [-74.7%, -74.1%] | [-74.7%, -74.1%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_256-16` | `allocs/op` | 165 | 45 | -72.7% | [-72.7%, -72.7%] | [-72.7%, -72.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_65536-16` | `ns/op` | 7.29e+05 | 1.2e+05 | -83.4% | [-84.8%, -82.8%] | [-85.0%, -82.2%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_65536-16` | `redis_commands/op` | 6 | 4 | -33.3% | [-33.3%, -33.3%] | [-33.3%, -33.3%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_65536-16` | `redis_network_bytes/op` | 2.65e+05 | 902 | -99.7% | [-99.7%, -99.7%] | [-99.7%, -99.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_65536-16` | `redis_round_trips/op` | 6 | 2 | -66.7% | [-66.7%, -66.7%] | [-66.7%, -66.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_65536-16` | `B/op` | 4.33e+05 | 2.05e+03 | -99.5% | [-99.5%, -99.5%] | [-99.5%, -99.5%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_1/payload_65536-16` | `allocs/op` | 167 | 45 | -73.1% | [-73.1%, -72.9%] | [-73.1%, -72.9%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_256-16` | `ns/op` | 5.36e+06 | 2.4e+05 | -95.5% | [-95.7%, -95.4%] | [-96.0%, -95.3%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_256-16` | `redis_commands/op` | 81 | 49 | -39.5% | [-39.5%, -39.5%] | [-39.5%, -39.5%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_256-16` | `redis_network_bytes/op` | 5.33e+04 | 1.26e+04 | -76.4% | [-76.4%, -76.4%] | [-76.4%, -76.4%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_256-16` | `redis_round_trips/op` | 81 | 2 | -97.5% | [-97.5%, -97.5%] | [-97.5%, -97.5%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_256-16` | `B/op` | 1.22e+05 | 2.67e+04 | -78.1% | [-78.1%, -78.1%] | [-78.1%, -78.0%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_256-16` | `allocs/op` | 2.52e+03 | 394 | -84.4% | [-84.4%, -84.4%] | [-84.4%, -84.4%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_65536-16` | `ns/op` | 1.08e+07 | 2.34e+05 | -97.8% | [-98.0%, -97.8%] | [-98.1%, -97.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_65536-16` | `redis_commands/op` | 81 | 49 | -39.5% | [-39.5%, -39.5%] | [-39.5%, -39.5%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_65536-16` | `redis_network_bytes/op` | 4.23e+06 | 1.26e+04 | -99.7% | [-99.7%, -99.7%] | [-99.7%, -99.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_65536-16` | `redis_round_trips/op` | 81 | 2 | -97.5% | [-97.5%, -97.5%] | [-97.5%, -97.5%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_65536-16` | `B/op` | 6.92e+06 | 2.67e+04 | -99.6% | [-99.6%, -99.6%] | [-99.6%, -99.6%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_16/payload_65536-16` | `allocs/op` | 2.56e+03 | 394 | -84.6% | [-84.6%, -84.6%] | [-84.6%, -84.6%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_256-16` | `ns/op` | 2.11e+07 | 4.84e+05 | -97.7% | [-97.8%, -97.6%] | [-97.8%, -97.6%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_256-16` | `redis_commands/op` | 321 | 193 | -39.9% | [-39.9%, -39.9%] | [-39.9%, -39.9%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_256-16` | `redis_network_bytes/op` | 2.13e+05 | 5e+04 | -76.6% | [-76.6%, -76.6%] | [-76.6%, -76.6%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_256-16` | `redis_round_trips/op` | 321 | 2 | -99.4% | [-99.4%, -99.4%] | [-99.4%, -99.4%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_256-16` | `B/op` | 4.87e+05 | 1.06e+05 | -78.1% | [-78.2%, -78.1%] | [-78.2%, -78.1%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_256-16` | `allocs/op` | 1.01e+04 | 1.5e+03 | -85.1% | [-85.1%, -85.1%] | [-85.1%, -85.1%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_65536-16` | `ns/op` | 4.31e+07 | 5.3e+05 | -98.8% | [-98.9%, -98.7%] | [-98.9%, -98.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_65536-16` | `redis_commands/op` | 321 | 193 | -39.9% | [-39.9%, -39.9%] | [-39.9%, -39.9%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_65536-16` | `redis_network_bytes/op` | 1.69e+07 | 5e+04 | -99.7% | [-99.7%, -99.7%] | [-99.7%, -99.7%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_65536-16` | `redis_round_trips/op` | 321 | 2 | -99.4% | [-99.4%, -99.4%] | [-99.4%, -99.4%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_65536-16` | `B/op` | 2.77e+07 | 1.06e+05 | -99.6% | [-99.6%, -99.6%] | [-99.6%, -99.6%] |
| snapshot | `BenchmarkSnapshotCosts/metrics/tenants_64/payload_65536-16` | `allocs/op` | 1.02e+04 | 1.5e+03 | -85.3% | [-85.3%, -85.3%] | [-85.3%, -85.3%] |

## Decision summary

- **publish**: 21/27 comparisons lower; 0 exceeded the 15% regression guard.
- **setup-key**: 3/9 comparisons lower; 0 exceeded the 15% regression guard.
- **snapshot**: 72/72 comparisons lower; 0 exceeded the 15% regression guard.
