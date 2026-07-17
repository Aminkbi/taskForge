<!-- Generated from analysis.json; do not edit. -->
| Workload | Variant | Status | p99 completion (ms) | Throughput (tasks/s) | Jain fairness | SLO violations | Peak concurrency | Recovery (ms) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| delayed-backlog | taskforge-fifo-static | measured | 201.1 [190.9, 226.5] | 704 [654, 749] | 1.000 [1.000, 1.000] | 0 [0, 0] | 2 [2, 2] | 0 [0, 0] |
| delayed-backlog | taskforge-no-fairness | measured | 229.7 [215.6, 250.3] | 609 [585, 663] | 1.000 [1.000, 1.000] | 0 [0, 2] | 2 [2, 2] | 0 [0, 0] |
| delayed-backlog | taskforge-no-admission | measured | 341.5 [287.9, 355.0] | 436 [419, 513] | 0.998 [0.987, 0.999] | 40 [26, 46] | 2 [2, 2] | 0 [0, 0] |
| delayed-backlog | taskforge-no-adaptive | measured | 228.8 [212.1, 244.0] | 480 [447, 517] | 1.000 [1.000, 1.000] | 0 [0, 0] | 2 [2, 2] | 0 [0, 0] |
| delayed-backlog | taskforge-no-dependency-budget | measured | 249.9 [241.4, 258.1] | 484 [456, 508] | 1.000 [1.000, 1.000] | 2 [0, 8] | 2 [2, 2] | 0 [0, 0] |
| delayed-backlog | taskforge-full | measured | 250.8 [226.8, 295.0] | 470 [440, 517] | 1.000 [0.999, 1.000] | 2 [0, 8] | 2 [1, 2] | 0 [0, 0] |
| delayed-backlog | asynq | measured | 81.4 [78.6, 84.9] | 1561 [1541, 1644] | 1.000 [1.000, 1.000] | 0 [0, 0] | 4 [4, 4] | 0 [0, 0] |
| hot-dependency | taskforge-fifo-static | measured | 252.4 [239.2, 256.2] | 541 [533, 560] | 0.989 [0.983, 0.996] | 73 [68, 76] | 4 [3, 4] | 0 [0, 0] |
| hot-dependency | taskforge-no-fairness | measured | 487.7 [483.1, 500.5] | 299 [294, 314] | 0.978 [0.971, 0.988] | 112 [108, 114] | 2 [2, 2] | 0 [0, 0] |
| hot-dependency | taskforge-no-admission | measured | 491.2 [489.1, 495.5] | 298 [295, 300] | 0.986 [0.976, 0.990] | 114 [111, 116] | 2 [2, 2] | 0 [0, 0] |
| hot-dependency | taskforge-no-adaptive | measured | 391.7 [372.0, 398.6] | 375 [362, 382] | 0.985 [0.978, 0.990] | 103 [98, 106] | 2 [2, 2] | 0 [0, 0] |
| hot-dependency | taskforge-no-dependency-budget | measured | 324.1 [276.1, 347.2] | 432 [408, 488] | 0.984 [0.979, 0.990] | 96 [80, 104] | 3 [3, 3] | 0 [0, 0] |
| hot-dependency | taskforge-full | measured | 477.8 [460.7, 503.5] | 304 [290, 315] | 0.985 [0.979, 0.989] | 112 [109, 115] | 2 [2, 2] | 0 [0, 0] |
| hot-dependency | asynq | measured | 167.1 [162.6, 169.2] | 862 [857, 876] | 1.000 [0.999, 1.000] | 18 [15, 20] | 4 [4, 4] | 0 [0, 0] |
| noisy-neighbor | taskforge-fifo-static | measured | 273.7 [240.0, 286.2] | 592 [565, 661] | 0.966 [0.922, 0.979] | 127 [121, 141] | 3 [3, 3] | 0 [0, 0] |
| noisy-neighbor | taskforge-no-fairness | measured | 211.1 [197.4, 220.5] | 648 [617, 674] | 0.962 [0.931, 0.984] | 116 [110, 122] | 3 [3, 3] | 0 [0, 0] |
| noisy-neighbor | taskforge-no-admission | measured | 380.9 [343.4, 405.7] | 440 [420, 491] | 0.639 [0.615, 0.680] | 152 [146, 158] | 2 [2, 2] | 0 [0, 0] |
| noisy-neighbor | taskforge-no-adaptive | measured | 267.7 [253.4, 306.5] | 438 [416, 469] | 0.838 [0.807, 0.846] | 111 [107, 116] | 2 [2, 2] | 0 [0, 0] |
| noisy-neighbor | taskforge-no-dependency-budget | measured | 303.1 [263.5, 324.0] | 419 [405, 440] | 0.836 [0.808, 0.844] | 110 [108, 115] | 2 [2, 2] | 0 [0, 0] |
| noisy-neighbor | taskforge-full | measured | 283.1 [246.9, 313.8] | 430 [412, 442] | 0.831 [0.785, 0.843] | 113 [108, 121] | 2 [2, 2] | 0 [0, 0] |
| noisy-neighbor | asynq | measured | 142.6 [139.4, 146.2] | 1167 [1152, 1175] | 0.996 [0.988, 0.998] | 61 [58, 64] | 4 [4, 4] | 0 [0, 0] |
| retry-storm | taskforge-fifo-static | measured | 240.1 [228.9, 274.5] | 575 [512, 610] | 0.998 [0.996, 0.999] | 40 [38, 51] | 42 [40, 42] | 0 [0, 0] |
| retry-storm | taskforge-no-fairness | measured | 311.4 [282.2, 374.4] | 453 [389, 490] | 0.999 [0.996, 1.000] | 68 [54, 84] | 28 [26, 32] | 0 [0, 0] |
| retry-storm | taskforge-no-admission | measured | 416.5 [379.3, 445.9] | 354 [332, 386] | 0.989 [0.981, 0.997] | 90 [86, 96] | 45 [45, 46] | 0 [0, 0] |
| retry-storm | taskforge-no-adaptive | measured | 323.4 [310.7, 373.8] | 365 [335, 383] | 0.985 [0.964, 0.994] | 70 [62, 77] | 26 [26, 26] | 0 [0, 0] |
| retry-storm | taskforge-no-dependency-budget | measured | 342.7 [334.6, 367.3] | 341 [333, 357] | 0.988 [0.957, 1.000] | 79 [68, 86] | 27 [26, 28] | 0 [0, 0] |
| retry-storm | taskforge-full | measured | 346.7 [332.5, 357.6] | 344 [325, 355] | 0.990 [0.960, 0.997] | 84 [78, 86] | 27 [26, 28] | 0 [0, 0] |
| retry-storm | asynq | measured | 98.7 [94.4, 100.7] | 1385 [1332, 1449] | 1.000 [1.000, 1.000] | 0 [0, 0] | 36 [34, 40] | 0 [0, 0] |
| tenant-skew | taskforge-fifo-static | measured | 234.2 [215.0, 240.6] | 667 [650, 730] | 0.982 [0.953, 0.989] | 126 [114, 130] | 2 [2, 2] | 0 [0, 0] |
| tenant-skew | taskforge-no-fairness | measured | 209.3 [200.5, 221.3] | 733 [706, 753] | 0.970 [0.966, 0.984] | 112 [106, 121] | 2 [2, 2] | 0 [0, 0] |
| tenant-skew | taskforge-no-admission | measured | 363.1 [341.4, 398.4] | 458 [422, 488] | 0.977 [0.961, 0.988] | 148 [142, 156] | 2 [2, 2] | 0 [0, 0] |
| tenant-skew | taskforge-no-adaptive | measured | 354.3 [339.7, 375.4] | 472 [456, 493] | 0.982 [0.972, 0.994] | 148 [144, 150] | 2 [2, 2] | 0 [0, 0] |
| tenant-skew | taskforge-no-dependency-budget | measured | 352.4 [294.6, 382.1] | 481 [441, 562] | 0.981 [0.961, 0.997] | 154 [148, 158] | 2 [2, 2] | 0 [0, 0] |
| tenant-skew | taskforge-full | measured | 336.6 [324.5, 358.6] | 499 [474, 506] | 0.979 [0.959, 0.988] | 148 [141, 152] | 2 [2, 2] | 0 [0, 0] |
| tenant-skew | asynq | measured | 91.7 [89.2, 97.7] | 1625 [1570, 1702] | 1.000 [1.000, 1.000] | 0 [0, 0] | 4 [4, 4] | 0 [0, 0] |
| worker-crash | taskforge-fifo-static | measured | 163.9 [151.0, 190.4] | 316 [102, 563] | 1.000 [1.000, 1.000] | 0 [0, 1] | 3 [3, 3] | 703 [201, 1255] |
| worker-crash | taskforge-no-fairness | measured | 173.8 [165.8, 187.6] | 563 [106, 582] | 1.000 [1.000, 1.000] | 0 [0, 1] | 3 [3, 3] | 201 [201, 1199] |
| worker-crash | taskforge-no-admission | measured | 257.5 [239.0, 269.5] | 429 [410, 457] | 1.000 [0.992, 1.000] | 6 [0, 12] | 2 [2, 2] | 202 [201, 202] |
| worker-crash | taskforge-no-adaptive | measured | 246.3 [235.6, 271.4] | 457 [417, 473] | 1.000 [1.000, 1.000] | 0 [0, 15] | 2 [2, 3] | 202 [201, 202] |
| worker-crash | taskforge-no-dependency-budget | measured | 259.1 [242.3, 283.8] | 419 [397, 468] | 0.998 [0.994, 1.000] | 7 [2, 18] | 2 [2, 2] | 201 [201, 202] |
| worker-crash | taskforge-full | measured | 271.0 [261.0, 284.2] | 412 [390, 432] | 0.997 [0.994, 0.999] | 14 [6, 22] | 2 [2, 2] | 201 [201, 202] |
| worker-crash | asynq | not measured | not measured | not measured | not measured | not measured | not measured | not measured |
