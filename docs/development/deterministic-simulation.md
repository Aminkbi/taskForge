# Deterministic failure simulation

`internal/sim` is a bounded, in-process semantic model of TaskForge's delivery
ownership and scheduler-fencing protocols. It runs in normal CI without Redis,
goroutines, sleeps, or wall-clock time:

```sh
make simulation-test
```

The simulator has a fake UTC clock, deterministic broker and state backend,
two worker actors, two scheduler actors, and a SplitMix64 event scheduler whose
ordering does not depend on Go's `math/rand` implementation. A normal CI run
executes 128 exploratory seeds and these named regression seeds:

- `stale_ack_after_reclaim` (`1592590337`)
- `ambiguous_publish_deduplicates` (`1592590338`)
- `leader_turnover_fences_old_epoch` (`1592590339`)

Each bounded schedule injects a worker crash, pause, dropped renewal, late
renewal, network partition, stale acknowledgement, committed publish with a
lost reply, and scheduler leader turnover. Checks run after every event for:

- one active fenced owner per task and stale-owner rejection;
- scheduler writes carrying the live owner, token, and monotonic epoch;
- dependency-budget use staying within `[0, capacity]` with exact lease
  accounting;
- logical terminal states never changing or becoming active again;
- expired deliveries and scheduler leadership recovering within explicit
  logical-tick bounds, with all tasks terminal and budgets released by the
  horizon.

The model imports only the canonical `taskforge.State` and
`taskforge.LeadershipFence` protocol types and implements the existing clock
boundary. It does not run Redis scripts or production worker goroutines, so it
complements rather than
replaces unit, race, and Redis integration checks. Keeping it isolated avoids
adding simulation branches to production hot paths.

## Replay and regression seeds

Every invariant failure is a `sim.Violation` whose error includes the decimal
seed, event number, invariant, and a bounded tail of the exact event trace. To
replay a reported seed and print its trace:

```sh
TASKFORGE_SIM_SEED=1592590337 make simulation-replay
```

The fixed-seed and named-regression tests pin SHA-256 hashes of their compact
traces, making unintended event-order drift visible. When a simulation finds a
real bug, minimize its reproducing seed if useful, add a descriptive entry to
`sim.RegressionSeeds`, pin the trace digest, and keep only that small seed in
the repository. Large exploratory traces and local failure corpora stay out of
version control.
