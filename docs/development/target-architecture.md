# Target Architecture and Baseline Deletion Map

Status: T00 architecture freeze for the breaking simplification wave.

Baseline commit: `447facc` (audited 2026-07-16).

This document records the current repository, the evidence for each proposed
merge or deletion, and the target architecture for the next tasks. T00 changes
documentation only. It does not change production behavior, Redis data, or any
public API.

## Product decision

TaskForge is primarily an **embeddable Go worker runtime** with a Redis
transport. The scheduler and read-only operations API are optional sidecars.
Application code owns handler registration and runs the worker in its own Go
process; TaskForge does not invent a plugin, RPC, or dynamic-code-loading
boundary.

The supported product surface is:

- the root Go package for task, delivery, state, retry, error, and broker
  contracts;
- a public Redis package that implements those contracts;
- a public worker package that executes registered Go handlers;
- `taskforge-scheduler` for delayed/retry release and recurring schedules;
- `taskforge-api` for read-only task and operator state;
- one public-API-only demo used as an executable adoption contract.

There is no supported standalone worker binary. The current binary cannot load
application handlers and acknowledges every task after a no-op handler. It is a
placeholder, not a product boundary.

## Baseline validation

The audit began with `git status --short`.


| Command | Baseline result |
| --- | --- |
| `make test` | Passed. The first restricted-sandbox run failed only because `httptest.NewServer` could not bind loopback; the same command passed with loopback permission. |
| `GOCACHE=/tmp/taskforge-gocache go test ./... -coverprofile=/tmp/taskforge-cover.out` | Passed; Redis integration tests compiled but were skipped because `TASKFORGE_RUN_INTEGRATION` was not set. |
| `go tool cover -func=/tmp/taskforge-cover.out` | **37.7% total statement coverage**. |
| `make lint` | Passed `go vet`, `gofmt` verification, and installed `staticcheck`. |

Coverage exposes risk concentrations rather than an acceptance target:

| Package | Statement coverage |
| --- | ---: |
| `internal/brokerredis` | 3.9% |
| `internal/scheduler` | 27.0% |
| `internal/runtime` | 69.8% |
| `internal/config` | 76.3% |
| `internal/observability` | 56.7% |
| `pkg/taskforge` | 23.4% |
| `internal/app/worker` | 0.0% |
| all `cmd/*` packages | 0.0% |

There are 135 unit/integration test functions in 24 files. The Redis integration
suite was not executed for T00 because it is opt-in and requires a local Redis;
it must run for the model/transport refactor.

The non-test Go baseline is 13,326 lines. Four files hold 4,862 of those lines:
`internal/brokerredis/redis.go` (1,558), `internal/runtime/worker.go` (1,352),
`internal/observability/metrics.go` (1,035), and `internal/config/config.go`
(917). Their responsibilities are mapped below; line count alone is not a
reason to split them.

## Current architecture audit

### Core models and ownership problems

| Concept | Current owners and call-site evidence | Problem | Target owner |
| --- | --- | --- | --- |
| Logical task | `pkg/taskforge.Task` and `internal/broker.TaskMessage` have the same 13 fields. `broker.TaskMessage` is used by the Redis broker, runtime, scheduler, DLQ, routing, stores, observability, four example commands, benchmark tests, and integration tests. The public type is used only inside `pkg/taskforge`, its tests, and README snippets. | Every public publish/handle path copies the full struct. | Root `taskforge.Task`. |
| Delivery ownership | Public and internal `Delivery` and `ExecutionMetadata` structs are field-for-field duplicates. Internal delivery is used by broker, runtime, scheduler, DLQ, logging, store, benchmark, and integration code; public delivery has no in-repository caller outside its wrapper. | The most important fencing object crosses a conversion boundary that can drift. `ExecutionMetadata.State` is also an untyped string. | Root `taskforge.Delivery` and typed `taskforge.State`. |
| Publish/admission result | `PublishSource`, `PublishOptions`, `AdmissionDecision`, and `PublishResult` are duplicated in `pkg/taskforge/publish.go` and `internal/broker/broker.go`. | Cast-and-copy adapters add no semantic boundary. | Root `taskforge` package. |
| Broker errors | Public error variables alias internal variables, while `AdmissionError` is reimplemented in both packages. `ErrLeaseExpired` and `ErrUnknownLease` occur only at their declarations. | Two error taxonomies plus stale compatibility names obscure delivery terminology. | Root `taskforge` package; only delivery-oriented names. |
| Retry | `RetryPolicy` is duplicated publicly and in `internal/tasks`; config has a third raw representation. Runtime, scheduler, config, and integration tests use the internal type. `MaxAttempts` and `MaxDeliveries` normalize into each other; per-task overrides are named `MaxAttempts` while enforcement counts broker deliveries. | Duplicate types and two names for one limit make retry behavior harder to state. | Root `taskforge.RetryPolicy` with one `MaxDeliveries` limit. `Task.Attempt` remains the scheduled retry number; `DeliveryCount` remains broker delivery count. |
| State and task record | `State` is duplicated in `pkg/taskforge` and `internal/tasks`; `TaskRecord` is duplicated in `pkg/taskforge` and `internal/store`. Redis, worker, API, and integration code use internal types. | Public reads require another full copy. `retry_scheduled` ends the current delivery but is not final for the logical task, so `IsTerminal` is misleading without that qualification. | Root `taskforge.State` and `taskforge.TaskRecord`; name helpers for delivery completion explicitly. |
| Failure and DLQ | Failure classes, envelopes, and entries are duplicated between `pkg/taskforge/dlq.go` and `internal/dlq`. Runtime and integration code use internal models; the public package copies them for concrete Redis methods. | The public operator model can drift from the value actually persisted. | Root failure/DLQ values and contracts; Redis package owns persistence. |
| Handler | `pkg/taskforge.Handler` takes public `Task`; `internal/runtime.Handler` takes `broker.TaskMessage`; `runtimeHandler` converts between them. All runnable examples implement the internal form. | The supported handler contract is not the contract exercised by examples. | Root `taskforge.Handler` and `Registry`; worker consumes them directly. |
| Broker implementation | `pkg/taskforge.RedisBroker` wraps `internal/brokerredis.RedisBroker`, `internal/dlq.Service`, and `internal/storeredis.RedisStore`. Its public options configure only basic Redis/retention settings. | The wrapper owns no unique algorithm and hides the differentiating controls. | Public `redis.Broker`, containing the implementation directly. |
| Worker implementation | `pkg/taskforge.Worker` wraps `internal/runtime.Worker` and `Manager`. The wrapper cannot configure fairness, admission, routing, dependency budgets, adaptive concurrency, task limits, lifecycle persistence, or multi-pool execution. | The claimed public integration path cannot use the product thesis. | Public `worker.Worker` implementation; no wrapper layer. |
| Scheduler | `internal/scheduler` owns delayed movement, recurring definitions/state, and fenced Redis leadership. Its three small consumer-side interfaces are exercised by scheduler tests and concrete Redis implementations; `LeadershipFence` also crosses into brokerredis and the Redis schedule store. | Ownership is mostly coherent. The shared fence value must not force the public Redis implementation to expose an internal-package type. | Keep the scheduler state machine in `internal/scheduler`; move only the shared fence value to root `taskforge`. Expose scheduling through the sidecar, not a new public framework. |
| Configuration | `internal/config.Config` mixes deployment settings, compiled policies, worker settings, Redis settings, and scheduler settings. `pkg/taskforge.RedisOptions` and `WorkerOptions` are a separate, smaller configuration system. App packages convert config into broker/runtime types. | Environment and embedded paths have different capabilities and validation. | Public typed options in `redis` and `worker`; `internal/config` only decodes environment values into those types plus sidecar-only settings. |
| Operational status | Queue, fairness, admission, budget, adaptive, worker, and scheduler snapshots and provider interfaces live in `internal/observability`; worker and Redis domain code import that package to store state. Scheduler has separate leadership/safety types that app wiring converts to an observability snapshot. | Domain state is owned by the metrics package, reversing the dependency. | Root `taskforge` owns typed operational snapshots; `internal/observability` owns metrics and tracing only. |

### Inaccessible or contradictory product paths

Whole-repository searches found the following contradictions:

- README says embedding `pkg/taskforge` is the intended real integration path,
  but no Go command or example imports that package. `cmd/demo` and all three
  `cmd/example-*` programs import `internal/*` packages.
- README and Compose present worker, scheduler, and API as a runnable stack,
  while `internal/app/worker/app.go` installs
  `HandlerFunc(func(...) error { return nil })`. The released worker therefore
  reports successful execution without application behavior.
- The environment worker path configures fairness, admission, adaptive
  concurrency, routing, and dependency budgets, but has no real handler. The
  embedded worker has a real handler but exposes none of those controls.
- `TASKFORGE_METRICS_ADDR` is loaded, tested, documented, and set in Compose,
  but no runtime reads `Config.MetricsAddr`; `/metrics` is served from
  `HTTPAddr`. The configuration reference acknowledges this inert option.
- Public Redis/worker constructors and public reserve/ack/nack/state/DLQ methods
  have zero test call sites. Public tests cover task options and registry
  dispatch only. The aggregate public package coverage is 23.4%.
- HTTP reference material says every process exposes health/readiness/metrics,
  while an embedded public worker exposes only a metrics handler. The command
  and library operational contracts are different.
- `tasks.IsTerminal(StateRetryScheduled)` is correct only for the current
  delivery. Documentation calls the Redis record a logical task record, so the
  distinction must be explicit rather than implying that the logical task has
  finished.

### Oversized files and stable split points

These are moves within an owner, not new abstraction layers:

- `redis.go`: constructor/options, publish/routing, reserve/reclaim,
  ack/nack/lease validation, delayed release, key construction, metrics queries,
  and embedded Lua scripts. In the target `redis` package, split by those stable
  protocol responsibilities while keeping one `Broker` and one key-schema
  owner.
- `worker.go`: reserve/prefetch loop, execution, shutdown/drain, lease/budget
  renewal, adaptive controller, lifecycle status, and state recording. In the
  target `worker` package, split those responsibilities while keeping one
  `Worker` state machine.
- `metrics.go`: status DTOs/providers, metric instruments, registration, and
  Prometheus collectors. Move DTOs to root `taskforge`, then split instruments
  from collectors inside `internal/observability`.
- `config.go`: environment access, raw JSON DTOs, parsing, defaults, validation,
  and compiled policy construction. Keep raw decoding/validation in
  `internal/config`, but move runtime option ownership to the package that uses
  it.

## Deletion and merge map with evidence

Every item below was checked with a whole-repository search. “Delete” means
delete in the breaking refactor after its callers are moved atomically; it does
not authorize removing the behavior.

| Planned deletion or merge | Call-site evidence | Replacement |
| --- | --- | --- |
| Delete the `pkg/taskforge` location. | Exact import search found no Go file importing `github.com/aminkbi/taskforge/pkg/taskforge`; only README snippets and its own package tests use its constructors. | Move canonical values/contracts to module root and implementations to `redis`/`worker`; update README and tests. |
| Merge `internal/broker` models/contracts into root `taskforge`. | Its task type is referenced across brokerredis, runtime, scheduler, DLQ, routing, store, observability, examples, benchmark, integration, and the public adapter. It is heavily used, not dead. | Update all callers to the single root contract, then remove `internal/broker`. |
| Merge `internal/tasks` state/retry/task helpers into root `taskforge`. | Consumers are config, runtime, scheduler, brokerredis, storeredis, logging, observability, public adapters, benchmark, and integration code. | Methods/functions on canonical root values; keep protocol header constants unexported in their implementing package unless users must set them. |
| Merge `internal/store.TaskRecord` and the public record. | `store.TaskRecord` is consumed by storeredis, API tests, runtime tests, and the public conversion; the public record is returned only by the wrapper. | Root `TaskRecord`; consumer-owned read/write interfaces. |
| Delete `internal/store.ResultStore` and `RedisStore.Save`. | `ResultStore` occurs only at its declaration. `Save` occurs only at its method declaration; all production state writes call `RecordQueued` or `RecordDelivery`. | Retain the state writer/reader operations that have call sites. |
| Merge internal/public DLQ values; delete `internal/dlq.Admin`. | Runtime uses `dlq.Publisher`; public Redis methods call the concrete service directly. `dlq.Admin` occurs only at its declaration. | Root DLQ values/publisher contract; concrete list/replay/discard methods on `redis.Broker`. |
| Delete field-copy adapters in `pkg/taskforge`. | `toBrokerMessage`, `taskFromBrokerMessage`, delivery conversions, publish conversions, retry conversions, state conversion, admission error conversion, and DLQ conversions are called only by the public façade. | One canonical value crosses all layers. Copy mutable payload/header inputs only at ownership boundaries. |
| Delete the duplicate runtime handler and `runtimeHandler` bridge. | Public handlers are used by registry tests/README; internal handlers are used by runtime, examples, benchmark, integration, and the public bridge. | Root handler contract used directly by public `worker`. |
| Delete `ErrLeaseExpired`, `ErrUnknownLease`, and `Delivery.WithState`. | Each symbol occurs only at its declaration. `WithLastError` has one runtime caller and can move to the canonical delivery or become a local copy operation. | Delivery-oriented errors and explicit state transition helper. |
| Delete `brokerredis.New`. | Search found only its declaration; every construction call uses `NewWithOptions`. | One validated `redis.New(options)` constructor. |
| Merge `internal/brokerredis` and `internal/storeredis` into public `redis`. | All production constructions are in app wiring, examples, public façade, benchmark, and integration tests. `storeredis` is used by the three apps and public façade. | Direct public Redis implementation with one client, options, key schema, state store, DLQ store, and metrics registration path. |
| Merge `internal/fairness` and `internal/routing` implementation into `redis`. | Outside their tests, fairness is imported only by config and brokerredis; routing is imported only by config and brokerredis. | Public policy option values plus unexported evaluation code beside Redis admission/reservation. |
| Move `internal/scheduler.LeadershipFence` to root. | The value is produced by the Redis leader elector and consumed by scheduler interfaces, brokerredis due movement, Redis schedule state, scheduler tests, benchmark code, and integration tests. | Root `taskforge.LeadershipFence` as the shared fencing protocol value; leadership behavior remains internal. |
| Delete repeated app conversion helpers. | `admissionPoliciesByQueue` is duplicated in worker, scheduler, and API apps; `dependencyBudgetCapacities` is duplicated in all three; worker additionally copies every adaptive and task-budget field. | Environment decoder returns validated `redis.Options` and `worker.Options` components directly. |
| Delete `MetricsAddr`. | Runtime search found reads only in config load/tests; docs and Compose set it, but apps pass only `HTTPAddr` to `httpserver.New`. | One operator HTTP listen option until a real second listener is implemented. |
| Delete `cmd/worker`, `internal/app/worker`, worker Dockerfile, release artifact, Compose service, and their script/Makefile references. | The app's only handler is a no-op. The binary is nevertheless built by `build-release.sh`, smoked by `release-smoke.sh`, built in Compose, and advertised by README/Makefile. | Applications embed public `worker`; the demo supplies executable handlers. |
| Replace current demo/example command wiring and delete internal-only example packages after migration. | `cmd/demo` and all three `cmd/example-*` files import internals; none imports the supported public package. | One deterministic `examples/overload` adoption demo using only root, `redis`, and `worker` public packages. Preserve useful handler examples only when they teach idempotency, timeout, retry, or dependency isolation. |
| Move operational snapshot DTOs out of observability. | Snapshot types are produced/consumed by runtime, brokerredis, API, scheduler app, and metrics collectors. Feature code imports observability to persist its own status. | Root `taskforge` snapshot values consumed by feature, API, and telemetry packages; keep provider interfaces at the consumer. |

## Target package graph

The target deliberately has three public packages and a small internal support
graph:

```text
github.com/aminkbi/taskforge                 canonical values and contracts
├── redis                                    Redis broker/store implementation
├── worker                                   embeddable execution runtime
├── cmd/scheduler
│   └── internal/app/scheduler
├── cmd/api
│   └── internal/app/api
└── internal
    ├── config                               env decoding for sidecars/demo only
    ├── scheduler                            delayed, recurring, fenced leadership
    ├── observability                        metrics and tracing only
    ├── httpserver                           health/readiness/operator HTTP server
    ├── clock                                deterministic time seam
    ├── healthcheck                          loop readiness state
    ├── logging                              structured logging helpers
    └── shutdown                             signal context helper
```

Dependency direction is strict:

```text
taskforge <- redis
taskforge <- worker
taskforge <- internal/scheduler
taskforge <- internal/observability

redis/worker/scheduler -> taskforge operational snapshots
cmd -> internal/app -> public implementation packages + internal services
```

The root package must not import `redis`, `worker`, or any `internal` package.
That makes it the stable model boundary and prevents a façade cycle. `redis`
and `worker` contain their implementations directly instead of wrapping another
same-purpose internal package.

### Canonical ownership

| Model or behavior | Sole owner |
| --- | --- |
| Task, delivery, execution metadata | root `taskforge` |
| State and task record | root `taskforge` |
| Retry policy and retry/delivery counters | root `taskforge` |
| Publish options/result and admission error | root `taskforge` |
| Handler, registry, error classification | root `taskforge` |
| Broker ownership contract | root `taskforge` |
| Failure class and DLQ values/contracts | root `taskforge` |
| Redis connection, key schema, transport, state/DLQ persistence | `redis` |
| Routing, fairness, admission, dependency-budget policy execution | `redis` |
| Worker lifecycle, prefetch, execution, adaptive concurrency | `worker` |
| Delayed/recurring scheduling and leadership fencing | `internal/scheduler` |
| Leadership fence value shared with Redis | root `taskforge` |
| Typed Redis policy options | `redis` |
| Typed worker options | `worker` |
| Environment decoding and sidecar-only settings | `internal/config` |
| Operator snapshot DTOs | root `taskforge` |
| Prometheus instruments/collectors and tracing | `internal/observability` |

Raw JSON structs may remain inside `internal/config` as decoding DTOs. They are
not domain models and must be converted once, at the environment boundary, into
the same validated public option types used by embedded applications.

## Required invariants

The breaking refactor may change names, imports, and the development Redis
schema. It must preserve and test these semantics:

1. Execution is at least once. Duplicate handler execution is possible and
   handlers must be idempotent; no layer may claim exactly once.
2. Task ID identifies logical work. Delivery ID identifies one leased ownership
   attempt. Ack, nack, and lease extension validate the current delivery ID and
   owner.
3. An expired, unknown, or stale delivery cannot finalize or extend newer work.
4. Lease loss prevents the old owner from publishing a retry, DLQ result, or
   success acknowledgement as if it still owned the delivery.
5. Retry publication is deduplicated by the failed delivery, preserves logical
   task identity, increments the retry number, and is bounded by delivery count
   and optional task age.
6. `retry_scheduled` ends the current delivery, not the logical task. The next
   leased retry continues the same logical task record.
7. DLQ publication precedes acknowledgement of a permanently failed delivery;
   failed DLQ publication leaves the original work recoverable.
8. Delayed and recurring release requires a valid leadership fence. A stale
   leader cannot write after a newer epoch takes ownership.
9. Routing is decided for new publishes; retry, due release, recurring release,
   DLQ, and broker requeue preserve established placement unless an explicit
   invariant says otherwise.
10. Fairness and admission operate on normalized queue and fairness keys.
    Overload protection may defer or reject according to configured policy but
    cannot silently drop accepted work.
11. Dependency-budget tokens and broker leases are renewed for owned work and
    released on every completed/abandoned path.
12. Task payloads and header maps are copied when ownership crosses an API
    boundary; callers cannot mutate queued data after publish.
13. State and result retention remains explicit. A zero retention value keeps
    data indefinitely under the current contract.
14. Metrics, logs, state records, and HTTP views observe the canonical model;
    they do not define a second version of it.

## Redis development compatibility

The model consolidation should use a new versioned TaskForge key prefix rather
than try to deserialize pre-refactor development data. There are no supported
users or released compatibility commitments, and retaining the ambiguous
`MaxAttempts`/`MaxDeliveries` wire vocabulary would preserve the wrong model.

T01 must document how to remove old TaskForge development keys and must never
recommend flushing a shared Redis database. It must test that a new prefix
cannot read or mutate stale pre-refactor ownership records. Production migration
or rolling mixed-version operation is explicitly unsupported for this reset.

## Migration order

T01 is one atomic repository refactor; the following is implementation order,
not a compatibility period:

1. Add invariant-focused tests around canonical values, stale ack/nack/extend,
   retry/DLQ ordering, state recording, and scheduler fencing before moving
   implementation code.
2. Create the dependency-free root package with the canonical models,
   contracts, state semantics, and error classification. Update internal
   callers in one sweep; do not add aliases back to old packages.
3. Move brokerredis/storeredis/DLQ persistence into direct public `redis`
   ownership. Introduce the versioned development key prefix and update all
   integration tests.
4. Move the runtime into direct public `worker` ownership and make it consume
   root handlers and models. Preserve one worker state machine and consumer-side
   injection interfaces.
5. Make `internal/config` compile environment input into the public Redis and
   worker option values. Centralize app wiring conversions and validation.
6. Move status DTOs out of observability, then split the four oversized files
   only along the stable responsibility boundaries listed above.
7. Update commands, benchmark code, integration tests, and public tests to the
   new graph. Delete old packages and adapters in the same change; no
   compatibility façade remains.
8. Run format, package tests, `make test`, `make lint`, `make race-test`, and the
   Redis integration suite. Record any environment exception explicitly.
9. In T02, remove the placeholder worker artifact and replace all demos/examples
   with the single supported public path. Command/release claims change only
   when that executable contract exists.
10. In T05, finish the typed configuration surface and make environment and Go
    validation identical. T06 and T07 then harden HTTP and Redis operations
    without reopening model ownership.

## Explicit non-goals

- Exactly-once execution or automatic side-effect deduplication.
- A standalone generic worker without application handlers.
- Dynamic Go plugins, handler RPC, or a language-neutral worker protocol.
- Additional broker backends, a generic storage abstraction, or public Redis
  key/layout APIs.
- Workflows/DAG orchestration, gang scheduling, cron syntax beyond the accepted
  recurring scope, or a Kubernetes operator.
- Multi-language SDKs or preserving the current pre-release Go import paths.
- Redis Cluster support unless key-slot and Lua invariants are separately
  proven; HA/TLS choices are handled by the Redis operating-model task.
- A large admin UI or unauthenticated task payload/result exposure.
- Runtime reconfiguration in this refactor. Dynamic versus restart-required
  settings are defined with the supported control surface later.
- Fabricated benchmarks, users, citations, partners, or adoption evidence.

## Review gates for T01/T02

The architecture is implemented only when:

- each canonical model has the single owner named above;
- no old package, alias, field-copy adapter, or same-purpose forwarding wrapper
  remains;
- every repository caller and example uses the new graph;
- the placeholder worker is either still clearly excluded during T01 or removed
  with its build/release/Compose claims in T02;
- Redis reset behavior is explicit and ownership invariants pass integration
  and race tests;
- production concepts and lines decrease unless a retained increase is tied to
  an invariant test or the direct supported API.
