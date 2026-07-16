# HTTP and Operations API Reference

TaskForge exposes a minimal public probe surface and a separately protected
operator surface on one HTTP listener. The listener binds to
`127.0.0.1:8080` by default.

## Security boundary

| Surface | Classification | Default behavior |
| --- | --- | --- |
| `GET/HEAD /healthz` | Public liveness | Enabled; returns only `{"status":"ok"}` |
| `GET/HEAD /readyz` | Public aggregate readiness | Enabled; returns only `ready` or `not_ready`; dependency errors and check details are redacted |
| `GET/HEAD /metrics` | Sensitive operational state | Disabled without an operator token; authenticated otherwise |
| `GET/HEAD /dashboard[/]` | Sensitive operator UI and configuration metadata | Disabled without an operator token; authenticated otherwise |
| `GET/HEAD /` and `/v1/admin/ping` | Operator discovery | Disabled without an operator token; authenticated otherwise |
| `GET/HEAD /v1/admin/admission` | Sensitive queue/admission state | Disabled without an operator token; authenticated otherwise |
| `GET/HEAD /v1/admin/adaptive` | Sensitive capacity/budget state | Disabled without an operator token; authenticated otherwise |
| `GET/HEAD /v1/admin/workers` | Sensitive worker identity/state | Disabled without an operator token; authenticated otherwise |
| `GET/HEAD /v1/admin/leadership` | Sensitive scheduler identity/state | Disabled without an operator token; authenticated otherwise |
| `GET/HEAD /v1/tasks/{task_id}` | Highly sensitive task metadata | Disabled without an operator token; authenticated otherwise; raw errors and results are never returned |
| Future mutating operator actions | Critical control-plane action | Must remain inside the operator surface and use a non-safe HTTP method plus Bearer authentication |

The API has no mutating operator endpoint today. Unsupported methods on every
current route return `405 Method Not Allowed` with `Allow: GET, HEAD`.

Set a single shared token of at least 32 non-whitespace characters:

```env
TASKFORGE_HTTP_AUTH_TOKEN=replace-with-a-random-secret-of-at-least-32-characters
```

Automation sends `Authorization: Bearer <token>`. The dashboard supports HTTP
Basic authentication with username `taskforge` and the token as its password.
Basic authentication is read-only; non-GET/HEAD operator requests require a
Bearer token. Credential comparison is constant-time. When the setting is
empty, operator routes return `404`; when configured, missing or invalid
credentials return `401`.

The token is authentication, not transport encryption. Use TLS as described in
the [HTTP security deployment guide](../operations/http-security.md).

## Operator endpoints

`/v1/admin/admission` reports each queue's configured mode, current admission
state, controlled reason code, latest signal snapshot, and `defer_interval`.

`/v1/admin/adaptive` reports each worker pool's effective concurrency,
configured bounds, latest controlled adjustment reason, sampled adaptive
signals, and cluster-wide dependency budget usage.

`/v1/admin/workers` reports each worker instance's lifecycle state, current
pending and running ownership, drain timestamps, shutdown outcome,
abandoned-delivery count, and drain-time lease losses.

`/v1/admin/leadership` reports the scheduler's local leadership state, current
fenced epoch, live Redis leadership record, stale-write rejections, and
control-plane failure counters.

`/v1/tasks/{task_id}` returns state, timestamps, delivery count, last delivery
ID, and lease owner. It returns `error_present` instead of the raw last error.
Task input payloads and retained result bytes are not exposed through HTTP.

The dashboard is embedded and has no third-party runtime assets. It is a
read-only view backed by the admin endpoints plus a local configuration
builder; it never changes the running service.

## HTTP limits and headers

The server has non-zero read-header, read, write, idle, header-size,
request-body, and graceful-shutdown limits. Oversized requests with a declared
length receive `413`; chunked request bodies without a content length receive
`411`. The defaults and environment overrides are in the
[configuration reference](configuration.md).

Every response sets `Cache-Control: no-store`, a restrictive Content Security
Policy, `Permissions-Policy`, `Referrer-Policy`, `X-Content-Type-Options`, and
`X-Frame-Options`. Cross-origin access is not enabled. Set HSTS at the TLS
terminating reverse proxy.

## Metrics

Worker and API metrics include queue, admission, adaptive-concurrency, worker
lifecycle, dependency-budget, success, failure, retry, reclaim, and active-task
series. Scheduler metrics add leadership epoch, renewal, stale-write rejection,
control-plane failure, and lag series. Names and labels expose deployment
topology and tenant/queue identifiers, which is why `/metrics` is protected.
