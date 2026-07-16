# HTTP Security Deployment

TaskForge's HTTP server is an operational sidecar surface, not a public
multi-user API. Its security model is a narrow shared-token boundary intended
for a trusted operator group and monitoring automation.

## Threat model

The protected surface can reveal queue names, capacity and overload state,
worker and scheduler identities, delivery metadata, and task existence/state.
Metrics labels can disclose the same topology at scale. Task payloads, retained
results, and raw backend/handler errors are treated as more sensitive and are
not returned over HTTP. An attacker who obtains the operator token can read all
remaining operator data. Any future mutating endpoint could affect availability
or delivery behavior, so it must use a non-safe method and Bearer credentials.

The public liveness and readiness responses deliberately reveal only whether
the process is alive and whether its aggregate checks pass. They do not reveal
dependency addresses, error strings, leadership epochs, or component names.

## Safe deployment

The default bind address is loopback and the protected surface is disabled
until `TASKFORGE_HTTP_AUTH_TOKEN` is set. Keep that arrangement for local
process supervision. In Kubernetes, bind to the pod interface only when probes
or a sidecar require it, restrict ingress with NetworkPolicy, and point probes
at `/healthz` and `/readyz` without credentials.

For remote operators, terminate TLS at a reverse proxy on the same host, in the
same pod, or across an authenticated private network. Keep TaskForge bound to
loopback when the proxy is colocated. Forward the `Authorization` header and do
not place tokens in URLs, query strings, dashboard fields, source control, or
proxy access logs. Configure the proxy with:

- TLS 1.2 or newer and an automatically renewed certificate;
- request/header limits no larger than TaskForge's limits;
- a response timeout shorter than the intended operator client timeout;
- HSTS on the HTTPS virtual host, plus access-log filtering for credentials;
- network allow-lists or a private ingress where available.

Example proxy shape (certificate paths and TLS policy are deployment-owned):

```nginx
server {
    listen 443 ssl;
    server_name taskforge-ops.example.com;

    ssl_certificate     /run/tls/tls.crt;
    ssl_certificate_key /run/tls/tls.key;
    add_header Strict-Transport-Security "max-age=31536000" always;
    client_max_body_size 1m;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Authorization $http_authorization;
        proxy_set_header Host $host;
        proxy_read_timeout 30s;
    }
}
```

Prometheus can use `authorization.credentials_file` (Bearer) or `basic_auth`
with username `taskforge`. Prefer a mounted secret file over a literal value in
Prometheus configuration. Rotate the TaskForge token by updating clients and
restarting the service; there is no live reload.

The repository's Docker Compose file uses a fixed, clearly marked development
credential so its bundled Prometheus can scrape the protected endpoints. Every
published Compose port is loopback-only. Do not copy that credential into a
shared or production deployment.

## Residual risks

- The shared token provides no users, roles, per-action authorization,
  revocation list, or security audit trail.
- The token is present in process environment and memory. Host/process access
  can expose it; use platform secret injection and restrict debug access.
- Basic authentication is suitable only for the read-only dashboard over TLS.
  Mutating requests reject Basic credentials and require Bearer authentication.
- TLS is not implemented in TaskForge itself. A misconfigured proxy or
  plaintext hop can disclose credentials and operator data.
- Health status can still reveal that a service exists and whether it is
  unavailable. Network policy should hide even this signal where necessary.
- Authenticated task metadata can confirm task IDs and expose worker/delivery
  identifiers. Use unguessable task IDs and minimize access to the operator
  token.
- HTTP protection does not secure Redis, logs, traces, or the host. Those need
  independent access control, transport security, and retention policies.
