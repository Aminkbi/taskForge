# Security Policy

TaskForge has not reached a stable release line yet.
Until the first supported release is published, security fixes are handled on the main branch and included in the next tagged release.

## Reporting a Vulnerability

Please report suspected vulnerabilities privately by opening a GitHub security advisory for the repository.
If advisories are not available, contact the maintainer privately before opening a public issue.

Include:

- Affected version or commit.
- A short reproduction or proof of concept.
- Expected impact.
- Any known mitigations.

Do not include exploit details in a public issue until a fix is available.

## Scope

Security-sensitive areas include:

- Task payload handling.
- Redis key ownership and multi-tenant queue isolation.
- Admission control and replay paths.
- Worker lease ownership and stale acknowledgement rejection.
- Admin and metrics endpoints exposed in deployments.

Deployment guidance for the shared-token HTTP boundary, reverse-proxy TLS, and
its residual risks is in the [HTTP security guide](docs/operations/http-security.md).
