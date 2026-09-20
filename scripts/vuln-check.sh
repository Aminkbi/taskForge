#!/usr/bin/env bash
set -euo pipefail

# govulncheck reports vulnerabilities reachable through this module's source.
# Its exit status is intentionally fatal: suppressions require an explicit,
# reviewed policy exception in SECURITY.md, never a blanket ignore file.
# The scan is module-scoped, so the nested research module is scanned too.
go tool govulncheck ./...
go -C research tool govulncheck ./...
