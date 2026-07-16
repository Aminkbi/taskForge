#!/usr/bin/env bash
set -euo pipefail

# govulncheck reports vulnerabilities reachable through this module's source.
# Its exit status is intentionally fatal: suppressions require an explicit,
# reviewed policy exception in SECURITY.md, never a blanket ignore file.
go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...
