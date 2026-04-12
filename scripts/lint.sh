#!/usr/bin/env bash
set -euo pipefail

go vet ./...
test -z "$(gofmt -l .)" || { echo "gofmt reported unformatted files"; gofmt -l .; exit 1; }
if command -v staticcheck >/dev/null 2>&1; then
  staticcheck ./...
fi
