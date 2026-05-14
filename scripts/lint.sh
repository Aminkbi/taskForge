#!/usr/bin/env bash
set -euo pipefail

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-/tmp/taskforge-cache}"

go vet ./...
test -z "$(gofmt -l .)" || { echo "gofmt reported unformatted files"; gofmt -l .; exit 1; }
if command -v staticcheck >/dev/null 2>&1; then
  staticcheck_output="$(mktemp)"
  if ! staticcheck ./... >"$staticcheck_output" 2>&1; then
    if grep -q "Staticcheck was built with" "$staticcheck_output" && [[ "${TASKFORGE_REQUIRE_STATICCHECK:-}" != "1" ]]; then
      cat "$staticcheck_output"
      echo "staticcheck is installed but incompatible with this module Go version; install the pinned version from docs/development/toolchain.md"
    else
      cat "$staticcheck_output"
      exit 1
    fi
  else
    cat "$staticcheck_output"
  fi
  rm -f "$staticcheck_output"
fi
