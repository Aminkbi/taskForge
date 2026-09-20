#!/usr/bin/env bash
set -euo pipefail

export GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-/tmp/taskforge-cache}"

# vet and staticcheck are module-scoped, so the nested research module is
# checked explicitly rather than silently dropped from the gate.
go vet ./...
go -C research vet ./...

test -z "$(gofmt -l .)" || { echo "gofmt reported unformatted files"; gofmt -l .; exit 1; }

run_staticcheck() {
  local output
  output="$(mktemp)"
  if ! staticcheck "$@" >"$output" 2>&1; then
    if grep -q "Staticcheck was built with" "$output" && [[ "${TASKFORGE_REQUIRE_STATICCHECK:-}" != "1" ]]; then
      cat "$output"
      echo "staticcheck is installed but incompatible with this module Go version; install the pinned version from docs/development/toolchain.md"
    else
      cat "$output"
      rm -f "$output"
      exit 1
    fi
  else
    cat "$output"
  fi
  rm -f "$output"
}

if command -v staticcheck >/dev/null 2>&1; then
  run_staticcheck ./...
  (cd research && run_staticcheck ./...)
fi
