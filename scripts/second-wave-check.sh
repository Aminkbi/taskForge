#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# The paired study's code lock binds individual source files by repository
# path, so it can only be verified in the tree that produced it. This check
# extracts the recorded artifact commit and runs that tree's own
# self-contained check, which keeps the frozen claim verifiable after the
# research code moved into its own module.
COMMIT="${TASKFORGE_SECOND_WAVE_COMMIT:-751a0bd084bc3f06a7037f8cb536abb8edc145b6}"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
SOURCE="$TMP/source"
mkdir -p "$SOURCE"

git cat-file -e "$COMMIT^{commit}"

git archive "$COMMIT" | tar -x -C "$SOURCE"

# Guard: the frozen artifact bytes must still match the recorded commit.
if ! diff -ru research/second-wave "$SOURCE/research/second-wave"; then
  echo "frozen second-wave artifact differs from recorded commit $COMMIT" >&2
  exit 1
fi

# Binary digests are only reproducible under the recorded toolchain, so pin it
# from the archived module file rather than using whatever is installed.
toolchain="go$(awk '/^go /{ print $2; exit }' "$SOURCE/go.mod")"

(
  cd "$SOURCE"
  GOCACHE="${GOCACHE:-/tmp/taskforge-gocache}" GOTOOLCHAIN="$toolchain" ./scripts/second-wave-check.sh
)

echo "verified under recorded toolchain $toolchain"

echo "second-wave artifact verified against recorded commit $COMMIT"
