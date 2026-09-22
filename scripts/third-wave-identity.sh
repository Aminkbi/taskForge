#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

baseline="${TASKFORGE_WAVE3_BASELINE:-4446ab3a699f9fc749f58b74fdcf35164c492f0c}"
treatment="${TASKFORGE_WAVE3_TREATMENT:-b2947f3}"
git cat-file -e "$baseline^{commit}"
git cat-file -e "$treatment^{commit}"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
git diff --binary "$baseline" "$treatment" >"$tmp"
printf 'baseline %s\n' "$baseline"
printf 'treatment %s\n' "$treatment"
printf 'treatment_sha256 '
sha256sum "$tmp" | awk '{print $1}'
