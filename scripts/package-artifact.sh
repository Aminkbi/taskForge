#!/usr/bin/env bash
set -euo pipefail

# Builds the Zenodo-ready research artifact tarball from committed evidence.
# It packages and checksums only; uploading or minting a DOI is a separate,
# human-approved action.

OUT_DIR="${1:-dist}"
STAGE="$(mktemp -d)/taskforge-research-artifact"
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(git show -s --format=%ct HEAD)}"
trap 'rm -rf "$(dirname "$STAGE")"' EXIT

mkdir -p "$STAGE" "$OUT_DIR"
cp -r research "$STAGE/research"
cp CITATION.cff .zenodo.json LICENSE "$STAGE/"

(
  cd "$STAGE"
  find . -type f ! -name MANIFEST.sha256 -print0 | sort -z |
    xargs -0 sha256sum >MANIFEST.sha256
)

tar --sort=name --mtime="@$SOURCE_DATE_EPOCH" --owner=0 --group=0 --numeric-owner \
  -cf - -C "$(dirname "$STAGE")" "$(basename "$STAGE")" |
  gzip -n >"$OUT_DIR/taskforge-research-artifact.tar.gz"
sha256sum "$OUT_DIR/taskforge-research-artifact.tar.gz"
