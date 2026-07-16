#!/usr/bin/env bash
set -euo pipefail

status=0
while IFS=: read -r file line link; do
	target="${link#](}"
	target="${target%%#*}"
	if [[ ! -e "$(dirname "$file")/$target" ]]; then
		echo "$file:$line: missing relative Markdown target: $target" >&2
		status=1
	fi
done < <(rg --no-heading -n -o '\]\((\./|\.\./)[^ )#]+' --glob '*.md' --glob '!docs/roadmap/**' .)

phony=" $(sed -n 's/^\.PHONY: //p' Makefile) "
while read -r command target; do
	if [[ " $phony " != *" $target "* ]]; then
		echo "documentation references unknown Make target: $target" >&2
		status=1
	fi
done < <(rg --no-filename -o 'make[[:space:]]+[a-z][a-z-]*' --glob '*.md' --glob '!docs/roadmap/**' . | tr -s ' ')

exit "$status"
