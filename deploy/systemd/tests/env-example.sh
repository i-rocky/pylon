#!/usr/bin/env bash
set -euo pipefail

CONFIG_RS="src/server/config.rs"
REFERENCE="website/docs/user-guide/configuration.md"
ENV_EXAMPLE="deploy/systemd/pylon.env.example"

ref_vars=$(grep -oE '^\| `PYLON_[A-Z0-9_]+`' "$REFERENCE" | sed -E 's/^\| `//; s/`$//' | sort -u)

missing=()
while IFS= read -r var; do
    if [ -n "$var" ] && ! grep -qE "^#?${var}=" "$ENV_EXAMPLE"; then
        missing+=("$var")
    fi
done <<< "$ref_vars"

if [ "${#missing[@]}" -gt 0 ]; then
    echo "FAIL: missing from $ENV_EXAMPLE: ${missing[*]}"
    exit 1
fi

assign_vars=$(grep -oE '^#?PYLON_[A-Z0-9_]+=' "$ENV_EXAMPLE" | sed -E 's/^#//; s/=$//' | sort)

unknown=()
while IFS= read -r var; do
    if [ -n "$var" ] && ! grep -qw -- "$var" "$CONFIG_RS"; then
        unknown+=("$var")
    fi
done <<< "$assign_vars"

if [ "${#unknown[@]}" -gt 0 ]; then
    echo "FAIL: assigned in $ENV_EXAMPLE but absent from $CONFIG_RS: ${unknown[*]}"
    exit 1
fi

dupes=$(printf '%s\n' "$assign_vars" | uniq -d | tr '\n' ' ')
if [ -n "${dupes// /}" ]; then
    echo "FAIL: assigned more than once in $ENV_EXAMPLE: $dupes"
    exit 1
fi

if ! ( set -a; . "$ENV_EXAMPLE"; set +a ); then
    echo "FAIL: $ENV_EXAMPLE did not source cleanly"
    exit 1
fi

ref_count=$(printf '%s\n' "$ref_vars" | grep -c .)
assign_count=$(printf '%s\n' "$assign_vars" | grep -c .)

echo "OK: $ref_count reference variables all documented, $assign_count assignment lines resolve against $CONFIG_RS, none duplicated, active lines source cleanly"
