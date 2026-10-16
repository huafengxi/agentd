#!/bin/sh
# Run in place. PATH and the selected interpreter are caller-trusted executables.
set -eu
BASE=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
PYTHON=${PLATFORM_PYTHON:-python3}
case "$PYTHON" in
    /*) ;;
    */*) PYTHON="$BASE/$PYTHON" ;;
esac
if ! command -v "$PYTHON" >/dev/null 2>&1; then
    printf '%s\n' 'portable: Missing dependency: Python >= 3.10; install python3 or set PLATFORM_PYTHON' >&2
    exit 2
fi
exec env -i PATH="${PATH:-/usr/bin:/bin}" PLATFORM_WORKSPACE="${PLATFORM_WORKSPACE:-}" \
    "$PYTHON" -I -S -B "$BASE/portable/cli.py" "$@"
