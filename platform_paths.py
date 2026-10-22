"""Resolve the caller workspace from an explicit PLATFORM_WORKSPACE or the real source location."""
import os
from pathlib import Path
import sys


def workspace_root(source):
    """Return nonempty override (expanded/absolute), else source repo's parent."""
    override = os.environ.get("PLATFORM_WORKSPACE")
    if override:
        return str(Path(override).expanduser().resolve())
    return str(Path(source).resolve().parent.parent)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: platform_paths.py <component entry path>")
    print(workspace_root(sys.argv[1]))
