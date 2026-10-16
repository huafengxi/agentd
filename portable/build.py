#!/usr/bin/env python3
"""Build an allowlisted source archive offline; not an installer."""
import argparse
import gzip
import io
import json
from pathlib import Path
import sys
import tarfile

BASE = Path(__file__).resolve().parent.parent


def manifest():
    paths = json.loads((BASE / 'portable/files.json').read_text(encoding='utf-8'))
    if len(paths) != len(set(paths)):
        raise ValueError('duplicate allowlist member')
    for name in paths:
        p = Path(name)
        if p.is_absolute() or '..' in p.parts or any(x.startswith('.') for x in p.parts):
            raise ValueError('unsafe allowlist member: ' + name)
        source = BASE / p
        if not source.is_file() or any(x.is_symlink() for x in [source, *source.parents]):
            raise ValueError('allowlist requires regular non-symlink files: ' + name)
    return paths


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--output', required=True, type=Path)
    args = ap.parse_args()
    paths = manifest()
    # Exclusive creation; metadata does not disclose the build host's identity.
    with args.output.open('xb') as raw:
        with gzip.GzipFile(filename='', fileobj=raw, mode='wb', mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode='w') as archive:
                for name in paths:
                    info = tarfile.TarInfo('agentd-minimal/' + name)
                    data = (BASE / name).read_bytes()
                    info.size = len(data)
                    info.mode = 0o755 if name == 'portable/run.sh' else 0o644
                    archive.addfile(info, io.BytesIO(data))
    print(args.output.resolve())


if __name__ == '__main__':
    try:
        if sys.version_info < (3, 10):
            raise ValueError('Python >= 3.10 required; select your own Python interpreter')
        main()
    except (OSError, ValueError) as exc:
        print('portable build: ' + str(exc), file=sys.stderr)
        sys.exit(2)
