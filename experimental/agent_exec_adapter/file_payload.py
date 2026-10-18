"""Cooperative synthetic fixture ONLY, not model work. All children retain lease."""
import json
import os
from pathlib import Path
import signal
import sys
import time

track = Path(sys.argv[1])
mode, report, code = sys.argv[2], sys.argv[3] == 'report', int(sys.argv[4])
track.mkdir(parents=True, exist_ok=True)
fd = int(os.environ['RUND_LEASE_FD'])
st = os.fstat(fd)

def identity():
    tail = Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()
    return {'pid': os.getpid(), 'procStart': int(tail[19]), 'pgid': os.getpgrp(),
            'session': os.getsid(0), 'uid': os.getuid()}

with (track / 'effects').open('a') as f:
    f.write('one real invocation\n')
if mode == 'ignore-term':
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
if mode == 'fork':
    child = os.fork()
    if child == 0:
        (track / 'child.json').write_text(json.dumps(identity()))
        while not (track / 'child-finish').exists():
            time.sleep(.01)
        os._exit(0)
    while not (track / 'child.json').exists():
        time.sleep(.01)
(track / 'ready.json').write_text(json.dumps({'identity': identity(), 'env': dict(os.environ),
    'cwd': os.getcwd(), 'lease': {'fd': fd, 'dev': st.st_dev, 'ino': st.st_ino}}))
if mode in ('barrier', 'ignore-term'):
    while not (track / 'finish').exists():
        time.sleep(.01)
if report:
    (Path(os.environ['AGENT_HOME']) / 'report.md').write_text(
        '# SYNTHETIC FIXTURE DELIVERY — not model work\n\n'
        'Explicit pinned cooperative fixture wrote this report before exit.\n')
(track / 'done').write_text('real payload exit\n')
sys.exit(code)
