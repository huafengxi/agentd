"""Immutable cooperative foreground fixture; this report is NOT model output."""
import json
import os
from pathlib import Path
import signal
import sys
import time

track = Path(sys.argv[1])
mode, report, rc = sys.argv[2], sys.argv[3] == 'report', int(sys.argv[4])
track.mkdir(parents=True, exist_ok=True)
lease = int(os.environ['RUND_LEASE_FD'])
st = os.fstat(lease)  # Must really hold S's inherited lease for entire lifetime.
with (track / 'effects').open('a') as f:
    f.write('one invocation\n')
if mode == 'ignore-term':
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
(track / 'ready.json').write_text(json.dumps({'pid': os.getpid(), 'pgid': os.getpgrp(),
    'sid': os.getsid(0), 'cwd': os.getcwd(), 'env': dict(os.environ),
    'lease': {'fd': lease, 'ino': st.st_ino, 'dev': st.st_dev}}))
if mode in ('barrier', 'ignore-term'):
    while not (track / 'finish').exists():
        time.sleep(0.01)
if report:
    (Path(os.environ['AGENT_HOME']) / 'report.md').write_text(
        '# SYNTHETIC FIXTURE DELIVERY — not model work\n\n'
        'This pinned payload itself wrote this report before exiting.\n')
(track / 'done').write_text('payload reached exit\n')
sys.exit(rc)
