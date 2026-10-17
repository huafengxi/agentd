"""Cooperative foreground P3b fixture; each real process keeps the lease FD."""
import json
import os
from pathlib import Path
import signal
import sys
import time

track = Path(sys.argv[1]); mode, report, rc = sys.argv[2], sys.argv[3] == 'report', int(sys.argv[4])
track.mkdir(parents=True, exist_ok=True)
st = os.fstat(int(os.environ['RUND_LEASE_FD']))
facts = {'pid': os.getpid(), 'pgid': os.getpgrp(), 'sid': os.getsid(0),
         'cwd': os.getcwd(), 'env': dict(os.environ), 'lease': {'ino': st.st_ino, 'dev': st.st_dev}}
with (track / 'effects').open('a') as f:
    f.write(json.dumps(facts) + '\n')
if mode == 'ignore-term':
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
(track / ('%d.ready' % os.getpid())).write_text(json.dumps(facts))
if mode in ('barrier', 'ignore-term'):
    while not (track / ('%d.finish' % os.getpid())).exists():
        time.sleep(0.01)
if report:
    (Path(os.environ['AGENT_HOME']) / 'report.md').write_text('# SYNTHETIC P3b delivery, not model work\n')
(track / ('%d.done' % os.getpid())).write_text('natural exit\n')
sys.exit(rc)
