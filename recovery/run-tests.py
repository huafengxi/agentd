#!/usr/bin/env python3
"""Offline standalone recovery validation; retain logs in a new isolated root."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

REPO = Path(__file__).resolve().parent.parent
base = Path(tempfile.mkdtemp(prefix='agentd-validation.', dir='/tmp')).resolve()
for name in ('home', 'tmp', 'logs'):
    (base / name).mkdir()
env = {'HOME': str(base / 'home'), 'TMPDIR': str(base / 'tmp'),
       'PATH': str(Path(sys.executable).parent) + os.pathsep + os.defpath,
       'LANG': 'C.UTF-8', 'PYTHONDONTWRITEBYTECODE': '1'}
commands = [[sys.executable, str(REPO / 'recovery/isolated-test.py'), str(p)]
            for p in sorted(REPO.glob('test_*.py'))]
commands += [[sys.executable, '-B', str(REPO / 'e2e.py'), '--core'],
             [sys.executable, '-I', '-S', '-B', str(REPO / 'portable/test_portable.py')]]
results = []
print('Validation root:', base, flush=True)
for i, command in enumerate(commands):
    label = Path(command[-1]).name if '--core' not in command else 'e2e-core'
    log = base / 'logs' / ('%02d-' % i + label + '.log')
    with log.open('w') as out:
        try:
            p = subprocess.run(command, cwd=base, env=env, stdout=out,
                               stderr=subprocess.STDOUT, timeout=900)
            rc = p.returncode
        except subprocess.TimeoutExpired:
            rc = 124
    results.append({'command': command, 'returncode': rc, 'log': str(log)})
    print(label, rc, log, flush=True)
    (base / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
sys.exit(0 if all(r['returncode'] == 0 for r in results) else 1)
