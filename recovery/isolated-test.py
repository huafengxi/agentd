#!/usr/bin/env python3
"""Execute one stdlib test in a self-created root with safe fixture cleanup.

Private harness only; not a production entrypoint. No network/model/service calls.
The parent sets TMPDIR to a newly created run directory and HOME to its child.
"""
import os
from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
import safe_delete

BASE = Path(tempfile.mkdtemp(prefix='isolated-test.')).resolve()
tempfile.tempdir = str(BASE)
# Keep safe_delete's low-level primitive separate from the harness interception.
safe_delete.shutil = SimpleNamespace(rmtree=shutil.rmtree)


def cleanup(target, *args, **kwargs):
    # stdlib TemporaryDirectory can call cleanup twice; absence is not a delete.
    if not os.path.exists(target):
        return
    deleted, reason = safe_delete.safe_rmtree(target, authorized_root=BASE)
    if not deleted:
        raise RuntimeError(reason)


shutil.rmtree = cleanup
script = Path(os.environ.get('AGENTD_TEST_SCRIPT') or sys.argv[1]).resolve()
assert script.parent == REPO and script.name.startswith('test_'), script
os.environ['AGENTD_TEST_SCRIPT'] = str(script)
sys.argv = [str(script)]
print('Retained harness authorization root:', BASE, flush=True)
# exec keeps multiprocessing spawn's __mp_main__ module identity intact.
__file__ = str(script)
exec(compile(script.read_bytes(), str(script), 'exec'), globals())
