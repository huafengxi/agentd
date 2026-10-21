"""Test-only subprocess probe: separate A / fixed S owner, effect audit and faults."""
import json
import os
from pathlib import Path
import sys
import time
import traceback

role, config_path = sys.argv[1:]
c = json.loads(Path(config_path).read_text())
sys.dont_write_bytecode = True
source = Path(c['s_source'] if role == 'owner' else c['a_source'])
sys.path.insert(0, str(source))
log = Path(c['base']) / (role + '-audit.jsonl')


def record(value):
    with log.open('a') as f:
        f.write(json.dumps(value, default=str) + '\n')


def audit(event, args):
    if event not in ('subprocess.Popen', 'os.kill', 'os.killpg'):
        return
    stack = traceback.extract_stack()
    kernel = str(Path(c['s_source']) / 'runtime/kernel.py')
    prepare = str(Path(c['s_source']) / 'runtime/prepare.py')
    frames = [(f.filename, f.name) for f in stack]
    category = None
    if role == 'owner':
        if event == 'subprocess.Popen' and (kernel, 'launch') in frames:
            category = 'payload'
        elif event == 'subprocess.Popen' and (prepare, 'run') in frames:
            category = 'prepare-git'
        elif event in ('os.killpg', 'os.kill') and (kernel, 'stop') in frames:
            category = 'payload-control'
    record({'event': event, 'category': category, 'args': args, 'stack': [str(f) for f in stack]})
    if category is None:
        raise AssertionError('unexpected second execution/supervision: ' + role + ':' + event)


sys.addaudithook(audit)
if role == 'owner':
    from runtime.file_owner.owner import Owner
    def hook(event, envelope):
        if c.get('fault') == [event, envelope.get('action')]:
            (Path(c['base']) / 'fault.json').write_text(json.dumps({'event': event, 'envelope': envelope}))
            os._exit(93)  # original pending record preserved; real OS exit, not a new object
    def kernel_hook(event, run):
        record({'event': 'kernel-' + event, 'run': run, 'stack': traceback.format_stack()})
    with_owner = Owner(c['runtime'], c['protocol'], 'fixture', hook=hook, kernel_hook=kernel_hook)
    import hashlib
    record({'event': 'fixed-owner-imports', 'ownerPid': os.getpid(),
            'ownerEpoch': with_owner.runtime.kernel.epoch,
            'imports': {n: {'path': str(Path(m.__file__)), 'sha256': hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest()}
                        for n, m in sys.modules.items() if (n == 'runtime' or n.startswith('runtime.') or n.startswith('runtime.file_owner')) and getattr(m, '__file__', None)}})
    try:
        with_owner.run(.02)
    finally:
        with_owner.close()
elif role == 'adapter':
    from experimental.agent_exec_adapter.file_backend import FileAdapter
    import runner
    def forbidden(*a, **kw):
        raise AssertionError('legacy execution forbidden')
    for method in ('service', 'spawn', 'observe', 'kill_proc', 'do_stop', 'do_restart', 'do_clear', 'handle_controls'):
        setattr(runner.Runner, method, forbidden)
    if 'expected_generation' in c:
        pending = runner.Runner.pending_reqs
        def faulted_pending(self, participant):
            return [dict(r, expectedGeneration=c['expected_generation']) for r in pending(self, participant)]
        runner.Runner.pending_reqs = faulted_pending  # explicit read-side control fault, not an ack writer
    original = runner.Runner.write_pid
    def projected(self, participant, doc):
        stack = traceback.extract_stack()
        assert any(f.name == 'project' and f.filename == str(source / 'experimental/agent_exec_adapter/file_backend.py') for f in stack)
        record({'event': 'agent-projection', 'participant': participant, 'doc': doc, 'stack': [str(f) for f in stack]})
        return original(self, participant, doc)
    runner.Runner.write_pid = projected
    with FileAdapter(sandbox_root=c['base'], workspace=c['workspace'], runtime_root=c['runtime'],
                     protocol_root=c['protocol'], runtime_source=c['s_source'], manifest=c['manifest']) as a:
        # Block even an accidental direct construction before it can take an S lock.
        assert 'runtime.lifecycle' not in sys.modules and 'runtime.kernel' not in sys.modules
        if c.get('negative_audit_probe'):
            import subprocess
            try:
                subprocess.run([sys.executable, str(Path(c['base']) / 'source/payload.py')], check=True)
            except AssertionError as error:
                record({'event': 'negative-audit-rejected', 'reason': str(error)})
            else:
                raise AssertionError('negative supervision audit failed open')
        end = time.monotonic() + c.get('seconds', 90)
        while time.monotonic() < end:
            a.tick()
            time.sleep(.02)
else:
    raise ValueError(role)
