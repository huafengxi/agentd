"""Resident-only OS probe; not imported by the old one-shot suite.

Copied mechanical audit/fault helpers from mrkdre, rechecked for fixed new S.
All deferrals/crashes below are labelled test scheduling/IO fault injection.
"""
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
    from experimental.file_owner.owner import Owner
    if c.get('defer_action'):
        original_process = Owner.process
        def deferred_process(self, path):
            envelope = json.loads(path.read_text())
            if (envelope['action'] == c['defer_action'] and
                    not (Path(c['base']) / 'allow-acceptance').exists()):
                record({'event': 'deferred-before-acceptance', 'envelope': envelope})
                return  # only delay request handling; original policy ticks still run
            return original_process(self, path)
        Owner.process = deferred_process
    def hook(event, envelope):
        if c.get('fault') == [event, envelope.get('action')]:
            (Path(c['base']) / 'fault.json').write_text(json.dumps({'event': event, 'envelope': envelope}))
            os._exit(93)  # original pending record preserved; real OS exit, not a new object
    def kernel_hook(event, run):
        record({'event': 'kernel-' + event, 'run': run, 'stack': traceback.format_stack()})
    if c.get('replacement_failure'):
        import runtime.lifecycle as lifecycle
        original_prepare = lifecycle.prepare
        original_launch = lifecycle.Kernel.launch
        def fail_prepare(d, root):
            if c['replacement_failure'] == 'prepare' and list((Path(root) / 'state/runs').glob('*/*/*/*/result.json')):
                record({'event': 'injected-replacement-prepare-failure'})
                raise OSError('explicit fixture replacement prepare failure')
            return original_prepare(d, root)
        def fail_launch(self, run, d, prepared):
            if c['replacement_failure'] == 'start' and run['generation'] > 1:
                record({'event': 'injected-replacement-start-failure'})
                raise OSError('explicit fixture replacement start failure')
            return original_launch(self, run, d, prepared)
        lifecycle.prepare = fail_prepare
        lifecycle.Kernel.launch = fail_launch
    with_owner = Owner(c['runtime'], c['protocol'], 'fixture', hook=hook, kernel_hook=kernel_hook)
    import hashlib
    record({'event': 'fixed-owner-imports', 'ownerPid': os.getpid(),
            'ownerEpoch': with_owner.runtime.kernel.epoch,
            'imports': {n: {'path': str(Path(m.__file__)), 'sha256': hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest()}
                        for n, m in sys.modules.items() if (n == 'runtime' or n.startswith('runtime.') or n.startswith('experimental.file_owner')) and getattr(m, '__file__', None)}})
    try:
        with_owner.run(.02)
    finally:
        with_owner.close()
elif role == 'adapter':
    from experimental.agent_exec_adapter.file_backend import FileAdapter
    projection_module = 'file_backend.py'
    if c.get('resident'):
        from experimental.agent_exec_adapter.resident_file import ResidentFileAdapter as FileAdapter
        projection_module = 'resident_file.py'
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
    if c.get('real_claim'):
        import grp
        from types import SimpleNamespace
        original_group = grp.getgrnam
        def fixture_group(name):
            return SimpleNamespace(gr_gid=os.getgid()) if name == 'replica' else original_group(name)
        grp.getgrnam = fixture_group
        original_claim = runner.Runner._claim_origin
        def real_claim(self, path):
            original_claim(self, path)
            record({'event': 'real-claim', 'path': path, 'uid': os.stat(path).st_uid, 'gid': os.stat(path).st_gid})
        runner.Runner._claim_origin = real_claim
    if c.get('history_fault'):
        import builtins
        original_backup = runner.Runner._backup_and_clear_session
        original_open = builtins.open
        def fault_open(path, mode='r', *args, **kwargs):
            if c['history_fault'] == 'truncate' and str(path).endswith('/session/session.jsonl') and mode == 'w':
                raise OSError('explicit fixture truncate failure')
            return original_open(path, mode, *args, **kwargs)
        def fail_claim(self, path):
            raise OSError('explicit fixture exception at weak claim boundary')
        if c['history_fault'] == 'claim':
            runner.Runner._claim_origin = fail_claim
        def history_fault(self, participant):
            record({'event': 'injected-history-failure', 'point': c['history_fault']})
            if c['history_fault'] == 'backup':
                raise OSError('explicit fixture backup failure')
            builtins.open = fault_open
            try:
                return original_backup(self, participant)
            finally:
                builtins.open = original_open
        runner.Runner._backup_and_clear_session = history_fault
    backup = runner.Runner._backup_and_clear_session
    def checked_backup(self, participant):
        state = a.read_state(participant)
        control = state['control']
        assert control['stage'] == 'history'
        stopped, head = control['stopped'], control['stoppedHead']
        if head:
            assert head['hold'] and head['phase'] == 'stopped'
        if stopped:
            a.validate_run(state, stopped, generation=stopped['generation'])
            if stopped.get('stopEvidence'):
                evidence = stopped['stopEvidence'][-1]
                assert not evidence['leaseBusy']
                assert not [m for m in evidence['members'] if m.get('state') not in ('Z', 'X')]
        record({'event': 'history-after-exact-stop-before-backup', 'participant': participant,
                'stopped': stopped, 'head': head})
        return backup(self, participant)
    runner.Runner._backup_and_clear_session = checked_backup
    if c.get('ack_fault'):
        def fail_ack(*args, **kwargs):
            raise OSError('explicit fixture ack storage failure')
        runner.Runner.write_ack = fail_ack
    original = runner.Runner.write_pid
    def projected(self, participant, doc):
        stack = traceback.extract_stack()
        assert any(f.name == 'project' and f.filename == str(source / 'experimental/agent_exec_adapter' / projection_module) for f in stack)
        record({'event': 'agent-projection', 'participant': participant, 'doc': doc, 'stack': [str(f) for f in stack]})
        return original(self, participant, doc)
    runner.Runner.write_pid = projected
    if c.get('fault_before_send') or c.get('fault_after_send'):
        fixture_client, _ = FileAdapter.load_client(c['s_source'])
        original_send = fixture_client.send
        def fault_send(protocol, envelope):
            if envelope['action'] == c.get('fault_before_send'):
                record({'event': 'injected-before-send-crash', 'envelope': envelope})
                os._exit(94)
            result = original_send(protocol, envelope)
            if envelope['action'] == c.get('fault_after_send'):
                record({'event': 'injected-after-send-crash', 'envelope': envelope})
                os._exit(95)
            return result
        fixture_client.send = fault_send
    with FileAdapter(sandbox_root=c['base'], workspace=c['workspace'], runtime_root=c['runtime'],
                     protocol_root=c['protocol'], runtime_source=c['s_source'], manifest=c['manifest']) as a:
        # Block even an accidental direct construction before it can take an S lock.
        assert 'runtime.lifecycle' not in sys.modules and 'runtime.kernel' not in sys.modules
        class NoExecutionImport:
            def find_spec(self, fullname, path=None, target=None):
                if fullname in ('runtime.lifecycle', 'runtime.kernel'):
                    raise AssertionError('A cannot import execution owner: ' + fullname)
        sys.meta_path.insert(0, NoExecutionImport())
        if c.get('negative_audit_probe'):
            for module in ('runtime.lifecycle', 'runtime.kernel'):
                try:
                    __import__(module)
                except AssertionError:
                    record({'event': 'negative-import-rejected', 'module': module})
                else:
                    raise AssertionError('execution import sentinel failed open')
            try:
                os.kill(os.getpid(), 0)
            except AssertionError:
                record({'event': 'negative-signal-rejected'})
            else:
                raise AssertionError('signal sentinel failed open')
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
