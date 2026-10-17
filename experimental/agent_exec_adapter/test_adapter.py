"""Retained P3a integration fixtures. No model, legacy runner, manual enable/final/ack.

Required P3A_TEST_OUTPUT is a NEW absolute directory. Required
P3A_RUNTIME_SOURCE is a fixed S archive; test artifacts are never auto-deleted.
Run with miniconda Python -B -m unittest experimental.agent_exec_adapter.test_adapter -v.
"""
import ctypes
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import traceback
import unittest
from unittest.mock import patch

from .adapter import Adapter, A_ROOT, runtime_id, workdir, load_runtime
import proto
import runner
import scheduler
import scheduling
import task_state
from safe_delete import safe_rmtree

BASE = Path(os.environ['P3A_TEST_OUTPUT'])
if not BASE.is_absolute() or BASE.exists():
    raise RuntimeError('P3A_TEST_OUTPUT must be new absolute directory')
BASE.mkdir(parents=True)
S_SOURCE = Path(os.environ['P3A_RUNTIME_SOURCE'])
Runtime, DEP, PACKAGE = load_runtime(S_SOURCE)
K = __import__(PACKAGE + '.kernel', fromlist=['proc'])
ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0)  # adopt ONLY our crashed-owner fixtures


def forbidden(*args, **kwargs):
    raise AssertionError('legacy runner supervision was invoked')


def wait_for(predicate, seconds=8):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        result = predicate()
        if result:
            return result
        time.sleep(0.01)
    raise AssertionError('barrier/deadline not reached')


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.base = BASE / self._testMethodName
        self.base.mkdir()
        self.workspace, self.node = self.base / 'workspace', self.base / 'node'
        (self.workspace / 'agents').mkdir(parents=True)
        (self.workspace / 'env').mkdir()
        (self.workspace / 'env/host-id').write_text(socket.gethostname() + ' fixture\n')
        self.source = self.base / 'source'
        self.source.mkdir()
        (self.source / 'payload.py').write_bytes(Path(__file__).with_name('fixture_payload.py').read_bytes())
        self.commands, self.events, self.adapters, self.owners, self.logs = [], [], [], [], []
        self.git('init', '-q')
        self.git('add', '--', 'payload.py')
        message = self.base / 'fixture-message.txt'
        message.write_text('Synthetic P3a fixture source, taskId zmo14t\n')
        self.git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                 'commit', '-q', '-F', str(message), '--', 'payload.py')
        self.revision = self.git('rev-parse', 'HEAD').strip()
        self.tasks = {}
        self.manifest = self.base / 'manifest.json'
        self.sched = scheduler.Scheduler(str(self.workspace), {'fixture'}, max_concurrent=1)
        self.patches = []
        for method in ('spawn', 'kill_proc', 'observe', 'service', 'do_stop', 'do_restart', 'do_clear', 'handle_controls'):
            p = patch.object(runner.Runner, method, forbidden)
            p.start(); self.patches.append(p)
        real_write = runner.Runner.write_pid
        def write_pid(agent, participant, doc):
            stack = traceback.extract_stack()
            self.assertTrue(any(f.filename.endswith(('/agent_exec_adapter/adapter.py', '/agent_exec_adapter/p3b.py')) and f.name == 'project'
                                for f in stack), 'pid writer must be adapter.project')
            self.events.append({'event': 'pid-projection', 'participant': participant,
                                'writer': os.getpid(), 'doc': doc, 'stack': [str(f) for f in stack[-5:]]})
            return real_write(agent, participant, doc)
        p = patch.object(runner.Runner, 'write_pid', write_pid)
        p.start(); self.patches.append(p)
        real_atomic = proto.atomic_write_json
        def atomic(path, value):
            if str(path).endswith('/enable.json'):
                stack = traceback.extract_stack()
                self.assertTrue(any(f.filename.endswith('/scheduler.py') and f.name == 'tick' for f in stack))
                self.events.append({'event': 'scheduler-enable', 'path': str(path), 'value': value})
            return real_atomic(path, value)
        p = patch.object(proto, 'atomic_write_json', atomic)
        p.start(); self.patches.append(p)

    def command(self, argv, *, cwd=None, data=None, check=True):
        p = subprocess.run(argv, input=data, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           cwd=cwd or self.base, env=os.environ.copy(), timeout=20)
        self.commands.append({'argv': list(map(str, argv)), 'cwd': str(cwd or self.base),
                              'rc': p.returncode, 'stdout': p.stdout, 'stderr': p.stderr})
        if check:
            self.assertEqual(p.returncode, 0, p.stderr)
        return p

    def git(self, *args):
        return self.command(['git', '-c', 'core.hooksPath=/dev/null', '-C', str(self.source), *args]).stdout

    def ctl(self, *args, check=True):
        return self.command([sys.executable, '-B', str(A_ROOT / 'agentctl.py'), '--root', str(self.workspace), *args], check=check)

    def register(self, name='a', *, mode='exit', report=True, rc=0, provides=(), needs=(), resources=('slot',), bad_pin=False):
        participant = 'task/' + name
        track = self.base / 'tracks' / name
        binding = {'command': 'p3a-fixture ' + name,
            'sources': [{'alias': 'fixture', 'repository': self.source.as_uri(),
                         'revision': '0' * 40 if bad_pin else self.revision, 'requires': []}],
            'argv': [sys.executable, '${code.fixture}/payload.py', str(track), mode,
                     'report' if report else 'none', str(rc)],
            'env': {'SYNTHETIC_VALUE': 'fixture'}, 'toolchain': {'python': sys.executable}}
        spec = {'command': binding['command'], 'workdir': str(workdir(self.node, 'fixture', participant)),
                'creator': 'queue/dispatcher', 'reaper': 'queue/dispatcher', 'createdAt': proto.now_ts(),
                'createdByHost': 'fixture', 'host': 'fixture', 'restartPolicy': 'one-shot',
                'resources': list(resources), 'provides': list(provides), 'needs': list(needs)}
        request = {'name': name, 'spec': spec,
                   'prompt': '# SYNTHETIC fixture only\nWrite fixture report if declared; not model work.\n'}
        p = self.command([sys.executable, '-B', str(A_ROOT / 'agentctl.py'), '--root', str(self.workspace), 'register-json'],
                         data=json.dumps(request))
        self.assertEqual(json.loads(p.stdout)['taskId'], name)
        self.tasks[participant] = binding
        self.manifest.write_text(json.dumps({'experiment': 'p3a-one-shot-fixtures-v1', 'host': 'fixture', 'tasks': self.tasks}))
        return participant

    def adapter(self, **extra):
        a = Adapter(sandbox_root=str(self.base), workspace=str(self.workspace), runtime_root=str(self.node),
                    runtime_source=str(S_SOURCE), manifest=str(self.manifest), **extra)
        self.adapters.append(a)
        return a

    def close(self, a):
        a.__exit__()
        self.adapters.remove(a)

    def doc(self, pid='task/a'):
        return proto.read_json(proto.pid_path(str(self.workspace), pid))

    def enabled(self, pid='task/a'):
        return Path(proto.enable_path(str(self.workspace), pid)).exists()

    def state(self, pid='task/a'):
        return task_state.classify_provider(self.doc(pid),
            task_state.report_nonempty(str(Path(proto.agent_dir(str(self.workspace), pid)) / 'report.md')),
            task_state.stop_requested(str(self.workspace), pid))

    def track(self, name='a'):
        return self.base / 'tracks' / name

    def finish(self, a, pid='task/a'):
        return wait_for(lambda: self._progress(a, pid))

    def _progress(self, a, pid):
        a.tick()
        d = self.doc(pid)
        return d if d and d.get('final') else None

    def assert_final(self, a, status, code, state, pid='task/a'):
        doc = self.finish(a, pid)
        self.assertEqual((doc['status'], doc['exitcode'], doc['final']), (status, code, True))
        self.assertEqual(self.state(pid), state)
        self.assertNotIn(pid, scheduling.scheduling_view(str(self.workspace)).occupants)
        before = Path(proto.pid_path(str(self.workspace), pid)).read_bytes()
        a.tick(); a.tick()
        self.assertEqual(Path(proto.pid_path(str(self.workspace), pid)).read_bytes(), before)
        return doc

    def run_for(self, a, pid='task/a'):
        return a.runtime.task(runtime_id('fixture', pid), 'a1')

    def stop(self, pid='task/a'):
        return self.ctl('cancel', pid, '--from', 'queue/dispatcher', '--reason', 'synthetic cancel').stdout.strip()

    def ack(self, rid, pid='task/a'):
        return proto.read_json(str(Path(proto.control_path(str(self.workspace), pid)) / 'ack' / rid))

    def test_01_real_success_needs_chain_and_isolation(self):
        self.register(provides=['fixture-done'])
        self.register('b', needs=['fixture-done'])
        self.sched.tick()  # target missing: original gate must actually block
        self.assertFalse(self.enabled())
        a = self.adapter()
        self.sched.tick()
        self.assertTrue(self.enabled()); self.assertFalse(self.enabled('task/b'))
        a.tick()
        self.assert_final(a, 'exited', 0, 'success')
        self.sched.tick()
        self.assertTrue(self.enabled('task/b'))
        a.tick()
        self.assert_final(a, 'exited', 0, 'success', 'task/b')
        ready = json.loads((self.track() / 'ready.json').read_text())
        env = ready['env']
        self.assertEqual(env['AGENT_SELF'], 'task/a')
        self.assertEqual(env['AGENTD_TASK'], '1')
        self.assertEqual(env['AGENT_HOME'], proto.agent_dir(str(self.workspace), 'task/a'))
        self.assertEqual(env['AGENTD_ROOT'], str(self.workspace))
        self.assertEqual(env['AGENTD_ASK_INBOX'], proto.position_inbox(str(self.workspace)))
        for key in ('PI_SESSION_ID', 'PI_WEB_PASSWORD', 'AGENTD_RESIDENT', 'DISPATCH_PROFILE', 'OPENAI_API_KEY', 'AI_AGENT'):
            self.assertNotIn(key, env)
        run = self.run_for(a)
        self.assertEqual(ready['cwd'], str(workdir(self.node, 'fixture', 'task/a')))
        self.assertTrue(str(run['stdout']).startswith(str(self.node / 'logs')))
        lease = a.runtime.kernel.run_dir(run) / 'lease.lock'
        self.assertEqual((ready['lease']['dev'], ready['lease']['ino']), (lease.stat().st_dev, lease.stat().st_ino))
        self.assertEqual(list((self.node / 'protocol').iterdir()), [])
        self.assertEqual(list((self.workspace / 'agents').rglob('lease.lock')), [])
        self.assertFalse((self.workspace / 'run/locks').exists())
        self.assertEqual(self.state('task/b'), 'success')
        notices = list(Path(proto.position_inbox(str(self.workspace))).glob('*.msg'))
        self.assertEqual(len(notices), 2)
        for p in notices:
            envelope = json.loads(p.read_text()); body = json.loads(envelope['body'])
            self.assertEqual((envelope['type'], body['event']), ('inform', 'task_done'))
            self.assertIn('SYNTHETIC FIXTURE', Path(body['report']).read_text())
        self.assertTrue(any(e['event'] == 'scheduler-enable' for e in self.events))

    def test_02_exit0_without_report_idle_needs_blocked(self):
        self.register(report=False, provides=['no-report'])
        self.register('b', needs=['no-report'])
        a = self.adapter(); self.sched.tick(); a.tick()
        self.assert_final(a, 'exited', 0, 'idle')
        self.sched.tick(); self.assertFalse(self.enabled('task/b'))
        notice = json.loads(next(Path(proto.position_inbox(str(self.workspace))).glob('*.msg')).read_text())
        self.assertEqual(json.loads(notice['body'])['warn'], 'no_report')

    def test_03_nonzero_report_failed(self):
        self.register(rc=7); a = self.adapter(); self.sched.tick(); a.tick()
        self.assert_final(a, 'exited', 7, 'failed')
        self.assertTrue((self.track() / 'done').exists())

    def test_04_nonzero_without_report_failed(self):
        self.register(rc=7, report=False); a = self.adapter(); self.sched.tick(); a.tick()
        self.assert_final(a, 'exited', 7, 'failed')

    def test_05_prepare_error_no_execution_or_repeat(self):
        self.register(bad_pin=True); a = self.adapter(); self.sched.tick(); a.tick()
        self.assert_final(a, 'exited', 126, 'failed')
        self.assertFalse(self.track().exists())
        run = self.run_for(a)
        self.assertTrue(run['reason'].startswith('prepare:'))
        self.assertIsNone(run['pid']); self.assertFalse(run['waitObserved'])
        self.assertEqual(run['phase'], 'failed')

    def test_06_queued_cancel_before_enable_frees_queue(self):
        self.register(needs=['missing'], provides=['canceled'])
        self.register('b')
        a = self.adapter(); rid = self.stop(); a.tick()
        self.assertFalse(self.enabled())
        doc = self.assert_final(a, 'killed', proto.EXITCODE_KILLED, 'canceled')
        self.assertEqual(doc['gen'], 0)
        self.assertEqual(self.ack(rid)['outcome'], 'applied')
        self.assertFalse(self.track().exists())
        self.assertEqual(self.run_for(a)['phase'], 'canceled')
        self.sched.tick(); self.assertTrue(self.enabled('task/b'))
        a.tick(); self.assert_final(a, 'exited', 0, 'success', 'task/b')

    def test_07_released_not_started_cancel_frees_slot(self):
        self.register(); self.register('b')
        a = self.adapter(); self.sched.tick()
        self.assertIn('task/a', scheduling.scheduling_view(str(self.workspace)).occupants)
        rid = self.stop(); a.tick()
        self.assert_final(a, 'killed', proto.EXITCODE_KILLED, 'canceled')
        self.assertEqual(self.ack(rid)['outcome'], 'applied')
        self.assertFalse(self.track().exists())
        self.sched.tick(); self.assertTrue(self.enabled('task/b'))
        a.tick(); self.assert_final(a, 'exited', 0, 'success', 'task/b')

    def test_08_running_cancel_term_kill_and_ack(self):
        self.register(mode='ignore-term', report=False); self.register('b')
        a = self.adapter(); self.sched.tick(); a.tick()
        ready = wait_for(lambda: proto.read_json(str(self.track() / 'ready.json')))
        self.assertIsNotNone(K.proc(ready['pid']))
        rid = self.stop(); a.tick()
        self.assert_final(a, 'killed', proto.EXITCODE_KILLED, 'canceled')
        self.assertEqual(self.ack(rid)['outcome'], 'applied')
        run = self.run_for(a)
        self.assertTrue(run['waitObserved'])
        self.assertEqual([e['signal'] for e in run['stopEvidence'] if e['event'] == 'signal'], [15, 9])
        self.assertFalse([p for p in K.group_members(run) if p['state'] not in ('Z', 'X')])
        self.assertFalse(a.runtime.kernel.lease_busy(run))
        self.sched.tick(); self.assertTrue(self.enabled('task/b'))
        a.tick(); self.assert_final(a, 'exited', 0, 'success', 'task/b')

    def test_09_duplicate_release_request_and_final_restart_clear(self):
        self.register(mode='barrier'); a = self.adapter(); self.sched.tick(); a.tick()
        wait_for(lambda: (self.track() / 'ready.json').exists())
        before = self.run_for(a)
        for _ in range(4):
            self.sched.tick(); a.tick()
            again = a.runtime.accept_task(before['declaration'])
            self.assertEqual(again['pid'], before['pid'])
        self.assertEqual((self.track() / 'effects').read_text().splitlines(), ['one invocation'])
        (self.track() / 'finish').write_text('finish')
        self.assert_final(a, 'exited', 0, 'success')
        final = Path(proto.pid_path(str(self.workspace), 'task/a')).read_bytes()
        for action in ('restart', 'clear', 'stop'):
            rid = self.ctl('control', 'task/a', action, '--from', 'queue/dispatcher').stdout.strip()
            a.tick()
            self.assertEqual(self.ack(rid)['outcome'], 'noop' if action == 'stop' else 'rejected')
        self.assertEqual(Path(proto.pid_path(str(self.workspace), 'task/a')).read_bytes(), final)
        self.assertEqual((self.track() / 'effects').read_text().splitlines(), ['one invocation'])

    def crash_owner(self, report):
        self.register(mode='barrier', report=report, provides=['lost-delivery'])
        self.register('b', needs=['lost-delivery'])
        # Child invokes real CLI. Its Popen is harness-owned, not an agent runner.
        code = "from experimental.agent_exec_adapter import adapter; import runner;\n" + \
               "def forbidden(*a,**k): raise AssertionError('legacy runner execution')\n" + \
               "for n in ('spawn','kill_proc','observe','service','do_stop','do_restart','do_clear'): setattr(runner.Runner,n,forbidden)\n" + \
               "adapter.main()\n"
        argv = [sys.executable, '-B', '-c', code, '--sandbox-root', str(self.base),
                '--workspace', str(self.workspace), '--runtime-root', str(self.node),
                '--runtime-source', str(S_SOURCE), '--manifest', str(self.manifest), '--seconds', '30']
        env = dict(os.environ, PYTHONPATH=str(A_ROOT))
        log = (self.base / 'owner.log').open('w'); self.logs.append(log)
        owner = subprocess.Popen(argv, cwd=self.base, env=env, stdout=log, stderr=subprocess.STDOUT)
        self.owners.append(owner)
        self.events.append({'event': 'owner-start', 'argv': argv, 'pid': owner.pid, 'env': env})
        wait_for(lambda: Path(proto.agentd_lock_path(str(self.workspace), 'fixture')).exists())
        self.sched.tick()
        ready = wait_for(lambda: proto.read_json(str(self.track() / 'ready.json')))
        wait_for(lambda: self.doc() and self.doc()['pid'] == ready['pid'])
        self.events.append({'event': 'owner-crash', 'owner': K.proc(owner.pid), 'payload': K.proc(ready['pid'])})
        owner.kill(); self.assertEqual(owner.wait(timeout=5), -9)
        self.owners.remove(owner)
        a = self.adapter()
        # Orphan identity is live; no second invocation, no guessed success.
        a.tick(); self.assertFalse(self.doc()['final'])
        self.assertEqual(self.state(), 'pending')
        (self.track() / 'finish').write_text('natural orphan exit')
        wait_for(lambda: (self.track() / 'done').exists())
        os.waitpid(ready['pid'], 0)  # harness subreaper; NEVER passed to Runtime as a wait result
        self.assert_final(a, 'stale', 127, 'success' if report else 'failed')
        run = self.run_for(a)
        self.assertEqual((run['phase'], run['exitCode'], run['waitObserved']), ('lost', None, False))
        self.assertFalse(a.runtime.kernel.lease_busy(run))
        self.assertEqual((self.track() / 'effects').read_text().splitlines(), ['one invocation'])
        self.sched.tick()
        self.assertEqual(self.enabled('task/b'), report)
        if report:
            a.tick(); self.assert_final(a, 'exited', 0, 'success', 'task/b')
        return a

    def test_10_owner_crash_lost_with_report_original_success(self):
        self.crash_owner(True)

    def test_11_owner_crash_lost_without_report_failed(self):
        self.crash_owner(False)

    def test_12_unknown_identity_and_group_conservatively_hold(self):
        self.register(mode='barrier'); self.register('b')
        a = self.adapter(); self.sched.tick(); a.tick()
        wait_for(lambda: (self.track() / 'ready.json').exists())
        original = K.proc
        pid = self.doc()['pid']
        def unknown(p):
            if p == pid:
                raise PermissionError('synthetic unreadable /proc identity')
            return original(p)
        rid = self.stop()
        with patch.object(K, 'proc', unknown):
            a.tick()
            self.assertFalse(self.doc()['final'])
            self.assertEqual(self.doc()['runtime']['phase'], 'cleanupUnknown')
            self.assertIsNone(self.ack(rid))
            self.assertEqual(self.state(), 'pending')
        with patch.object(K, 'group_members', side_effect=PermissionError('synthetic unknown group')):
            a.tick()
            self.assertFalse(self.doc()['final']); self.assertIsNone(self.ack(rid))
        with patch.object(a.runtime.kernel, 'lease_busy', side_effect=PermissionError('synthetic unknown lease')):
            a.tick()
            self.assertFalse(self.doc()['final']); self.assertIsNone(self.ack(rid))
        def mismatch(p):
            value = original(p)
            return dict(value, procStart=value['procStart'] + 1) if p == pid and value else value
        with patch.object(K, 'proc', mismatch):
            a.tick()
            self.assertFalse(self.doc()['final']); self.assertIsNone(self.ack(rid))
        self.assertIsNotNone(original(pid))
        self.sched.tick(); self.assertFalse(self.enabled('task/b'))
        (self.track() / 'finish').write_text('cooperative natural exit')
        # No blind replay of ambiguous runtime control. Natural convergence closes factually.
        self.finish(a)
        self.assertTrue(self.doc()['final']); self.assertIsNotNone(self.ack(rid))

    def test_13_node_lock_and_workspace_binding_no_second_writer(self):
        self.register(mode='barrier'); a = self.adapter(); self.sched.tick(); a.tick()
        wait_for(lambda: (self.track() / 'ready.json').exists())
        with self.assertRaises(BlockingIOError):
            self.adapter()
        with self.assertRaisesRegex(ValueError, 'workspace bound'):
            Adapter(sandbox_root=str(self.base), workspace=str(self.workspace), runtime_root=str(self.base/'other-node'),
                    runtime_source=str(S_SOURCE), manifest=str(self.manifest))
        self.assertEqual((self.track() / 'effects').read_text().splitlines(), ['one invocation'])
        (self.track() / 'finish').write_text('finish')
        self.assert_final(a, 'exited', 0, 'success')

    def test_14_unsupported_mapping_and_changed_release_refused(self):
        self.register(mode='barrier'); a = self.adapter()
        spec = self.agent_spec()
        for changes in ({'resident': True}, {'restartPolicy': 'auto'}, {'command': 'echo not-declared'}):
            # Input rejection test uses API preflight, not protocol mutation.
            with patch.object(a.agent, 'read_spec', return_value=dict(spec, **changes)):
                with self.assertRaises(ValueError):
                    a.tick()
            self.assertFalse(self.track().exists())
        self.sched.tick(); a.tick()
        wait_for(lambda: (self.track() / 'ready.json').exists())
        original = proto.read_json
        ep = proto.enable_path(str(self.workspace), 'task/a')
        def different(path):
            value = original(path)
            return dict(value, note='conflicting release body') if str(path) == ep else value
        with patch.object(proto, 'read_json', different):
            with self.assertRaisesRegex(ValueError, 'immutable spec'):
                a.tick()
        self.assertEqual((self.track() / 'effects').read_text().splitlines(), ['one invocation'])
        (self.track() / 'finish').write_text('finish')
        self.assert_final(a, 'exited', 0, 'success')

    def agent_spec(self):
        return proto.read_json(proto.spec_path(str(self.workspace), 'task/a'))

    def test_15_stale_control_does_not_signal_current_task(self):
        self.register(mode='barrier'); a = self.adapter(); self.sched.tick(); a.tick()
        wait_for(lambda: (self.track() / 'ready.json').exists())
        run = self.run_for(a)
        ctl = {'apiVersion': 'rund-control/v1alpha1', 'kind': 'Task', 'id': run['id'],
               'attemptId': 'a1', 'expectedGeneration': 2, 'action': 'cancel', 'requestId': 'late-old-control'}
        self.assertEqual(a.runtime.control(ctl)['reason'], 'stale-generation')
        self.assertIsNotNone(K.proc(run['pid']))
        self.assertFalse(a.runtime.task(run['id'], 'a1').get('stopEvidence'))
        (self.track() / 'finish').write_text('finish')
        self.assert_final(a, 'exited', 0, 'success')

    def test_16_queued_stale_control_does_not_admit_or_reserve(self):
        self.register(needs=['missing']); a = self.adapter()
        rid = self.stop()
        real_pending = a.agent.pending_reqs
        def stale(pid):
            return [dict(req, expectedGeneration=1) for req in real_pending(pid)]
        with patch.object(a.agent, 'pending_reqs', stale):
            a.tick()
        self.assertEqual(self.ack(rid)['outcome'], 'noop')
        self.assertIsNone(self.doc()); self.assertFalse(self.enabled())
        self.assertNotIn('task/a', scheduling.scheduling_view(str(self.workspace)).occupants)
        self.assertFalse(self.track().exists())

    def test_17_harmless_cleanup_decoy_and_fixed_dependency(self):
        self.register()
        decoy = self.base / 'decoy'; decoy.mkdir(); (decoy/'keep').write_text('keep')
        authorized = self.base / 'cleanup-authorized'; authorized.mkdir()
        deleted, why = safe_rmtree(decoy, authorized_root=authorized)
        self.assertFalse(deleted); self.assertTrue((decoy/'keep').exists())
        self.assertIn('不在授权根', why)
        a = self.adapter()
        imports = json.loads((self.node / 'state/adapter-imports.json').read_text())
        self.assertEqual(imports['revision'], DEP['revision'])
        self.assertTrue(imports['imports'])
        self.assertTrue(all(str(S_SOURCE) in p for p in imports['imports'].values()))
        self.assertEqual(list((self.workspace/'agents').rglob('node.lock')), [])

    def test_18_resource_gate_with_free_concurrency_then_release(self):
        self.register(mode='barrier'); self.register('b')
        self.sched.max_concurrent = 2  # resource, NOT slot count, must block b
        a = self.adapter(); self.sched.tick(); a.tick()
        wait_for(lambda: (self.track() / 'ready.json').exists())
        self.assertTrue(self.enabled()); self.assertFalse(self.enabled('task/b'))
        self.assertIn('resource', self.sched._last_block['task/b'])
        self.events.append({'event':'actual-resource-gate', 'blocked':dict(self.sched._last_block),
                            'occupants':scheduling.scheduling_view(str(self.workspace)).occupants})
        (self.track() / 'finish').write_text('release resource')
        self.assert_final(a, 'exited', 0, 'success')
        self.sched.tick(); self.assertTrue(self.enabled('task/b'))
        a.tick(); self.assert_final(a, 'exited', 0, 'success', 'task/b')

    def tearDown(self):
        errors = []
        # First record unmodified protocol/runtime facts, including failed cases.
        for a in self.adapters:
            for participant in self.tasks:
                try:
                    run = a.runtime.task(runtime_id('fixture', participant), 'a1')
                    self.events.append({'event': 'before-teardown', 'participant': participant, 'run': run})
                    if run['phase'] not in ('succeeded','failed','canceled','lost'):
                        # Cleanup is an explicit S control, not a second supervisor.
                        a.runtime.control({'apiVersion':'rund-control/v1alpha1','kind':'Task','id':run['id'],
                          'attemptId':'a1','expectedGeneration':1,'action':'cancel','requestId':'test-cleanup'})
                except FileNotFoundError:
                    pass
                except BaseException as e:
                    errors.append(repr(e))
            a.__exit__()
        for owner in self.owners:
            owner.kill(); owner.wait(timeout=5)  # only our saved Popen
        for f in self.logs:
            f.close()
        remaining = []
        for ready_path in (self.base / 'tracks').glob('*/ready.json'):
            ready = json.loads(ready_path.read_text())
            actual = K.proc(ready['pid'])
            self.events.append({'event':'payload-after','ready':ready,'actual':actual})
            if actual and actual['state'] not in ('Z', 'X'):
                remaining.append(actual)
        (self.base / 'evidence.json').write_text(json.dumps({'commands':self.commands, 'events':self.events,
            'remaining':remaining,'errors':errors}, indent=2))
        for p in reversed(self.patches):
            p.stop()
        self.assertEqual(errors, [])
        self.assertEqual(remaining, [])


if __name__ == '__main__':
    unittest.main()
