"""Retained two-OS-process file adapter integration; all mutable inputs new.

No Runtime objects in this harness, no hand-written enable/pid/final/strong ack.
Owner and A processes audited independently; all roots retained on failure.
"""
import ctypes
import json
import os
from pathlib import Path
import platform
import signal
import socket
import subprocess
import sys
import time
import traceback
import unittest
from unittest.mock import patch

from .adapter import A_ROOT, runtime_id, workdir
from .file_backend import load_client, FileAdapter, S_REVISION
import proto
import scheduler
import scheduling
import task_state
from safe_delete import safe_rmtree

BASE = Path(os.environ['AF_TEST_OUTPUT'])
S_SOURCE = Path(os.environ['AF_RUNTIME_SOURCE'])
if not BASE.is_absolute() or BASE.exists():
    raise RuntimeError('AF_TEST_OUTPUT must be a new absolute directory')
BASE.mkdir(parents=True)
client, DEP = load_client(S_SOURCE)
libc = ctypes.CDLL(None, use_errno=True)
libc.syscall.restype = ctypes.c_long
if platform.machine() not in ('x86_64', 'aarch64') or libc.prctl(36, 1, 0, 0, 0):
    raise RuntimeError('Linux subreaper/pidfd required')


def proc(pid):
    try:
        s = Path('/proc', str(pid), 'stat').read_text().rsplit(')', 1)[1].split()
        return {'pid': pid, 'procStart': int(s[19]), 'pgid': int(s[2]), 'session': int(s[3]),
                'uid': Path('/proc', str(pid)).stat().st_uid, 'state': s[0]}
    except FileNotFoundError:
        return None


def same(a, b):
    return a and b and all(a[k] == b[k] for k in ('pid', 'procStart', 'pgid', 'session', 'uid'))


def wait_for(fn, seconds=12):
    end = time.monotonic() + seconds
    while True:
        value = fn()
        if value:
            return value
        if time.monotonic() >= end:
            raise AssertionError('deadline: ' + repr(fn))
        time.sleep(.01)


fd = libc.syscall(434, os.getpid(), 0)
if fd < 0 or libc.syscall(424, fd, 0, ctypes.c_void_p(0), 0):
    raise RuntimeError('pidfd preflight failed')
os.close(fd)


class FileBackendTests(unittest.TestCase):
    def setUp(self):
        self.base = BASE / self._testMethodName
        self.base.mkdir()
        self.workspace, self.node, self.protocol = (self.base / n for n in ('workspace', 'node', 'protocol'))
        (self.workspace / 'agents').mkdir(parents=True)
        (self.workspace / 'env').mkdir()
        (self.workspace / 'env/host-id').write_text(socket.gethostname() + ' fixture\n')
        for n in ('home', 'tmp', 'cwd'):
            (self.base / n).mkdir()
        self.env = {'HOME': str(self.base / 'home'), 'TMPDIR': str(self.base / 'tmp'),
                    'PATH': str(Path(sys.executable).parent) + ':/usr/bin:/bin',
                    'PYTHONPATH': str(A_ROOT), 'PYTHONDONTWRITEBYTECODE': '1',
                    'GIT_CONFIG_GLOBAL': '/dev/null', 'GIT_CONFIG_NOSYSTEM': '1', 'LC_ALL': 'C.UTF-8'}
        self.commands, self.children, self.identities, self.events = [], [], {}, []
        self.source = self.base / 'source'
        self.source.mkdir()
        (self.source / 'payload.py').write_bytes(Path(__file__).with_name('file_payload.py').read_bytes())
        self.git('init', '-q')
        self.git('add', '--', 'payload.py')
        msg = self.base / 'fixture-message'
        msg.write_text('Synthetic ibs7lv fixture source, not model work\n')
        self.git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-q', '-F', str(msg), '--', 'payload.py')
        self.revision = self.git('rev-parse', 'HEAD').strip()
        self.tasks = {}
        self.manifest = self.base / 'manifest.json'
        self.config = self.base / 'config.json'
        self.c = {'base': str(self.base), 'workspace': str(self.workspace), 'runtime': str(self.node),
                  'protocol': str(self.protocol), 's_source': str(S_SOURCE), 'a_source': str(A_ROOT),
                  'manifest': str(self.manifest)}
        self.sched = scheduler.Scheduler(str(self.workspace), {'fixture'}, max_concurrent=2)
        original = proto.atomic_write_json
        def atomic(path, doc):
            if str(path).endswith('/enable.json'):
                stack = traceback.extract_stack()
                self.assertTrue(any(f.filename == str(A_ROOT / 'scheduler.py') and f.name == 'tick' for f in stack))
                self.events.append({'event': 'scheduler-enable', 'path': path, 'doc': doc, 'stack': [str(f) for f in stack]})
            self.assertFalse(str(path).endswith('/pid.json'), 'harness must not project')
            return original(path, doc)
        self.guard = patch.object(proto, 'atomic_write_json', atomic)
        self.guard.start()

    def command(self, argv, data=None, good=True, env=None):
        env = env or self.env
        p = subprocess.run(list(map(str, argv)), input=data, capture_output=True, text=True,
                           env=env, cwd=self.base / 'cwd', timeout=60)
        self.commands.append({'argv': list(map(str, argv)), 'env': env, 'cwd': str(self.base / 'cwd'),
                              'rc': p.returncode, 'stdout': p.stdout, 'stderr': p.stderr})
        if good:
            self.assertEqual(p.returncode, 0, p.stderr)
        return p

    def git(self, *args):
        return self.command(['git', '-c', 'core.hooksPath=/dev/null', '-C', self.source, *args]).stdout

    def ctl(self, *args, data=None, good=True):
        return self.command([sys.executable, '-B', A_ROOT / 'agentctl.py', '--root', self.workspace, *args], data=data, good=good)

    def register(self, name='a', mode='exit', report=True, code=0, needs=(), provides=(), resources=('slot',), bad_pin=False, reaper='queue/dispatcher', policy='one-shot'):
        pid = 'task/' + name
        b = {'command': 'p3a-fixture ' + name,
             'sources': [{'alias': 'fixture', 'repository': self.source.as_uri(),
                          'revision': '0'*40 if bad_pin else self.revision, 'requires': []}],
             'argv': [sys.executable, '${code.fixture}/payload.py', str(self.track(name)), mode, 'report' if report else 'none', str(code)],
             'env': {'SYNTHETIC_VALUE': 'file-fixture'}, 'toolchain': {'python': sys.executable}}
        spec = {'command': b['command'], 'workdir': str(workdir(self.node, 'fixture', pid)),
                'host': 'fixture', 'createdByHost': 'fixture', 'createdAt': proto.now_ts(), 'creator': 'queue/dispatcher',
                'reaper': reaper, 'restartPolicy': policy, 'resources': list(resources), 'needs': list(needs), 'provides': list(provides)}
        result = self.ctl('register-json', data=json.dumps({'name': name, 'spec': spec, 'prompt': '# SYNTHETIC fixture only — no model\n'}))
        self.assertEqual(json.loads(result.stdout)['taskId'], name)
        self.tasks[pid] = b
        self.manifest.write_text(json.dumps({'experiment': FileAdapter.EXPERIMENT, 'host': 'fixture', 'tasks': self.tasks}))
        return pid

    def spawn(self, role, **extra):
        config = dict(self.c, **extra)
        path = self.base / ('config-' + role + '-' + str(len(self.children)) + '.json')
        path.write_text(json.dumps(config))
        logpath = self.base / (role + '-' + str(len(self.children)) + '.log')
        out = logpath.open('w')
        env = dict(self.env, PYTHONPATH=config['s_source'] if role == 'owner' else config['a_source'])
        argv = [sys.executable, '-B', str(A_ROOT / 'experimental/agent_exec_adapter/file_probe.py'), role, str(path)]
        p = subprocess.Popen(argv, env=env, cwd=self.base / 'cwd', stdout=out, stderr=subprocess.STDOUT)
        self.children.append((p, out, logpath))
        event = {'event': 'harness-child', 'role': role, 'argv': argv, 'env': env,
                 'cwd': str(self.base / 'cwd'), 'pid': p.pid, 'identity': proc(p.pid)}
        self.events.append(event)
        with (self.base / 'children.jsonl').open('a') as f:
            f.write(json.dumps(event) + '\n')
        return p

    def owner(self, **extra):
        p = self.spawn('owner', **extra)
        def ready_owner():
            snapshot = proto.read_json(str(self.protocol / 'snapshot.json')) or {}
            return snapshot.get('owner', {}).get('pid') == p.pid or p.poll() is not None
        wait_for(ready_owner)  # old binding/snapshot alone is NOT this new process being ready
        self.assertIsNone(p.poll(), self.children[-1][2].read_text())
        return p

    def adapter(self, release=True, **extra):
        p = self.spawn('adapter', **extra)
        wait_for(lambda: (proto.read_json(proto.agentd_lock_path(str(self.workspace), 'fixture')) or {}).get('pid') == p.pid or p.poll() is not None)
        self.assertIsNone(p.poll(), self.children[-1][2].read_text())
        if release:
            self.sched.tick()
        return p

    def pipeline(self):
        o = self.owner()
        a = self.adapter()
        return o, a

    def crash(self, p):
        self.remember_payloads()
        if p.poll() is None:
            p.kill()
        rc = p.wait(timeout=5)
        self.events.append({'event': 'harness-reaped', 'pid': p.pid, 'rc': rc})
        return rc

    def doc(self, name='a'):
        return proto.read_json(proto.pid_path(str(self.workspace), 'task/' + name))

    def track(self, name='a'):
        return self.base / 'tracks' / name

    def ready(self, name='a'):
        value = wait_for(lambda: proto.read_json(str(self.track(name) / 'ready.json')))
        self.identities[value['identity']['pid']] = value['identity']
        wait_for(lambda: self.doc(name) and self.doc(name).get('pid') == value['identity']['pid'])
        return value

    def enabled(self, name='a'):
        return Path(proto.enable_path(str(self.workspace), 'task/' + name)).exists()

    def final(self, name='a', status='exited', code=0, state='success'):
        d = wait_for(lambda: (d if (d := self.doc(name)) and d.get('final') else None))
        self.assertEqual((d['status'], d['exitcode']), (status, code))
        self.assertEqual(self.state(name), state)
        return d

    def state(self, name='a'):
        pid = 'task/' + name
        return task_state.classify_provider(self.doc(name), task_state.report_nonempty(str(Path(proto.agent_dir(str(self.workspace), pid)) / 'report.md')),
                                             task_state.stop_requested(str(self.workspace), pid))

    def run_fact(self, name='a'):
        id = runtime_id('fixture', 'task/' + name)
        base = self.node / 'state/runs/Task' / id / 'a1/1'
        return proto.read_json(str(base / ('result.json' if (base / 'result.json').exists() else 'run.json')))

    def stop(self, name='a'):
        return self.ctl('cancel', 'task/' + name, '--from', 'queue/dispatcher', '--reason', 'synthetic cancel').stdout.strip()

    def ack(self, rid, name='a'):
        return proto.read_json(str(Path(proto.control_path(str(self.workspace), 'task/' + name)) / 'ack' / rid))

    def journal(self, name='a'):
        return proto.read_json(str(self.workspace / 'run/file-adapter' / (runtime_id('fixture', 'task/' + name) + '.json')))

    def diag(self):
        return proto.read_json(str(self.workspace / 'run/file-adapter/diagnostics.json')) or {}

    def notices(self):
        return [json.loads(p.read_text()) for p in (self.workspace / 'agents').glob('**/inbox/*.msg')]

    def test_01_chain_provider_needs_resources(self):
        self.register(mode='barrier', provides=['done'])
        self.register('b', needs=['done'])
        self.sched.tick()
        self.assertFalse(self.enabled())
        o, a = self.pipeline()
        r = self.ready()
        self.assertFalse(self.enabled('b'))
        self.assertTrue(same(proc(r['identity']['pid']), r['identity']))
        self.assertEqual(r['env']['AGENT_SELF'], 'task/a')
        self.assertEqual(r['cwd'], str(workdir(self.node, 'fixture', 'task/a')))
        self.assertTrue(r['env']['HOME'].startswith(str(self.node)))
        self.assertEqual(r['env']['AGENTD_ASK_INBOX'], proto.position_inbox(str(self.workspace)))
        (self.track() / 'finish').touch()
        self.final()
        self.assertTrue(self.run_fact()['waitObserved'])
        self.sched.tick()
        self.assertTrue(self.enabled('b'))
        self.final('b')
        wait_for(lambda: len(self.notices()) == 2)
        self.assertEqual({json.loads(n['body'])['event'] for n in self.notices()}, {'task_done'})
        self.assertFalse(list((self.workspace / 'agents').rglob('lease.lock')))
        imports = proto.read_json(str(self.workspace / 'run/file-adapter/imports.json'))
        self.assertEqual(imports['revision'], S_REVISION)
        self.assertNotIn('runtime.kernel', imports['imports'])
        self.assertTrue(all(v['path'].startswith(str(S_SOURCE)) for v in imports['imports'].values()))

    def test_02_exit0_no_report_idle(self):
        self.register(report=False, provides=['done'])
        self.register('b', needs=['done'])
        self.pipeline()
        self.final(state='idle')
        self.sched.tick()
        self.assertFalse(self.enabled('b'))
        wait_for(lambda: self.notices())
        self.assertEqual(json.loads(self.notices()[0]['body'])['warn'], 'no_report')

    def test_03_nonzero_with_report(self):
        self.register(code=7)
        self.pipeline()
        self.final(code=7, state='failed')
        self.assertTrue(self.run_fact()['waitObserved'])
        wait_for(lambda: self.notices())
        self.assertEqual(json.loads(self.notices()[0]['body'])['event'], 'task_failed')

    def test_04_nonzero_without_report(self):
        self.register(code=7, report=False)
        self.pipeline()
        self.final(code=7, state='failed')

    def test_05_bad_prepare_pin(self):
        self.register(bad_pin=True)
        self.pipeline()
        self.final(code=126, state='failed')
        self.assertFalse(self.track().exists())
        self.assertFalse(self.run_fact()['waitObserved'])
        self.assertIsNone(self.run_fact()['pid'])
        self.assertTrue(self.run_fact()['reason'].startswith('prepare:'))

    def test_06_queued_stop_no_enable_gen0_to_s1(self):
        self.register(needs=['absent'])
        self.owner()
        rid = self.stop()
        self.adapter()
        self.final(status='killed', code=137, state='canceled')
        self.assertFalse(self.enabled())
        self.assertEqual(self.doc()['gen'], 0)
        self.assertEqual(self.run_fact()['generation'], 1)
        self.assertFalse(self.track().exists())
        wait_for(lambda: self.ack(rid))
        self.assertEqual(self.ack(rid)['outcome'], 'applied')

    def test_07_async_stop_barrier_when_enable_arrives(self):
        self.register()
        o = self.owner(fault=['pending', 'accept'])
        rid = self.stop()
        a = self.adapter(release=False)
        wait_for(lambda: o.poll() is not None)
        self.assertEqual(o.wait(), 93)
        self.sched.tick()  # real enable arrives while accept/cancel pending
        self.assertTrue(self.enabled())
        self.crash(a)
        self.owner()
        self.adapter()
        wait_for(lambda: 'pending' in str(self.diag()))
        time.sleep(.15)
        self.assertFalse(self.track().exists())
        self.assertIsNone(self.doc())
        self.assertIsNone(self.ack(rid))
        self.assertNotIn('start', self.journal()['operations'])

    def test_08_running_cancel_waits_group_and_duplicate(self):
        self.register(mode='ignore-term', report=False)
        self.register('b')
        self.pipeline()
        self.ready()
        rid, rid2 = self.stop(), self.stop()
        self.final(status='killed', code=137, state='canceled')
        wait_for(lambda: self.ack(rid) and self.ack(rid2))
        self.assertEqual(self.ack(rid)['outcome'], 'applied')
        run = self.run_fact()
        signals = [e['signal'] for e in run['stopEvidence'] if e['event'] == 'signal']
        self.assertEqual(signals, [15, 9])
        self.assertTrue(run['waitObserved'])
        self.assertNotIn('leaseBusy', run['stopEvidence'][-1])
        prior_ops = self.journal()['operations']
        late = self.ctl('control', 'task/a', 'stop', '--from', 'queue/dispatcher').stdout.strip()
        wait_for(lambda: self.ack(late))
        self.assertEqual(self.ack(late)['outcome'], 'noop')
        self.assertEqual(self.journal()['operations'], prior_ops)
        self.assertEqual((self.track() / 'effects').read_text().splitlines(), ['one real invocation'])
        self.sched.tick()
        self.final('b')

    def test_09_real_adapter_kill_reopen_same_payload_wait(self):
        self.register(mode='barrier')
        o, a = self.pipeline()
        ready = self.ready()
        epoch = self.run_fact()['ownerEpoch']
        self.assertEqual(self.crash(a), -9)
        self.assertIsNone(o.poll())
        self.assertTrue(same(proc(ready['identity']['pid']), ready['identity']))
        self.adapter()
        wait_for(lambda: self.doc() and self.doc()['pid'] == ready['identity']['pid'])
        self.assertEqual(self.run_fact()['ownerEpoch'], epoch)
        (self.track() / 'finish').touch()
        self.final()
        self.assertTrue(self.run_fact()['waitObserved'])
        self.assertEqual(len((self.track() / 'effects').read_text().splitlines()), 1)

    def orphan(self, report):
        self.register(mode='barrier', report=report, provides=['done'])
        self.register('b', needs=['done'])
        o, a = self.pipeline()
        ready = self.ready()
        original_epoch = self.run_fact()['ownerEpoch']
        self.crash(o)
        # Last snapshot survives. No pid refresh or inferred death while owner offline.
        pidfile = Path(proto.pid_path(str(self.workspace), 'task/a'))
        time.sleep(.12)
        before = pidfile.read_bytes()
        time.sleep(1.15)
        self.assertEqual(pidfile.read_bytes(), before)
        wait_for(lambda: 'no new backend run fact' in str(self.diag()))
        self.assertFalse(self.doc()['final'])
        self.assertTrue(same(proc(ready['identity']['pid']), ready['identity']))
        self.owner()
        wait_for(lambda: self.run_fact()['ownerEpoch'] != original_epoch)
        self.assertEqual(self.run_fact()['pid'], ready['identity']['pid'])
        (self.track() / 'finish').touch()
        self.final(status='stale', code=127, state='success' if report else 'failed')
        run = self.run_fact()
        self.assertIsNone(run['exitCode'])
        self.assertIsNone(run['signal'])
        self.assertFalse(run['waitObserved'])
        self.sched.tick()
        self.assertEqual(self.enabled('b'), report)
        if report:
            self.final('b')
        self.assertEqual(len((self.track() / 'effects').read_text().splitlines()), 1)

    def test_10_real_owner_crash_lost_report(self):
        self.orphan(True)

    def test_11_real_owner_crash_lost_no_report(self):
        self.orphan(False)

    def test_12_duplicate_send_conflict_exact_done_not_completion(self):
        self.register(mode='barrier')
        self.pipeline()
        self.ready()
        state = self.journal()
        e = state['operations']['start']
        rec = wait_for(lambda: client.receipt(self.protocol, e['requestId']))
        self.assertEqual(rec['status'], 'done')
        self.assertEqual(rec['result']['phase'], 'running')
        self.assertFalse(self.doc()['final'])
        raw = client.canonical(rec)
        published = client.send(self.protocol, e)
        self.assertEqual(published['digest'], rec['digest'])
        self.assertEqual(client.canonical(client.receipt(self.protocol, e['requestId'])), raw)
        release = {'apiVersion': client.API, 'requestId': 'duplicate-known-release', 'node': 'fixture',
                   'action': 'release', 'declaration': state['declaration'],
                   'declarationDigest': client.digest(client.canonical(state['declaration']))}
        client.send(self.protocol, release)
        original_release = wait_for(lambda: (r if (r := client.receipt(self.protocol, release['requestId'])) and r.get('status') == 'done' else None))
        client.send(self.protocol, release)
        self.assertEqual(client.receipt(self.protocol, release['requestId']), original_release)
        with self.assertRaises(ValueError):
            client.send(self.protocol, dict(e, action='observe'))
        for _ in range(3):
            self.sched.tick()
        (self.track() / 'finish').touch()
        self.final()
        self.assertEqual(client.canonical(client.receipt(self.protocol, e['requestId'])), raw)
        self.assertEqual(len((self.track() / 'effects').read_text().splitlines()), 1)

    def test_13_second_adapter_local_fence_and_binding(self):
        self.register(mode='barrier')
        self.pipeline()
        self.ready()
        second = self.spawn('adapter')
        self.assertNotEqual(second.wait(timeout=5), 0)
        self.assertIn('BlockingIOError', self.children[-1][2].read_text())
        alternate = self.base / 'manifest-other-host.json'
        alternate.write_text(json.dumps(dict(json.loads(self.manifest.read_text()), host='other')))
        other = self.spawn('adapter', manifest=str(alternate))
        self.assertNotEqual(other.wait(timeout=5), 0)
        self.assertIn('workspace binding conflict', self.children[-1][2].read_text())
        # Same workspace can't switch root/selection after original adapter exits.
        a = self.children[1][0]
        self.crash(a)
        second = self.spawn('adapter', runtime=str(self.base / 'other-node'))
        self.assertNotEqual(second.wait(timeout=5), 0)
        self.assertIn('workspace binding conflict', self.children[-1][2].read_text())
        self.adapter()
        (self.track() / 'finish').touch()
        self.final()

    def test_14_final_absorption_old_controls_and_unsupported(self):
        self.register()
        self.pipeline()
        self.final()
        path = Path(proto.pid_path(str(self.workspace), 'task/a'))
        before = path.read_bytes()
        rejected_cancel = self.ctl('cancel', 'task/a', '--from', 'queue/dispatcher', good=False)
        self.assertEqual(rejected_cancel.returncode, 2)  # original CLI use-case gate retained
        rid = self.ctl('control', 'task/a', 'stop', '--from', 'queue/dispatcher').stdout.strip()
        restart = self.ctl('control', 'task/a', 'restart', '--from', 'queue/dispatcher').stdout.strip()
        clear = self.ctl('control', 'task/a', 'clear', '--from', 'queue/dispatcher').stdout.strip()
        wait_for(lambda: self.ack(rid) and self.ack(restart) and self.ack(clear))
        self.assertEqual(self.ack(rid)['outcome'], 'noop')
        self.assertEqual(self.ack(restart)['outcome'], 'rejected')
        self.assertEqual(self.ack(clear)['outcome'], 'rejected')
        self.assertEqual(path.read_bytes(), before)

    def test_15_pending_start_effect_before_receipt_no_new_id(self):
        self.register(mode='barrier')
        o = self.owner(fault=['before-receipt', 'start'])
        a = self.adapter()
        wait_for(lambda: o.poll() is not None)
        self.assertEqual(o.wait(), 93)
        wait_for(lambda: (self.track() / 'ready.json').exists())
        self.remember_payloads()
        self.crash(a)
        self.owner()
        self.adapter()
        wait_for(lambda: 'pending' in str(self.diag()))
        original = self.journal()['operations']['start']
        self.assertEqual(client.receipt(self.protocol, original['requestId'])['status'], 'pending')
        time.sleep(.2)
        self.assertEqual(len((self.track() / 'effects').read_text().splitlines()), 1)
        self.assertEqual(set(self.journal()['operations']), {'accept', 'start'})
        self.assertFalse(self.doc() and self.doc().get('final'))

    def test_16_owner_reopen_residual_group_conservative(self):
        self.register(mode='fork', report=False)
        o, a = self.pipeline()
        wait_for(lambda: (self.track() / 'child.json').exists())
        self.remember_payloads()
        run = wait_for(lambda: (r if (r := self.run_fact()) and r['phase'] == 'cleanupUnknown' else None))
        self.assertTrue(run['observation']['waitObserved'])
        self.assertFalse(self.doc() and self.doc().get('final'))
        self.crash(o)
        self.owner()
        wait_for(lambda: (r := self.run_fact()) and r['phase'] == 'cleanupUnknown' and not r['observation']['waitObserved'])
        child = proto.read_json(str(self.track() / 'child.json'))
        self.assertTrue(same(proc(child['pid']), child))
        rid = self.stop()
        time.sleep(.25)
        self.assertIsNone(self.ack(rid))
        self.assertFalse(self.doc() and self.doc().get('final'))
        (self.track() / 'child-finish').touch()
        self.final(status='stale', code=127, state='canceled')
        self.assertIsNone(self.ack(rid), 'nonterminal exact done is not terminal control receipt')

    def test_17_notify_none_legal_live_and_fallback(self):
        self.register('receiver', mode='barrier', reaper='none', resources=())
        self.register(mode='barrier', reaper='task/receiver', resources=())
        self.register('missing', reaper='bot/missing', resources=())
        self.register('none', reaper='none', resources=())
        self.sched.max_concurrent = 8
        self.pipeline()
        self.ready('receiver')
        self.ready()
        # Route must be bound after live recipient exists. a ask route binds on first admission,
        # terminal notification independently resolves current live recipient.
        (self.track() / 'finish').touch()
        self.final()
        self.final('missing')
        self.final('none')
        wait_for(lambda: len(self.notices()) >= 2)
        legal = list((self.workspace / 'agents/task/receiver/inbox').glob('*.msg'))
        self.assertEqual(len(legal), 1)
        fallback = list((self.workspace / 'agents/queue/dispatcher/inbox').glob('*.msg'))
        self.assertEqual(len(fallback), 1)
        self.assertIn('note', json.loads(json.loads(fallback[0].read_text())['body']))
        self.assertFalse((self.workspace / 'agents/task/none/notified.json').exists())
        (self.track('receiver') / 'finish').touch()
        self.final('receiver')
        self.assertFalse((self.workspace / 'agents/task/receiver/notified.json').exists())

    def test_18_resource_gate_free_slot_and_unsupported_policy(self):
        self.register(mode='barrier')
        self.register('b')
        self.pipeline()
        self.ready()
        self.assertFalse(self.enabled('b'))
        self.assertEqual(len(scheduling.scheduling_view(str(self.workspace)).occupants), 1)
        self.assertEqual(self.sched.max_concurrent, 2)
        self.assertIn('resource', self.sched._last_block['task/b'])
        self.events.append({'event': 'actual-resource-gate', 'blocked': dict(self.sched._last_block)})
        (self.track() / 'finish').touch()
        self.final()
        self.sched.tick()
        self.final('b')

    def test_19_binding_damaged_and_readonly_unknown_status(self):
        self.register(mode='barrier')
        self.pipeline()
        self.ready()
        e = {'apiVersion': client.API, 'requestId': 'unknown-status', 'node': 'fixture',
             'action': 'status', 'kind': 'Task', 'id': 'absent', 'attemptId': 'a1'}
        client.send(self.protocol, e)
        wait_for(lambda: client.receipt(self.protocol, e['requestId']))
        self.assertFalse((self.node / 'state/objects/Task/absent').exists())
        with self.assertRaises(ValueError):
            client.send(self.protocol, dict(e, requestId='wrong-node', node='other'))
        with self.assertRaises(ValueError):
            client.send(self.protocol, dict(e, requestId='wrong-api', apiVersion='wrong'))
        # No mutation of S facts to forge result; corrupt a private protocol snapshot only with owner off.
        self.crash(self.children[0][0])
        p = self.protocol / 'snapshot.json'
        snapshot = client.snapshot(self.protocol)
        snapshot['runs'][0]['generation'] = 2
        p.write_bytes(client.canonical(snapshot))
        before = Path(proto.pid_path(str(self.workspace), 'task/a')).read_bytes()
        wait_for(lambda: 'run binding mismatch' in str(self.diag()))
        self.assertEqual(Path(proto.pid_path(str(self.workspace), 'task/a')).read_bytes(), before)

    def test_20_decoy_fixed_imports_unsupported_auto(self):
        self.register(policy='auto')
        self.owner()
        self.adapter()
        wait_for(lambda: 'only explicit nonresident one-shot' in str(self.diag()))
        self.assertFalse(self.track().exists())
        decoy = self.base / 'decoy'
        decoy.mkdir()
        (decoy / 'KEEP').write_text('self-created harmless refusal fixture')
        deleted, reason = safe_rmtree(decoy, authorized_root=decoy)
        self.assertFalse(deleted)
        self.assertTrue((decoy / 'KEEP').exists())
        self.events.append({'event': 'decoy-refusal', 'reason': reason})

    def test_21_stale_generation_controls_no_effect(self):
        self.register(mode='barrier')
        self.owner()
        rid = self.stop()
        a = self.adapter(expected_generation=1)  # genuine req, explicit read-side fault fixture
        wait_for(lambda: self.ack(rid))
        self.assertEqual(self.ack(rid)['outcome'], 'noop')
        # Permission was real scheduler enable; stale queued control does not prevent release.
        self.ready()
        e = {'apiVersion': client.API, 'requestId': 'stale-s-cancel', 'node': 'fixture',
             'action': 'cancel', 'kind': 'Task', 'id': runtime_id('fixture', 'task/a'),
             'attemptId': 'a1', 'expectedGeneration': 2}
        client.send(self.protocol, e)
        rec = wait_for(lambda: (r if (r := client.receipt(self.protocol, e['requestId'])) and r.get('status') == 'done' else None))
        self.assertEqual(rec['result'], {'noop': True, 'reason': 'stale-generation'})
        self.assertFalse(self.run_fact().get('stopEvidence'))
        self.crash(a)
        self.adapter(expected_generation=0)
        rid2 = self.stop()
        wait_for(lambda: self.ack(rid2))
        self.assertEqual(self.ack(rid2)['outcome'], 'noop')
        self.assertFalse(self.run_fact().get('stopEvidence'))
        (self.track() / 'finish').touch()
        # Original stop-request policy still classifies signal independently; report+exit0 wins.
        self.final()

    def test_22_pending_cancel_reopen_no_ack_or_replay(self):
        self.register(mode='barrier', report=False)
        o = self.owner(fault=['pending', 'cancel'])
        a = self.adapter()
        ready = self.ready()
        rid = self.stop()
        wait_for(lambda: o.poll() is not None)
        self.assertEqual(o.wait(), 93)
        self.crash(a)
        self.owner()
        self.adapter()
        wait_for(lambda: self.diag().get('tasks'))  # pending target may be reported as snapshot blocked
        self.assertIsNone(self.ack(rid))
        self.assertTrue(same(proc(ready['identity']['pid']), ready['identity']))
        self.assertFalse(self.run_fact().get('stopEvidence'))
        op = self.journal()['operations']['cancel:' + rid]
        self.assertEqual(client.receipt(self.protocol, op['requestId'])['status'], 'pending')
        self.assertEqual(len((self.track() / 'effects').read_text().splitlines()), 1)

    def test_23_corrupt_exact_receipt_never_fallback_or_retry(self):
        self.register(mode='barrier')
        o, a = self.pipeline()
        self.ready()
        self.crash(o)
        self.crash(a)
        op = self.journal()['operations']['start']
        path = self.node / 'state/requests' / (client.outer_key(op['requestId']) + '.json')
        rec = client.receipt(self.protocol, op['requestId'])
        rec['digest'] = '0' * 64  # explicit self-owned damaged input, NOT a valid result
        path.write_bytes(client.canonical(rec))
        before = Path(proto.pid_path(str(self.workspace), 'task/a')).read_bytes()
        self.adapter()
        wait_for(lambda: 'damaged receipt digest' in str(self.diag()))
        self.assertEqual(Path(proto.pid_path(str(self.workspace), 'task/a')).read_bytes(), before)
        self.assertEqual(set(self.journal()['operations']), {'accept', 'start'})
        self.assertFalse(self.doc()['final'])

    def test_24_damaged_local_journal_no_readmission(self):
        self.register(mode='barrier')
        o, a = self.pipeline()
        self.ready()
        self.crash(a)
        path = self.workspace / 'run/file-adapter' / (runtime_id('fixture', 'task/a') + '.json')
        path.write_text('{broken')  # private A-only intent corruption, not a runtime result
        before = Path(proto.pid_path(str(self.workspace), 'task/a')).read_bytes()
        requests = sorted(p.name for p in (self.protocol / 'requests').glob('*.json'))
        self.adapter()
        wait_for(lambda: self.diag().get('tasks'))
        self.assertEqual(sorted(p.name for p in (self.protocol / 'requests').glob('*.json')), requests)
        self.assertEqual(Path(proto.pid_path(str(self.workspace), 'task/a')).read_bytes(), before)
        self.assertFalse(self.doc()['final'])
        self.assertIsNone(o.poll())
        self.assertEqual(len((self.track() / 'effects').read_text().splitlines()), 1)

    def test_25_audit_rejects_non_kernel_payload_attempt(self):
        self.register(mode='barrier')
        self.owner()
        self.adapter(negative_audit_probe=True)
        self.ready()
        audit = self.base / 'adapter-audit.jsonl'
        wait_for(lambda: any(json.loads(line).get('event') == 'negative-audit-rejected' for line in audit.read_text().splitlines()))
        attempts = [json.loads(line) for line in audit.read_text().splitlines() if json.loads(line).get('event') == 'subprocess.Popen']
        self.assertEqual(len(attempts), 1)
        self.assertIsNone(attempts[0]['category'])
        self.assertEqual(len((self.track() / 'effects').read_text().splitlines()), 1)
        (self.track() / 'finish').touch()
        self.final()

    def remember_payloads(self):
        for path in (self.base / 'tracks').glob('*/ready.json'):
            v = proto.read_json(str(path))
            if v:
                self.identities[v['identity']['pid']] = v['identity']
        for path in (self.base / 'tracks').glob('*/child.json'):
            v = proto.read_json(str(path))
            if v:
                self.identities[v['pid']] = v
        # Also record direct children of our saved owner if assertion failed before ready.
        for p, _, _ in self.children:
            if p.poll() is None:
                children = Path('/proc', str(p.pid), 'task', str(p.pid), 'children')
                if children.exists():
                    for pid in children.read_text().split():
                        identity = proc(int(pid))
                        if identity:
                            self.identities[identity['pid']] = identity

    def tearDown(self):
        self.guard.stop()
        errors = []
        self.remember_payloads()
        for p, out, logpath in self.children:
            if p.poll() is None:
                p.kill()
            p.wait(timeout=5)
            out.close()
            self.events.append({'event': 'child-reaped', 'pid': p.pid, 'rc': p.returncode, 'log': str(logpath)})
        time.sleep(.03)
        self.remember_payloads()
        for identity in self.identities.values():
            try:
                actual = proc(identity['pid'])
                if actual and actual['state'] not in ('Z', 'X'):
                    self.assertTrue(same(actual, identity), 'cleanup identity mismatch')
                    fd = libc.syscall(434, identity['pid'], 0)
                    if fd < 0:
                        raise OSError(ctypes.get_errno(), 'pidfd')
                    try:
                        self.assertTrue(same(proc(identity['pid']), identity))
                        if libc.syscall(424, fd, signal.SIGKILL, ctypes.c_void_p(0), 0):
                            raise OSError(ctypes.get_errno(), 'pidfd signal')
                    finally:
                        os.close(fd)
                wait_for(lambda: (p := proc(identity['pid'])) is None or p['state'] in ('Z', 'X'))
                try:
                    os.waitpid(identity['pid'], 0)
                except ChildProcessError:
                    pass
            except BaseException as e:
                errors.append(repr(e))
        remaining = {pid: proc(pid) for pid in self.identities if proc(pid) is not None}
        self.events.append({'event': 'independent-cleanup', 'identities': self.identities, 'remaining': remaining, 'errors': errors})
        (self.base / 'evidence.json').write_text(json.dumps({'commands': self.commands, 'events': self.events}, indent=2))
        self.assertEqual(errors, [])
        self.assertEqual(remaining, {})


if __name__ == '__main__':
    unittest.main()
