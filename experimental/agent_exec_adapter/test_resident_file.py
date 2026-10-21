"""Independent OS A/S fixtures; reuse one-shot harness, never its state machine."""
import json
from pathlib import Path
import time
import unittest

from unittest.mock import patch
from .resident_file import (ResidentFileAdapter, lifecycle_mapping, initial_workdir,
                            load_client, S_REVISION)
# Reuse mechanical fixture helpers while explicitly selecting the resident
# client dependency contract for this separate suite.
with patch('experimental.agent_exec_adapter.file_backend.load_client', load_client):
    from . import test_file_backend as base
from .adapter import runtime_id
import proto


class ResidentTests(unittest.TestCase):
    def setUp(self):
        base.FileBackendTests.setUp(self)
        self.c['resident'] = True

    def spawn(self, role, **extra):
        config = dict(self.c, **extra)
        path = self.base / ('config-' + role + '-' + str(len(self.children)) + '.json')
        path.write_text(json.dumps(config))
        logpath = self.base / (role + '-' + str(len(self.children)) + '.log')
        out = logpath.open('w')
        env = dict(self.env, PYTHONPATH=config['s_source'] if role == 'owner' else config['a_source'])
        argv = [base.sys.executable, '-B', str(base.A_ROOT / 'experimental/agent_exec_adapter/resident_probe.py'), role, str(path)]
        p = base.subprocess.Popen(argv, env=env, cwd=self.base / 'cwd', stdout=out, stderr=base.subprocess.STDOUT)
        self.children.append((p, out, logpath))
        event = dict(event='harness-child', role=role, argv=argv, env=env,
                     cwd=str(self.base / 'cwd'), pid=p.pid, identity=base.proc(p.pid))
        self.events.append(event)
        with (self.base / 'children.jsonl').open('a') as f:
            f.write(json.dumps(event) + '\n')
        return p

    def register(self, name='a', mode='barrier', report=True, code=0, needs=(), provides=(),
                 resources=('slot',), bad_pin=False, reaper='queue/dispatcher', policy='manual', resident=True):
        pid = 'task/' + name
        binding = dict(command='p3a-fixture ' + name,
                       sources=[dict(alias='fixture', repository=self.source.as_uri(),
                                     revision='0'*40 if bad_pin else self.revision, requires=[])],
                       argv=[base.sys.executable, '${code.fixture}/payload.py', str(self.track(name)), mode,
                             'report' if report else 'none', str(code)],
                       env={'SYNTHETIC_VALUE': 'file-fixture'}, toolchain={'python': base.sys.executable})
        spec = dict(command=binding['command'], host='fixture', createdByHost='fixture', createdAt=proto.now_ts(),
                    creator='queue/dispatcher', reaper=reaper, resources=list(resources), needs=list(needs), provides=list(provides))
        if policy is not None:
            spec['restartPolicy'] = policy
        if resident is not None:
            spec['resident'] = resident
        spec['workdir'] = str(initial_workdir(self.node, 'fixture', pid, spec))
        result = self.ctl('register-json', data=json.dumps(dict(name=name, spec=spec, prompt='# SYNTHETIC resident fixture only\n')))
        self.assertEqual(json.loads(result.stdout)['taskId'], name)
        self.tasks[pid] = binding
        self.manifest.write_text(json.dumps(dict(experiment=ResidentFileAdapter.EXPERIMENT, host='fixture', tasks=self.tasks)))
        return pid

    def run_fact(self, name='a', generation=None):
        state = self.journal(name)
        if not state:
            return None
        d = state['declaration']
        if d['kind'] == 'Service':
            h = self.head(name)
            generation = generation or (h or {}).get('generation')
            if not generation:
                return None
        else:
            generation = 1
        root = self.node / 'state/runs' / d['kind'] / d['metadata']['id'] / d['spec'].get('attemptId', 'service') / str(generation)
        return proto.read_json(str(root / ('result.json' if (root / 'result.json').exists() else 'run.json')))

    def head(self, name='a'):
        return proto.read_json(str(self.node / 'state/objects/Service' / runtime_id('fixture', 'task/' + name) / 'service.json'))

    def control(self, action, name='a', *args):
        return self.ctl('control', 'task/' + name, action, '--from', 'queue/dispatcher', *args).stdout.strip()

    def stopped(self, rid, name='a'):
        ack = base.wait_for(lambda: self.ack(rid, name))
        self.assertEqual(ack['outcome'], 'applied', self.diag())
        self.assertTrue(self.doc(name)['final'])
        return ack

    def new_generation(self, gen, name='a'):
        d = base.wait_for(lambda: (d if (d := self.doc(name)) and d['gen'] == gen and d['status'] == 'running' else None))
        r = base.wait_for(lambda: (r if (r := proto.read_json(str(self.track(name) / 'ready.json'))) and r['identity']['pid'] == d['pid'] else None))
        self.identities[r['identity']['pid']] = r['identity']
        return d

    def test_01_original_predicate_all_eight_real_combinations(self):
        names = []
        for resident in (False, True):
            for policy in (None, 'manual', 'one-shot', 'auto'):
                name = ('r' if resident else 'n') + str(len(names))
                final = policy == 'one-shot' or policy != 'auto' and not resident
                self.assertEqual(lifecycle_mapping({'resident': resident, 'restartPolicy': policy})['kind'], 'Task' if final else 'Service')
                self.register(name, mode='exit', resident=resident, policy=policy, resources=())
                names.append((name, final, policy))
        self.sched.max_concurrent = 20
        self.pipeline()
        for name, final, policy in names:
            base.wait_for(lambda: self.doc(name))
            if final:
                self.final(name)
            elif policy == 'auto':
                base.wait_for(lambda: self.head(name)['generation'] >= 2)
                self.assertFalse(self.doc(name)['final'])
            else:
                base.wait_for(lambda: self.doc(name)['status'] == 'exited')
                self.assertFalse(self.doc(name)['final'])
                self.assertEqual(self.head(name)['generation'], 1)
            if not final:
                rid = self.control('stop', name)
                base.wait_for(lambda: self.ack(rid, name))
        for spec in ({'restartPolicy': 'on-failure'}, {'resident': 'true'}):
            with self.assertRaises(ValueError):
                lifecycle_mapping(spec)

    def auto(self, code):
        self.register(policy='auto', resident=False, mode='exit', code=code)
        self.register('b', policy='one-shot', resident=False, mode='exit')
        o = self.owner(); a = self.adapter()
        base.wait_for(lambda: self.head() and self.head()['generation'] >= 4)
        self.assertFalse(self.enabled('b'))
        self.assertEqual(self.doc()['status'], 'running')
        self.assertFalse(self.doc()['final'])
        runs = [self.run_fact(generation=g) for g in range(1, 4)]
        self.assertEqual([r['exitCode'] for r in runs], [code]*3)
        self.assertTrue(all(r['waitObserved'] for r in runs))
        for r in runs:
            self.assertEqual(r['kind'], 'Service')
        base.wait_for(lambda: self.doc() and self.head()['phase'] == 'backoff' and
                      self.doc()['gen'] == self.head()['generation'] and self.doc()['runtime']['servicePhase'] == 'backoff')
        rid = self.control('stop')
        base.wait_for(lambda: self.ack(rid))
        self.assertIn(self.ack(rid)['outcome'], ('applied', 'noop'))
        self.assertTrue(self.doc()['final'], self.diag())
        h = self.head(); self.assertTrue(h['hold'])
        count = len((self.track() / 'effects').read_text().splitlines())
        time.sleep(.5)
        self.assertEqual(len((self.track() / 'effects').read_text().splitlines()), count)
        self.sched.tick(); self.final('b')

    def test_02_auto_exit0_backoff_reservation(self):
        self.auto(0)

    def test_03_auto_nonzero_backoff_reservation(self):
        self.auto(7)

    def test_04_resident_resource_exemption_and_environment(self):
        self.register(policy='manual', resident=True)
        self.register('b', policy='manual', resident=True)
        self.sched.max_concurrent = 1
        self.pipeline(); r = self.ready(); self.ready('b')
        self.assertTrue(self.enabled('b'))
        self.assertEqual(r['env']['AGENTD_RESIDENT'], '1')
        self.assertNotIn('AGENTD_TASK', r['env'])
        self.assertNotIn('AGENTD_ASK_INBOX', r['env'])
        for name in ('a', 'b'):
            self.stopped(self.control('stop', name), name)

    def test_05_queued_service_stop_exact_gen0_no_run(self):
        self.register(needs=['absent'])
        self.owner(); rid = self.control('stop'); self.adapter()
        self.stopped(rid)
        self.assertEqual(self.doc()['gen'], 0)
        self.assertEqual(self.head()['generation'], 0)
        self.assertTrue(self.head()['hold'])
        self.assertFalse(self.track().exists())
        self.assertEqual(set(self.journal()['operations']), {'admit'})

    def test_06_queued_clear_noop_no_backend(self):
        self.register(needs=['absent'])
        self.owner(); rid = self.control('clear'); self.adapter()
        base.wait_for(lambda: self.ack(rid))
        self.assertEqual(self.ack(rid)['outcome'], 'noop')
        self.assertIsNone(self.doc()); self.assertIsNone(self.head())
        self.assertFalse(self.track().exists())

    def test_07_queued_restart_service_and_task(self):
        self.register(needs=['absent'])
        self.register('b', resident=False, policy='one-shot', needs=['absent'], resources=())
        self.owner(); r1 = self.control('restart'); r2 = self.control('restart', 'b'); self.adapter()
        for name, rid in (('a', r1), ('b', r2)):
            base.wait_for(lambda: self.ack(rid, name))
            self.assertEqual(self.ack(rid, name)['outcome'], 'applied', self.diag())
            self.assertFalse(self.enabled(name)); self.ready(name)
            self.assertEqual(self.doc(name)['gen'], 1)
        self.assertEqual(self.run_fact('b')['attemptId'], 'a2')

    def lifecycle_controls(self, resident, policy):
        self.register(resident=resident, policy=policy, mode='ignore-term')
        self.owner(); self.adapter(real_claim=True); first = self.ready()
        session = Path(proto.agent_dir(str(self.workspace), 'task/a')) / 'session/session.jsonl'
        session.parent.mkdir(exist_ok=True)
        contents = b'{"synthetic":"history"}\n'
        session.write_bytes(contents)
        restart = self.control('restart', 'a', '--inject', 'synthetic preserved pre-message')
        stale = self.control('clear')  # same old generation, must not clear successor
        base.wait_for(lambda: self.ack(restart))
        self.assertEqual(self.ack(restart)['outcome'], 'applied', self.diag())
        d = self.new_generation(2)
        self.assertEqual(d['resumed'], 1)
        base.wait_for(lambda: self.ack(stale))
        self.assertEqual(self.ack(stale)['outcome'], 'noop')
        self.assertEqual(session.read_bytes(), contents)
        clear = self.control('clear')
        base.wait_for(lambda: self.ack(clear))
        self.assertEqual(self.ack(clear)['outcome'], 'applied', self.diag())
        self.new_generation(3)
        self.assertNotIn('resumed', self.doc())
        self.assertEqual(session.read_bytes(), b'')
        backups = list((self.workspace / 'run/backup').glob('*.jsonl'))
        self.assertEqual([p.read_bytes() for p in backups], [contents])
        logs = [json.loads(line) for line in (session.parent.parent / 'pid.log').read_text().splitlines()]
        self.assertEqual(len(logs), 2)
        self.assertTrue(all(x['runtimeHistory']['fact']['phase'] in ('stopped', 'canceled') for x in logs))
        for item in logs:
            converged = item['runtimeHistory']['fact']['stopEvidence'][-1]
            self.assertNotIn('leaseBusy', converged); self.assertEqual(converged['members'], [])
        audit = [json.loads(line) for line in (self.base / 'adapter-audit.jsonl').read_text().splitlines()]
        self.assertTrue(any(x.get('event') == 'real-claim' for x in audit))
        self.assertFalse(base.same(base.proc(first['identity']['pid']), first['identity']))
        self.stopped(self.control('stop'))
        before = Path(proto.pid_path(str(self.workspace), 'task/a')).read_bytes()
        for action in ('restart', 'clear'):
            rid = self.control(action); base.wait_for(lambda: self.ack(rid))
            self.assertEqual(self.ack(rid)['outcome'], 'rejected')
        self.assertEqual(Path(proto.pid_path(str(self.workspace), 'task/a')).read_bytes(), before)

    def test_08_service_restart_clear_history_final(self):
        self.lifecycle_controls(True, 'manual')

    def test_09_task_restart_clear_attempts_final(self):
        self.lifecycle_controls(False, 'one-shot')

    def test_10_A_crash_S_auto_many_generations_rejoin(self):
        self.register(policy='auto', mode='exit', resident=False)
        o, a = self.pipeline()
        base.wait_for(lambda: self.doc())
        self.crash(a)
        generation = self.head()['generation']
        base.wait_for(lambda: self.head()['generation'] >= generation + 3)
        self.assertIsNone(o.poll())
        self.adapter()
        base.wait_for(lambda: self.doc()['gen'] >= generation + 3)
        self.assertTrue((Path(proto.agent_dir(str(self.workspace), 'task/a')) / 'pid.log').exists())
        rid = self.control('stop'); base.wait_for(lambda: self.ack(rid))

    def lost(self, report):
        self.register(policy='manual', mode='barrier', report=report)
        o, a = self.pipeline(); ready = self.ready()
        self.crash(o)
        before = Path(proto.pid_path(str(self.workspace), 'task/a')).read_bytes()
        time.sleep(1.2)
        self.assertEqual(Path(proto.pid_path(str(self.workspace), 'task/a')).read_bytes(), before)
        self.assertTrue(base.same(base.proc(ready['identity']['pid']), ready['identity']))
        self.owner()
        (self.track() / 'finish').touch()
        base.wait_for(lambda: self.doc()['status'] == 'stale')
        self.assertEqual(self.run_fact()['exitCode'], None)
        self.assertFalse(self.run_fact()['waitObserved'])
        self.assertFalse(self.doc()['final'])
        rid = self.control('stop'); base.wait_for(lambda: self.ack(rid))
        self.assertTrue(self.doc()['final'])
        self.assertEqual(self.state(), 'success' if report else 'canceled')

    def test_11_S_crash_lost_with_report_original_success(self):
        self.lost(True)

    def test_12_S_crash_lost_without_report(self):
        self.lost(False)

    def pending(self, action):
        self.register(policy='auto', resident=False, mode='barrier')
        o = self.owner(fault=['pending', 'stop']); a = self.adapter(); ready = self.ready()
        rid = self.control(action)
        base.wait_for(lambda: o.poll() is not None)
        self.assertEqual(o.wait(), 93)
        self.crash(a); self.owner(); self.adapter()
        later = self.control('restart')
        time.sleep(.5)
        self.assertIsNone(self.ack(rid)); self.assertIsNone(self.ack(later))
        self.assertTrue(base.same(base.proc(ready['identity']['pid']), ready['identity']))
        self.assertEqual(self.head()['generation'], 1)
        ops = self.journal()['operations']
        self.assertEqual(len([k for k in ops if k.startswith('control:')]), 1)
        op = ops['control:' + rid + ':stop']
        self.assertEqual(base.client.receipt(self.protocol, op['requestId'])['status'], 'pending')
        self.assertFalse(self.doc()['final'])

    def test_13_pending_stop_barrier(self):
        self.pending('stop')

    def test_14_pending_restart_barrier(self):
        self.pending('restart')

    def test_15_pending_clear_barrier(self):
        self.pending('clear')

    def test_16_clear_backup_failure_no_new_generation(self):
        self.register(); self.owner(); self.adapter(history_fault='backup'); self.ready()
        rid = self.control('clear')
        base.wait_for(lambda: self.ack(rid))
        self.assertEqual(self.ack(rid)['outcome'], 'rejected')
        self.assertEqual(self.head()['generation'], 1)
        self.assertEqual(self.journal()['control']['stage'], 'history')
        later = self.control('restart'); time.sleep(.2)
        self.assertIsNone(self.ack(later)); self.assertTrue(self.head()['hold'])

    def test_17_residual_child_unknown_no_clear_history(self):
        self.register(mode='fork')
        self.pipeline()
        base.wait_for(lambda: self.run_fact() and self.run_fact()['phase'] == 'cleanupUnknown')
        session = Path(proto.agent_dir(str(self.workspace), 'task/a')) / 'session/session.jsonl'
        session.parent.mkdir(exist_ok=True); session.write_bytes(b'untouched\n')
        rid = self.control('clear'); time.sleep(.5)
        self.assertIsNone(self.ack(rid)); self.assertFalse(self.doc()['final'])
        self.assertEqual(session.read_bytes(), b'untouched\n')
        self.assertFalse((self.workspace / 'run/backup').exists())
        (self.track() / 'child-finish').touch()

    def test_18_second_A_and_negative_audit(self):
        self.register(); self.owner(); self.adapter(negative_audit_probe=True); ready = self.ready()
        second = self.spawn('adapter')
        self.assertNotEqual(second.wait(timeout=5), 0)
        self.assertTrue(base.same(base.proc(ready['identity']['pid']), ready['identity']))
        audit = self.base / 'adapter-audit.jsonl'
        self.assertTrue(any(json.loads(line).get('event') == 'negative-audit-rejected' for line in audit.read_text().splitlines()))

    def test_19_queued_pending_submit_later_scheduler_enable(self):
        self.register(policy='auto', resident=False)
        o = self.owner(fault=['pending', 'submit'])
        rid = self.control('stop'); a = self.adapter(release=False)
        base.wait_for(lambda: o.poll() is not None)
        self.assertEqual(o.wait(), 93)
        self.sched.tick(); self.assertTrue(self.enabled())
        self.crash(a); self.owner(); self.adapter()
        time.sleep(.3)
        self.assertIsNone(self.doc()); self.assertIsNone(self.ack(rid)); self.assertFalse(self.track().exists())
        self.assertEqual(set(self.journal()['operations']), {'admit'})

    def test_20_replacement_prepare_failure_no_applied(self):
        self.register(); self.owner(replacement_failure='prepare'); self.adapter(); self.ready()
        rid = self.control('restart')
        base.wait_for(lambda: self.head()['generation'] == 2)
        base.wait_for(lambda: self.run_fact(generation=2)['phase'] == 'failed')
        time.sleep(.2)
        self.assertIsNone(self.ack(rid)); self.assertFalse(self.doc()['final'])
        self.assertEqual(self.journal()['control']['stage'], 'replace')
        self.assertFalse(self.run_fact(generation=2)['waitObserved'])
        self.assertEqual(len((self.track() / 'effects').read_text().splitlines()), 1)

    def test_21_replacement_start_failure_no_applied(self):
        self.register(); self.owner(replacement_failure='start'); self.adapter(); self.ready()
        rid = self.control('clear')
        base.wait_for(lambda: self.head()['generation'] == 2)
        base.wait_for(lambda: self.run_fact(generation=2)['phase'] == 'failed')
        self.assertIsNone(self.ack(rid)); self.assertFalse(self.doc()['final'])
        self.assertEqual(len((self.track() / 'effects').read_text().splitlines()), 1)

    def test_22_corrupt_receipt_no_snapshot_success(self):
        self.register(); o, a = self.pipeline(); self.ready()
        self.crash(o); self.crash(a)
        op = self.journal()['operations']['admit']
        path = self.node / 'state/requests' / (base.client.outer_key(op['requestId']) + '.json')
        rec = base.client.receipt(self.protocol, op['requestId']); rec['digest'] = '0'*64
        path.write_bytes(base.client.canonical(rec))
        before = Path(proto.pid_path(str(self.workspace), 'task/a')).read_bytes()
        self.adapter(); base.wait_for(lambda: self.diag().get('tasks'))
        self.assertEqual(Path(proto.pid_path(str(self.workspace), 'task/a')).read_bytes(), before)

    def test_23_clear_truncate_failure_retains_history_barrier(self):
        self.history_failure('truncate')

    def test_24_clear_claim_exception_not_strengthened(self):
        self.history_failure('claim')

    def history_failure(self, point):
        self.register(); self.owner(); self.adapter(history_fault=point); self.ready()
        session = Path(proto.agent_dir(str(self.workspace), 'task/a')) / 'session/session.jsonl'
        session.parent.mkdir(exist_ok=True); session.write_bytes(b'history-before-fault\n')
        rid = self.control('clear'); base.wait_for(lambda: self.ack(rid))
        self.assertEqual(self.ack(rid)['outcome'], 'rejected')
        self.assertEqual(self.journal()['control']['stage'], 'history')
        self.assertTrue(self.head()['hold']); self.assertEqual(self.head()['generation'], 1)
        backups = list((self.workspace / 'run/backup').glob('*.jsonl'))
        self.assertEqual([p.read_bytes() for p in backups], [b'history-before-fault\n'])
        self.assertEqual(session.read_bytes(), b'' if point == 'claim' else b'history-before-fault\n')

    def test_25_missing_control_publication_never_replayed(self):
        self.register(); o, a = self.pipeline(); self.ready()
        self.crash(a)
        rid = self.control('restart')
        # Fault at the A journal-before-publication boundary, not a fabricated ack/run.
        state = self.journal()
        req = proto.read_json(str(Path(proto.control_path(str(self.workspace), 'task/a')) / (rid + '.req')))
        state['control'] = dict(request=req, stage='stop', run=self.run_fact(), head=self.head(), agentDoc=self.doc())
        path = self.workspace / 'run/file-adapter' / (runtime_id('fixture', 'task/a') + '.json')
        path.write_text(json.dumps(state))
        self.adapter(); base.wait_for(lambda: 'before publication' in str(self.diag()))
        self.assertIsNone(self.ack(rid)); self.assertFalse(self.head()['hold'])
        self.assertEqual(set(self.journal()['operations']), {'admit'})


    def b1_window(self, published):
        self.register(policy='auto', resident=False)
        o = self.owner(**({'defer_action': 'stop'} if published else {}))
        a = self.adapter(**({'fault_after_send': 'stop'} if published else {'fault_before_send': 'stop'}))
        self.ready()
        session = Path(proto.agent_dir(str(self.workspace), 'task/a')) / 'session/session.jsonl'
        session.parent.mkdir(exist_ok=True); session.write_bytes(b'B1 history unchanged\n')
        rid = self.control('clear')
        base.wait_for(lambda: a.poll() is not None)
        self.assertEqual(a.wait(), 95 if published else 94)
        state = self.journal(); envelope = state['operations']['control:' + rid + ':stop']
        publication = self.protocol / 'requests' / (envelope['requestId'] + '.json')
        self.assertEqual(publication.exists(), published)
        self.assertIsNone(base.client.receipt(self.protocol, envelope['requestId']))
        before_ops = state['operations']
        self.adapter()  # real new A must not resend or allocate a fresh rid
        later = self.control('restart')
        (self.track() / 'finish').touch()
        base.wait_for(lambda: self.head()['generation'] >= 4)
        self.assertIsNone(o.poll())
        self.assertFalse(self.head()['hold'])
        self.assertIsNone(self.ack(rid)); self.assertIsNone(self.ack(later))
        self.assertEqual(self.journal()['operations'], before_ops)
        self.assertEqual(session.read_bytes(), b'B1 history unchanged\n')
        self.assertFalse((self.workspace / 'run/backup').exists())
        effects = (self.track() / 'effects').read_text().splitlines()
        self.assertGreaterEqual(len(effects), 3)
        self.events.append(dict(event='B1-published-unaccepted' if published else 'B1-local-only',
                                envelope=envelope, head=self.head(), effects=effects, journal=self.journal()))
        if published:
            # Original old-generation request only: no retargeting or fresh rid.
            (self.base / 'allow-acceptance').touch()
            base.wait_for(lambda: self.ack(rid))
            self.assertEqual(self.ack(rid)['outcome'], 'noop')
            rec = base.client.receipt(self.protocol, envelope['requestId'])
            self.assertEqual(rec['result']['reason'], 'stale-generation')
            self.assertFalse(self.head()['hold'])
            self.assertEqual(session.read_bytes(), b'B1 history unchanged\n')

    def test_26_B1_actual_local_intent_before_publication(self):
        self.b1_window(False)

    def test_27_B1_actual_publication_before_acceptance_stale_noop(self):
        self.b1_window(True)

    def test_28_B1_accepted_pending_blocks_whole_target_after_payload_exit(self):
        self.register(policy='auto', resident=False)
        o = self.owner(fault=['pending', 'stop']); a = self.adapter(); self.ready()
        rid = self.control('clear')
        base.wait_for(lambda: o.poll() is not None)
        self.assertEqual(o.wait(), 93)
        self.crash(a)
        op = self.journal()['operations']['control:' + rid + ':stop']
        rec = base.client.receipt(self.protocol, op['requestId'])
        self.assertEqual(rec['status'], 'pending')
        self.owner(); self.adapter()
        later = self.control('restart')
        (self.track() / 'finish').touch()
        base.wait_for(lambda: (self.track() / 'done').exists())
        time.sleep(.65)  # longer than max configured backoff; owner is really running
        self.assertEqual(self.head()['generation'], 1)
        self.assertEqual(len((self.track() / 'effects').read_text().splitlines()), 1)
        self.assertIsNone(self.ack(rid)); self.assertIsNone(self.ack(later))
        self.assertEqual(base.client.receipt(self.protocol, op['requestId']), rec)
        self.assertFalse((self.workspace / 'run/backup').exists())
        snapshot = base.client.snapshot(self.protocol)
        self.assertIn(['Service', op['id']], snapshot['blockedTargets'])
        self.events.append(dict(event='B1-accepted-whole-target-gate', envelope=op, receipt=rec,
                                snapshot=snapshot, head=self.head()))

    def test_29_exact_duplicate_and_conflicting_body_preserve_original_done(self):
        self.register(); self.pipeline(); self.ready()
        op = self.journal()['operations']['admit']
        rec = base.client.receipt(self.protocol, op['requestId'])
        sent = base.client.send(self.protocol, op)
        self.assertEqual(sent['digest'], rec['digest'])
        self.assertEqual(base.client.receipt(self.protocol, op['requestId']), rec)
        conflict = dict(op, declarationDigest='0'*64)
        with self.assertRaises(ValueError):
            base.client.send(self.protocol, conflict)
        self.assertEqual(len((self.track() / 'effects').read_text().splitlines()), 1)
        self.assertFalse(self.doc()['final'])  # submit done is NOT payload completion

    def test_30_done_ack_failure_reopen_no_repeat_history_or_spawn(self):
        self.register(); self.owner(); a = self.adapter(ack_fault=True); self.ready()
        rid = self.control('clear')
        base.wait_for(lambda: (self.journal()['control'] or {}).get('stage') == 'done')
        self.new_generation(2)
        self.assertIsNone(self.ack(rid))
        ops = self.journal()['operations']
        backups = sorted((self.workspace / 'run/backup').glob('*')) if (self.workspace / 'run/backup').exists() else []
        self.crash(a); self.adapter()
        base.wait_for(lambda: self.ack(rid))
        self.assertEqual(self.ack(rid)['outcome'], 'applied')
        time.sleep(.15)
        self.assertEqual(self.journal()['operations'], ops)
        self.assertEqual(self.head()['generation'], 2)
        self.assertEqual(len((self.track() / 'effects').read_text().splitlines()), 2)
        self.assertEqual(sorted((self.workspace / 'run/backup').glob('*')) if (self.workspace / 'run/backup').exists() else [], backups)

    def test_31_resident_oneshot_provider_needs_notify_and_fixed_imports(self):
        self.register(policy='one-shot', resident=True, provides=['done'])
        self.register('b', policy='one-shot', resident=False, needs=['done'], mode='exit', reaper='none')
        self.pipeline(); self.ready()
        self.assertFalse(self.enabled('b'))
        (self.track() / 'finish').touch()
        self.final(); self.sched.tick(); self.final('b')
        base.wait_for(lambda: self.notices())
        self.assertEqual(len(self.notices()), 1)
        self.assertEqual(json.loads(self.notices()[0]['body'])['event'], 'task_done')
        imports = proto.read_json(str(self.workspace / 'run/file-adapter/imports.json'))
        self.assertEqual(imports['revision'], S_REVISION)
        self.assertNotIn('runtime.kernel', imports['imports'])
        self.assertTrue(all(v['path'].startswith(str(base.S_SOURCE)) for v in imports['imports'].values()))

# Share only mechanical OS fixture helpers, not old one-shot tests or policy code.
for _name, _method in base.FileBackendTests.__dict__.items():
    if not _name.startswith('test_') and not _name.startswith('__') and _name not in ResidentTests.__dict__:
        setattr(ResidentTests, _name, _method)

if __name__ == '__main__':
    unittest.main()
