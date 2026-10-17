"""Real Linux P3b chain; new roots only. See P3B.md for execution environment.

Reuses P3a's registration/command/sentinel utilities, not its lifecycle assertions.
The original 18 are run separately. No fake agent enable/final/strong ack is made.
"""
import json
import os
from pathlib import Path
import signal
import sys
import time
import traceback
import unittest
from unittest.mock import patch

from . import test_adapter as base
from .adapter import A_ROOT, runtime_id
from .p3b import Adapter, initial_workdir, lifecycle_mapping
import proto
import runner
import scheduling

AUDIT = []
def audit(event, args):
    if event not in ('subprocess.Popen', 'os.killpg'):
        return
    if event == 'subprocess.Popen' and not (len(args[1]) > 1 and str(args[1][1]).endswith('/payload.py')):
        return
    stack = traceback.extract_stack()
    if not any(f.filename.endswith('/runtime/kernel.py') and f.name in ('launch', 'stop') for f in stack):
        raise AssertionError('second payload supervisor: ' + event)
    AUDIT.append({'event': event, 'args': repr(args), 'stack': [str(f) for f in stack]})
sys.addaudithook(audit)


class CompatibilityTests(base.AdapterTests):
    def setUp(self):
        super().setUp()
        self.audit_start = len(AUDIT)
        self.retired_handles = []
        self.pidfds = {}
        self.libc = base.ctypes.CDLL(None,use_errno=True)
        self.libc.syscall.restype = base.ctypes.c_long
        (self.source / 'payload.py').write_bytes(Path(__file__).with_name('p3b_payload.py').read_bytes())
        self.git('add', '--', 'payload.py')
        msg = self.base / 'p3b-fixture-message.txt'; msg.write_text('Synthetic P3b payload taskId a41wn0\n')
        self.git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-q', '-F', str(msg), '--', 'payload.py')
        self.revision = self.git('rev-parse', 'HEAD').strip()

    def register(self, name='a', *, resident=False, policy='one-shot', mode='barrier', report=True,
                 rc=0, provides=(), needs=(), resources=(), reaper='queue/dispatcher'):
        pid = 'task/' + name
        binding = {'command': 'p3a-fixture ' + name,
            'sources': [{'alias': 'fixture', 'repository': self.source.as_uri(), 'revision': self.revision, 'requires': []}],
            'argv': [sys.executable, '${code.fixture}/payload.py', str(self.track(name)), mode, 'report' if report else 'none', str(rc)],
            'env': {'SYNTHETIC_VALUE': 'p3b'}, 'toolchain': {'python': sys.executable}}
        spec = {'command': binding['command'], 'resident': resident,
                'creator': 'queue/dispatcher', 'reaper': reaper, 'createdAt': proto.now_ts(),
                'createdByHost': 'fixture', 'host': 'fixture', 'resources': list(resources),
                'provides': list(provides), 'needs': list(needs)}
        if policy is not None:
            spec['restartPolicy'] = policy
        spec['workdir'] = str(initial_workdir(self.node, 'fixture', pid, spec))
        p = self.command([sys.executable, '-B', str(A_ROOT / 'agentctl.py'), '--root', str(self.workspace), 'register-json'],
                         data=json.dumps({'name': name, 'spec': spec, 'prompt': '# SYNTHETIC P3b fixture\n'}))
        self.assertEqual(json.loads(p.stdout)['taskId'], name)
        self.tasks[pid] = binding
        self.manifest.write_text(json.dumps({'experiment': Adapter.EXPERIMENT, 'host': 'fixture', 'tasks': self.tasks}))
        return pid

    def adapter(self, **extra):
        a = Adapter(sandbox_root=str(self.base), workspace=str(self.workspace), runtime_root=str(self.node),
                    runtime_source=str(base.S_SOURCE), manifest=str(self.manifest), **extra)
        self.adapters.append(a)
        return a

    def close(self, a):
        self.retired_handles.extend(a.runtime.kernel.processes.values())
        super().close(a)

    def current(self, a, pid='task/a'):
        state = proto.read_json(str(a.policy_path(pid)))
        return a.current(state)

    def ready(self, pid='task/a'):
        doc = self.doc(pid)
        facts = base.wait_for(lambda: proto.read_json(str(self.track(pid.split('/')[1]) / ('%d.ready' % doc['pid']))))
        actual = base.K.proc(facts['pid'])
        if actual and actual['state'] not in ('Z','X') and facts['pid'] not in self.pidfds:
            self.assertEqual(actual['procStart'],doc['procStart'])
            fd = self.libc.syscall(434,facts['pid'],0)
            if fd < 0: raise OSError(base.ctypes.get_errno(),'pidfd_open')
            self.pidfds[facts['pid']] = fd
        return facts

    def release(self, pid='task/a'):
        d = self.doc(pid)
        (self.track(pid.split('/')[1]) / ('%d.finish' % d['pid'])).write_text('natural exit')

    def control_cli(self, action, pid='task/a', *extra):
        return self.ctl('control', pid, action, '--from', 'queue/dispatcher', *extra).stdout.strip()

    def progress(self, a, predicate):
        def tick():
            a.tick()
            return predicate()
        return base.wait_for(tick)

    def envelopes(self):
        return [json.loads(p.read_text()) for p in (self.workspace/'agents').glob('*/*/inbox/*.msg')]

    def terminal_payloads(self):
        return [json.loads(e['body']) for e in self.envelopes() if e['from'] == 'agentd']

    def test_p3b_01_mapping_all_original_predicate_combinations(self):
        for resident in (False, True):
            for policy in (None, 'manual', 'one-shot', 'auto'):
                m = lifecycle_mapping({'resident': resident, 'restartPolicy': policy})
                final = policy == 'one-shot' or (policy != 'auto' and not resident)
                self.assertEqual(m['finalOnExit'], final)
                self.assertEqual(m['kind'], 'Task' if final else 'Service')
                self.assertEqual(m['policy'], 'always' if policy == 'auto' else 'manual')
        with self.assertRaisesRegex(ValueError, 'unsupported'):
            lifecycle_mapping({'restartPolicy': 'on-failure'})
        with self.assertRaisesRegex(ValueError, 'invalid resident'):
            lifecycle_mapping({'resident': 1})

    def auto_generations(self, rc):
        self.register(policy='auto', rc=rc, report=False)
        a = self.adapter(); self.sched.tick(); a.tick()
        old_results = []
        for gen in range(1, 5):
            self.progress(a, lambda: self.doc()['gen'] == gen and self.doc()['status'] == 'running')
            ready = self.ready(); run, head = self.current(a)
            self.assertEqual((head['generation'], run['generation'], self.doc()['gen']), (gen, gen, gen))
            self.assertEqual(ready['pid'], run['pid'])
            self.assertEqual(ready['pgid'], ready['sid'])
            self.assertTrue(a.runtime.kernel.lease_busy(run))
            self.assertEqual(ready['env']['AGENT_SELF'], 'task/a')
            self.assertEqual(ready['env']['AGENTD_TASK'], '1')
            self.release()
            self.progress(a, lambda: self.current(a)[1]['phase'] == 'backoff')
            run, head = self.current(a)
            self.assertEqual((run['exitCode'], run['waitObserved']), (rc, True))
            self.assertEqual(head['backoffSeconds'], min(.4, .1 * 2 ** (gen - 1)))
            self.assertFalse(self.doc()['final'])
            self.assertEqual(self.state(), 'pending')
            self.assertFalse(a.runtime.kernel.lease_busy(run))
            self.assertFalse([p for p in base.K.group_members(run) if p['state'] not in ('Z', 'X')])
            result = a.runtime.kernel.run_dir(run)/'result.json'
            old_results.append((result, result.read_bytes(), result.stat().st_ino))
            deadline = head['nextStartAt']; a.tick()
            self.assertEqual(self.current(a)[1]['nextStartAt'], deadline)
            for p, data, ino in old_results:
                self.assertEqual((p.read_bytes(), p.stat().st_ino), (data, ino))
        rid = self.stop(); a.tick()
        self.assertTrue(self.doc()['final']); self.assertEqual(self.ack(rid)['outcome'], 'noop')
        self.assertTrue(self.current(a)[1]['hold'])
        time.sleep(.45); a.tick()
        self.assertEqual(self.current(a)[1]['generation'], 4)
        effects = (self.track()/'effects').read_text().splitlines()
        self.assertEqual(len(effects), 4)
        self.assertEqual(len((Path(a.agent.adir('task/a'))/'pid.log').read_text().splitlines()), 3)
        self.assertEqual(len([x for x in self.terminal_payloads() if x['taskId']=='task/a']), 1)

    def test_p3b_02_auto_exit_zero_four_real_generations(self):
        self.auto_generations(0)

    def test_p3b_03_auto_exit_nonzero_four_real_generations(self):
        self.auto_generations(7)

    def test_p3b_04_manual_default_oneshot_and_resident_special(self):
        combinations = [(False,None),(False,'manual'),(False,'one-shot'),(True,'one-shot'),(True,None),(True,'manual')]
        for i,(resident,policy) in enumerate(combinations):
            self.register('m'+str(i), resident=resident, policy=policy)
        a = self.adapter()
        for i,(resident,policy) in enumerate(combinations):
            pid='task/m'+str(i)
            self.sched.tick(); a.tick(); facts=self.ready(pid)
            if resident:
                self.assertEqual(facts['env']['AGENTD_RESIDENT'], '1')
                self.assertNotIn('AGENTD_TASK', facts['env'])
            self.release(pid)
            final = policy=='one-shot' or not resident
            self.progress(a, lambda: self.doc(pid)['status']=='exited')
            self.assertEqual(self.doc(pid)['final'], final)
            for _ in range(3): a.tick()
            self.assertEqual(len((self.track('m'+str(i))/'effects').read_text().splitlines()),1)
            if not final:
                rid=self.control_cli('restart',pid); a.tick(); self.ready(pid)
                self.assertEqual((self.doc(pid)['gen'],self.ack(rid,pid)['outcome']),(2,'applied'))
                rid=self.control_cli('stop',pid); a.tick(); self.assertTrue(self.doc(pid)['final'])
            else:
                for action in ('restart','clear'):
                    rid=self.control_cli(action,pid); a.tick(); self.assertEqual(self.ack(rid,pid)['outcome'],'rejected')

    def test_p3b_05_resident_slot_exemption_resources_and_needs(self):
        self.register(resident=True,policy='manual',resources=['r'])
        self.register('b',resident=True,policy='manual',resources=['s'])
        self.register('c',resources=['r'],provides=['c-done'])
        self.register('d',needs=['c-done'])
        self.register('e',resources=['e'])
        a=self.adapter(); self.sched.tick(); a.tick()
        self.ready(); self.ready('task/b'); self.ready('task/c')
        self.assertTrue(self.enabled('task/c')) # resident resources are exempt too, exactly the old predicate
        self.assertFalse(self.enabled('task/d')); self.assertFalse(self.enabled('task/e'))
        self.assertIn('slots',self.sched._last_block['task/e'])
        self.assertNotIn('task/a',scheduling.scheduling_view(str(self.workspace)).occupants)
        self.release('task/c'); self.finish(a,'task/c')
        self.sched.tick(); self.assertTrue(self.enabled('task/d'))
        a.tick(); self.ready('task/d'); self.release('task/d'); self.finish(a,'task/d')
        self.sched.tick(); a.tick(); self.ready('task/e')

    def test_p3b_06_queued_stop_restart_clear_real_requests(self):
        self.register(resident=True,policy='auto')
        self.register('b'); self.register('c',resident=True,policy='manual')
        a=self.adapter()
        stop=self.stop(); a.tick()
        self.assertEqual((self.doc()['gen'],self.doc()['final'],self.ack(stop)['outcome']),(0,True,'applied'))
        self.assertFalse(self.track().exists()); self.assertFalse(self.enabled())
        clear=self.control_cli('clear','task/b'); a.tick()
        self.assertIsNone(self.doc('task/b')); self.assertEqual(self.ack(clear,'task/b')['outcome'],'noop')
        for pid in ('task/b','task/c'):
            restart=self.control_cli('restart',pid); a.tick(); self.ready(pid)
            self.assertFalse(self.enabled(pid)) # original external restart permits pre-release start
            self.assertEqual((self.doc(pid)['gen'],self.ack(restart,pid)['outcome']),(1,'applied'))

    def test_p3b_07_running_oneshot_restart_clear_history_inject_old_duplicate(self):
        self.register(); a=self.adapter(); self.sched.tick(); a.tick(); self.ready()
        old=self.doc(); delayed=self.control_cli('restart')
        first=self.control_cli('restart','task/a','--inject','synthetic continuation')
        original=a.agent.pending_reqs
        with patch.object(a.agent,'pending_reqs',lambda pid:[r for r in original(pid) if r['id']!=delayed]):
            a.tick()
        self.ready(); self.assertEqual(self.doc()['gen'],2)
        self.assertEqual(self.doc()['resumed'],1); self.assertEqual(self.ack(first)['outcome'],'applied')
        before=self.doc()['pid']; a.tick(); self.assertEqual(self.ack(delayed)['outcome'],'noop')
        self.assertEqual(self.doc()['pid'],before)
        a.tick(); self.assertEqual(self.doc()['pid'],before) # exact old request remains, ack prevents repeats
        self.assertIsNone(base.K.proc(old['pid']))
        session=Path(a.agent.adir('task/a'))/'session/session.jsonl'; session.parent.mkdir(exist_ok=True)
        contents='{"role":"user","text":"synthetic prior session"}\n'; session.write_text(contents)
        original_backup=a.agent._backup_and_clear_session
        def backup(pid):
            run,_=self.current(a,pid)
            self.assertIn(run['phase'],('canceled','succeeded','failed','lost'))
            self.assertFalse(a.runtime.kernel.lease_busy(run))
            self.assertFalse([p for p in base.K.group_members(run) if p['state'] not in ('Z','X')])
            self.events.append({'event':'backup-after-confirmed-stop','run':run})
            return original_backup(pid)
        with patch.object(a.agent,'_backup_and_clear_session',backup):
            rid=self.control_cli('clear'); a.tick()
        self.ready(); self.assertEqual(self.doc()['gen'],3); self.assertNotIn('resumed',self.doc())
        self.assertEqual(self.ack(rid)['outcome'],'applied'); self.assertEqual(session.read_bytes(),b'')
        backups=list((self.workspace/'run/backup').glob('*.jsonl'))
        self.assertEqual([p.read_text() for p in backups],[contents])
        history=[json.loads(l) for l in (Path(a.agent.adir('task/a'))/'pid.log').read_text().splitlines()]
        self.assertEqual([d['gen'] for d in history],[1,2])
        self.assertTrue(all(d['runtimeStop']['phase']=='canceled' for d in history))
        runs=list((self.node/'state/runs/Task'/runtime_id('fixture','task/a')).glob('*/1/run.json'))
        self.assertEqual(len(runs),3)
        self.release(); self.finish(a)
        for action in ('clear','restart'):
            rid=self.control_cli(action); a.tick(); self.assertEqual(self.ack(rid)['outcome'],'rejected')
        self.assertEqual(len((self.track()/'effects').read_text().splitlines()),3)

    def test_p3b_08_service_clear_stop_order_and_manual_no_auto(self):
        self.register(resident=True,policy='manual',mode='ignore-term',report=False)
        a=self.adapter(); self.sched.tick(); a.tick(); self.ready()
        session=Path(a.agent.adir('task/a'))/'session/session.jsonl'; session.parent.mkdir(exist_ok=True); session.write_text('synthetic\n')
        original=a.agent._backup_and_clear_session
        def backup(pid):
            run,head=self.current(a,pid)
            self.assertEqual((run['phase'],head['hold']),('stopped',True))
            self.assertTrue(run['waitObserved']); self.assertFalse(a.runtime.kernel.lease_busy(run))
            self.assertEqual([e['signal'] for e in run['stopEvidence'] if e['event']=='signal'],[15,9])
            self.events.append({'event':'service-backup-after-stop','run':run})
            return original(pid)
        claimed=[]; real_chown=os.chown
        def chown(path,uid,gid):
            self.assertEqual(Path(path),session)
            self.assertEqual(self.current(a)[1]['hold'],True)
            claimed.append({'path':str(path),'uid':uid,'gid':gid})
            return real_chown(path,uid,gid) # actual own-file operation, no privileged gid used
        # Simulate only the local replica role label using our actual group; the
        # original helper executes a real chown on this fixture's session.
        import types
        with patch.object(a.agent,'_backup_and_clear_session',backup), \
             patch('grp.getgrnam',return_value=types.SimpleNamespace(gr_gid=os.getgid())), \
             patch.object(os,'chown',chown):
            rid=self.control_cli('clear'); a.tick()
        self.assertEqual(len(claimed),1); self.events.append({'event':'real-own-session-claim','claims':claimed})
        self.ready(); self.assertEqual((self.doc()['gen'],self.ack(rid)['outcome']),(2,'applied'))
        self.assertEqual(session.read_bytes(),b''); self.assertNotIn('resumed',self.doc())
        rid=self.stop(); a.tick(); self.assertTrue(self.doc()['final']); self.assertEqual(self.ack(rid)['outcome'],'applied')
        time.sleep(.45); a.tick(); self.assertEqual(self.current(a)[1]['generation'],2)

    def test_p3b_09_notification_policy_none_live_and_fallback_dedupe(self):
        # A real registered resident is the live recipient; no fixture pid/enable/final.
        self.register('receiver',resident=True,policy='manual',reaper='none')
        self.register('done',mode='exit',reaper='task/receiver')
        self.register('fail',mode='exit',rc=7,report=True,reaper='task/receiver')
        self.register('idle',mode='exit',report=False,reaper='task/receiver')
        self.register('none',mode='exit',reaper='none')
        self.register('absent',mode='exit',reaper='bot/missing')
        a=self.adapter()
        for name in ('done','fail','idle','none','absent'):
            self.sched.tick(); a.tick(); self.ready('task/receiver'); self.finish(a,'task/'+name)
        payloads={x['taskId']:x for x in self.terminal_payloads()}
        self.assertEqual(payloads['task/done']['event'],'task_done')
        self.assertEqual(payloads['task/fail']['event'],'task_failed')
        self.assertEqual(payloads['task/idle']['warn'],'no_report')
        self.assertEqual(self.state('task/idle'),'idle')
        self.assertNotIn('task/none',payloads)
        self.assertIn('note',payloads['task/absent'])
        direct=list((Path(a.agent.adir('task/receiver'))/'inbox').glob('*.msg'))
        self.assertEqual(len(direct),3)
        # Stop the real recipient; next notification must fall back with note.
        self.stop('task/receiver'); a.tick()
        # Selection immutable, so use an already registered blocked follower in a separate test for dead recipient.
        before=len(self.envelopes()); a.tick(); a.tick(); self.close(a)
        a=self.adapter(); a.tick(); self.assertEqual(len(self.envelopes()),before)
        self.assertFalse((Path(a.agent.adir('task/none'))/'notified.json').exists())

    def test_p3b_10_notification_dead_recipient_retry_and_cold_guard(self):
        self.register('receiver',resident=True,policy='manual',reaper='none')
        self.register(mode='barrier',reaper='task/receiver')
        a=self.adapter(); self.sched.tick(); a.tick(); self.ready(); self.ready('task/receiver')
        self.stop('task/receiver'); a.tick()
        # Real terminal and failed envelope I/O before mark; original helper retries.
        self.release(); original=proto.atomic_write_json
        def fail_envelope(path,value):
            if str(path).endswith('.msg') and value.get('from')=='agentd':
                raise OSError('synthetic envelope I/O failure')
            return original(path,value)
        with patch.object(proto,'atomic_write_json',fail_envelope):
            with self.assertRaisesRegex(OSError,'synthetic envelope'):
                self.progress(a,lambda:self.doc()['final'])
        self.assertFalse((Path(a.agent.adir('task/a'))/'notified.json').exists())
        a.tick(); msg=self.terminal_payloads()[0]
        self.assertIn('note',msg); self.assertEqual(msg['event'],'task_done')
        before=len(self.envelopes()); a.tick(); self.assertEqual(len(self.envelopes()),before)
        # Cold guard tested on a real terminal whose first notify never ran: reset
        # neither pid nor mark. Separate real task/tree below is test11.

    def test_p3b_11_cold_replay_suppresses_unseen_old_terminal(self):
        self.register(mode='exit'); a=self.adapter(); self.sched.tick()
        with patch.object(a.agent,'notify_tick',lambda:None):
            self.finish(a)
        self.assertEqual(self.envelopes(),[])
        self.close(a); a=self.adapter()
        # Advance only the notification policy's observation epoch, not payload/S time.
        a.agent.run_started_epoch=time.time()+a.agent.replay_grace+5
        a.tick(); self.assertEqual(self.envelopes(),[])
        self.assertFalse((Path(a.agent.adir('task/a'))/'notified.json').exists())

    def test_p3b_12_pending_clear_failure_never_replays_or_starts(self):
        self.register(resident=True,policy='auto'); a=self.adapter(); self.sched.tick(); a.tick(); self.ready()
        old=self.doc()['pid']; rid=self.control_cli('clear')
        with patch.object(a.agent,'_backup_and_clear_session',side_effect=OSError('synthetic backup failure')):
            a.tick()
        self.assertEqual(self.ack(rid)['outcome'],'rejected')
        self.assertEqual(self.current(a)[1]['generation'],1)
        self.assertTrue(self.current(a)[1]['hold']); self.assertIsNone(base.K.proc(old))
        self.close(a); a=self.adapter(); time.sleep(.45); a.tick()
        self.assertEqual(self.current(a)[1]['generation'],1)
        self.assertEqual(a.agent.pending_reqs('task/a'),[])
        self.assertEqual(proto.read_json(str(a.policy_path('task/a')))['control']['status'],'pending')

    def test_p3b_13_nonresident_auto_backoff_keeps_resources_and_slot(self):
        self.register(policy='auto',resources=['shared'],report=False)
        self.register('b',resources=['shared'])
        self.sched.max_concurrent=2
        a=self.adapter(); self.sched.tick(); a.tick(); self.ready()
        self.assertFalse(self.enabled('task/b'))
        self.release(); self.progress(a,lambda:self.current(a)[1]['phase']=='backoff')
        self.sched.tick()
        self.assertFalse(self.enabled('task/b'))
        self.assertIn('resource',self.sched._last_block['task/b'])
        self.assertIn('task/a',scheduling.scheduling_view(str(self.workspace)).occupants)
        self.stop(); a.tick(); self.sched.tick(); a.tick(); self.ready('task/b')
        self.assertTrue(self.enabled('task/b'))

    def test_p3b_14_resident_auto_old_control_cannot_stop_successor(self):
        self.register(resident=True,policy='auto',report=False)
        a=self.adapter(); self.sched.tick(); a.tick(); facts=self.ready()
        self.assertEqual(facts['env']['AGENTD_RESIDENT'],'1'); self.assertNotIn('AGENTD_TASK',facts['env'])
        old=self.control_cli('stop'); original=a.agent.pending_reqs
        self.release()
        with patch.object(a.agent,'pending_reqs',lambda pid:[r for r in original(pid) if r['id']!=old]):
            self.progress(a,lambda:self.doc()['gen']==2)
        facts=self.ready(); before=self.doc()['pid']; a.tick()
        self.assertEqual(self.ack(old)['outcome'],'noop'); self.assertEqual(self.doc()['pid'],before)
        self.assertIsNotNone(base.K.proc(before)); self.assertFalse(self.doc()['final'])

    def test_p3b_15_real_task_orphan_notifications_stale_report_both(self):
        self.register(report=True); self.register('b',report=False)
        a=self.adapter()
        for pid,has_report in [('task/a',True),('task/b',False)]:
            self.sched.tick(); a.tick(); self.ready(pid)
            run,_=self.current(a,pid)
            handle=a.runtime.kernel.processes[a.runtime.kernel.key(run)]
            self.close(a); a=self.adapter()
            a.tick(); self.assertFalse(self.doc(pid)['final'])
            self.release(pid); handle.wait(timeout=5) # harness wait is deliberately not given to new S owner
            self.finish(a,pid)
            self.assertEqual((self.doc(pid)['status'],self.doc(pid)['exitcode']),('stale',127))
            run,_=self.current(a,pid)
            self.assertEqual((run['exitCode'],run['signal'],run['waitObserved']),(None,None,False))
            self.assertEqual(self.state(pid),'success' if has_report else 'failed')
        payloads={p['taskId']:p for p in self.terminal_payloads()}
        self.assertEqual(payloads['task/a']['event'],'task_done')
        self.assertIn('stale/127',payloads['task/a']['note'])
        self.assertEqual(payloads['task/b']['event'],'task_failed')

    def test_p3b_16_cancellation_notification_tiers(self):
        self.register(report=False); self.register('b',report=False)
        a=self.adapter(); self.sched.tick(); a.tick(); self.ready()
        running=self.stop(); queued=self.stop('task/b'); a.tick()
        self.assertEqual((self.ack(running)['outcome'],self.ack(queued,'task/b')['outcome']),('applied','applied'))
        payloads=self.terminal_payloads(); self.assertEqual(len(payloads),2)
        for p in payloads:
            self.assertEqual(p['event'],'task_canceled'); self.assertNotIn('warn',p)
            self.assertEqual(p['stopReason'],'synthetic cancel')
            self.assertEqual(self.state(p['taskId']),'canceled')
        self.assertEqual(self.doc('task/b')['gen'],0); self.assertFalse(self.track('b').exists())

    def test_p3b_17_cold_recent_replay_and_envelope_mark_crash_window(self):
        self.register(mode='exit'); a=self.adapter(); self.sched.tick()
        # Envelope succeeds, mark fails: original at-least-once permits one duplicate.
        original=proto.atomic_write_json
        def fail_mark(path,value):
            if str(path).endswith('/notified.json'):
                raise OSError('synthetic crash after envelope before mark')
            return original(path,value)
        with patch.object(proto,'atomic_write_json',fail_mark):
            with self.assertRaisesRegex(OSError,'synthetic crash'):
                self.finish(a)
        self.assertEqual(len(self.envelopes()),1)
        self.close(a); a=self.adapter(); a.tick()
        self.assertEqual(len(self.envelopes()),2) # recent real final, no fake pid or manual mark deletion
        a.tick(); self.assertEqual(len(self.envelopes()),2)
        self.assertTrue(proto.read_json(str(Path(a.agent.adir('task/a'))/'notified.json'))['complete'])

    def test_p3b_18_unknown_lease_and_partial_control_blocks_successor(self):
        self.register(policy='auto',report=False,resources=['r'])
        self.register('b',resources=['r'])
        self.sched.max_concurrent=2
        a=self.adapter(); self.sched.tick(); a.tick(); self.ready()
        run,_=self.current(a); old=run['pid']; rid=self.control_cli('restart')
        with patch.object(a.runtime.kernel,'lease_busy',side_effect=PermissionError('synthetic lease unreadable')):
            # S's conservative observation returns unknown rather than authorizing a signal.
            try: a.tick()
            except PermissionError: pass
        self.assertIsNone(self.ack(rid)); self.assertIsNotNone(base.K.proc(old))
        self.assertEqual(self.current(a)[1]['generation'],1)
        self.sched.tick(); self.assertFalse(self.enabled('task/b'))
        self.close(a); a=self.adapter(); a.tick()
        self.assertIsNone(self.ack(rid)); self.assertEqual(self.current(a)[1]['generation'],1)
        self.assertEqual(proto.read_json(str(a.policy_path('task/a')))['control']['status'],'pending')

    def test_p3b_19_owner_fence_and_immutable_release(self):
        self.register(); a=self.adapter(); self.sched.tick(); a.tick(); self.ready()
        with self.assertRaises(BlockingIOError): self.adapter()
        original=proto.read_json; ep=proto.enable_path(str(self.workspace),'task/a')
        def different(path):
            value=original(path)
            return dict(value,note='synthetic changed release') if str(path)==ep else value
        with patch.object(proto,'read_json',different):
            with self.assertRaisesRegex(ValueError,'immutable scheduler release'): a.tick()
        self.assertEqual(len((self.track()/'effects').read_text().splitlines()),1)

    def test_p3b_20_actual_ask_reply_ref_via_env_and_clear_preserves_inbox(self):
        self.register('receiver',resident=True,policy='manual',reaper='none')
        self.register(reaper='task/receiver')
        a=self.adapter(); self.sched.tick(); a.tick(); facts=self.ready(); self.ready('task/receiver')
        inbox=Path(a.agent.adir('task/receiver'))/'inbox'
        self.assertEqual(facts['env']['AGENTD_ASK_INBOX'],str(inbox))
        self.assertNotIn('AGENTD_ASK_NOTE',facts['env'])
        ask=self.ctl('send','task/receiver','--type','ask','--from','task/a','--body','synthetic nonblocking question').stdout.strip()
        request=proto.read_json(str(inbox/(ask+'.msg')))
        self.assertEqual(request['via'],proto.ASK_VIA_SEND_MESSAGE)
        result=self.ctl('answer','task/a','--from','task/receiver','--body','synthetic reply','--deliver','steer').stdout
        replyid=result.splitlines()[0]
        replypath=Path(a.agent.adir('task/a'))/'inbox'/(replyid+'.msg')
        reply=proto.read_json(str(replypath))
        self.assertEqual((reply['ref'],reply['via'],reply['deliver']),(ask,request['via'],'steer'))
        session=Path(a.agent.adir('task/a'))/'session/session.jsonl'; session.parent.mkdir(exist_ok=True)
        session.write_text('synthetic history\n')
        rid=self.control_cli('clear'); a.tick(); self.ready()
        self.assertEqual(self.ack(rid)['outcome'],'applied')
        self.assertEqual(proto.read_json(str(replypath)),reply)
        self.assertEqual(proto.read_json(str(inbox/(ask+'.msg'))),request)
        self.assertEqual(list((self.workspace/'agents').rglob('current-waits.json')),[])
        self.events.append({'event':'real-ask-reply-envelopes-not-injected','ask':request,'reply':reply})

    def test_p3b_21_bounded_real_cli_owner_register_scheduler_control(self):
        self.register(resident=True,policy='auto',report=False)
        code="import runner,runpy\ndef forbidden(*a,**k): raise AssertionError('legacy supervision')\n"+\
             "for n in ('spawn','kill_proc','observe','service','do_stop','do_restart','do_clear','handle_controls'): setattr(runner.Runner,n,forbidden)\n"+\
             "runpy.run_module('experimental.agent_exec_adapter.p3b',run_name='__main__')\n"
        argv=[sys.executable,'-B','-c',code,'--sandbox-root',str(self.base),'--workspace',str(self.workspace),
              '--runtime-root',str(self.node),'--runtime-source',str(base.S_SOURCE),'--manifest',str(self.manifest),'--seconds','2']
        log=(self.base/'cli.log').open('w'); self.logs.append(log)
        owner=base.subprocess.Popen(argv,cwd=self.base,env=os.environ.copy(),stdout=log,stderr=base.subprocess.STDOUT)
        self.owners.append(owner)
        base.wait_for(lambda:Path(proto.agentd_lock_path(str(self.workspace),'fixture')).exists())
        self.sched.tick()
        base.wait_for(lambda:self.doc() and self.doc()['status']=='running'); self.ready()
        rid=self.stop()
        base.wait_for(lambda:self.doc()['final'] and self.ack(rid))
        self.assertEqual(self.ack(rid)['outcome'],'applied'); self.assertEqual(owner.wait(timeout=5),0)
        self.owners.remove(owner)
        self.commands.append({'argv':argv,'cwd':str(self.base),'env':dict(os.environ),'rc':owner.returncode,'log':str(self.base/'cli.log')})
        a=self.adapter(); a.tick(); self.assertEqual(self.current(a)[1]['generation'],1)
        self.assertEqual(len((self.track()/'effects').read_text().splitlines()),1)

    def test_p3b_22_manual_resident_orphan_stale_never_finalizes_by_exit(self):
        self.register(resident=True,policy='manual')
        a=self.adapter(); self.sched.tick(); a.tick(); self.ready()
        run,_=self.current(a); handle=a.runtime.kernel.processes[a.runtime.kernel.key(run)]
        self.close(a); a=self.adapter(); a.tick()
        self.assertFalse(self.doc()['final']); self.release(); handle.wait(timeout=5)
        self.progress(a,lambda:self.doc()['status']=='stale')
        self.assertEqual((self.doc()['exitcode'],self.doc()['final']),(127,False))
        self.assertEqual(self.current(a)[0]['exitCode'],None)
        self.assertEqual(self.terminal_payloads(),[])
        rid=self.stop(); a.tick()
        self.assertEqual(self.ack(rid)['outcome'],'noop')
        self.assertEqual((self.doc()['status'],self.doc()['final']),('stale',True))
        self.assertEqual(self.state(),'success') # original stale+nonempty-report exception survives explicit stop

    def test_p3b_23_replacement_prepare_failure_retains_failed_history_pending(self):
        self.register(resident=True,policy='auto',report=False)
        self.register('b',report=False)
        a=self.adapter(); self.sched.tick(); a.tick(); self.ready(); self.ready('task/b')
        module=sys.modules[a.package+'.lifecycle']
        for pid in ('task/a','task/b'):
            rid=self.control_cli('restart',pid)
            with patch.object(module,'prepare',side_effect=ValueError('synthetic replacement prepare failure')):
                a.tick()
            self.assertIsNone(self.ack(rid,pid)); self.assertFalse(self.doc(pid)['final'])
            state=proto.read_json(str(a.policy_path(pid)))
            self.assertEqual(state['control']['status'],'pending')
            if pid=='task/a':
                head=a.runtime.service(runtime_id('fixture',pid))
                failed=a.runtime.read_run('Service',head['id'],'service',2)
                self.assertEqual((head['generation'],failed['phase'],failed['pid']),(2,'failed',None))
            else:
                failed=a.runtime.task(runtime_id('fixture',pid),'a2')
                self.assertEqual((failed['phase'],failed['pid']),('failed',None))
            result=a.runtime.kernel.run_dir(failed)/'result.json'
            data=result.read_bytes(); a.tick(); self.assertEqual(result.read_bytes(),data)
            self.events.append({'event':'replacement-prepare-failure','participant':pid,'run':failed,'requestId':rid})
        self.close(a); a=self.adapter(); a.tick()
        for pid in ('task/a','task/b'):
            self.assertEqual(proto.read_json(str(a.policy_path(pid)))['control']['status'],'pending')
            self.assertEqual(len((self.track(pid.split('/')[1])/'effects').read_text().splitlines()),1)

    def test_p3b_24_completed_control_ack_failure_restores_without_effect_replay(self):
        self.register(); self.register('b',resident=True,policy='manual')
        a=self.adapter(); self.sched.tick(); a.tick(); self.ready(); self.ready('task/b')
        for pid in ('task/a','task/b'):
            rid=self.control_cli('restart',pid); original=a.agent.write_ack
            def fail_ack(participant,id,*args):
                if id==rid: raise OSError('synthetic control ack storage failure')
                return original(participant,id,*args)
            with patch.object(a.agent,'write_ack',fail_ack):
                with self.assertRaisesRegex(OSError,'synthetic control ack'): a.tick()
            self.ready(pid); before=self.doc(pid)['pid']
            self.assertEqual(self.doc(pid)['gen'],2); self.assertIsNone(self.ack(rid,pid))
            self.assertEqual(proto.read_json(str(a.policy_path(pid)))['control']['status'],'done')
            self.close(a); a=self.adapter(); a.tick()
            self.assertEqual((self.doc(pid)['pid'],self.doc(pid)['gen']),(before,2))
            self.assertEqual(self.ack(rid,pid)['outcome'],'applied')
            self.assertEqual(self.ack(rid,pid)['id'],rid)
            self.assertEqual(len((self.track(pid.split('/')[1])/'effects').read_text().splitlines()),2)

    def test_p3b_25_initial_service_prepare_failure_no_pid_retry_or_false_final(self):
        self.register(policy='auto'); a=self.adapter(); self.sched.tick()
        module=sys.modules[a.package+'.lifecycle']
        with patch.object(module,'prepare',side_effect=ValueError('synthetic initial service prepare failure')):
            a.tick()
        state=proto.read_json(str(a.policy_path('task/a')))
        self.assertEqual(state['admission'],'pending')
        self.assertEqual(state['submitReceipt']['phase'],'failed')
        self.assertIn('synthetic initial',state['submitReceipt']['reason'])
        self.assertIsNone(self.doc()); self.assertFalse(self.track().exists())
        self.assertIn('task/a',scheduling.scheduling_view(str(self.workspace)).occupants)
        self.close(a); a=self.adapter(); a.tick()
        self.assertIsNone(self.doc()); self.assertFalse(self.track().exists())
        requests=list((self.node/'state/requests').glob('*.json'))
        self.assertEqual(len(requests),1)
        self.assertEqual(proto.read_json(str(requests[0]))['result']['phase'],'failed')

    def tearDown(self):
        errors=[]; handles=list(self.retired_handles)
        for a in self.adapters:
            handles.extend(a.runtime.kernel.processes.values())
            for pid in self.tasks:
                try:
                    state=proto.read_json(str(a.policy_path(pid)))
                    if not state or state['admission']!='done': continue
                    run,head=a.current(state)
                    self.events.append({'event':'before-teardown','participant':pid,'run':run,'service':head})
                    if run and run['phase'] not in ('succeeded','failed','canceled','lost','exited','stopped'):
                        a.runtime.control({'apiVersion':'rund-control/v1alpha1','kind':run['kind'],'id':run['id'],
                            'attemptId':run['attemptId'],'expectedGeneration':run['generation'],
                            'action':'stop' if head is not None else 'cancel',
                            'requestId':'cleanup-'+pid.replace('/','-')})
                except BaseException as e: errors.append(repr(e))
            a.__exit__()
        for owner in self.owners:
            if owner.poll() is None: owner.kill()
            owner.wait(timeout=5)
        for f in self.logs: f.close()
        emergency=[]
        for pid,fd in self.pidfds.items():
            actual=base.K.proc(pid)
            if actual and actual['state'] not in ('Z','X'):
                emergency.append(actual)
                if self.libc.syscall(424,fd,signal.SIGKILL,base.ctypes.c_void_p(0),0) < 0:
                    if base.ctypes.get_errno()!=3: errors.append('pidfd_send_signal failed')
            os.close(fd)
        self.events.append({'event':'independent-emergency-cleanup','processes':emergency})
        # Independent final fallback only for our saved real Popen objects. Audit
        # supervision assertion intentionally remains active for product effects;
        # fallback uses per-process Popen.kill, not a second payload group owner.
        for p in handles:
            if p.poll() is None: p.kill()
            p.wait(timeout=5)
        # Adopted CLI orphans (if any) are ours; do not fabricate their wait in S.
        for pid in self.pidfds:
            try: os.waitpid(pid,os.WNOHANG)
            except ChildProcessError: pass
        remaining=[]
        for ready in (self.base/'tracks').glob('*/*.ready'):
            facts=json.loads(ready.read_text()); actual=base.K.proc(facts['pid'])
            self.events.append({'event':'payload-after','facts':facts,'actual':actual})
            if actual and actual['state'] not in ('Z','X'): remaining.append(actual)
        (self.base/'evidence.json').write_text(json.dumps({'commands':self.commands,'events':self.events,
            'audit':AUDIT[self.audit_start:],'remaining':remaining,'errors':errors},indent=2))
        for p in reversed(self.patches): p.stop()
        self.assertEqual(errors,[]); self.assertEqual(remaining,[]); self.assertEqual(emergency,[])


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(CompatibilityTests(n) for n in sorted(dir(CompatibilityTests)) if n.startswith('test_p3b_'))

if __name__=='__main__':
    unittest.main()
