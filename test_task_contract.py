"""Real serviced integration: sole execution/state authority and agent protocol."""
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import unittest

import proto
import execution
import registration
from watcher import ProtocolWatcher

class ExecutionContract(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='agent-contract-',dir='/tmp');self.addCleanup(self.tmp.cleanup)
        self.base=Path(self.tmp.name);self.root=self.base/'workspace';(self.root/'agents').mkdir(parents=True)
        (self.root/'env').mkdir();(self.root/'env/host-id').write_text('fixture fixture\n')
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'serviced'))
        from runtime.store import write
        from runtime.owner import Owner
        self.write=write
        self.node=dict(node='fixture',sharedRoot=str(self.base/'shared'),privateRoot=str(self.base/'private'),python=sys.executable,home=str(self.base),path=os.environ['PATH'],publishers={'owner-fixture':'fixture','agent-fixture':'fixture'},version=1,scheduler={'node':'fixture','maxConcurrent':2})
        cfg=self.base/'node.json';write(cfg,self.node);self.owner=Owner(self.node)
        self.c=dict(host='fixture',workspace=str(self.root),nodeConfig=str(cfg),runtimeSource=str(Path(__file__).resolve().parents[1]/'serviced'),agentCode=str(Path(__file__).resolve().parent),piCode=str(self.base),python=sys.executable,home=str(self.base))
        write(self.root/'run/execution.json',dict(nodes={'fixture':self.c},nodeConfig=str(cfg),runtimeSource=self.c['runtimeSource']))
    def tearDown(self):
        for child in self.owner.processes.handles.values():
            if child.poll() is None:
                try:os.killpg(child.pid,signal.SIGKILL)
                except ProcessLookupError:pass
            child.wait(timeout=5)
        self.owner.close()
    def register(self,name='demo',command='printf done',**extra):
        spec=dict(command=command,workdir=str(self.root),creator='queue/dispatcher',createdAt=proto.now_ts(),host='fixture',createdByHost='fixture',reaper='none',restartPolicy='one-shot',resources=[])
        spec.update(extra);registration.register_task(str(self.root),name,spec)
        return 'task/'+name
    def until(self,participant,phase):
        end=time.monotonic()+8
        while time.monotonic()<end:
            self.owner.round();d,s,t=execution.facts(str(self.root),participant)
            if t and t['phase']==phase:return d,s,t
            time.sleep(.02)
        self.fail(str((d,s,t)))
    def test_executes_without_watcher_or_mirrored_state(self):
        participant=self.register();d,_,state=self.until(participant,'succeeded')
        self.assertTrue(state['waitObserved']);self.assertEqual(state['exitCode'],0)
        directory=Path(proto.agent_dir(str(self.root),participant))
        self.assertEqual(json.loads((directory/'execution.json').read_text()),{'id':d['id']})
        self.assertFalse((directory/'pid.json').exists());self.assertFalse((directory/'enable.json').exists())
        self.assertFalse(execution.EXECUTION_KEYS & json.loads((directory/'spec.json').read_text()).keys())
        self.assertTrue(proto.execution_state(str(self.root),participant)['final'])
    def test_cancellation_before_start_never_spawns(self):
        participant=self.register(command='sleep 100');execution.desired(str(self.root),participant,'cancel')
        _,_,state=self.until(participant,'canceled')
        self.assertFalse(self.owner.processes.handles);self.assertEqual(proto.execution_state(str(self.root),participant)['runtime']['phase'],'canceled')
        self.assertFalse(Path(proto.control_path(str(self.root),participant)).exists())
    def test_file_dependencies_and_global_resources_reach_owner(self):
        ready=self.base/'ready';participant=self.register(needs=[str(ready)],resources=['mac-repo'])
        d,_,_=self.until(participant,'preparing');self.assertEqual(d['resources'],['mac-repo'])
        ready.write_text('ready');self.until(participant,'succeeded')
    def test_service_changes_are_read_from_current_declaration(self):
        participant=self.register(resident=True,restartPolicy='auto',command='sleep 100')
        d,_,_=self.until(participant,'running')
        from runtime.config import publish
        updated=dict(d,revision=2,version=2,task=dict(d['task'],cwd=str(self.base)))
        publish(self.node['sharedRoot'],updated,writer='fixture',publishers=self.node['publishers'])
        self.assertEqual(proto.participant_spec(str(self.root),participant)['workdir'],str(self.base))
        self.assertEqual(execution.submit(str(self.root),participant,{}),updated)
    def test_no_offline_publication_or_capability_dependency(self):
        with self.assertRaises(FileNotFoundError):execution.client(str(self.base/'missing'))
        with self.assertRaises(ValueError):self.register(needs=['cap:old'])
    def test_terminal_state_frozen_and_notification_once(self):
        participant=self.register(reaper='queue/dispatcher')
        watcher=ProtocolWatcher(self.c)
        try:
            watcher.service(participant);d,_,_=self.until(participant,'succeeded')
            statepath=self.base/'shared/tasks'/d['id']/'state.json';before=statepath.read_bytes()
            watcher.service(participant);watcher.notify_tick();watcher.notify_tick();self.owner.round()
            self.assertEqual(statepath.read_bytes(),before)
            messages=list((self.root/'agents/queue/dispatcher/inbox').glob('*.msg'))
            self.assertEqual(len(messages),1);self.assertEqual(json.loads(json.loads(messages[0].read_text())['body'])['event'],'task_done')
            self.assertFalse(hasattr(watcher,'spawn'));self.assertFalse(hasattr(watcher,'popen'))
        finally:watcher.ownerlock.close()
    def test_clear_session_is_a_finite_owner_task(self):
        from unittest.mock import patch
        participant=self.register(resident=True,restartPolicy='manual')
        session=Path(proto.agent_dir(str(self.root),participant))/'session/session.jsonl'
        session.write_text('session transcript')
        with patch.object(execution,'service_action') as action, patch('time.sleep',side_effect=lambda _:self.owner.round()):
            state=execution.clear_service_session(str(self.root),participant)
        self.assertEqual(state['phase'],'succeeded');self.assertTrue(state['waitObserved'])
        self.assertEqual(session.read_text(),'')
        self.assertEqual(next((self.root/'run/backup').glob('*.jsonl')).read_text(),'session transcript')
        self.assertEqual([call.args[-1] for call in action.call_args_list],['stop','start'])


if __name__=='__main__':unittest.main()
