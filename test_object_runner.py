import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parent))
import proto
from object_runner import ObjectRunner

class ObjectIntegration(unittest.TestCase):
    def setUp(self):
        self.base=Path(tempfile.mkdtemp(prefix='agent-objects-')).resolve();self.ws=self.base/'workspace';(self.ws/'agents/task/demo').mkdir(parents=True);(self.ws/'env').mkdir();(self.ws/'env/host-id').write_text('fixture fixture\n')
        sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'serviced'))
        from runtime.store import write
        from runtime.owner import Owner
        self.write=write;self.c=dict(node='fixture',sharedRoot=str(self.base/'shared'),privateRoot=str(self.base/'private'),python=sys.executable,home=str(self.base),path=os.environ['PATH'],publishers={'owner-fixture':'fixture','agent-fixture':'fixture'},version=1)
        cfg=self.base/'node.json';write(cfg,self.c);self.owner=Owner(self.c)
        c=dict(host='fixture',workspace=str(self.ws),nodeConfig=str(cfg),runtimeSource=str(Path(__file__).resolve().parents[1]/'serviced'),agentCode=str(Path(__file__).resolve().parent),piCode=str(self.base),python=sys.executable,home=str(self.base))
        self.agent=ObjectRunner(c)
    def tearDown(self):
        for p in self.owner.processes.handles.values():
            if p.poll() is None:
                try:os.killpg(p.pid,signal.SIGKILL)
                except ProcessLookupError:pass
            p.wait(timeout=5)
        self.agent.ownerlock.close();self.owner.close()
    def register(self,command,**kw):
        spec=dict(host='fixture',command=command,createdAt=proto.now_ts(),restartPolicy='one-shot',**kw)
        self.write(Path(proto.spec_path(str(self.ws),'task/demo')),spec)
        self.write(Path(proto.enable_path(str(self.ws),'task/demo')),dict(by='agentd-scheduler',ts=proto.now_ts()))
    def until(self,fn):
        end=time.monotonic()+8
        while time.monotonic()<end:
            self.agent.service('task/demo');self.owner.round();v=fn()
            if v:return v
            time.sleep(.03)
        self.fail('timeout')
    def test_task_projection_and_no_execution_authority(self):
        self.register('printf ok');doc=self.until(lambda:(d if (d:=self.agent.read_pid('task/demo')) and d.get('final') else None))
        self.assertEqual((doc['status'],doc['exitcode']),('exited',0));self.assertFalse(self.agent.popen)
        self.assertNotIn('runtime.owner',getattr(self.agent.client,'__dict__',{}))
    def test_queued_stop_has_no_payload(self):
        self.register('sleep 100')
        self.write(Path(proto.control_path(str(self.ws),'task/demo'))/'stop.req',dict(id='stop',action='stop',ts=proto.now_ts()))
        doc=self.until(lambda:(d if (d:=self.agent.read_pid('task/demo')) and d.get('final') else None));self.assertEqual(doc['status'],'killed');self.assertFalse(self.owner.processes.handles)

if __name__=='__main__':unittest.main()
