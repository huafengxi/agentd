"""Real isolated A policy -> S owner -> pinned exec shell; no model or production."""
import base64,hashlib,importlib.util,io,json,os,signal,socket,subprocess,sys,tarfile,time,unittest,uuid
from pathlib import Path
A=Path(__file__).resolve().parent;R=A.parent;S=Path(os.environ['A_RUND_RUNTIME_SOURCE']).resolve()
sys.path.insert(0,str(A));sys.path.insert(0,str(S))
import proto,registration
from scheduler import Scheduler
from runtime.file_owner import protocol as C
BASE=Path(os.environ['A_RUND_TEST_OUTPUT']).resolve();BASE.mkdir(mode=0o700)
REV=os.environ['A_RUND_RUNTIME_REVISION']

def until(fn,seconds=18):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  value=fn()
  if value:return value
  time.sleep(.02)
 raise AssertionError('bounded fixture observation expired')

class AgentRuntimeTests(unittest.TestCase):
 def setUp(self):
  self.area=BASE/self._testMethodName;self.area.mkdir();self.ws=self.area/'workspace';self.ws.mkdir()
  for path in ('agents','env','home','run'):(self.ws/path).mkdir()
  (self.ws/'env/host-id').write_text(socket.gethostname()+' fixture\n')
  self.repo=self.area/'source';self.repo.mkdir()
  (self.repo/'exec_task.py').write_bytes((A/'exec_task.py').read_bytes())
  (self.repo/'probe.py').write_text('''import os,signal,sys,time,json
from pathlib import Path
home=Path(os.environ['AGENT_HOME'])
(home/'report.md').write_text('synthetic successful report\\n')
(home/'ready.json').write_text(json.dumps({'pid':os.getpid(),'self':os.environ['AGENT_SELF']}))
if len(sys.argv)>1 and sys.argv[1]=='hold':
 signal.signal(signal.SIGTERM,lambda *a:sys.exit(0))
 deadline=time.monotonic()+50
 while time.monotonic()<deadline:time.sleep(.01)
''')
  def git(*args):return subprocess.check_output(['/usr/bin/git','-C',str(self.repo),*args],stderr=subprocess.STDOUT).decode().strip()
  git('init','-q','--template=');git('add','--','exec_task.py','probe.py');msg=self.area/'message';msg.write_text('6ggidb private shell fixture\n');git('-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-q','-F',str(msg),'--','exec_task.py','probe.py');pin=git('rev-parse','HEAD')
  self.root=self.area/'node';self.protocol=self.area/'protocol';self.children=[];self.logs=[]
  env=dict(HOME=str(self.ws/'home'),PATH=str(Path(sys.executable).parent)+':/usr/bin:/bin',PYTHONPATH=str(S),PYTHONDONTWRITEBYTECODE='1')
  self.owner=self.child([sys.executable,'-B','-m','runtime.file_owner','owner','--root',str(self.root),'--node','fixture','--protocol',str(self.protocol)],env,'owner')
  until(lambda:(self.protocol/'binding.json').exists())
  manifest=self.area/'runtime-source.json';manifest.write_text(json.dumps({'revision':REV,'files':{str(p.relative_to(S)):hashlib.sha256(p.read_bytes()).hexdigest() for p in S.rglob('*') if p.is_file()}}))
  self.config=dict(apiVersion='rund-agent/v1',workspace=str(self.ws),host='fixture',runtimeRoot=str(self.root),protocolRoot=str(self.protocol),runtimeSource=str(S),runtimeRevision=REV,runtimeManifest=str(manifest),sources=[dict(alias='agentd',repository=self.repo.as_uri(),revision=pin,requires=[])],python=sys.executable,home=str(self.ws/'home'),path=env['PATH'],interval=.05)
  self.configfile=self.area/'agent.json';self.configfile.write_text(json.dumps(self.config))
  self.agent=self.start_agent()
  until(lambda:Path(proto.agentd_lock_path(str(self.ws),'fixture')).exists())
  self.scheduler=Scheduler(str(self.ws),proto.local_hosts('fixture',''),max_concurrent=1)
 def child(self,argv,env,label):
  log=(self.area/(label+'.log')).open('wb');self.logs.append(log)
  p=subprocess.Popen(argv,env=env,cwd=self.area,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True);self.children.append(p);return p
 def start_agent(self):
  env=dict(HOME=str(self.ws/'home'),PATH=self.config['path'],PYTHONDONTWRITEBYTECODE='1',PI_HOST_DIR=str(R/'pi-wrap'))
  return self.child([sys.executable,'-B',str(A/'rund_runner.py'),'--config',str(self.configfile)],env,'agent-'+str(len(self.children)))
 def register(self,name,hold=False,needs=None,provides=None):
  command='exec '+sys.executable+' "$AGENT_ROOT/agent-platform/agentd/probe.py"'+(' hold' if hold else '')
  spec=dict(command=command,workdir=str(self.ws),creator='fixture',host='fixture',createdByHost='fixture',createdAt=proto.now_ts(),restartPolicy='manual' if hold else 'one-shot')
  if hold:spec['resident']=True
  if needs:spec['needs']=needs
  if provides:spec['provides']=provides
  registration.register_task(str(self.ws),name,spec,'Synthetic report instructions')
  return 'task/'+name
 def doc(self,pid):return proto.read_json(proto.pid_path(str(self.ws),pid))
 def advance(self,fn):
  def tick():
   self.assertIsNone(self.owner.poll(),'owner exited');self.assertIsNone(self.agent.poll(),'agent exited')
   self.scheduler.tick();return fn()
  return until(tick)
 def control(self,pid,action):
  argv=[sys.executable,'-B',str(A/'agentctl.py'),'--root',str(self.ws),'control',pid,action,'--from','bot/fixture']
  r=subprocess.run(argv,capture_output=True,text=True,env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'),timeout=5);self.assertEqual(r.returncode,0,r.stderr);return r.stdout.strip()
 def tearDown(self):
  if self.agent.poll() is None:self.agent.terminate()
  self.agent.wait(timeout=3)
  if self.owner.poll() is None:
   snap=C.snapshot(self.protocol)
   for run in snap.get('runs',[]):
    if run['phase'] in ('running','starting','cleanupUnknown','canceling'):
     e=dict(apiVersion=C.API,requestId='cleanup-'+uuid.uuid4().hex,node='fixture',action='stop' if run['kind']=='Service' else 'cancel',kind=run['kind'],id=run['id'],attemptId=run['attemptId'],expectedGeneration=run['generation'])
     C.send(self.protocol,e);until(lambda:(r if (r:=C.receipt(self.protocol,e['requestId'])) and r['status']=='done' else None))
   self.owner.terminate()
  self.owner.wait(timeout=3)
  for p in self.children:
   if p.poll() is None:p.terminate()
   p.wait(timeout=3)
  for f in self.logs:f.close()
 def test_single_tilde_uses_declared_home(self):
  name='home';pid='task/'+name
  spec=dict(command='exec '+sys.executable+' "$AGENTD_CODE/probe.py"',workdir='~',creator='fixture',host='fixture',createdByHost='fixture',createdAt=proto.now_ts(),restartPolicy='one-shot')
  registration.register_task(str(self.ws),name,spec,'Synthetic home fixture')
  self.advance(lambda:(d if (d:=self.doc(pid)) and d.get('final') else None))
  self.assertEqual(self.doc(pid)['runtime']['phase'],'succeeded')
  run=next(r for r in C.snapshot(self.protocol)['runs'] if r['id']==self.doc(pid)['runtime']['id']);self.assertEqual(run['cwd'],self.config['home'])

 def test_explicit_versioned_declaration_retains_prior_history(self):
  import copy
  pid=self.register('versioned',hold=True);self.advance(lambda:(d if (d:=self.doc(pid)) and d.get('status')=='running' else None))
  self.agent.terminate();self.agent.wait(timeout=3)
  before=self.doc(pid);statefile=self.ws/'run/rund-agent'/(before['runtime']['id']+'.json');state=json.loads(statefile.read_text());old=state['declaration'];new=copy.deepcopy(old);new['metadata']['version']+=1;new['spec']['context']['cwd']=self.config['home']
  e=dict(apiVersion=C.API,node='fixture',action='submit',declaration=new,declarationDigest=C.digest(C.canonical(new)))
  from experimental.agent_exec_adapter.adapter import fingerprint
  e['requestId']='af-'+fingerprint([pid,'admit',e])[:48]
  state['priorDeclarations']=[old];state['declaration']=new;state['operations']['prior-admit']=state['operations']['admit'];state['operations']['admit']=e;statefile.write_text(json.dumps(state));C.send(self.protocol,e)
  until(lambda:(r if (r:=C.receipt(self.protocol,e['requestId'])) and r['status']=='done' else None))
  self.agent=self.start_agent();self.advance(lambda:(d if (d:=self.doc(pid)) and d.get('gen')==2 and d.get('status')=='running' else None))
  self.assertNotEqual(self.doc(pid)['pid'],before['pid']);self.assertIn('"gen": 1',(self.ws/'agents/task/versioned/pid.log').read_text())
  current=next(r for r in C.snapshot(self.protocol)['runs'] if r['id']==before['runtime']['id']);self.assertEqual(current['cwd'],self.config['home'])

 def test_queued_stop_has_no_payload_effect(self):
  pid=self.register('queued',hold=True);rid=self.control(pid,'stop')
  doc=until(lambda:(d if (d:=self.doc(pid)) and d.get('final') else None))
  self.assertEqual((doc['gen'],doc['pid'],doc['status']),(0,0,'killed'))
  self.assertFalse((self.ws/'agents/task/queued/ready.json').exists())
  self.assertEqual(proto.read_json(str(self.ws/'agents/task/queued/control/ack'/rid))['outcome'],'applied')

 def test_dynamic_registration_scheduler_report_and_dependency(self):
  first=self.register('producer',provides=['native-provider']);second=self.register('consumer',needs=['native-provider'])
  self.advance(lambda:(d if (d:=self.doc(first)) and d.get('final') else None))
  self.advance(lambda:(d if (d:=self.doc(second)) and d.get('final') else None))
  for pid in (first,second):
   d=self.doc(pid);self.assertEqual((d['status'],d['exitcode'],d['gen']),('exited',0,1));self.assertEqual(d['runtime']['phase'],'succeeded');self.assertTrue(Path(proto.agent_dir(str(self.ws),pid),'report.md').is_file())
  self.advance(lambda:len(list((self.ws/'agents/queue/dispatcher/inbox').glob('*.msg')))>=2)
 def test_client_restart_keeps_owner_wait_and_exact_stop(self):
  pid=self.register('resident',hold=True)
  self.advance(lambda:(d if (d:=self.doc(pid)) and d.get('status')=='running' else None));until(lambda:(self.ws/'agents/task/resident/ready.json').exists());before=self.doc(pid)
  self.agent.terminate();self.agent.wait(timeout=3)
  os.kill(before['pid'],0)
  self.agent=self.start_agent();self.advance(lambda:(d if (d:=self.doc(pid)) and d.get('pid')==before['pid'] else None))
  rid=self.control(pid,'stop')
  self.advance(lambda:(d if (d:=self.doc(pid)) and d.get('final') else None))
  until(lambda:(self.ws/'agents/task/resident/control/ack'/rid).exists())
  self.assertEqual(self.doc(pid)['status'],'killed')
  run=next(r for r in C.snapshot(self.protocol)['runs'] if r['id']==self.doc(pid)['runtime']['id']);self.assertTrue(run['waitObserved']);self.assertEqual(run['exitCode'],0)

 def test_resident_restart_and_clear_preserve_then_backup_history(self):
  pid=self.register('history',hold=True)
  self.advance(lambda:(d if (d:=self.doc(pid)) and d.get('status')=='running' else None))
  home=Path(proto.agent_dir(str(self.ws),pid));until(lambda:(home/'ready.json').exists())
  session=home/'session/session.jsonl';session.parent.mkdir(exist_ok=True);session.write_bytes(b'original history\n')
  before=self.doc(pid);rid=self.control(pid,'restart')
  self.advance(lambda:(d if (d:=self.doc(pid)) and d.get('gen')==2 and (home/'control/ack'/rid).exists() else None))
  self.assertEqual(session.read_bytes(),b'original history\n');self.assertNotEqual(self.doc(pid)['pid'],before['pid'])
  until(lambda:json.loads((home/'ready.json').read_text())['pid']==self.doc(pid)['pid'])
  rid=self.control(pid,'clear')
  self.advance(lambda:(d if (d:=self.doc(pid)) and d.get('gen')==3 and (home/'control/ack'/rid).exists() else None))
  self.assertEqual(session.read_bytes(),b'');self.assertTrue(any(p.read_bytes()==b'original history\n' for p in (self.ws/'run/backup').glob('*.jsonl')))
  until(lambda:json.loads((home/'ready.json').read_text())['pid']==self.doc(pid)['pid'])
  rid=self.control(pid,'stop');self.advance(lambda:(d if (d:=self.doc(pid)) and d.get('final') else None))

 def test_legacy_cutover_preserves_agent_generation(self):
  from runner import Runner,acquire_single_instance_lock
  from runtime.kernel import proc,group_members
  self.agent.terminate();self.agent.wait(timeout=3)
  pid=self.register('legacy',hold=True)
  legacy_code=self.ws/'agent-platform/agentd';legacy_code.mkdir(parents=True);(legacy_code/'probe.py').write_bytes((self.repo/'probe.py').read_bytes())
  lock,_=acquire_single_instance_lock(str(self.ws),'fixture')
  old=Runner(str(self.ws),'fixture','',.05)
  try:
   self.scheduler.tick();self.assertTrue(Path(proto.enable_path(str(self.ws),pid)).exists())
   doc=old.spawn(pid,old.read_spec(pid),5);until(lambda:(self.ws/'agents/task/legacy/ready.json').exists())
   identity=proc(doc['pid']);self.assertTrue(old.kill_proc(pid,doc));self.assertFalse(group_members(identity))
   proof=self.area/'legacy-cutover.json';proof.write_text(json.dumps({pid:dict(gen=doc['gen'],pid=doc['pid'],procStart=doc.get('procStart'),groupEmpty=True)}))
   self.config['legacyCutoverFile']=str(proof)
   self.configfile.write_text(json.dumps(self.config))
  finally:lock.close()
  self.agent=self.start_agent()
  self.advance(lambda:(d if (d:=self.doc(pid)) and d.get('gen')==6 and d.get('status')=='running' else None))
  home=Path(proto.agent_dir(str(self.ws),pid));until(lambda:json.loads((home/'ready.json').read_text())['pid']==self.doc(pid)['pid'])
  self.assertEqual(self.doc(pid)['runtime']['generation'],1)
  rid=self.control(pid,'restart');self.advance(lambda:(d if (d:=self.doc(pid)) and d.get('gen')==7 and (home/'control/ack'/rid).exists() else None))
  self.assertEqual(self.doc(pid)['runtime']['generation'],2)

if __name__=='__main__':unittest.main(verbosity=2)
