"""No task-tab wait guesses or persistent observation side channel.

Keep the real TS ask producer, RPC wrapper, widget/HTML and socket regressions.
Existing session JSONL calls/results cannot establish a current blocking wait.
Mutable fixtures and sockets live only below this invocation's fresh temp base.
"""
import datetime
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import proto
sys.path.insert(0, str(HERE.parent / 'pi-wrap'))
from rpc_dialogs import DialogRequests
from safe_delete import safe_rmtree

spec = importlib.util.spec_from_file_location('wait_wrap', HERE / 'pi-rpc-wrap.py')
wrapmod = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(HERE.parent / 'pi-wrap'))
spec.loader.exec_module(wrapmod)

# The trace is fixture-only: a copy of existing RPC events, not a new production
# session protocol or source used by the report.
FAKE = r'''#!/usr/bin/env python3
import json,os,subprocess,sys,threading,time
lock=threading.Lock()
def emit(obj):
 with lock:
  line=json.dumps(obj)
  with open(os.environ['AGENT_HOME']+'/fixture-rpc.jsonl','a') as f: f.write(line+'\n')
  print(line,flush=True)
def finish():
 emit({'type':'tool_execution_end','toolCallId':'dialog-tool','toolName':'ask_user'})
 emit({'type':'agent_end'})
 emit({'type':'agent_settled'})
def ask():
 cmd=['node',os.environ['ASK_WAIT_DRIVER'],os.environ['AGENT_ROOT']]
 if os.environ.get('WAIT_TEST_MODE')=='timeout': cmd.append('timeout')
 p=subprocess.Popen(cmd,stdout=subprocess.PIPE,text=True)
 for line in p.stdout: emit(json.loads(line))
 if p.wait(): os._exit(2)
 emit({'type':'agent_end'})
 emit({'type':'agent_settled'})
for line in sys.stdin:
 cmd=json.loads(line)
 if cmd['type']=='prompt':
  emit({'type':'response','command':'prompt','id':cmd['id'],'success':True})
  emit({'type':'agent_start'})
  if os.environ.get('WAIT_TEST_MODE','').startswith('dialog'):
   emit({'type':'tool_execution_start','toolCallId':'dialog-tool','toolName':'ask_user'})
   request={'type':'extension_ui_request','method':'select','id':'current-dialog','title':'Choose live option','options':['yes','no']}
   if os.environ['WAIT_TEST_MODE']=='dialog-timeout': request['timeout']=1000
   emit(request)
   if os.environ['WAIT_TEST_MODE']=='dialog-timeout': threading.Timer(3,finish).start()
  else: threading.Thread(target=ask).start()
 elif cmd['type']=='extension_ui_response':
  with open(os.path.join(os.environ['AGENT_HOME'],'dialog-response.json'),'w') as f: json.dump(cmd,f)
  finish()
'''


class WaitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = Path(tempfile.mkdtemp(prefix='task-tab-wait-'))
        print('isolated base:', cls.base, flush=True)

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(dir=self.base))
        self.addCleanup(lambda: safe_rmtree(self.root, authorized_root=self.base))
        target = self.root / 'agentd'
        target.mkdir(parents=True)
        for name in ('report.py', 'session_tools.py', 'notifications.py', 'proto.py', 'task_state.py', 'tasks.md', 'agentctl.py', 'registration.py', 'execution.py'):
            shutil.copyfile(HERE / name, target / name)
        shutil.copyfile(HERE / 'platform_paths.py', target / 'platform_paths.py')
        self.home = self.root / 'agents/task/wait-fixture'
        (self.home / 'inbox').mkdir(parents=True)
        (self.root / 'agents/queue/dispatcher/inbox').mkdir(parents=True)
        self.put('spec.json', {'name': 'fixture', 'host': 'fixture-host', 'reaper': 'queue/dispatcher', 'command': 'exec python3 pi-rpc-wrap.py', 'resources': []})
        runtime=str(HERE.parent/'rund')
        import socket
        (self.root/'config').mkdir();(self.root/'config/host-id').write_text(socket.gethostname()+' fixture-host\n')
        proto.atomic_write_json(str(self.root/'run/execution.json'),dict(runtimeSource=runtime,runtimeRoot=str(self.root),nodes={}))
        proto.atomic_write_json(str(self.home/'execution.json'),{'id':'fixture'})
        proto.atomic_write_json(str(self.root/'shared/tasks/fixture/declaration-v1.json'),dict(kind='Task',id='fixture',node='fixture-host',desired='run',argv=['pi-rpc-wrap.py'],needs=[],cwd=str(self.root),env={}))
        self.pid = dict(pid=os.getpid(), procStart=proto.proc_starttime(os.getpid()), gen=1,
                        status='running', final=False, lastAliveAt=proto.now_ts(), startedAt=proto.now_ts())
        self.put('pid.json', self.pid)
        self.env = dict(os.environ, PLATFORM_WORKSPACE=str(self.root), AGENT_ROOT=str(self.root),
                        AGENT_SELF='task/wait-fixture', AGENT_HOME=str(self.home),
                        PATH=str(Path(sys.executable).parent) + os.pathsep + os.environ['PATH'])

    def put(self, file, doc):
        if file == 'pid.json':
            from datetime import datetime,timezone
            stamp=datetime.now(timezone.utc).isoformat()
            phase='succeeded' if doc.get('final') else 'running'
            state=dict(id='fixture',phase=phase,startedAt=stamp,updatedAt=stamp,finishedAt=stamp if doc.get('final') else None,exitCode=doc.get('exitcode'),process={'pid':doc['pid'],'start':doc['procStart']})
            proto.atomic_write_json(str(self.root/'shared/tasks/fixture/state.json'),state)
        else:proto.atomic_write_json(str(self.home / file), doc)

    def page(self):
        page = (self.root / 'agentd/tasks.md').read_text()
        command = next(c for c in re.findall(r'<cmd:([^>]+)>', page) if c.endswith('-s active'))
        result = subprocess.run(['bash', '-c', command], cwd=self.root, env=self.env,
                                text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def assert_no_hint(self, text=None):
        text = self.page() if text is None else text
        for label in ('待答复', '待用户答复', '已等待', '**❓', '--type reply --ref'):
            self.assertNotIn(label, text)
        return text

    def session(self, tool='ask_dispatcher', result=None, timeout=0):
        # Native session format: a toolCall is a model proposal, not a wait
        # entry. Missing toolResult does not prove execute is still blocking.
        directory = self.home / 'session'
        directory.mkdir(exist_ok=True)
        stamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        rows = [dict(type='session', version=3, id='session-fixture', timestamp=stamp, cwd=str(self.root)),
                dict(type='message', id='call-entry', parentId=None, timestamp=stamp,
                     message=dict(role='assistant', stopReason='toolUse', content=[dict(type='toolCall', id='call', name=tool,
                         arguments=dict(question='Which option? | [one] <script> 待用户答复', timeoutMinutes=timeout, type='ask'))]))]
        if result is not None:
            rows.append(dict(type='message', id='result-entry', parentId='call-entry', timestamp=stamp,
                             message=dict(role='toolResult', toolCallId='call', toolName=tool,
                                          content=[dict(type='text', text=result)], isError=False)))
        (directory / 'session.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in rows))






    def test_dialog_memory_baseline_no_file_ask_observer(self):
        state = DialogRequests()
        state.event(dict(type='tool_execution_start', toolCallId='ask', toolName='ask_dispatcher'))
        state.event(dict(type='tool_execution_update', toolCallId='ask', toolName='ask_dispatcher',
                         partialResult={'details':{'agentdWait':{'askId':'old','question':'legacy update'}}}))
        self.assertEqual(state.waits, {})
        for method in ('select', 'confirm', 'input', 'editor'):
            with self.subTest(method=method):
                state.event(dict(type='extension_ui_request', method=method, id='d', title='Live'))
                self.assertEqual(state.dialogs()[0]['id'], 'd')
                state.sent(dict(type='abort'))  # no evidence of native Promise settlement
                self.assertTrue(state.dialogs())
                state.sent(dict(type='extension_ui_response', id='d', cancelled=True))
                self.assertEqual(state.dialogs(), [])
        state.event(dict(type='extension_ui_request', method='notify', id='notice', message='not a wait'))
        self.assertEqual(state.dialogs(), [])
        state.event(dict(type='extension_ui_request', method='input', id='expired', timeout=1))
        time.sleep(.01)
        self.assertEqual(state.dialogs(), [])
        self.assertFalse((self.home / 'wait-state.json').exists())

    def test_replay_current_dialog_not_history_even_after_ring_eviction(self):
        with patch.dict(os.environ, self.env): w = wrapmod.Wrap()
        obj = dict(type='extension_ui_request', id='dialog', method='input', title='Current', timeout=10000)
        w._on_event(obj)
        w.ring.append((json.dumps(obj)+'\n').encode())
        replay = [json.loads(b) for b in w.replay_lines()]
        self.assertEqual(len(replay), 1)
        self.assertLess(replay[0]['timeout'], 10000)
        w.ring.clear()
        self.assertEqual(len(list(w.replay_lines())), 1)
        w.dialog_requests.sent(dict(type='extension_ui_response', id='dialog', value='done'))
        w.ring.append((json.dumps(obj)+'\n').encode())
        self.assertEqual(list(w.replay_lines()), [])
        self.assertFalse((self.home / 'wait-state.json').exists())

    def test_tool_end_and_turn_settlement_keep_dialog_replay_semantics(self):
        state = DialogRequests()
        state.event(dict(type='agent_start'))
        state.event(dict(type='agent_start'))
        state.event(dict(type='tool_execution_start', toolCallId='tool'))
        state.event(dict(type='extension_ui_request', id='d', method='input', title='Live'))
        state.event(dict(type='agent_settled'))
        state.event(dict(type='agent_end'))
        self.assertTrue(state.dialogs())
        state.event(dict(type='tool_execution_end', toolCallId='other-tool'))
        self.assertTrue(state.dialogs())
        state.event(dict(type='tool_execution_end', toolCallId='tool'))
        self.assertFalse(state.dialogs())
        state.event(dict(type='extension_ui_request', id='d2', method='input'))
        state.event(dict(type='agent_end'))
        self.assertFalse(state.dialogs())

    def run_wrapper(self, mode, source=FAKE):
        if not mode.startswith('dialog') and not os.environ.get('ASK_WAIT_DRIVER'):
            self.skipTest('set ASK_WAIT_DRIVER for the actual TS producer')
        fake = self.root / 'fake-pi'
        fake.write_text(source.replace('#!/usr/bin/env python3', '#!' + sys.executable))
        fake.chmod(0o700)
        (self.home / 'prompt.md').write_text('fixture prompt')
        env = dict(self.env, AGENTD_WRAP_PI_BIN=str(fake), AGENTD_WRAP_SETTLE_WINDOW='0.05',
                   AGENTD_WRAP_ARM_TIMEOUT='0.05', WAIT_TEST_MODE=mode)
        err = open(self.root / 'wrapper.log', 'w')
        p = subprocess.Popen([sys.executable, str(HERE / 'pi-rpc-wrap.py')], env=env, stderr=err)
        def cleanup():
            if p.poll() is None: p.terminate()
            try: p.wait(timeout=5)
            except subprocess.TimeoutExpired: p.kill(); p.wait()
            err.close()
        self.addCleanup(cleanup)
        self.pid.update(pid=p.pid, procStart=proto.proc_starttime(p.pid))
        self.put('pid.json', self.pid)
        return p

    def until(self, predicate):
        limit = time.monotonic() + 10
        while time.monotonic() < limit:
            if predicate(): return
            time.sleep(.02)
        self.fail('isolated RPC condition timed out: ' + (self.root / 'wrapper.log').read_text())

    def trace(self):
        try: return [json.loads(l) for l in (self.home / 'fixture-rpc.jsonl').read_text().splitlines()]
        except (OSError, ValueError): return []

    def test_real_producer_exact_ref_reply_preserves_old_nonblocking_ask(self):
        old_id, _ = proto.write_message(str(self.root / 'agents/queue/dispatcher/inbox'),
            dict(type='ask', **{'from':'task/wait-fixture'}, via='send_message', body=json.dumps({'question':'Older nonblocking question'})))
        p = self.run_wrapper('answer')
        self.until(lambda: len(proto.list_task_asks(str(self.root), 'task/wait-fixture')) == 2)
        self.assert_no_hint()
        asks = proto.list_task_asks(str(self.root), 'task/wait-fixture')
        current = next(a for a in asks if a['id'] != old_id)
        answer = self.root / 'answer.txt'
        answer.write_text('fixture answer')
        result = subprocess.run([sys.executable, 'agentd/agentctl.py', 'send', 'task/wait-fixture',
            '--type','reply','--ref',current['id'],'--body-file',str(answer)], cwd=self.root,
            env=dict(self.env, AGENT_SELF='queue/dispatcher'), text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(p.wait(timeout=10), 0, (self.root / 'wrapper.log').read_text())
        self.assertEqual(proto.find_pending_ask(str(self.root), 'task/wait-fixture')['id'], old_id)
        replies = [m for m in proto.list_messages(str(self.home / 'inbox')) if m.get('type')=='reply']
        self.assertEqual([m['ref'] for m in replies], [current['id']])
        self.assertFalse(any(e.get('type')=='tool_execution_update' for e in self.trace()))
        self.assertFalse((self.home / 'wait-state.json').exists())
        self.assert_no_hint()

    def test_real_wrapper_file_ask_timeout(self):
        p = self.run_wrapper('timeout')
        self.assertEqual(p.wait(timeout=10), 0, (self.root / 'wrapper.log').read_text())
        self.assertEqual(len(proto.list_task_asks(str(self.root), 'task/wait-fixture')), 1)
        self.assertFalse((self.home / 'wait-state.json').exists())
        self.assert_no_hint()

    def dialog_client(self):
        import socket
        self.until(lambda: any(e.get('type')=='extension_ui_request' for e in self.trace()))
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.addCleanup(client.close)
        client.settimeout(5)
        client.connect(proto.task_sock_path(str(self.root), 'wait-fixture'))
        stream = client.makefile('rb')
        self.addCleanup(stream.close)
        while True:
            obj = json.loads(stream.readline())
            if obj.get('type')=='extension_ui_request': break
        self.assertEqual(obj['id'], 'current-dialog')
        return client

    def check_dialog_response(self, cancelled):
        p = self.run_wrapper('dialog')
        client = self.dialog_client()
        self.assert_no_hint()
        client.sendall((json.dumps(dict(type='extension_ui_response', id='current-dialog', value='yes', cancelled=cancelled))+'\n').encode())
        self.assertEqual(p.wait(timeout=10), 0)
        response = json.loads((self.home / 'dialog-response.json').read_text())
        self.assertIs(response['cancelled'], cancelled)
        self.assertFalse((self.home / 'wait-state.json').exists())
        self.assert_no_hint()

    def test_real_wrapper_dialog_attach_reply(self):
        self.check_dialog_response(False)

    def test_real_wrapper_dialog_cancelled_response(self):
        self.check_dialog_response(True)

    def test_real_wrapper_deadline_before_tool_end_and_generation(self):
        p = self.run_wrapper('dialog-timeout')
        self.until(lambda: any(e.get('type')=='extension_ui_request' for e in self.trace()))
        self.assert_no_hint()
        time.sleep(1.1)
        self.assertIsNone(p.poll())
        self.assertFalse(any(e.get('type')=='tool_execution_end' for e in self.trace()))
        self.assert_no_hint()
        self.pid.update(gen=2, procStart=None)
        self.put('pid.json', self.pid)
        self.assert_no_hint()
        self.assertEqual(p.wait(timeout=10), 0)
        self.assertFalse((self.home / 'wait-state.json').exists())

    def test_native_signal_resolved_with_tool_still_running_no_hint(self):
        # Execute the installed native helper unchanged. This tests the report's
        # no-guess policy, NOT a fix to the native RPC/replay settlement gap.
        cli = shutil.which('pi')
        if not cli: self.skipTest('pi native source unavailable')
        native = Path(cli).resolve().parents[1] / 'dist/modes/rpc/rpc-mode.js'
        if not native.exists(): self.skipTest('pi native source unavailable')
        source = '#!' + shutil.which('node') + '\n' + r'''
const fs=require('node:fs'),crypto=require('node:crypto'),readline=require('node:readline');
const emit=o=>console.log(JSON.stringify(o));
const pendingExtensionRequests=new Map();
const source=fs.readFileSync(NATIVE,'utf8');
const start=source.indexOf('    function createDialogPromise(');
const end=source.indexOf('\n    /**',start);
const createDialogPromise=new Function('crypto','output','pendingExtensionRequests',
 'return ('+source.slice(start,end).trim()+')')(crypto,emit,pendingExtensionRequests);
readline.createInterface({input:process.stdin}).on('line',async line=>{
 const cmd=JSON.parse(line);
 if(cmd.type==='prompt'){
  emit({type:'response',command:'prompt',id:cmd.id,success:true});
  emit({type:'agent_start'});
  emit({type:'tool_execution_start',toolCallId:'native-tool',toolName:'native-fixture'});
  const controller=new AbortController();
  setTimeout(()=>controller.abort(),300);
  await createDialogPromise({signal:controller.signal},undefined,
    {method:'input',title:'Signal cancelled native dialog'},r=>r.value);
  fs.writeFileSync(process.env.AGENT_HOME+'/native-resolved.json',JSON.stringify({resolved:true,pending:pendingExtensionRequests.size}));
  // No tool end: continue non-dialog work after the real Promise resolves.
 }
});
'''.replace('NATIVE', json.dumps(str(native)))
        p = self.run_wrapper('dialog-signal', source)
        self.until(lambda: (self.home / 'native-resolved.json').exists())
        self.assertEqual(json.loads((self.home / 'native-resolved.json').read_text()), {'resolved':True,'pending':0})
        self.assertIsNone(p.poll())
        self.assert_no_hint()
        self.assertFalse((self.home / 'wait-state.json').exists())


if __name__ == '__main__':
    unittest.main(verbosity=2)
