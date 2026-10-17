"""Current waits through real wrapper RPC -> actual tasks.md widget -> HTML.

Run with AGENTD_TEST_WRAP=<frozen wrapper source>/pi-rpc-wrap.py,
ASK_WAIT_DRIVER=<caller's ask-wait.test.mjs> and TASK_MARKED_JS=<caller's
marked.min.js> to include those integrations. Wrapper imports this candidate's
single protocol via AGENTD_DIR, never a unified-repo fallback.
Every mutable root is created below a new temporary base; no live sockets.
"""
import importlib.util
import json
import os
from pathlib import Path
import re
import shlex
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
import report
from wait_state import CurrentWaits, current_waits
from safe_delete import safe_rmtree

WRAPPER = Path(os.environ['AGENTD_TEST_WRAP']).resolve() if os.environ.get('AGENTD_TEST_WRAP') else None
wrapmod = None
if WRAPPER is not None:
    spec = importlib.util.spec_from_file_location('wait_wrap', WRAPPER)
    wrapmod = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(WRAPPER.parent))
    with patch.dict(os.environ, AGENTD_DIR=str(HERE)):
        spec.loader.exec_module(wrapmod)
    assert Path(wrapmod.proto.__file__).resolve() == HERE / 'proto.py'
    assert wrapmod.CurrentWaits is CurrentWaits, 'wrapper must use the candidate protocol'

# A real subprocess speaking RPC, without a model or browser. File asks invoke
# the real TS extension driver; dialogs are native RPC requests with responses.
FAKE = r'''#!/usr/bin/env python3
import json,os,subprocess,sys,threading,time
lock=threading.Lock()
def emit(obj):
 with lock:
  print(json.dumps(obj),flush=True)
def finish():
 emit({'type':'tool_execution_end','toolCallId':'dialog-tool','toolName':'ask_user'})
 emit({'type':'agent_end'})
 emit({'type':'agent_settled'})
def ask():
 cmd=['node',os.environ['ASK_WAIT_DRIVER'],os.environ['AGENT_ROOT']]
 if os.environ.get('WAIT_TEST_MODE')=='timeout': cmd.append('timeout')
 p=subprocess.Popen(cmd,stdout=subprocess.PIPE,text=True)
 for line in p.stdout:
  with lock: print(line.strip(),flush=True)
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
   if os.environ['WAIT_TEST_MODE']=='dialog-timeout': request['timeout']=2000
   emit(request)
   # Leave a gap after the native deadline, before tool completion, so the
   # widget must use the deadline rather than rely only on tool_end cleanup.
   if os.environ['WAIT_TEST_MODE']=='dialog-timeout': threading.Timer(4,finish).start()
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
        target = self.root / 'candidate source with spaces/agentd'
        target.mkdir(parents=True)
        for name in ('report.py', 'wait_state.py', 'proto.py', 'scheduling.py', 'task_state.py', 'task_labels.py', 'tasks.md', 'agentctl.py', 'registration.py'):
            shutil.copyfile(HERE / name, target / name)
        shutil.copyfile(HERE / 'platform_paths.py', target / 'platform_paths.py')
        self.home = self.root / 'agents/task/wait-fixture'
        (self.home / 'inbox').mkdir(parents=True)
        (self.root / 'agents/queue/dispatcher/inbox').mkdir(parents=True)
        self.put('spec.json', {'name': 'fixture', 'host': 'fixture-host', 'reaper': 'queue/dispatcher', 'command': 'exec python3 pi-rpc-wrap.py', 'resources': []})
        self.put('enable.json', {})
        self.pid = dict(pid=os.getpid(), procStart=proto.proc_starttime(os.getpid()), gen=1,
                        status='running', final=False, lastAliveAt=proto.now_ts(), startedAt=proto.now_ts())
        self.put('pid.json', self.pid)
        self.state = CurrentWaits(str(self.home), 'task/wait-fixture')
        self.env = dict(os.environ, PLATFORM_WORKSPACE=str(self.root), AGENT_ROOT=str(self.root),
                        AGENTD_DIR=str(HERE), src=str(target / 'tasks.md'),
                        AGENT_SELF='task/wait-fixture', AGENT_HOME=str(self.home),
                        PATH=str(Path(sys.executable).parent) + os.pathsep + os.environ['PATH'])

    def put(self, file, doc):
        proto.atomic_write_json(str(self.home / file), doc)

    def page(self):
        # Execute precisely the active widget command from the real page, with
        # identical copied source and a caller-supplied isolated workspace.
        page = Path(self.env['src']).read_text()
        command = next(c for c in re.findall(r'\$\{([^}]+)\}', page) if c.endswith('-s active'))
        result = subprocess.run(['bash', '-c', command], cwd=self.root, env=self.env,
                                text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout

    def start_ask(self, deadline=None, since=None):
        self.state.event(dict(type='tool_execution_start', toolCallId='ask-tool', toolName='ask_dispatcher'))
        self.state.event(dict(type='tool_execution_update', toolCallId='ask-tool', toolName='ask_dispatcher',
             partialResult={'details': {'agentdWait': dict(askId='current-ask', question='Which option? | [a] <script>',
             recipient='queue/dispatcher', since=time.time()-65 if since is None else since, deadline=deadline)}}))

    def test_file_wait_and_exact_reply_entrance(self):
        self.start_ask()
        text = self.page()
        self.assertIn('**❓ 待答复（协调者）**', text)
        self.assertIn('已等待 1m', text)
        self.assertIn('/fixture-host/agents/task/wait-fixture/spec.json?v=chat', text)
        self.assertIn('--ref current-ask', text)
        self.assertNotIn('待用户答复', text)
        self.assertIn('&lt;script&gt;', text)
        self.assertIn('\\|', text)

    def test_report_root_reply_entrance(self):
        # B1: report and reply must agree even with unrelated cwd, no inherited
        # workspace (or conflicting environments), and same-task decoys. Use the
        # real report CLI and execute only its displayed command, not a helper.
        for mode in ('explicit-clean', 'explicit-conflict', 'default'):
            with self.subTest(mode=mode):
                base = self.root / mode
                repo = base / 'checkout with spaces/agentd'
                shutil.copytree(self.root / 'candidate source with spaces/agentd', repo)
                default = repo.parent
                external = base / "selected state's with spaces"
                envroot = base / 'environment decoy with spaces'
                selected = default if mode == 'default' else external
                participant = 'task/root-fixture'
                ref = mode + '-root-specific-ask'
                for root in (default, external, envroot):
                    home = root / 'agents' / participant
                    (home / 'inbox').mkdir(parents=True)
                    (root / 'agents/queue/dispatcher/inbox').mkdir(parents=True)
                    proto.atomic_write_json(str(home / 'spec.json'), dict(
                        name='root fixture', host='fixture-host', reaper='queue/dispatcher',
                        resources=[], command='true'))
                    proto.atomic_write_json(str(home / 'pid.json'), self.pid)
                    proto.atomic_write_json(str(home / 'enable.json'), {})
                home = selected / 'agents' / participant
                state = CurrentWaits(str(home), participant)
                state.event(dict(type='tool_execution_start', toolCallId='ask', toolName='ask_dispatcher'))
                state.event(dict(type='tool_execution_update', toolCallId='ask', toolName='ask_dispatcher',
                    partialResult={'details': {'agentdWait': dict(askId=ref, question='Which root?',
                        recipient='queue/dispatcher', since=time.time())}}))
                cwd = base / 'unrelated cwd'
                cwd.mkdir()
                isolated_home = base / 'home'
                isolated_home.mkdir()
                env = dict(HOME=str(isolated_home), LANG='C.UTF-8',
                           PATH=str(Path(sys.executable).parent) + os.pathsep + '/usr/bin:/bin',
                           PYTHONDONTWRITEBYTECODE='1')
                if mode == 'explicit-conflict':
                    env['PLATFORM_WORKSPACE'] = str(envroot)
                command = [sys.executable, '-I', '-S', '-B', str(repo / 'report.py')]
                if mode != 'default':
                    # A relative root must become independent of the reply cwd.
                    command += ['--root', os.path.relpath(selected, cwd)]
                command += ['-s', 'active']
                page = subprocess.run(command, cwd=cwd, env=env, text=True,
                                      capture_output=True, timeout=10)
                self.assertEqual(page.returncode, 0, page.stderr)
                self.assertIn(ref, page.stdout)
                self.assertTrue(current_waits(str(selected), participant, self.pid, time.time()))
                displayed = re.search(r'协调者在 fixture-host 执行：`([^`]+)`', page.stdout).group(1)
                answer = base / 'answer with spaces.txt'
                answer.write_text('root fixture answer')
                reply_env = dict(env)
                # Even a default-root page must not depend on the answerer's env.
                if mode != 'explicit-clean':
                    reply_env['PLATFORM_WORKSPACE'] = str(envroot)
                reply = subprocess.run(['bash', '-c', displayed.replace('<答复文件>', shlex.quote(str(answer)))],
                    cwd=base, env=reply_env, text=True, capture_output=True, timeout=10)
                self.assertEqual(reply.returncode, 0, reply.stderr)
                for root in (default, external, envroot):
                    messages = proto.list_messages(str(root / 'agents' / participant / 'inbox'))
                    self.assertEqual([m.get('ref') for m in messages], [ref] if root == selected else [])
                    if root == selected:
                        self.assertEqual(messages[0]['body'], 'root fixture answer')
                self.assertFalse(current_waits(str(selected), participant, self.pid, time.time()))
                refreshed = subprocess.run(command, cwd=cwd, env=env, text=True,
                                           capture_output=True, timeout=10)
                self.assertEqual(refreshed.returncode, 0, refreshed.stderr)
                self.assertNotIn(ref, refreshed.stdout)
                self.assertNotIn('**❓', refreshed.stdout)
                print('root reply contract:', mode, displayed, flush=True)

    def test_reply_clears_before_tool_end_and_next_refresh(self):
        self.start_ask()
        proto.write_message(str(self.home / 'inbox'), dict(type='reply', ref='current-ask', body='done'))
        self.assertNotIn('**❓', self.page())

    def test_tool_return_timeout_and_terminal_clear(self):
        self.start_ask()
        self.state.event(dict(type='tool_execution_end', toolCallId='ask-tool'))
        self.assertNotIn('**❓', self.page())
        self.start_ask(deadline=time.time()-1)
        self.assertNotIn('**❓', self.page())
        self.start_ask()
        self.pid.update(final=True, status='exited', exitcode=0)
        self.put('pid.json', self.pid)
        self.assertNotIn('**❓', self.page())

    def test_no_wait_nonblocking_historical_and_old_gen(self):
        # Real pending envelopes, including a nonblocking send_message ask,
        # never suffice to imply a current tool wait.
        for via in (None, 'send_message'):
            msg = dict(type='ask', **{'from':'task/wait-fixture'}, body=json.dumps({'question':'old ask'}))
            if via: msg['via'] = via
            proto.write_message(str(self.root / 'agents/queue/dispatcher/inbox'), msg)
        self.assertNotIn('**❓', self.page())
        self.state.event(dict(type='tool_execution_start', toolCallId='send', toolName='send_message', args={'type':'ask'}))
        self.assertNotIn('**❓', self.page())
        self.start_ask()
        self.pid.update(gen=2, procStart=self.pid['procStart']+1)
        self.put('pid.json', self.pid)
        self.assertNotIn('**❓', self.page())

    def test_unrelated_settlement_does_not_erase_inflight_wait(self):
        self.state.event(dict(type='agent_start'))
        self.state.event(dict(type='agent_start'))
        self.start_ask()
        self.state.event(dict(type='agent_settled'))
        self.state.event(dict(type='agent_end'))
        self.assertIn('待答复（协调者）', self.page())
        self.state.event(dict(type='agent_end'))
        self.assertNotIn('**❓', self.page())

    def test_unreliable_time_not_invented_and_stale_heartbeat(self):
        self.start_ask(since='unknown')
        self.assertNotIn('已等待', self.page())
        self.pid['lastAliveAt'] = '2000-01-01-00-00-00.000'
        self.put('pid.json', self.pid)
        self.assertNotIn('**❓', self.page())

    def test_dialog_methods_response_timeout_abort_and_notice(self):
        for method in ('select', 'confirm', 'input', 'editor'):
            with self.subTest(method=method):
                self.state.event(dict(type='extension_ui_request', method=method, id='dialog', title='Live question'))
                text = self.page()
                self.assertIn('**❓ 待用户答复**', text)
                self.assertIn('[打开并答复](/fixture-host/agents/task/wait-fixture/spec.json?v=chat)', text)
                self.state.sent(dict(type='extension_ui_response', id='dialog', value='yes'))
                self.assertNotIn('**❓', self.page())
        self.state.event(dict(type='extension_ui_request', method='notify', id='notice', message='not a wait'))
        self.assertNotIn('**❓', self.page())
        self.state.event(dict(type='extension_ui_request', method='input', id='timeout', title='Old', timeout=1))
        time.sleep(.01)
        self.assertNotIn('**❓', self.page())
        self.state.event(dict(type='extension_ui_request', method='input', id='abort', title='Old'))
        self.state.sent(dict(type='abort'))
        self.assertIn('**❓', self.page())  # bare abort cannot resolve an un-signalled UI Promise
        self.state.sent(dict(type='extension_ui_response', id='abort', cancelled=True))
        self.assertNotIn('**❓', self.page())

    def test_replay_current_dialog_not_history_even_after_ring_eviction(self):
        if wrapmod is None: self.skipTest('set AGENTD_TEST_WRAP for wrapper integration')
        with patch.dict(os.environ, self.env): w = wrapmod.Wrap()
        self.state = w.waits
        obj = dict(type='extension_ui_request', id='dialog', method='input', title='Current', timeout=10000)
        w._on_event(obj)
        w.ring.append((json.dumps(obj)+'\n').encode())
        replay = [json.loads(b) for b in w.replay_lines()]
        self.assertEqual(len(replay), 1)
        self.assertLess(replay[0]['timeout'], 10000)
        w.ring.clear()  # live dialog still has a valid answer entrance
        self.assertEqual(len(list(w.replay_lines())), 1)
        w.waits.sent(dict(type='extension_ui_response', id='dialog', value='done'))
        w.ring.append((json.dumps(obj)+'\n').encode())
        self.assertEqual(list(w.replay_lines()), [])

    def test_dialog_terminal_and_generation_filter(self):
        self.state.event(dict(type='extension_ui_request', method='select', id='d', title='Live'))
        self.pid.update(final=True, status='exited', exitcode=0)
        self.put('pid.json', self.pid)
        self.assertNotIn('**❓', self.page())
        self.pid.update(final=False, status='running', gen=2, procStart=self.pid['procStart']+1)
        self.put('pid.json', self.pid)
        self.assertNotIn('**❓', self.page())

    def test_report_http_adapter_uses_current_snapshot(self):
        adapter_spec = importlib.util.spec_from_file_location('wait_adapter', HERE / 'agentd4web.py')
        adapter = importlib.util.module_from_spec(adapter_spec)
        adapter_spec.loader.exec_module(adapter)
        self.start_ask()
        with patch.dict(os.environ, self.env):
            body = adapter.interp(None)[1]
        self.assertIn('待答复（协调者）', body)
        self.assertIn('--ref current-ask', body)

    def test_actual_widget_html_with_site_marked(self):
        marked = os.environ.get('TASK_MARKED_JS')
        if not marked: self.skipTest('set TASK_MARKED_JS for the site renderer')
        self.start_ask()
        result = subprocess.run(['node', '-e', 'const fs=require("fs"),m=require(process.argv[1]);console.log(m.parse(fs.readFileSync(0,"utf8")))', marked], input=self.page(), text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('<strong>❓ 待答复（协调者）</strong>', result.stdout)
        self.assertIn('href="/fixture-host/agents/task/wait-fixture/spec.json?v=chat"', result.stdout)
        self.assertNotIn('<script>', result.stdout)
        self.assertEqual(result.stdout.count('<td>'), 10)

    def run_wrapper(self, mode):
        if WRAPPER is None: self.skipTest('set AGENTD_TEST_WRAP for wrapper integration')
        if not mode.startswith('dialog') and not os.environ.get('ASK_WAIT_DRIVER'):
            self.skipTest('set ASK_WAIT_DRIVER for the actual TS producer')
        fake = self.root / 'fake-pi'
        fake.write_text(FAKE.replace('#!/usr/bin/env python3', '#!' + sys.executable))
        fake.chmod(0o700)
        (self.home / 'prompt.md').write_text('fixture prompt')
        env = dict(self.env, AGENTD_WRAP_PI_BIN=str(fake), AGENTD_WRAP_SETTLE_WINDOW='0.05',
                   AGENTD_WRAP_ARM_TIMEOUT='0.05', WAIT_TEST_MODE=mode)
        err = open(self.root / 'wrapper.log', 'w')
        p = subprocess.Popen([sys.executable, str(WRAPPER)], env=env, stderr=err)
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

    def test_real_wrapper_file_ask_producer_answer(self):
        p = self.run_wrapper('answer')
        self.until(lambda: bool(current_waits(str(self.root), 'task/wait-fixture', self.pid, time.time())))
        text = self.page()
        self.assertIn('待答复（协调者）', text)
        asks = proto.list_task_asks(str(self.root), 'task/wait-fixture')
        self.assertEqual(len(asks), 1)
        self.assertIn('--ref ' + asks[0]['id'], text)
        # Execute the actual displayed entrance, not a hand-constructed reply.
        proto.write_message(str(self.root / 'agents/queue/dispatcher/inbox'),
             dict(type='ask', **{'from': 'task/wait-fixture'}, via='send_message', body='older unrelated ask'))
        command = re.search(r'协调者在 fixture-host 执行：`([^`]+)`', text).group(1)
        answer = self.root / 'answer with spaces.txt'
        answer.write_text('fixture answer')
        import shlex
        result = subprocess.run(['bash', '-c', command.replace('<答复文件>', shlex.quote(str(answer)))],
                                cwd=self.root, env=self.env, text=True, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(p.wait(timeout=10), 0, (self.root / 'wrapper.log').read_text())
        self.assertNotIn('**❓', self.page())
        self.assertTrue(any(m.get('via') == 'send_message' for m in
                            proto.list_task_asks(str(self.root), 'task/wait-fixture')))

    def test_widget_reply_command_targets_current_ask_not_oldest(self):
        # The displayed entrance must work, not merely contain a plausible ref.
        # An older nonblocking ask makes the generic `answer` shortcut unsafe.
        old_id, _ = proto.write_message(str(self.root / 'agents/queue/dispatcher/inbox'),
            dict(type='ask', **{'from': 'task/wait-fixture'}, via='send_message',
                 body=json.dumps({'question': 'Older nonblocking question'})))
        p = self.run_wrapper('answer')
        self.until(lambda: bool(current_waits(str(self.root), 'task/wait-fixture', self.pid, time.time())))
        text = self.page()
        command = next(c for c in re.findall(r'`([^`]+)`', text)
                       if '--type reply --ref ' in c)
        answer = self.root / 'answer.txt'
        answer.write_text('fixture answer')
        command = command.replace('<答复文件>', 'answer.txt')
        result = subprocess.run(['bash', '-c', command], cwd=self.root,
            env=dict(self.env, AGENT_SELF='queue/dispatcher'),
            text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        replies = [m for m in proto.list_messages(str(self.home / 'inbox')) if m.get('type') == 'reply']
        self.assertEqual(len(replies), 1)
        self.assertNotEqual(replies[0]['ref'], old_id)
        self.assertNotEqual(replies[0].get('via'), 'send_message')
        self.assertEqual(p.wait(timeout=10), 0, (self.root / 'wrapper.log').read_text())
        self.assertNotIn('**❓', self.page())
        # The unrelated unanswered ask still exists, without becoming a wait.
        self.assertEqual(proto.find_pending_ask(str(self.root), 'task/wait-fixture')['id'], old_id)

    def test_real_wrapper_file_ask_producer_timeout(self):
        p = self.run_wrapper('timeout')
        self.assertEqual(p.wait(timeout=10), 0, (self.root / 'wrapper.log').read_text())
        self.assertEqual(len(proto.list_task_asks(str(self.root), 'task/wait-fixture')), 1)
        self.assertNotIn('**❓', self.page())

    def dialog_client(self):
        import socket
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.addCleanup(client.close)
        client.settimeout(5)
        client.connect(proto.task_sock_path(str(self.root), 'wait-fixture'))
        stream = client.makefile('rb')
        self.addCleanup(stream.close)
        while True:
            obj = json.loads(stream.readline())
            if obj.get('type') == 'extension_ui_request': break
        self.assertEqual(obj['id'], 'current-dialog')
        return client

    def test_real_wrapper_no_browser_dialog_attach_and_reply(self):
        p = self.run_wrapper('dialog')
        self.until(lambda: bool(current_waits(str(self.root), 'task/wait-fixture', self.pid, time.time())))
        self.assertIn('待用户答复', self.page())
        client = self.dialog_client()
        client.sendall(b'{"type":"extension_ui_response","id":"current-dialog","value":"yes"}\n')
        self.assertEqual(p.wait(timeout=10), 0)
        self.assertNotIn('**❓', self.page())

    def test_real_wrapper_dialog_cancelled_response_clears_widget(self):
        p = self.run_wrapper('dialog')
        self.until(lambda: bool(current_waits(str(self.root), 'task/wait-fixture', self.pid, time.time())))
        self.assertIn('待用户答复', self.page())
        client = self.dialog_client()
        client.sendall(b'{"type":"extension_ui_response","id":"current-dialog","cancelled":true}\n')
        self.assertEqual(p.wait(timeout=10), 0)
        response = json.loads((self.home / 'dialog-response.json').read_text())
        self.assertIs(response['cancelled'], True)
        self.assertNotIn('**❓', self.page())

    def test_real_wrapper_dialog_deadline_clears_before_tool_end(self):
        p = self.run_wrapper('dialog-timeout')
        self.until(lambda: bool(current_waits(str(self.root), 'task/wait-fixture', self.pid, time.time())))
        self.assertIn('待用户答复', self.page())
        deadline = proto.read_json(str(self.home / 'wait-state.json'))['waits'][0]['deadline']
        time.sleep(max(0, deadline - time.time()) + .03)
        self.assertIsNone(p.poll())  # still blocked before the fixture's tool end
        self.assertNotIn('**❓', self.page())
        self.assertEqual(p.wait(timeout=10), 0)

    def test_real_wrapper_old_incarnation_cannot_supply_new_generation_wait(self):
        p = self.run_wrapper('dialog')
        self.until(lambda: bool(current_waits(str(self.root), 'task/wait-fixture', self.pid, time.time())))
        self.assertIn('待用户答复', self.page())
        self.pid.update(gen=2, procStart=self.pid['procStart'] + 1)
        self.put('pid.json', self.pid)
        self.assertIsNone(p.poll())  # old producer still live, not just a stale file
        self.assertNotIn('**❓', self.page())
        client = self.dialog_client()
        client.sendall(b'{"type":"extension_ui_response","id":"current-dialog","cancelled":true}\n')
        self.assertEqual(p.wait(timeout=10), 0)
        self.assertNotIn('**❓', self.page())


if __name__ == '__main__':
    unittest.main(verbosity=2)
