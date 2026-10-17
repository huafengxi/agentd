"""Bounded native-session observation, independent of lifecycle or wait inference."""
import datetime
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import report
from safe_delete import safe_rmtree


def iso(epoch):
    return datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).isoformat()


class CurrentToolsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = Path(tempfile.mkdtemp(prefix='current-tools-'))

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(dir=self.base))
        self.addCleanup(lambda: safe_rmtree(self.root, authorized_root=self.base))
        self.home = self.root / 'agents/task/fixture'
        (self.home / 'session').mkdir(parents=True)
        self.file = self.home / 'session/session.jsonl'
        self.now = 1800000120
        self.start = self.now - 60
        self.pid = dict(pid=os.getpid(), procStart=123, gen=1, final=False,
                        status='running', startedAt=iso(self.start))
        self.spec = dict(name='fixture', host='fixture', command='exec python3 pi-rpc-wrap.py', resources=[])
        self.put('spec.json', self.spec)
        self.put('pid.json', self.pid)

    def put(self, name, value):
        (self.home / name).write_text(json.dumps(value))

    def entry(self, eid, parent, message=None, age=30, kind='message'):
        row = dict(type=kind, id=eid, parentId=parent, timestamp=iso(self.now - age))
        if message is not None:
            row['message'] = message
        return row

    def call(self, eid='a', parent=None, cid='ask', name='ask_dispatcher', age=30):
        return self.entry(eid, parent, dict(role='assistant', content=[
            dict(type='toolCall', id=cid, name=name,
                 arguments={'question': 'SECRET | <script> question', 'timeoutMinutes': 1})]), age)

    def result(self, eid='r', parent='a', cid='ask', age=20):
        return self.entry(eid, parent, dict(role='toolResult', toolCallId=cid,
                                          content=[dict(type='text', text='SECRET answer')]), age)

    def session(self, *entries, header_age=55):
        header = dict(type='session', version=3, id='current-session', timestamp=iso(self.now - header_age))
        self.file.write_text(''.join(json.dumps(r) + '\n' for r in (header, *entries)))

    def observed(self):
        return report.current_tools(str(self.home), self.pid, self.now)

    def test_pending_ask_result_and_ordinary_tools(self):
        self.session(self.call())
        self.assertEqual(self.observed(), '`ask_dispatcher` (30s)')
        self.session(self.call(), self.result(cid='other'))
        self.assertIn('ask_dispatcher', self.observed())
        for body in ('answer', 'cancelled', 'timeout'):
            row = self.result()
            row['message']['content'][0]['text'] = body
            self.session(self.call(), row)
            self.assertEqual(self.observed(), '-')
        self.session(self.call(name='bash'))
        self.assertEqual(self.observed(), '`bash` (30s)')

    def test_multiple_calls_all_observed_not_one_executor(self):
        row = self.call()
        row['message']['content'].append(dict(type='toolCall', id='read', name='read', arguments={}))
        self.session(row)
        self.assertEqual(self.observed(), '2 项：`ask_dispatcher` (30s)；`read` (30s)')
        self.session(row, self.result())
        self.assertEqual(self.observed(), '`read` (30s)')
        row['message']['content'] += [dict(type='toolCall', id=str(n), name='bash') for n in range(10)]
        self.session(row)
        text = self.observed()
        self.assertIn('12 项：', text)
        self.assertIn('另 8 项', text)
        self.assertNotIn('SECRET', text)

    def test_branch_uses_last_persisted_leaf_not_siblings(self):
        root = self.entry('root', None, dict(role='user', content='fixture'), age=40)
        self.session(root, self.call(parent='root'), self.call('b', 'root', 'b', 'read', age=25))
        self.assertEqual(self.observed(), '`read` (25s)')
        # A result on an abandoned branch cannot clear a call on the selected path.
        self.session(root, self.call(parent='root'), self.result(),
                     self.entry('new', 'a', kind='custom', age=10))
        self.assertEqual(self.observed(), '`ask_dispatcher` (30s)')
        self.session(root, self.call(parent='root'), self.result(),
                     self.entry('new', 'r', kind='branch_summary', age=10))
        self.assertEqual(self.observed(), '-')

    def test_old_session_generation_and_current_continuation(self):
        self.session(self.call(age=10), header_age=80)
        self.assertEqual(self.observed(), '不可得')
        self.session(self.call(age=70), header_age=80)
        self.assertEqual(self.observed(), '不可得')
        self.pid['gen'] = 2
        self.put('pid.json', self.pid)
        self.assertEqual(self.observed(), '不可得')
        # Reused session becomes observable only after this generation appends.
        self.session(self.call(age=70), self.call('new', 'a', 'new', 'read', age=10), header_age=80)
        self.assertEqual(self.observed(), '`read` (10s)')
        self.session(self.call(age=70), self.result(age=10), header_age=80)
        self.assertEqual(self.observed(), '-')
        # Never choose another JSONL or reset backup by mtime.
        (self.file.parent / 'newer.jsonl').write_text(self.file.read_text())
        self.file.write_text('')
        self.assertEqual(self.observed(), '不可得')

    def test_missing_broken_partial_oversize_and_unsupported_session(self):
        self.assertEqual(self.observed(), '不可得')
        self.session(self.call())
        valid = self.file.read_text()
        for text in ('', '{bad}\n', valid + '{"type":', valid.rstrip('\n'),
                     valid + 'broken\n', valid.replace('"version": 3', '"version": 2'),
                     valid.replace('"parentId": null', '"parentId": "missing"'),
                     valid.replace('"parentId": null', '"parentId": "a"'),
                     valid + json.dumps(self.call()) + '\n'):
            with self.subTest(text=text[:50]):
                self.file.write_text(text)
                self.assertEqual(self.observed(), '不可得')
        self.file.write_text(valid)
        for limit in ('TOOL_SESSION_BYTES', 'TOOL_SESSION_LINE_BYTES', 'TOOL_SESSION_ENTRIES'):
            with patch.object(report, limit, 0):
                self.assertEqual(self.observed(), '不可得')
        self.file.write_bytes(valid.encode() + b'\xff\n')
        self.assertEqual(self.observed(), '不可得')
        self.file.write_text(valid + '[' * 2000 + '0' + ']' * 2000 + '\n')
        self.assertEqual(self.observed(), '不可得')
        with self.file.open('wb') as f:
            f.truncate(report.TOOL_SESSION_BYTES + 1)
        self.assertEqual(self.observed(), '不可得')

    def test_bad_metadata_and_generation_race_are_unavailable(self):
        self.session(self.call())
        for update in ({'startedAt': None}, {'startedAt': iso(self.now + 1)},
                       {'pid': 0}, {'procStart': None}, {'gen': 0}, {'gen': '1'}, {'status': 'exited'}):
            with self.subTest(update=update):
                with patch.dict(self.pid, update):
                    self.assertEqual(self.observed(), '不可得')
        for update in ({'gen': 2}, {'final': True}, {'pid': 99999}, {'status': 'exited'}):
            with patch.object(report, 'read_json_safe', return_value=({**self.pid, **update}, True)):
                self.assertEqual(self.observed(), '不可得')

    def test_queue_terminal_non_session_and_bot_do_not_scan(self):
        with patch.object(report, 'current_tools', side_effect=AssertionError('must not scan')):
            self.put('pid.json', {**self.pid, 'final': True, 'status': 'exited', 'exitcode': 0})
            self.assertEqual(report.collect_task(str(self.root), 'task/fixture', self.now)['current_tools'], '-')
            self.put('pid.json', self.pid)
            self.put('spec.json', {**self.spec, 'command': 'echo fixture'})
            self.assertEqual(report.collect_task(str(self.root), 'task/fixture', self.now)['current_tools'], '-')
            self.put('spec.json', self.spec)
            with patch.object(report.os.path, 'exists', return_value=False):
                queued = report.collect_task(str(self.root), 'task/fixture', self.now)
            self.assertFalse(queued['has_pid'])
            self.assertEqual(queued['current_tools'], '-')
            bot = self.root / 'agents/bot/fixture'
            bot.mkdir(parents=True)
            for name, doc in (('spec.json', self.spec), ('pid.json', self.pid)):
                (bot / name).write_text(json.dumps(doc))
            self.assertEqual(report.collect_task(str(self.root), 'bot/fixture', self.now)['current_tools'], '-')

    def test_observation_failure_does_not_change_lifecycle(self):
        self.session(self.call())
        good = report.collect_task(str(self.root), 'task/fixture', self.now)
        self.file.write_text('bad\n')
        bad = report.collect_task(str(self.root), 'task/fixture', self.now)
        self.assertEqual(good['current_tools'], '`ask_dispatcher` (30s)')
        self.assertEqual(bad['current_tools'], '不可得')
        for key in ('verdict', 'incomplete', 'final', 'has_pid'):
            self.assertEqual(good[key], bad[key])


if __name__ == '__main__':
    unittest.main()
