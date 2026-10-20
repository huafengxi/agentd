"""B1 regression: receipt identity, not request filename ordering.

All mutable data is freshly synthesized and retained for inspection. The tied
clock/reversed suffixes below are injected test data, NOT a reconstruction of
u5aq6g's original failure (its chosen request and receipt were not retained).
No runner, model, production socket or service is involved.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import e2e
import proto

# Patch only existing ID inputs in the CLI subprocess; use its real argparse,
# gates, writer and stdout. Identical timestamps + zzzz before aaaa make the
# old sorted(files)[-1] select control, deterministically, without racing.
BOOTSTRAP = '''
import sys
from unittest.mock import patch
sys.path.insert(0, sys.argv.pop(1))
import agentctl
with patch.object(agentctl.proto, 'now_ts', return_value='2030-01-01-00-00-00.000'), \\
     patch.object(agentctl.proto, 'rand_suffix', return_value=sys.argv.pop(1)):
    agentctl.main()
'''


class CancelReceiptTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix='cancel-receipt.', dir=e2e.TMPBASE))
        (self.root / 'agents/task').mkdir(parents=True)
        (self.root / 'agents/queue/dispatcher/inbox').mkdir(parents=True)
        (self.root / 'env').mkdir()
        (self.root / 'env/host-id').write_text('{}\n')
        self.records = []
        self.real_run = subprocess.run
        self.env = dict(os.environ, PLATFORM_WORKSPACE=str(self.root), AGENT_SELF='bot/s58-lead')
        print('retained fixture:', self.root, flush=True)

    def cli(self, args, suffix, **kwargs):
        argv = [sys.executable, '-B', '-c', BOOTSTRAP, str(HERE), suffix,
                '--root', str(self.root), *args]
        result = self.real_run(argv, **kwargs)
        docs = {str(p.relative_to(self.root)): json.loads(p.read_text())
                for p in self.root.glob('agents/task/*/control/*.req')}
        row = dict(argv=argv, rc=result.returncode, stdout=result.stdout,
                   stderr=result.stderr, requests=docs)
        self.records.append(row)
        print(json.dumps(row, ensure_ascii=False), flush=True)
        return result

    def test_s58_same_timestamp_reverse_suffixes(self):
        def run(argv, *args, **kwargs):
            if (len(argv) > 4 and Path(argv[1]) == HERE / 'agentctl.py'
                    and argv[4] in ('control', 'cancel')):
                return self.cli(argv[4:], 'zzzz' if argv[4] == 'control' else 'aaaa', **kwargs)
            return self.real_run(argv, *args, **kwargs)

        with patch.dict(os.environ, self.env, clear=True), \
             patch.object(e2e, 'ROOT', str(self.root)), \
             patch.object(e2e.subprocess, 'run', side_effect=run):
            e2e.s58()  # exercises the actual repaired assertion, not a copy
        control, cancel = [r for r in self.records if r['rc'] == 0]
        rid = cancel['stdout'].strip()
        docs = cancel['requests']
        latest_by_name = docs[sorted(docs)[-1]]
        target = next(d for d in docs.values() if d['id'] == rid)
        self.assertEqual(latest_by_name['id'], control['stdout'].strip())
        self.assertNotIn('reason', latest_by_name)
        self.assertNotEqual(latest_by_name['id'], rid)
        self.assertEqual(target['action'], 'stop')
        self.assertEqual(target['reason'], '需求作废')
        self.assertEqual(target['from'], 'bot/s58-lead')

    def test_real_cli_reason_defaults_and_rejection(self):
        participant = 'task/reason-contract'
        home = self.root / 'agents' / participant
        home.mkdir()
        proto.atomic_write_json(str(home / 'spec.json'), {'command': 'true', 'name': 'fixture'})

        def run(args, suffix, rc=0):
            result = self.cli(args, suffix, capture_output=True, text=True, env=self.env, timeout=30)
            self.assertEqual(result.returncode, rc, result.stderr)
            if rc:
                self.assertEqual(result.stdout, '')
                return None
            rid = result.stdout.strip()
            doc = json.loads((home / 'control' / (rid + '.req')).read_text())
            self.assertEqual(doc['id'], rid)
            self.assertEqual(doc['action'], 'stop')
            self.assertEqual(doc['from'], 'bot/s58-lead')
            return doc

        for index, (extra, want) in enumerate((
                ([], '（未给出原因）'), (['--reason', ' \t\n'], '（未给出原因）'),
                (['--reason', '  真实原因  '], '真实原因'))):
            self.assertEqual(run(['cancel', participant, *extra], 'c00' + str(index))['reason'], want)
        self.assertNotIn('reason', run(['control', participant, 'stop'], 'ctl0'))
        self.assertEqual(run(['control', participant, 'stop', '--reason', ' '], 'ctl1')['reason'],
                         '（未给出原因）')
        before = {p.name: p.read_bytes() for p in (home / 'control').iterdir()}
        proto.atomic_write_json(str(home / 'pid.json'), {'final': True, 'status': 'exited'})
        result = self.cli(['cancel', participant, '--reason', '拒绝'], 'nope',
                          capture_output=True, text=True, env=self.env, timeout=30)
        self.assertEqual(result.returncode, 2)
        self.assertIn('生命周期终态', result.stderr)
        self.assertEqual(before, {p.name: p.read_bytes() for p in (home / 'control').iterdir()})
        result = self.cli(['cancel', 'task/no-spec'], 'nope',
                          capture_output=True, text=True, env=self.env, timeout=30)
        self.assertEqual(result.returncode, 1)
        self.assertIn('无 spec.json', result.stderr)
        self.assertFalse((self.root / 'agents/task/no-spec').exists())
        # Protocol control bypasses that same final gate; its absent reason stays absent.
        self.assertNotIn('reason', run(['control', participant, 'stop'], 'ctl2'))


if __name__ == '__main__':
    unittest.main()
