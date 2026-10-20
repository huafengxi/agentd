"""Exercise public e2e's default adapter path without importing/running its main.

Full e2e contains opt-in real-pi scenarios. This bounded regression executes only
its original path assignments, fake env helper and fake WRAP_CMD in a new root.
"""
import ast
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
SOURCE = HERE / 'e2e.py'


def defaults(root):
    ns = dict(os=os, HERE=str(HERE), ROOT=str(root))
    with patch.dict(os.environ):
        os.environ.pop('PI_WRAP_DIR', None)
        for node in ast.parse(SOURCE.read_text()).body:
            names = {n.id for n in node.targets if isinstance(n, ast.Name)} if isinstance(node, ast.Assign) else set()
            if names & {'PI_WRAP_DIR', 'WRAP', 'WRAP_CMD'} or isinstance(node, ast.FunctionDef) and node.name == '_wrap_env':
                exec(compile(ast.Module(body=[node], type_ignores=[]), str(SOURCE), 'exec'), ns)
    return ns


class E2EPaths(unittest.TestCase):
    def test_default_is_local_agent_adapter_not_deleted_p_entry(self):
        root = Path(tempfile.mkdtemp(prefix='a-e2e-path-', dir='/tmp')).resolve()
        ns = defaults(root)
        self.assertEqual(Path(ns['WRAP']), HERE / 'pi-rpc-wrap.py')
        self.assertTrue(Path(ns['WRAP']).is_file())
        self.assertEqual(Path(ns['_wrap_env']('ok')['AGENTD_WRAP_PI_BIN']), HERE / 'fakepi_rpc.py')
        self.assertFalse((HERE.parent / 'pi-wrap/pi-rpc-wrap.py').exists())
        self.assertFalse((HERE.parent / 'pi-wrap/fakepi_rpc.py').exists())

    def test_original_default_command_reaches_fakepi_and_settles(self):
        root = Path(tempfile.mkdtemp(prefix='a-e2e-rpc-', dir='/tmp')).resolve()
        print('A_E2E_FAKE_ROOT=' + str(root), flush=True)
        home = root / 'agents/task/test'; home.mkdir(parents=True)
        private = root / 'home'; private.mkdir()
        (root / 'flags').mkdir()
        ns = defaults(root)
        env = dict(HOME=str(private), TMPDIR=str(root),
                   PATH=str(Path(sys.executable).parent) + os.pathsep + os.defpath,
                   AGENT_ROOT=str(root), AGENT_HOME=str(home), AGENT_SELF='task/test',
                   AGENTD_WRAP_SETTLE_WINDOW='.05', FAKE_FLAG_DIR=str(root / 'flags'),
                   PYTHONDONTWRITEBYTECODE='1',
                   **ns['_wrap_env']('ok'))
        # No PI_WRAP_DIR override, runner, real pi, assets or model endpoint.
        result = subprocess.run(['bash', '-c', ns['WRAP_CMD']], cwd=root, env=env,
                                text=True, capture_output=True, timeout=12)
        (root / 'stdout').write_text(result.stdout); (root / 'stderr').write_text(result.stderr)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('任务收敛完成', result.stderr)
        # The existing 'ok' fake does not persist a session. Its original trace
        # proves the actual argv and exactly one accepted stdin prompt instead.
        argv = json.loads((root / 'flags/argv').read_text())
        self.assertEqual(argv[argv.index('--session') + 1], str(home / 'session/session.jsonl'))
        self.assertEqual((root / 'flags/prompt').read_text(), '1\n')
        self.assertFalse((home / 'diagnosis.md').exists())
        self.assertEqual(list((root / 'run').rglob('*.sock')), [])
        self.assertEqual(len(list((root / 'run').rglob('*.lock'))), 2)

    def test_synthetic_s40_s60_commands_use_current_a_path(self):
        tree = ast.parse(SOURCE.read_text())
        for name in ('s40', 's60'):
            node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
            strings = [n.value for n in ast.walk(node) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
            self.assertTrue(any('agent-platform/agentd/pi-rpc-wrap.py' in s for s in strings), name)
            self.assertFalse(any('agent-platform/pi-wrap/pi-rpc-wrap.py' in s for s in strings), name)


if __name__ == '__main__':
    unittest.main(verbosity=2)
