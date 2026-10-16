"""Independent local-helper contract v1, real/symlink entry and shell fixtures."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from platform_paths import workspace_root

HERE = Path(__file__).resolve().parent


class LayoutTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agentd root contract ')
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name).resolve()
        self.repo = self.base / 'caller workspace' / 'independent checkout'
        self.repo.mkdir(parents=True)
        self.entry = self.repo / 'entry.py'
        self.entry.touch()
        self.link = self.base / 'symlink entry.py'
        self.link.symlink_to(self.entry)

    def test_source_identity_default_empty_and_nonempty_override(self):
        for entry in (self.entry, self.link):
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(workspace_root(entry), str(self.repo.parent))
            with patch.dict(os.environ, {'PLATFORM_WORKSPACE': ''}):
                self.assertEqual(workspace_root(entry), str(self.repo.parent))
            with patch.dict(os.environ, {'PLATFORM_WORKSPACE': str(self.base / 'state with spaces')}):
                self.assertEqual(workspace_root(entry), str(self.base / 'state with spaces'))
            with patch.dict(os.environ, {'HOME': str(self.base), 'PLATFORM_WORKSPACE': '~/chosen state'}):
                self.assertEqual(workspace_root(entry), str(self.base / 'chosen state'))
        # Relative explicit values follow the invoking cwd (unlike portable).
        with patch.dict(os.environ, {'PLATFORM_WORKSPACE': 'relative state'}):
            self.assertEqual(workspace_root(self.link), str(Path.cwd() / 'relative state'))

    def test_cli_root_precedes_environment_and_local_imports(self):
        state = self.base / 'selected state'
        (state / 'agents').mkdir(parents=True)
        for script in ('agentctl.py', 'report.py', 'needscheck.py', 'runner.py', 'scheduler.py'):
            link = self.base / ('linked ' + script)
            link.symlink_to(HERE / script)
            env = dict(os.environ, PLATFORM_WORKSPACE=str(self.base / 'wrong root'))
            result = subprocess.run([sys.executable, '-I', '-S', '-B', str(link), '--help'],
                                    cwd=self.base, env=env, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
        result = subprocess.run([sys.executable, '-I', '-S', '-B', str(self.base / 'linked agentctl.py'),
                                 '--root', str(state), 'list'], cwd=self.base, env=env,
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.base / 'wrong root').exists())

    def test_shell_real_relative_and_symlink_root_resolution(self):
        shutil.copyfile(HERE / 'platform_paths.py', self.repo / 'platform_paths.py')
        for name in ('loop.sh', 'scheduler-loop.sh'):
            text = (HERE / name).read_text()
            start = text.index('# Resolve real and symlink')
            stop = text.index('\nRUNNER=' if name == 'loop.sh' else '\nSCHEDULER=')
            fixture = self.repo / name
            fixture.write_text('set -u\n' + text[start:stop] + '\nprintf "%s\\n" "$MODULE_DIR" "$WS"\n')
            link = self.base / ('linked ' + name)
            link.symlink_to(os.path.relpath(fixture, self.base))
            for entry in (fixture, link, Path(os.path.relpath(fixture, self.base))):
                for chosen in ('', str(self.base / 'override workspace')):
                    env = dict(os.environ, PLATFORM_WORKSPACE=chosen)
                    env.pop('BASH_ENV', None)
                    result = subprocess.run(['/bin/bash', str(entry)], cwd=self.base,
                                            env=env, text=True, capture_output=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(result.stdout.splitlines(), [str(self.repo), chosen or str(self.repo.parent)])


if __name__ == '__main__':
    unittest.main()
