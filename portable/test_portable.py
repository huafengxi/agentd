#!/usr/bin/env python3
"""Portable regression; all runtime writes stay in a retained unique sandbox."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parent.parent


class PortableTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp(prefix='portable-zero-'))
        cls.cwd = cls.tmp / 'unrelated cwd'
        cls.cwd.mkdir()
        cls.home = cls.tmp / 'host home'
        (cls.home / '.pi').mkdir(parents=True)
        (cls.home / '.pi/auth.json').write_text('DO_NOT_EXPORT_SENTINEL')
        cls.poison = cls.tmp / 'bash startup'
        cls.poison.write_text('echo LEAK > "' + str(cls.tmp / 'startup-leak') + '"')
        cls.env = {'PATH': str(Path(sys.executable).parent) + os.pathsep + os.defpath,
                   'HOME': str(cls.home), 'PI_API_KEY': 'DO_NOT_INHERIT',
                   'AGENT_SELF': 'task/host-identity', 'SSH_AUTH_SOCK': '/secret/socket',
                   'PYTHONPATH': '/nonexistent', 'BASH_ENV': str(cls.poison)}
        print('Sandbox retained: ' + str(cls.tmp), flush=True)
        cls.archive = cls.tmp / 'minimal.tar.gz'
        cls.python(SOURCE / 'portable/build.py', '--output', cls.archive)
        cls.paths = json.loads((SOURCE / 'portable/files.json').read_text())
        cls.package = cls.unpack(cls.archive, cls.tmp / 'unpacked with spaces')

    @classmethod
    def unpack(cls, path, dest):
        with tarfile.open(path) as archive:
            assert sorted(archive.getnames()) == sorted('agent-platform-minimal/' + p for p in cls.paths)
            assert all(m.isfile() for m in archive.getmembers())
            for member in archive.getmembers():
                target = dest / member.name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.extractfile(member).read())
                target.chmod(member.mode)
        return dest / 'agent-platform-minimal'

    @classmethod
    def invoke(cls, command, *args, env=None, expected=0):
        result = subprocess.run([str(command), *map(str, args)], cwd=cls.cwd,
                                env=cls.env if env is None else env,
                                text=True, capture_output=True, timeout=30)
        if result.returncode != expected:
            raise AssertionError(f'rc={result.returncode}, expected={expected}\n{result.stdout}\n{result.stderr}')
        return result

    @classmethod
    def python(cls, script, *args, **kwargs):
        return cls.invoke(sys.executable, '-I', '-S', '-B', script, *args, **kwargs)

    def fresh(self, name):
        return self.unpack(self.archive, self.tmp / name)

    @staticmethod
    def snapshot(root):
        return {str(p.relative_to(root)): (p.stat().st_mode, p.read_bytes() if p.is_file() else None)
                for p in root.rglob('*')}

    def test_zero_config_unpack_and_readonly_doctor(self):
        package = self.fresh('zero configuration')
        before = self.snapshot(package)
        result = self.invoke(package / 'portable/run.sh', 'doctor')
        doc = json.loads(result.stdout)
        self.assertEqual(doc['workspace'], str(package / 'workspace'))
        self.assertEqual(doc['host'], 'local')
        self.assertEqual(before, self.snapshot(package))
        self.assertFalse((package / 'workspace').exists())
        self.invoke(package / 'portable/run.sh', 'ctl', '--help')
        result = self.invoke(package / 'portable/run.sh', 'demo')
        self.assertIn('no model was called', result.stdout)
        self.check_task(package / 'workspace', package / 'portable/run.sh')

    def check_task(self, root, script):
        tasks = list((root / 'agents/task').iterdir())
        self.assertEqual(len(tasks), 1)
        task = tasks[0]
        state = json.loads((task / 'pid.json').read_text())
        self.assertIs(state['final'], True)
        self.assertEqual(state['exitcode'], 0)
        self.assertEqual(state['status'], 'exited')
        self.assertTrue((task / 'enable.json').is_file())
        self.assertIn('PASS: real shell task', (task / 'report.md').read_text())
        result = self.invoke(script, 'ctl', 'list', env=dict(self.env, PLATFORM_WORKSPACE=str(root)))
        self.assertIn(task.name, result.stdout)

    def test_source_external_root_and_environment_isolation(self):
        root = self.tmp / 'external state'
        tools = self.tmp / 'tools'
        tools.mkdir()
        capture = self.tmp / 'shell-environment'
        bash = tools / 'bash'
        bash.write_text('#!/bin/sh\n/usr/bin/env > "' + str(capture) + '"\nexec "'
                        + shutil.which('bash') + '" "$@"\n')
        bash.chmod(0o755)
        env = dict(self.env, PLATFORM_WORKSPACE=str(root), PLATFORM_PYTHON=sys.executable,
                   PATH=str(tools) + os.pathsep + self.env['PATH'])
        before = self.snapshot(SOURCE / 'portable')
        self.invoke(SOURCE / 'portable/run.sh', 'doctor', env=env)
        self.assertFalse(root.exists())
        self.invoke(SOURCE / 'portable/run.sh', 'demo', env=env)
        self.assertEqual(before, self.snapshot(SOURCE / 'portable'))
        self.check_task(root, SOURCE / 'portable/run.sh')
        child = dict(line.split('=', 1) for line in capture.read_text().splitlines())
        self.assertEqual(child['HOME'], str(root / 'home'))
        self.assertTrue(child['AGENT_SELF'].startswith('task/shell-'))
        for key in ['PI_API_KEY', 'SSH_AUTH_SOCK', 'PYTHONPATH', 'BASH_ENV',
                    'PLATFORM_PYTHON', 'PLATFORM_WORKSPACE']:
            self.assertNotIn(key, child)
        self.assertFalse((self.tmp / 'startup-leak').exists())
        self.assertEqual((self.home / '.pi/auth.json').read_text(), 'DO_NOT_EXPORT_SENTINEL')
        self.assertEqual(list(self.home.iterdir()), [self.home / '.pi'])

    def test_relative_overrides_and_config_ignored(self):
        package = self.fresh('relative paths')
        (package / 'config.json').write_text('NOT JSON -- must never read')
        # Interpreter path with spaces, relative to source root, not caller cwd.
        (package / 'chosen python').symlink_to(sys.executable)
        env = dict(self.env, PLATFORM_WORKSPACE='../relative state', PLATFORM_PYTHON='./chosen python')
        doc = json.loads(self.invoke(package / 'portable/run.sh', 'doctor', env=env).stdout)
        root = package.parent / 'relative state'
        self.assertEqual(doc['workspace'], str(root))
        self.assertFalse(root.exists())
        self.invoke(package / 'portable/run.sh', 'demo', env=env)
        self.check_task(root, package / 'portable/run.sh')
        self.invoke(package / 'portable/run.sh', '--config', 'config.json', 'doctor', expected=2)

    def test_readonly_source_external_state(self):
        package = self.fresh('read only source')
        package.chmod(0o555)
        before = self.snapshot(package)
        try:
            result = self.invoke(package / 'portable/run.sh', 'doctor', expected=2)
            self.assertIn('PLATFORM_WORKSPACE', result.stderr)
            self.assertFalse((package / 'workspace').exists())
            env = dict(self.env, PLATFORM_WORKSPACE=str(self.tmp / 'readonly external'))
            self.invoke(package / 'portable/run.sh', 'doctor', env=env)
            self.invoke(package / 'portable/run.sh', 'demo', env=env)
            self.assertEqual(before, self.snapshot(package))
        finally:
            package.chmod(0o755)

    def test_missing_dependencies(self):
        empty = self.tmp / 'missing tools'
        empty.mkdir()
        # Launcher needs POSIX dirname even when Python is absent.
        (empty / 'dirname').symlink_to(shutil.which('dirname'))
        result = self.invoke(self.package / 'portable/run.sh', 'doctor',
                             env=dict(self.env, PATH=str(empty)), expected=2)
        self.assertIn('Missing dependency: Python >= 3.10', result.stderr)
        (empty / 'env').symlink_to(shutil.which('env'))
        result = self.invoke(self.package / 'portable/run.sh', 'doctor',
                             env=dict(self.env, PATH=str(empty), PLATFORM_PYTHON=sys.executable), expected=2)
        self.assertIn('Missing dependency: bash', result.stderr)

    def test_allowlist_privacy_and_no_legacy_installer(self):
        package = self.fresh('privacy decoys')
        for name in ['env/keys', '.git/config', 'agents/task/old/report.md',
                     'lore/desk/memory', 'work/private', 'bots/identity',
                     'pi-core/agent/auth.json', 'workspace/home/token', 'config.json']:
            p = package / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text('DO_NOT_EXPORT_SENTINEL')
        rebuilt = self.tmp / 'rebuilt.tar.gz'
        self.python(package / 'portable/build.py', '--output', rebuilt)
        self.assertEqual(self.archive.read_bytes(), rebuilt.read_bytes())
        self.python(package / 'portable/build.py', '--output', rebuilt, expected=2)
        self.python(package / 'portable/build.py', 'install', '--prefix', self.tmp / 'not-created', expected=2)
        self.assertFalse((self.tmp / 'not-created').exists())
        self.assertFalse((package / 'portable/install.py').exists())
        self.assertFalse((package / 'portable/example.json').exists())
        markers = [b'enc1:', b'PRIVATE KEY-----', b'gitlab.', b'~/m', b'yuanqi',
                   b'obrde.', b'DO_NOT_EXPORT_SENTINEL', b'DO_NOT_INHERIT']
        with tarfile.open(self.archive) as archive:
            for member in archive.getmembers():
                self.assertEqual(member.uid, 0)
                self.assertEqual(member.uname, '')
                name = member.name.removeprefix('agent-platform-minimal/')
                data = archive.extractfile(member).read()
                self.assertEqual(hashlib.sha256(data).digest(),
                                 hashlib.sha256((SOURCE / name).read_bytes()).digest())
                for marker in markers:
                    self.assertNotIn(marker, data, (name, marker))
                if name.endswith('run.sh'):
                    self.assertEqual(member.mode, 0o755)

    def test_source_symlink_rejected(self):
        package = self.fresh('symlink fixture')
        victim = package / 'portable/README.md'
        saved = victim.with_suffix('.saved')
        victim.rename(saved)
        victim.symlink_to(saved)
        result = self.python(package / 'portable/build.py', '--output',
                             self.tmp / 'symlink.tar.gz', expected=2)
        self.assertIn('non-symlink files', result.stderr)

    def test_stateless_relocation(self):
        package = self.fresh('before relocation')
        self.invoke(package / 'portable/run.sh', 'doctor')
        moved = self.tmp / 'relocated source'
        package.rename(moved)
        self.invoke(moved / 'portable/run.sh', 'demo')
        self.check_task(moved / 'workspace', moved / 'portable/run.sh')


if __name__ == '__main__':
    unittest.main(verbosity=2)
