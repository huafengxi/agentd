#!/usr/bin/env python3
"""Offline portable regression. Keeps its uniquely created sandbox for inspection."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parent.parent


class PortableTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp(prefix='portable-test-'))
        cls.cwd = cls.tmp / 'unrelated cwd'
        cls.cwd.mkdir()
        cls.home = cls.tmp / 'host home'
        cls.home.mkdir()
        (cls.home / '.pi').mkdir()
        (cls.home / '.pi/auth.json').write_text('DO_NOT_EXPORT_SENTINEL')
        cls.env = {'PATH': os.environ.get('PATH', os.defpath), 'HOME': str(cls.home),
                   'PI_API_KEY': 'DO_NOT_INHERIT', 'PYTHONPATH': '/nonexistent'}
        print('Sandbox retained: ' + str(cls.tmp), flush=True)
        cls.archive = cls.tmp / 'minimal.tar.gz'
        cls.invoke(SOURCE / 'portable/install.py', 'build', '--output', cls.archive)
        # Only extract after validating that every member is a regular allowlisted file.
        cls.paths = json.loads((SOURCE / 'portable/files.json').read_text())
        with tarfile.open(cls.archive) as archive:
            assert sorted(archive.getnames()) == sorted('agent-platform-minimal/' + p for p in cls.paths)
            assert all(m.isfile() for m in archive.getmembers())
            for member in archive.getmembers():
                target = cls.tmp / 'unpacked' / member.name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.extractfile(member).read())
        cls.package = cls.tmp / 'unpacked/agent-platform-minimal'
        cls.prefix = cls.tmp / 'new install'
        cls.invoke(cls.package / 'portable/install.py', 'install', '--prefix', cls.prefix)

    @classmethod
    def invoke(cls, script, *args, env=None, expected=0):
        result = subprocess.run([sys.executable, '-I', '-S', str(script), *map(str, args)],
                                cwd=cls.cwd, env=cls.env if env is None else env,
                                text=True, capture_output=True, timeout=30)
        if result.returncode != expected:
            raise AssertionError(f'rc={result.returncode}, expected={expected}\n{result.stdout}\n{result.stderr}')
        return result

    def test_real_shell_chain(self):
        cli = self.prefix / 'portable/cli.py'
        self.invoke(cli, 'doctor')
        self.invoke(cli, 'ctl', '--help')
        result = self.invoke(cli, 'demo')
        self.assertIn('no model was called', result.stdout)
        tasks = list((self.prefix / 'workspace/agents/task').iterdir())
        self.assertEqual(len(tasks), 1)
        task = tasks[0]
        state = json.loads((task / 'pid.json').read_text())
        self.assertIs(state['final'], True)
        self.assertEqual(state['exitcode'], 0)
        self.assertEqual(state['status'], 'exited')
        self.assertTrue((task / 'enable.json').is_file())
        self.assertIn('PASS: real shell task', (task / 'report.md').read_text())
        self.assertIn(task.name, self.invoke(cli, 'ctl', 'list').stdout)
        self.assertEqual((self.home / '.pi/auth.json').read_text(), 'DO_NOT_EXPORT_SENTINEL')

    def test_source_install(self):
        prefix = self.tmp / 'source-install'
        self.invoke(SOURCE / 'portable/install.py', 'install', '--prefix', prefix)
        self.invoke(prefix / 'portable/cli.py', 'doctor')
        self.invoke(prefix / 'portable/cli.py', 'demo')

    def test_extra_private_files_excluded(self):
        for name in ['env/keys', '.git/config', 'agents/task/old/report.md',
                     'lore/desk/memory', 'pi-core/agent/auth.json']:
            p = self.package / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text('DO_NOT_EXPORT_SENTINEL')
        rebuilt = self.tmp / 'rebuilt.tar.gz'
        self.invoke(self.package / 'portable/install.py', 'build', '--output', rebuilt)
        self.assertEqual(self.archive.read_bytes(), rebuilt.read_bytes())

    def test_missing_dependency(self):
        env = dict(self.env, PATH=str(self.tmp / 'no-executables'))
        result = self.invoke(self.prefix / 'portable/cli.py', 'doctor', env=env, expected=2)
        self.assertIn('Missing dependency: bash', result.stderr)

    def test_no_overwrite(self):
        result = self.invoke(self.package / 'portable/install.py', 'install',
                             '--prefix', self.prefix, expected=2)
        self.assertIn('File exists', result.stderr)
        self.invoke(SOURCE / 'portable/install.py', 'build', '--output', self.archive, expected=2)

    def test_bad_config(self):
        config = self.tmp / 'bad-config.json'
        config.write_text('{"workspace":"../outside","host":"local"}')
        result = self.invoke(self.prefix / 'portable/cli.py', '--config', config, 'doctor', expected=2)
        self.assertIn('workspace must be relative', result.stderr)

    def test_manifest_and_privacy_markers(self):
        markers = [b'enc1:', b'PRIVATE KEY-----', b'gitlab.', b'~/m',
                   b'yuanqi', b'obrde.', b'DO_NOT_EXPORT_SENTINEL', b'DO_NOT_INHERIT']
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

    def test_source_symlink_rejected(self):
        # Only mutate the self-created extracted package, never production source.
        victim = self.package / 'portable/example.json'
        original = victim.with_suffix('.saved')
        victim.rename(original)
        victim.symlink_to(original)
        try:
            result = self.invoke(self.package / 'portable/install.py', 'build', '--output',
                                 self.tmp / 'symlink.tar.gz', expected=2)
            self.assertIn('non-symlink files', result.stderr)
        finally:
            # Rename replaces the self-created symlink without recursive deletion.
            original.replace(victim)


if __name__ == '__main__':
    unittest.main(verbosity=2)
