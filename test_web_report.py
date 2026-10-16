"""Report transport works independently of workspace layout and host globals."""
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

source = Path(__file__).with_name('agentd4web.py')
spec = importlib.util.spec_from_file_location('web_report', source)
adapter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adapter)


class WebReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='report transport ')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.cli = self.root / 'report.py'

    def call(self, code, **env):
        self.cli.write_text(code)
        with patch.dict(os.environ, dict(AGENTD_REPORT_PATH=str(self.cli), **env)):
            return adapter.interp(None)[1]

    def test_success_uses_host_interpreter_and_utf8(self):
        body = self.call('import sys;print(sys.executable);print("报告")')
        self.assertEqual(body, sys.executable + '\n报告\n')

    def test_default_cli_is_adjacent_to_adapter(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(adapter.subprocess, 'run') as run:
            run.return_value = subprocess.CompletedProcess([], 0, b'fixture', b'')
            self.assertEqual(adapter.interp(None)[1], 'fixture')
            self.assertEqual(run.call_args.args[0], [sys.executable, str(source.with_name('report.py'))])

    def test_exit_failure_is_readable_and_stderr_bounded(self):
        body = self.call('import sys;sys.stderr.write("x"*3000);sys.exit(7)')
        self.assertIn('exitcode=7', body)
        self.assertIn('stderr 截断', body)
        self.assertLess(len(body), 2200)

    def test_timeout_is_readable(self):
        body = self.call('import time;time.sleep(10)', AGENTD_REPORT_TIMEOUT='0.05')
        self.assertIn('执行超时', body)

    def test_invalid_timeouts_do_not_start_a_process_and_large_values_are_capped(self):
        for value in ('bad', '-1', '0', 'nan', 'inf'):
            with self.subTest(value=value), patch.dict(os.environ, {'AGENTD_REPORT_TIMEOUT': value}), \
                    patch.object(adapter.subprocess, 'run') as run:
                self.assertIn('超时配置无效', adapter.interp(None)[1])
                run.assert_not_called()
        with patch.dict(os.environ, {'AGENTD_REPORT_TIMEOUT': '100'}), \
                patch.object(adapter.subprocess, 'run') as run:
            run.return_value = subprocess.CompletedProcess([], 0, b'', b'')
            adapter.interp(None)
            self.assertEqual(run.call_args.kwargs['timeout'], 15)

    def test_spawn_failure_is_readable(self):
        with patch.object(adapter.subprocess, 'run', side_effect=OSError('fixture')):
            self.assertIn('启动失败', adapter.interp(None)[1])

    def test_page_widgets_run_from_relocated_source_with_spaces_and_quotes(self):
        self.cli.write_text('import json,sys;print(json.dumps(sys.argv[1:]))')
        source_page = self.root / "page ' source.md"
        source_page.touch()
        page = Path(__file__).with_name('tasks.md').read_text()
        commands = [cmd for cmd in re.findall(r'\$\{([^}]+)\}', page)
                    if cmd.startswith('python3 ') and '/report.py' in cmd]
        self.assertEqual(len(commands), 7)
        for command in commands:
            env = dict(os.environ, src=str(source_page),
                       PATH=str(Path(sys.executable).parent) + os.pathsep + os.environ.get('PATH', ''))
            run = subprocess.run(['bash', '-c', command], cwd='/', env=env,
                                 text=True, capture_output=True, timeout=5)
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertEqual(json.loads(run.stdout), ['-s', command.rsplit(' ', 1)[1]])


if __name__ == '__main__':
    unittest.main()
