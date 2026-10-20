"""Agent-only policy is in A; byte transport is the actual generic P class."""
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
spec = importlib.util.spec_from_file_location('agent_rpc_adapter', HERE / 'pi-rpc-wrap.py')
M = importlib.util.module_from_spec(spec); spec.loader.exec_module(M)
from pi_host import Host
from transport import Transport


class AdapterTests(unittest.TestCase):
    def fixture(self):
        root = Path(tempfile.mkdtemp(prefix='a-adapter-', dir='/tmp')).resolve()
        home = root / 'task'; home.mkdir()
        with patch.dict(os.environ, AGENT_ROOT=str(root), AGENT_HOME=str(home), AGENT_SELF='task/fixture'):
            return root, M.Wrap()

    def test_actual_shared_transport_methods(self):
        self.assertIs(M.Wrap.__mro__[1], Transport)
        self.assertIs(Host.__mro__[1], Transport)
        for name in ('pi_send', 'launch', '_read_stdout', '_read_stderr', 'accept_loop',
                     'replay_lines', '_conn_relay', 'close_pi_stdin'):
            self.assertIs(getattr(M.Wrap, name), getattr(Host, name), name)

    def test_agent_lock_contention_does_not_touch_first_owner(self):
        root, first = self.fixture()
        _, second = self.fixture()
        # The second fixture intentionally targets the first endpoint/history.
        second.sock_path, second.pid_file = first.sock_path, first.pid_file
        second.session_file = first.session_file
        try:
            first.setup_socket()
            inode = Path(first.sock_path).stat().st_ino
            self.assertEqual(len(first._socket_locks), 2)
            self.assertEqual(len(list(root.rglob('*.lock'))), 2)
            with self.assertRaisesRegex(RuntimeError, 'lock is held'):
                second.setup_socket()
            self.assertTrue(first.owns_socket())
            self.assertEqual(Path(first.sock_path).stat().st_ino, inode)
        finally:
            second.cleanup_socket(); first.cleanup_socket()

    def test_agent_identity_hooks_remain_proto(self):
        _, wrapper = self.fixture()
        with patch.object(M.proto, 'proc_starttime', return_value=111) as start, \
                patch.object(M.proto, 'pid_identity_ok', return_value=True) as alive:
            self.assertEqual(wrapper.process_starttime(42), 111)
            self.assertTrue(wrapper.process_owner_alive(42, 111))
            start.assert_called_once_with(42); alive.assert_called_once_with(42, 111)


if __name__ == '__main__':
    unittest.main(verbosity=2)
