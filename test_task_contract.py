"""Registration publication and shared delivery contracts in isolated roots."""
import copy
import json
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import proto
import registration
import task_state


def contender(root, result):
    try:
        registration.register_task(root, "race01", spec(), "report instructions")
        result.put(True)
    except ValueError:
        result.put(False)


def spec():
    return {"command": "true", "workdir": "/tmp", "creator": "tester",
            "host": "testhost", "createdByHost": "testhost",
            "createdAt": "2026-01-01T00:00:00+08:00", "restartPolicy": "one-shot"}


class TaskContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / "agents").mkdir()

    def test_publication_makes_prompt_visible_before_spec(self):
        write = proto.atomic_write_json
        observed = []

        def observe(path, doc):
            home = Path(path).parent
            observed.append((home / "prompt.md").read_text())
            self.assertFalse(Path(path).exists())
            write(path, doc)

        with patch.object(proto, "atomic_write_json", side_effect=observe):
            result = registration.register_task(str(self.root), "sample", spec(), "正文\n")
        self.assertEqual(observed, ["正文\n"])
        self.assertEqual(json.loads((Path(result["agentDir"]) / "spec.json").read_text()), spec())
        with self.assertRaisesRegex(ValueError, "已存在"):
            registration.register_task(str(self.root), "sample", spec(), "overwrite")
        self.assertEqual((Path(result["agentDir"]) / "prompt.md").read_text(), "正文\n")

    def test_invalid_json_declarations_leave_no_task(self):
        base = {"name": "sample", "spec": spec(), "prompt": "正文"}
        invalid = []
        for key, value in (("command", ""), ("host", None), ("needs", [2]),
                           ("workdir", "relative"), ("restartPolicy", "unknown"),
                           ("reaper", "../bad"), ("extra", "ignored"),
                           ("profile", "executor")):
            req = copy.deepcopy(base)
            req["spec"][key] = value
            invalid.append(req)
        for name in ("../bad", ".hidden", "bad*", "task/nested", None):
            invalid.append(dict(base, name=name))
        invalid += [dict(base, prompt="  "), dict(base, prompt="\ud800"), [], {}]
        cli = Path(__file__).with_name("agentctl.py")
        for request in invalid:
            with self.subTest(request=request):
                run = subprocess.run([sys.executable, str(cli), "--root", str(self.root), "register-json"],
                                     input=json.dumps(request), text=True, capture_output=True)
                self.assertEqual(run.returncode, 2, run.stderr)
                self.assertEqual(list((self.root / "agents").iterdir()), [])

    def test_cli_and_json_share_publication(self):
        cli = Path(__file__).with_name("agentctl.py")
        body = self.root / "body.md"
        body.write_text("正文\n")
        run = subprocess.run([sys.executable, str(cli), "--root", str(self.root), "create",
                              "--name", "cli001", "--command", "true", "--workdir", "/tmp",
                              "--creator", "tester", "--restart-policy", "one-shot",
                              "--host", "testhost", "--created-by-host", "testhost",
                              "--prompt-file", str(body)], text=True, capture_output=True)
        self.assertEqual(run.returncode, 0, run.stderr)
        declaration = json.loads((self.root / "agents/task/cli001/spec.json").read_text())
        run = subprocess.run([sys.executable, str(cli), "--root", str(self.root), "register-json"],
                             input=json.dumps({"name": "json01", "spec": declaration, "prompt": body.read_text()}),
                             text=True, capture_output=True)
        self.assertEqual(run.returncode, 0, run.stderr)
        home = Path(json.loads(run.stdout)["agentDir"])
        self.assertEqual(json.loads((home / "spec.json").read_text()), declaration)
        self.assertEqual((home / "prompt.md").read_bytes(), body.read_bytes())

    def test_concurrent_creation_has_one_winner(self):
        ctx = multiprocessing.get_context("spawn")
        result = ctx.Queue()
        children = [ctx.Process(target=contender, args=(str(self.root), result)) for _ in range(2)]
        try:
            for child in children:
                child.start()
            outcomes = [result.get(timeout=10) for _ in children]
            self.assertEqual(sorted(outcomes), [False, True])
            for child in children:
                child.join(timeout=10)
                self.assertEqual(child.exitcode, 0)
        finally:
            for child in children:
                if child.is_alive():
                    child.terminate()
                    child.join()
            result.close()

    def test_delivery_matrix_preserves_orphan_and_cancel_semantics(self):
        home = self.root / "agents/task/sample"
        home.mkdir(parents=True)
        report = home / "report.md"
        for status, code, delivered, success in (
                ("exited", 0, True, True), ("stale", 127, True, True),
                ("exited", 127, True, False), ("killed", 137, True, False),
                ("exited", 0, False, False), ("stale", 127, False, False)):
            report.write_text(" " * 8192 + ("result" if delivered else ""))
            doc = {"status": status, "exitcode": code, "final": True}
            with self.subTest(status=status, code=code, delivered=delivered):
                self.assertEqual(task_state.provider_success(doc, str(self.root), "task/sample"), success)
                self.assertFalse(task_state.provider_success(dict(doc, final=False), str(self.root), "task/sample"))
        report.write_text(" ")
        doc = {"status": "exited", "exitcode": 0, "final": True}
        self.assertTrue(task_state.provider_idle(doc, str(self.root), "task/sample"))
        proto.atomic_write_json(str(home / "control/stop.req"), {"action": "stop"})
        self.assertTrue(task_state.provider_canceled(doc, str(self.root), "task/sample"))
        self.assertFalse(task_state.provider_idle(doc, str(self.root), "task/sample"))
        self.assertTrue(task_state.is_resident({"command": "AGENTD_RESIDENT=1 true"}))
        self.assertFalse(task_state.is_resident(spec()))


if __name__ == "__main__":
    unittest.main()
