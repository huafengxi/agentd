"""Shared observations and gate decisions across writer and read-only presenters."""
from collections import Counter
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import needscheck
import proto
import report
import scheduler
import scheduling
import task_state


class SchedulingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / "agents/task").mkdir(parents=True)
        self.sequence = 0

    def task(self, name, pid=None, body=None, **spec):
        home = self.root / "agents/task" / name
        home.mkdir()
        data = dict(command="true", host="test", resources=[])
        data.update(spec)
        proto.atomic_write_json(str(home / "spec.json"), data)
        self.sequence += 1
        stamp = time.time() - 100 + self.sequence
        os.utime(home / "spec.json", (stamp, stamp))
        if pid is not None:
            proto.atomic_write_json(str(home / "pid.json"), pid)
        if body is not None:
            (home / "report.md").write_text(body)
        return home

    def render(self):
        with patch.object(report, "effective_max_concurrent", return_value=(10, "test runtime")), \
             patch.object(report, "collect_system_health", return_value=([], True, [])), \
             patch.object(scheduling, "host_runner_alive", return_value=(True, "fresh", False)):
            return report.build_report(str(self.root), time.time())[0]

    def test_report_reads_spec_and_pid_once_and_leaves_tree_unchanged(self):
        homes = [self.task("source", pid={"status": "exited", "exitcode": 0, "final": True},
                           body="delivered", provides=["cap"]), self.task("consumer", needs=["cap"])]

        def snapshot():
            return {str(p.relative_to(self.root)): p.read_bytes()
                    for p in self.root.rglob("*") if p.is_file()}

        before = snapshot()
        with patch.object(proto, "read_json", wraps=proto.read_json) as read:
            text = self.render()
        reads = Counter(str(call.args[0]) for call in read.call_args_list)
        for home in homes:
            self.assertEqual(reads[str(home / "spec.json")], 1)
            self.assertEqual(reads[str(home / "pid.json")], 1)
        self.assertIn("consumer", text)
        self.assertEqual(snapshot(), before)

    def test_observations_keep_a_single_view_when_disk_changes(self):
        home = self.task("source", pid={"status": "running", "final": False}, provides=["cap"])
        records = scheduling.observe_tasks(str(self.root))
        proto.atomic_write_json(str(home / "pid.json"), {"status": "exited", "exitcode": 0, "final": True})
        (home / "report.md").write_text("delivered")
        old = scheduling.scheduling_view(str(self.root), records=records)
        new = scheduling.scheduling_view(str(self.root))
        self.assertEqual(old.providers_by_cap["cap"], [("task/source", "pending")])
        self.assertEqual(new.providers_by_cap["cap"], [("task/source", "success")])
        shown = report.collect_task(str(self.root), "task/source", time.time(), records["task/source"])
        self.assertFalse(shown["final"])
        self.assertFalse(shown["has_report"])

    def test_unreadable_pid_is_distinct_from_missing_pid_in_both_presenters(self):
        home = self.task("broken", needs=["missing"])
        (home / "pid.json").write_text('{"status":')
        view = scheduling.scheduling_view(str(self.root))
        self.assertTrue(view.records["task/broken"].pid_exists)
        self.assertIsNone(view.records["task/broken"].pid)
        self.assertEqual(needscheck.collect_unsat(str(self.root)), [])
        self.assertIn("不可判定（pid.json 读取不完整）", self.render())

    def test_structured_dependencies_preserve_cap_names_and_pending_precedence(self):
        cap = "cap with spaces,[literal]"
        self.task("failed", pid={"status": "exited", "exitcode": 1, "final": True}, provides=[cap])
        self.task("consumer", needs=[cap])
        unsat = needscheck.collect_unsat(str(self.root))
        self.assertEqual(unsat[0]["caps"][0]["cap"], cap)
        self.assertIn("task/failed（已失败）", unsat[0]["caps"][0]["why"])
        self.assertIn("`" + cap + "`（provider 无成功者：task/failed（已失败））", self.render())
        providers = {cap: [("task/old", "success"), ("task/new", "pending")]}
        detail = scheduling.dependency_details([cap], providers)[0]
        self.assertEqual((detail.state, detail.pending, detail.success),
                         ("wait", ("task/new",), ("task/old",)))
        verdict, reasons = scheduling.eval_needs([cap], providers)
        self.assertEqual(verdict, "wait")
        self.assertIn("ignoring older success provider task/old", reasons[0])

    def test_scheduler_reserves_same_round_resources_and_keeps_resident_exemption(self):
        a = self.task("a", resources=["repo"])
        b = self.task("b", resources=["repo"])
        c = self.task("c", resources=["other"])
        resident = self.task("resident", command="AGENTD_RESIDENT=1 true", resources=["repo"])
        with patch.object(scheduling, "host_runner_alive", return_value=(True, "fresh", False)):
            scheduler.Scheduler(str(self.root), ["test"], max_concurrent=2, all_hosts=True).tick()
        self.assertTrue((a / "enable.json").exists())
        self.assertFalse((b / "enable.json").exists())
        self.assertTrue((c / "enable.json").exists())
        self.assertTrue((resident / "enable.json").exists())

    def test_gate_priority_and_unknown_runtime_limit(self):
        probe = lambda spec: (False, "offline", False)
        spec = dict(command="true", host="test", resources=["repo"], needs=["missing"])
        for extra, providers, held, limit, expected in (
                ({"host": ""}, {}, {"repo"}, 0, "missing_host"),
                ({}, {}, {"repo"}, 0, "needs_unsat"),
                ({"needs": []}, {}, {"repo"}, 0, "resources"),
                ({"needs": [], "resources": []}, {}, set(), 0, "slots"),
                ({"needs": [], "resources": []}, {}, set(), None, "host")):
            decision = scheduling.evaluate_gate(dict(spec, **extra), [], held, providers, limit, probe)
            self.assertEqual(decision.code, expected)
        resident = dict(spec, command="AGENTD_RESIDENT=1 true", needs=[])
        decision = scheduling.evaluate_gate(resident, ["occupied"], {"repo"}, {}, 0,
                                            lambda spec: (True, "fresh", False))
        self.assertTrue(decision.ready)

    def test_read_only_entry_points_do_not_import_writer_modules(self):
        code = ("import sys;sys.path.insert(0,sys.argv[1]);import report,needscheck;"
                "assert not ({'scheduler','agentctl','registration'} & set(sys.modules))")
        run = subprocess.run([sys.executable, "-c", code, str(Path(__file__).parent)],
                             text=True, capture_output=True, timeout=5)
        self.assertEqual(run.returncode, 0, run.stderr)


if __name__ == "__main__":
    unittest.main()
