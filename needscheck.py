#!/usr/bin/env python3
"""needscheck.py — 只读「needs 不可满足（dead-ended）」判定 CLI。

用途：给 `task_status` 工具面（agentd 扩展的 `core.ts`）提供**判定单点在
Python 侧**的机器可读结果——TS 侧不重写 provider 状态分类与三态判定（否则与调度面口径漂移）。
事故来源： （provides `cap:dingmsg-2026-09-08`，host=mac）连击熔断收口且无 report.md
⇒ 其唯一下游（needs 同能力）按调度语义永久留在 not-started；调度员靠人工扫 task_status
全量列表才发现，而那一面当时**不评估 needs**（信号只活在 report.py 异常区与 dash task tab）。

判据（全部 import 复用，本文件零自实现）：
  - provider 五态分类（success/pending/failed/canceled/idle）= `scheduler.Scheduler._scan`
  - 三态判定（ok/wait/unsat，pending 优先于同能力旧 success）= `scheduler.eval_needs`
  - 终态非成功态的中文文案 = `report.PROVIDER_STATE_ZH` / `report._provider_states_zh`
判定范围与 `report.py` 异常区「🚨 needs 不可满足」逐条对齐（两面不得分歧）：
  `task/*` 族 ∧ 有 spec ∧ `needs` 非空 ∧ **无 pid.json**（判据 = 文件**不存在**，同 report.py 的
  has_pid；存在但半截 → 不可判定，两面都排除）∧ **无 enable.json**（enable 单调不回撤：
  已放行者的 needs 在放行时刻已判 ok）∧ `eval_needs == "unsat"`。
  ok / wait 一律不输出（wait = 仍有未终态 provider，属设计正确的等待，标出来就是误报）。
  `bot/*` 族不入（report.py 的 needs_unsat 亦只算 task 族）。

cap 级明细不用正则解析 `eval_needs` 的 reason 串（那会在本文件复制 report.py 的解析式）：
对任务级判 unsat 者逐 cap 再调 `eval_needs([cap], providers_by_cap)`，只列该 cap 亦判 unsat 的，
明细串按 `eval_needs` 同款形态（`task/a[failed]`）由 providers_by_cap 原样态拼出后交给
`report._provider_states_zh` 渲染。

严格只读：不写 `agents/` 树、不加锁、不起子进程。跨机同步的半截 JSON 由 `proto.read_json`
容错（解析失败视作缺字段）；任何异常 → stdout 输出 `{"ok": false, "error": …}` 且 exit 1，
调用方（core.ts）据此降级为「不标记 + 一行提示」，绝不让 task_status 失败。

输出（stdout 单个 JSON）：
  {"ok": true, "root": "...", "generatedAt": "...", "elapsedMs": 46,
   "unsat": [{"id": "task/x", "needs": ["cap:a"],
              "caps": [{"cap": "cap:a", "noProvider": false,
                        "providersZh": "task/y（已失败）",
                        "why": "provider 无成功者：task/y（已失败）"}]}]}

用法：
  python3 agentd/needscheck.py                  # 扫 <workspace-root>/agents/
  python3 agentd/needscheck.py --root <workspace-root>   # 指定工作区根
"""
import argparse
import json
import os
import sys
import time

# 复用同目录协议库 / 调度门禁谓词 / 报表文案 / CLI 的缺省根单点（均仅标准库）：判定、文案与
# `--root` 缺省各自单点，零漂移（缺省根 = agentctl.default_root()，现场发现，⛔ 不写死部署值）。
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import agentctl  # noqa: E402
import proto  # noqa: E402
import report  # noqa: E402
import scheduler  # noqa: E402


def providers_detail(provs):
    """[(taskId, state)] → `task/a[failed],task/b[idle]`（形态同 eval_needs 的 unsat 明细串，
    供 report._provider_states_zh 渲染；纯格式化，不做任何状态分类）。"""
    return ",".join("%s[%s]" % (t, st) for t, st in provs or [])


def collect_unsat(root):
    """扫一遍 agents/ 树，返回 dead-ended（needs 不可满足）任务清单。

    返回 [{"id", "needs", "caps": [{"cap", "noProvider", "why"}]}]；无命中返回 []。
    一次 `_scan` 全量取 providers_by_cap（与调度器/report.py 同一遍扫描口径），零重扫。
    """
    _occ, _held, providers_by_cap, _queue = scheduler.Scheduler(
        root, proto.local_hosts(), all_hosts=True)._scan()
    out = []
    for pid_ in proto.list_participants(root):
        if not pid_.startswith(proto.TASK_DIR + "/"):
            continue  # bot 族不入本口径（同 report.py needs_unsat）
        spec = proto.read_json(proto.spec_path(root, pid_))
        if not isinstance(spec, dict):
            continue  # 无 spec / 半截 JSON：保守跳过
        needs = scheduler.normalize_sched_fields(spec)["needs"]
        if not needs:
            continue
        # 排队面门 = `pid.json` **存在即出队**（与 report.py 的 has_pid 同口径、
        #🟡1）：不得用 `read_json(...) is not None` —— read_json 对「文件存在但
        # 解析失败」（跨机同步中途的半截 JSON）同样返回 None，会把该形态误当「无 pid.json」
        # 而入排队面、被标 ⛔；report.py 对同一形态置 incomplete=True 并排除出 needs_unsat
        # （verdict =「不可判定（pid.json 读取不完整）」）⇒ 保守跳过，两面不得分歧。
        if os.path.exists(proto.pid_path(root, pid_)):
            continue  # 已落地进程档案（运行中/终态/半截不可判定）：不在排队面
        if os.path.exists(proto.enable_path(root, pid_)):
            continue  # 已放行待拉起：enable 单调不回撤，放行时刻 needs 已 ok
        if scheduler.eval_needs(needs, providers_by_cap)[0] != "unsat":
            continue  # ok / wait 均不输出（wait 属正常等待，标出来是误报）
        caps = []
        for cap in needs:
            if scheduler.eval_needs([cap], providers_by_cap)[0] != "unsat":
                continue  # 只列真正不可满足的能力（同任务另有 wait/ok 的 cap 不混进来）
            provs = providers_by_cap.get(cap) or []
            zh = report._provider_states_zh(providers_detail(provs))
            caps.append({
                "cap": cap,
                "noProvider": not provs,
                # providersZh：终态非成功 provider 的可读清单（list 面紧凑呈现用）
                "providersZh": ("" if not provs else zh),
                # why：完整成因句（detail 面用；文案单点 = report._provider_states_zh）
                "why": "无 provider" if not provs else "provider 无成功者：" + zh,
            })
        if caps:
            out.append({"id": pid_, "needs": list(needs), "caps": caps})
    return out


def main():
    ap = argparse.ArgumentParser(
        prog="needscheck.py",
        description="只读输出「needs 不可满足（dead-ended）」任务清单（JSON）；判定单点 = "
                    "scheduler.eval_needs，文案单点 = report._provider_states_zh。")
    ap.add_argument("--root", default=agentctl.default_root(),
                    help="工作区根目录（扫描 <root>/agents/；缺省 = 本脚本所在仓的父目录，"
                         "现场发现，不依赖 cwd/env）")
    a = ap.parse_args()
    root = os.path.abspath(os.path.expanduser(a.root))
    t0 = time.time()
    try:
        unsat = collect_unsat(root)
    except Exception as e:                      # 半截文件/权限/意外：降级信号，不抛栈到调用方
        json.dump({"ok": False, "root": root,
                   "error": "%s: %s" % (type(e).__name__, e)},
                  sys.stdout, ensure_ascii=False)
        sys.stdout.write("\n")
        return 1
    json.dump({"ok": True, "root": root,
               "generatedAt": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
               "elapsedMs": int((time.time() - t0) * 1000),
               "unsat": unsat},
              sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
