#!/usr/bin/env python3
"""test_runner_notify.py — runner 终态通知面单测（判重 / 收件面解析 / ask 写侧 env）。

会话活性监督面（hang_watchdog / stall_alarm / 第三态判据）已整体退役，
本文件承接其幸存的通知面用例；H 编号沿用历史命名（原 test_runner_hang.py）：
  H9  终态通知判重：notified.json 标记唯一事实源（信箱信封不参与判重）
  H13 终态通知收件面解析（reaper 单收件方）：resolve_reaper 两档 + 活性代理三判据 +
      判重标记集合语义
  H14 子端 ask 写侧收件面 env：_ask_inbox_env 复用 resolve_reaper 单点

仅标准库。用法：python3 agentd/test_runner_notify.py
"""
import json
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import proto            # noqa: E402
import runner as R      # noqa: E402

PASS = 0
FAIL = 0
TMP = []


def ok(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("PASS  %s" % name)
    else:
        FAIL += 1
        print("FAIL  %s  %s" % (name, detail))


def mkroot():
    root = tempfile.mkdtemp(prefix="agentd-notify.%d." % os.getpid())
    TMP.append(root)
    os.makedirs(os.path.join(root, "agents", "task"))
    os.makedirs(proto.position_inbox(root), exist_ok=True)
    return root


def mkrunner(root, **kw):
    return R.Runner(root, "testhost", "", 0.5, **kw)


def mktask(root, tid, spec_extra=None, pid_extra=None, with_session=True):
    pid_ = "task/" + tid
    adir = proto.agent_dir(root, pid_)
    os.makedirs(os.path.join(adir, "inbox"), exist_ok=True)
    os.makedirs(os.path.join(adir, "session"), exist_ok=True)
    spec = {"name": "t-" + tid, "workdir": "~/w/" + tid, "host": "testhost",
            "restartPolicy": "one-shot", "command": "true"}
    spec.update(spec_extra or {})
    proto.atomic_write_json(proto.spec_path(root, pid_), spec)
    doc = {"gen": 1, "pid": 999999, "status": "running",
           "startedAt": proto.now_ts(), "lastAliveAt": proto.now_ts(),
           "restarts": 0, "final": False}
    doc.update(pid_extra or {})
    proto.atomic_write_json(proto.pid_path(root, pid_), doc)
    if with_session:
        open(os.path.join(adir, "session", "session.jsonl"), "w").close()
    return pid_, adir, spec, doc


def h9():
    """终态通知判重（回归面）：判重唯一事实源 = 自家 notified.json 标记（粒度 =
    (taskId, 收件方)）；信箱里的任何 from=agentd 信封都**不参与判重**（无信箱回落扫描面）。"""
    root = mkroot()
    r = mkrunner(root)
    pid_, adir, _s, _d = mktask(root, "h9")
    pos = proto.position_inbox(root)

    def alarm(mid, event="task_stall_unread"):   # 事件名任意：信封一律不判重
        proto.atomic_write_json(os.path.join(pos, mid + ".msg"), {
            "id": mid, "from": "agentd", "ts": proto.now_ts(), "type": "inform",
            "body": json.dumps({"event": event, "taskId": pid_,
                                "unackedCount": 1}, ensure_ascii=False)})

    alarm("h9-alarm-1")
    ok("H9 停滞告警不算已终态通知（判重不拺位）",
       r._already_notified(pid_, "topic/dispatcher") is False)
    alarm("h9-done", event="task_done")
    ok("H9 信箱里的终态信封也不参与判重（唯一事实源 = 标记；无标记即可投）",
       r._already_notified(pid_, "topic/dispatcher") is False)
    # notified.json 标记（权威判重；粒度 = (taskId, 收件方)）
    proto.atomic_write_json(os.path.join(adir, R.NOTIFY_MARK),
                            {"to": ["topic/dispatcher"], "ts": proto.now_ts(),
                             "event": "task_failed", "complete": True})
    ok("H9 标记含该收件方 → 已通知（不重投）",
       r._already_notified(pid_, "topic/dispatcher") is True)
    ok("H9 标记不含的新收件方 → 未通知（可投）",
       r._already_notified(pid_, "bot/w1") is False)
    ok("H9 标记缺失 → _mark_doc 返回 None、判未通知",
       r._mark_doc("task/h9-nothere") is None
       and r._already_notified("task/h9-nothere", "topic/dispatcher") is False)



# --------------------------------------------- H13（终态通知收件面解析：reaper/watchers）

def _mk_bot(root, name, pid_final=None, watcher=False, inbox=True):
    """合成 bot 目录（活性代理三判据的可控夹具）：pid_final=None 不写 pid.json；
    watcher=True 补一个非隐藏 watcher 条目（信箱型活性）。"""
    d = os.path.join(root, "agents", "bot", name)
    if inbox:
        os.makedirs(os.path.join(d, "inbox"), exist_ok=True)
    else:
        os.makedirs(d, exist_ok=True)
    if pid_final is not None:
        proto.atomic_write_json(os.path.join(d, "pid.json"),
                                {"gen": 1, "pid": 424242, "status": "exited",
                                 "final": pid_final, "exitcode": 0})
    if watcher:
        os.makedirs(os.path.join(d, "watcher"), exist_ok=True)
        open(os.path.join(d, "watcher", "sub-" + name), "w").close()
    return d


def h13():
    """终态通知收件面 = reaper 单收件方（纯判定面，不起 runner 进程）：
    reaper 两档解析（显式字段 / 缺失或文法非法 → 回落职位信箱带 note）、活性代理、
    判重标记 = 唯一事实源（无信箱回落扫描/补写面）。"""
    root = mkroot()
    r = mkrunner(root)
    pos = proto.position_inbox(root)
    _mk_bot(root, "live-proc", pid_final=False)                 # ① 进程型活性
    _mk_bot(root, "live-box", watcher=True)                     # ② 信箱型活性
    _mk_bot(root, "dead-final", pid_final=True)                 # 目录在场但进程已终态
    _mk_bot(root, "dead-empty")                                 # 目录在场、无任何消费者痕迹
    _mk_bot(root, "sub-only", inbox=False)                      # ③ 靠他人 spec.subscribes 声明
    proto.atomic_write_json(proto.spec_path(root, "task/subscriber"),
                            {"name": "subscriber", "host": "testhost", "command": "true",
                             "subscribes": ["topic/weekly", "bot/sub-only"]})
    # 合成 topic（watcher 条目 = 通道 A 订阅，主题型活性）：充当 watcher 收件方
    tdir = os.path.join(root, "agents", "topic", "weekly")
    os.makedirs(os.path.join(tdir, "inbox"), exist_ok=True)
    os.makedirs(os.path.join(tdir, "watcher"), exist_ok=True)
    open(os.path.join(tdir, "watcher", "sub-weekly"), "w").close()

    # ---- 活性代理三判据 ----
    for name, want in (("live-proc", True), ("live-box", True), ("sub-only", True),
                       ("dead-final", False), ("dead-empty", False), ("ghost", False)):
        alive, why = r._pid_active("bot/" + name)
        ok("H13 活性代理：bot/%s → %s" % (name, "活" if want else "不活"),
           alive is want and (bool(why) is not want), (alive, why))
    ok("H13 活性代理：不活原因区分「目录不存在」与「无消费者」",
       r._pid_active("bot/ghost")[1] == "目录不存在"
       and "无消费者" in r._pid_active("bot/dead-empty")[1],
       (r._pid_active("bot/ghost")[1], r._pid_active("bot/dead-empty")[1]))
    ok("H13 活性代理：id 文法非法不抛（返回不活）",
       r._pid_active("bot/a/b") == (False, "id 文法非法"), r._pid_active("bot/a/b"))
    cache = {}
    first = r._pid_active("bot/dead-empty", cache)
    ok("H13 活性备忘录：每轮 tick 一份缓存（判定结果 + 键 \"*\" = 订阅声明全集，判据③ 只扫一次全树）",
       r._pid_active("bot/dead-empty", cache) == first
       and cache.get("bot/dead-empty") == first and "*" in cache
       and "bot/sub-only" in cache["*"], (first, sorted(cache)[:4]))
    ok("H13 缓存面不改判定（带/不带缓存同结论）",
       r._pid_active("bot/sub-only", cache) == r._pid_active("bot/sub-only") == (True, ""))

    # ---- resolve_reaper 两档 ----
    got = r.resolve_reaper({"reaper": "bot/live-box", "creator": "topic/dispatcher"})
    ok("H13 reaper 显式字段（creator 不参与推导）",
       got["pid"] == "bot/live-box"
       and got["inbox"] == os.path.join(root, "agents", "bot", "live-box", "inbox")
       and got["note"] is None, got)
    got = r.resolve_reaper({"creator": "bot/live-proc"})
    ok("H13 无 reaper 字段 → 回落职位信箱 + note（creator 回落档已裁）",
       got["pid"] == "topic/dispatcher" and got["inbox"] == pos
       and "缺 reaper 字段" in (got["note"] or ""), got)
    got = r.resolve_reaper({})
    ok("H13 spec 空 → 回落职位信箱 + note",
       got["pid"] == "topic/dispatcher" and got["note"] is not None, got)
    got = r.resolve_reaper({"reaper": "bot/ghost"})
    ok("H13 reaper 目录不存在 → 回落职位信箱 + note「不存在，回落职位信箱」",
       got["pid"] == "topic/dispatcher" and got["inbox"] == pos
       and got["note"] == "reaper bot/ghost 不存在，回落职位信箱", got)
    got = r.resolve_reaper({"reaper": "bot/dead-final"})
    ok("H13 reaper 目录在场但无消费者 → 回落职位信箱 + note 点名成因",
       got["pid"] == "topic/dispatcher" and "无消费者" in (got["note"] or "")
       and got["note"].startswith("reaper bot/dead-final "), got)
    got = r.resolve_reaper({"reaper": "topic/dispatcher"})
    ok("H13 reaper = 职位信箱本身 → 直落回落面（不做活性判定、不带 note）",
       got["pid"] == "topic/dispatcher" and got["note"] is None, got)
    got = r.resolve_reaper({"reaper": "bot/a/b"})
    ok("H13 reaper 文法非法 → 回落职位信箱 + note（不抛）",
       got["pid"] == "topic/dispatcher" and "缺 reaper 字段" in (got["note"] or ""), got)
    # ---- 判重标记 = 唯一事实源（集合语义；无信箱回落扫描/补写面） ----
    pid_, adir, _sp, _doc = mktask(root, "h13m")
    ok("H13 标记缺失 → _mark_doc 返回 None", r._mark_doc(pid_) is None)
    proto.atomic_write_json(os.path.join(adir, R.NOTIFY_MARK),
                            {"to": ["topic/dispatcher"], "ts": proto.now_ts(),
                             "event": "task_done", "complete": True})
    ok("H13 集合内收件方 → 已通知；集合外 → 未通知（可投）",
       r._already_notified(pid_, "topic/dispatcher") is True
       and r._already_notified(pid_, "bot/w9") is False)
    # 信箱里的存量终态信封不参与判重（回落扫描面已裁）：无标记即未通知
    pid2, _adir2, _sp2, _doc2 = mktask(root, "h13b")
    mid = "2026-09-06-00-00-00.000-agentd-" + proto.rand_suffix()
    proto.atomic_write_json(os.path.join(pos, mid + ".msg"), {
        "id": mid, "from": "agentd", "ts": "2026-09-06-00:00:00.000", "type": "inform",
        "body": json.dumps({"event": "task_done", "taskId": pid2}, ensure_ascii=False)})
    ok("H13 信箱信封不判重（历史档案的重放由重放护栏兜住，见 e2e S44）",
       r._already_notified(pid2, "topic/dispatcher") is False
       and r._mark_doc(pid2) is None, r._mark_doc(pid2))


def h14():
    """子端 ask 写侧收件面 env（项 1，任务）：`_ask_inbox_env` **复用 resolve_reaper 单点**
    （不在 .ts 侧另立第二套解析），reaper 活 → AGENTD_ASK_INBOX 指 reaper 自家信箱、不写 note 枚；
    reaper 不活/不存在 → 回落职位信箱 + AGENTD_ASK_NOTE 点名成因。纯判定面（不起 runner、不真 spawn）。"""
    root = mkroot()
    r = mkrunner(root)
    pos = proto.position_inbox(root)
    _mk_bot(root, "live-proc", pid_final=False)     # 进程型活性 reaper
    _mk_bot(root, "dead-final", pid_final=True)     # 目录在场但无消费者
    live_inbox = os.path.join(root, "agents", "bot", "live-proc", "inbox")

    env = r._ask_inbox_env({"reaper": "bot/live-proc", "creator": "topic/dispatcher"})
    ok("H14 reaper 活 → AGENTD_ASK_INBOX = reaper 自家信箱、不带 AGENTD_ASK_NOTE",
       env.get("AGENTD_ASK_INBOX") == live_inbox and "AGENTD_ASK_NOTE" not in env, env)
    env = r._ask_inbox_env({"reaper": "bot/ghost"})
    ok("H14 reaper 不存在 → 回落职位信箱 + AGENTD_ASK_NOTE「不存在，回落职位信箱」",
       env.get("AGENTD_ASK_INBOX") == pos
       and env.get("AGENTD_ASK_NOTE") == "reaper bot/ghost 不存在，回落职位信箱", env)
    env = r._ask_inbox_env({"reaper": "bot/dead-final"})
    ok("H14 reaper 目录在场但无消费者 → 回落职位信箱 + AGENTD_ASK_NOTE 含「无消费者」",
       env.get("AGENTD_ASK_INBOX") == pos and "无消费者" in env.get("AGENTD_ASK_NOTE", "")
       and env["AGENTD_ASK_NOTE"].startswith("reaper bot/dead-final "), env)
    env = r._ask_inbox_env({"creator": "topic/dispatcher"})
    ok("H14 无 reaper 字段 → 回落职位信箱 + AGENTD_ASK_NOTE 点名成因",
       env.get("AGENTD_ASK_INBOX") == pos
       and "缺 reaper 字段" in env.get("AGENTD_ASK_NOTE", ""), env)
    # 单点复用自证：同一 spec 下 _ask_inbox_env 的 inbox/note 逐字取自 resolve_reaper（无第二套解析）
    for spec in ({"reaper": "bot/live-proc"}, {"reaper": "bot/ghost"}, {"reaper": "bot/dead-final"}):
        rr = r.resolve_reaper(spec)
        env = r._ask_inbox_env(spec)
        ok("H14 _ask_inbox_env 与 resolve_reaper 同源（单点复用：%s）" % spec.get("reaper"),
           env.get("AGENTD_ASK_INBOX") == rr["inbox"]
           and env.get("AGENTD_ASK_NOTE") == rr["note"], (env, rr))
    ok("H14 env 值恒为 str（spawn env.update 安全）",
       all(isinstance(v, str) for v in r._ask_inbox_env({"reaper": "bot/ghost"}).values()))




def main():
    for fn in (h9, h13, h14):
        fn()
    print("\n==== runner notify 单测：%d passed, %d failed" % (PASS, FAIL))
    for d in TMP:
        shutil.rmtree(d, ignore_errors=True)
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
