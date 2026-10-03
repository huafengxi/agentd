#!/usr/bin/env python3
"""test_runner_final.py — runner 生命周期收口面单测（F 系列）：`final` 的赋值判据。

裁定（agentfw 域，口径正文 = README「约定赋值与实现口径」）：`final` = **生命周期吸收态**，
其赋值**不依赖** `restartPolicy` 是否为 `one-shot`——非常驻参与方（判据单点 =
`scheduler.is_resident`）到**代终态**即 `final`；`restartPolicy` 只管**自愈**（判据逐字
`== "auto"`）；常驻体不因代终态收口。finalize **每轮幂等评估** ⇒ 存量「代终态 ∧ 未 final」
档案（历史上 = CLI 登记缺 `restartPolicy` 键者死后永不收口 ⇒ 终态通知永不发出、
`provides` 恒 pending 挡住 `needs`）在守护重启后自愈，不需人工补 `pid.json`。

用例：
  F1 无 `restartPolicy` 键的非常驻任务真跑到代终态 ⇒ `final=true`（两层谓词同时成立）
  F2 该档案被 `notify_tick` 发出终态通知（信封落 reaper 自家信箱 ∧ `notified.json` 落盘 ∧ 幂等）
  F3 其 `provides` 被 scheduler 解析为 `success` ⇒ 依赖它 `needs` 的任务放行（+ 抹掉 final 的对照）
  F4 存量自愈：预造「stale/127 ∧ final=false」档案，一轮 `service()` ⇒ final=true（只收口不换代）
  F5 常驻体（`AGENTD_RESIDENT=1`）到代终态 ⇒ final 仍为 false（常驻语义不变）
  F6 `restartPolicy=auto` 的非常驻体到代终态 ⇒ final 仍为 false **且自愈仍触发**（换代 running）
  F7 回归对照：`one-shot` 非常驻 ⇒ final；`one-shot` 常驻 ⇒ final；`manual` 非常驻 ⇒ final
  F8 两个不得被破坏的既有分支：他机任务只读（零写入）∧ 已 final 的吸收态早退（零写入）

仅标准库。用法：python3 agentd/test_runner_final.py
"""
import glob
import json
import os
import shutil
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import proto            # noqa: E402
import runner as R      # noqa: E402
import scheduler as S   # noqa: E402

PASS = 0
FAIL = 0
TMP = []

NO_KEY = object()      # 哨兵：spec 整个键不写（= `agentctl create` 缺省形态）
HOST = "testhost"


def ok(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("PASS  %s" % name)
    else:
        FAIL += 1
        print("FAIL  %s  %s" % (name, detail))


def mkroot():
    root = tempfile.mkdtemp(prefix="agentd-final.%d." % os.getpid())
    TMP.append(root)
    os.makedirs(os.path.join(root, "agents", "task"))
    os.makedirs(proto.position_inbox(root), exist_ok=True)
    return root


def mkrunner(root, **kw):
    return R.Runner(root, HOST, "", 0.5, **kw)


def mktask(root, tid, command="true", policy=NO_KEY, host=HOST, extra=None,
           pid=None, enable=False):
    """合成参与方档案。`pid=None` = 不写 pid.json（未启动形态）；否则以给定字典整体落盘。
    `policy=NO_KEY` = spec 不写 `restartPolicy` 键（CLI 缺省形态，本文件的主测面）。"""
    pid_ = "task/" + tid
    adir = proto.agent_dir(root, pid_)
    os.makedirs(os.path.join(adir, "inbox"), exist_ok=True)
    spec = {"name": "t-" + tid, "workdir": root, "host": host, "command": command}
    if policy is not NO_KEY:
        spec["restartPolicy"] = policy
    spec.update(extra or {})
    proto.atomic_write_json(proto.spec_path(root, pid_), spec)
    if pid is not None:
        proto.atomic_write_json(proto.pid_path(root, pid_), pid)
    if enable:
        proto.atomic_write_json(proto.enable_path(root, pid_),
                                {"ts": proto.now_ts(), "by": "test", "note": "F 夹具"})
    return pid_, adir, spec


def terminal_doc(status="stale", exitcode=proto.EXITCODE_STALE, gen=1, final=False,
                 pid=424242, **over):
    """代终态 pid.json 夹具（缺省 = 接管孤儿形态 stale/127）。"""
    base = proto.now_ts()
    doc = {"gen": gen, "pid": pid, "procStart": 1, "status": status,
           "exitcode": exitcode, "restarts": gen - 1, "final": final,
           "startedAt": base, "lastAliveAt": base, "endedAt": base}
    doc.update(over)
    return doc


def pdoc(root, pid_):
    return proto.read_json(proto.pid_path(root, pid_))


def spin(r, pid_, pred, timeout=15.0):
    """有界轮询：每轮跑一次 `service()` 后取**盘上现值**判定（⛔ 无上界 wait）。
    返回首个满足 pred 的 pid.json；超时返回 None（调用方按失败呈现）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        r.service(pid_)
        doc = pdoc(r.root, pid_)
        if pred(doc):
            return doc
        time.sleep(0.05)
    return None


def mkbot(root, name, pid_final=False):
    """合成 reaper bot（活性代理判据①：pid.json 可读 ∧ final != true）。"""
    d = os.path.join(root, "agents", "bot", name)
    os.makedirs(os.path.join(d, "inbox"), exist_ok=True)
    proto.atomic_write_json(os.path.join(d, "pid.json"),
                            {"gen": 1, "pid": 424242, "status": "running",
                             "final": pid_final})
    return d


def write_report(adir, text="# report\n\u2705 全部验收项通过\n"):
    p = os.path.join(adir, "report.md")
    with open(p, "w") as f:
        f.write(text)
    return p


def envelopes(directory):
    out = []
    for fn in sorted(glob.glob(os.path.join(directory, "*.msg"))):
        with open(fn) as f:
            env = json.load(f)
        try:
            payload = json.loads(env.get("body") or "{}")
        except ValueError:
            payload = {}
        out.append((fn, env, payload))
    return out


# --------------------------------------------------------------- F1（缺键 → 代终态即 final）

def f1():
    root = mkroot()
    r = mkrunner(root)
    pid_, adir, _spec = mktask(root, "f1", command="true", enable=True)
    with open(proto.spec_path(root, pid_)) as f:
        raw = f.read()
    ok("F1 夹具前提：spec 里 `restartPolicy` 键整个不在场（CLI 缺省形态）",
       "restartPolicy" not in raw, raw)
    doc = spin(r, pid_, lambda d: bool(d) and d.get("final") is True)
    ok("F1 无 restartPolicy 键的非常驻任务到代终态 ⇒ final=true",
       doc is not None and doc.get("final") is True, pdoc(root, pid_))
    doc = pdoc(root, pid_)
    ok("F1 死因记账如实（exited/0）且两层谓词同时成立（gen_terminal ∧ life_terminal）",
       doc.get("status") == "exited" and doc.get("exitcode") == 0
       and proto.gen_terminal(doc) is True and proto.life_terminal(doc) is True, doc)
    ok("F1 收尾后句柄已出表（无遗留子进程监督面）", pid_ not in r.popen, sorted(r.popen))


# --------------------------------------------------------------- F2（终态通知发出）

def f2():
    root = mkroot()
    r = mkrunner(root)
    mkbot(root, "rp")
    pid_, adir, _spec = mktask(root, "f2", command="true", enable=True,
                              extra={"reaper": "bot/rp"})
    write_report(adir)
    doc = spin(r, pid_, lambda d: bool(d) and d.get("final") is True)
    ok("F2 前置：档案已收口（final=true）", doc is not None, pdoc(root, pid_))
    r.notify_tick()
    rp_inbox = os.path.join(root, "agents", "bot", "rp", "inbox")
    got = envelopes(rp_inbox)
    ok("F2 终态通知发出：信封落 reaper 自家信箱（from=agentd、type=inform、event=task_done）",
       len(got) == 1 and got[0][1].get("from") == "agentd"
       and got[0][1].get("type") == "inform"
       and got[0][2].get("event") == "task_done"
       and got[0][2].get("taskId") == pid_, [(e[1].get("from"), e[2]) for e in got])
    ok("F2 载荷带 status/exitcode/report 路径（完成判定自证件在场）",
       got and got[0][2].get("status") == "exited" and got[0][2].get("exitcode") == 0
       and got[0][2].get("report") == os.path.join(adir, "report.md"),
       got[0][2] if got else None)
    mark_path = os.path.join(adir, R.NOTIFY_MARK)
    mark = proto.read_json(mark_path)
    ok("F2 notified.json 落盘（判重唯一事实源：to 含该收件方、complete=true）",
       isinstance(mark, dict) and mark.get("complete") is True
       and "bot/rp" in (mark.get("to") or []), mark)
    r.notify_tick()
    r.notify_tick()
    ok("F2 幂等：多轮 notify_tick 不重发（信封仍 1 份）",
       len(envelopes(rp_inbox)) == 1, len(envelopes(rp_inbox)))
    ok("F2 reaper 命中直投 ⇒ 职位信箱零份",
       not [e for e in envelopes(proto.position_inbox(root))
            if e[1].get("from") == "agentd"],
       envelopes(proto.position_inbox(root)))


# --------------------------------------------------------------- F3（provides 被解析 → 放行）

def f3():
    root = mkroot()
    r = mkrunner(root)
    prov, prov_dir, _sp = mktask(root, "f3-prov", command="true", enable=True,
                                extra={"provides": ["cap:f3"]})
    write_report(prov_dir)
    ok("F3 前置：provider 真跑到 final",
       spin(r, prov, lambda d: bool(d) and d.get("final") is True) is not None,
       pdoc(root, prov))
    dep, _dep_dir, _dsp = mktask(root, "f3-dep", extra={"needs": ["cap:f3"],
                                                       "resources": []})
    # 放行门禁第四条件 = 目标主机存活（探活锁新鲜）；单测里合成一枚新鲜锁
    proto.atomic_write_json(proto.agentd_lock_path(root, HOST),
                            {"host": HOST, "pid": os.getpid(), "procStart": 1,
                             "startedAt": proto.now_ts(), "updatedAt": proto.now_ts()})
    s = S.Scheduler(root, [HOST])
    _occ, _held, providers, _q = s._scan()
    ok("F3 provides 被解析为 success（缺键档案不再恒 pending）",
       providers.get("cap:f3") == [(prov, "success")], providers)
    ok("F3 eval_needs 判 ok（依赖可满足）",
       S.eval_needs(["cap:f3"], providers)[0] == "ok",
       S.eval_needs(["cap:f3"], providers))
    s.tick()
    en = proto.read_json(proto.enable_path(root, dep))
    ok("F3 依赖它 needs 的任务被放行（enable.json 落盘、by=agentd-scheduler、note 含 dag）",
       isinstance(en, dict) and en.get("by") == S.SCHEDULER_ID
       and "dag" in (en.get("note") or ""), en)
    # 对照（根因直证）：只把 final 抹掉，同一档案立刻退回 pending、依赖者判 wait
    doc = pdoc(root, prov)
    doc["final"] = False
    proto.atomic_write_json(proto.pid_path(root, prov), doc)
    _o2, _h2, prov2, _q2 = s._scan()
    ok("F3 对照：抹掉 final ⇒ 同档案退回 pending（根因 = final 缺位，不是判定面写错）",
       prov2.get("cap:f3") == [(prov, "pending")]
       and S.eval_needs(["cap:f3"], prov2)[0] == "wait", prov2)
    doc["final"] = True
    proto.atomic_write_json(proto.pid_path(root, prov), doc)


# --------------------------------------------------------------- F4（存量自愈）

def f4():
    root = mkroot()
    r = mkrunner(root)
    ended = "2026-10-03-12-05-27.000"
    pid_, adir, _spec = mktask(
        root, "f4", command="true",
        pid=terminal_doc(status="stale", exitcode=127, endedAt=ended))
    ok("F4 夹具前提：存量卡死形态（代终态 ∧ final=false ∧ 无 restartPolicy 键）",
       pdoc(root, pid_).get("final") is False
       and pdoc(root, pid_).get("status") == "stale"
       and "restartPolicy" not in open(proto.spec_path(root, pid_)).read())
    r.service(pid_)                      # 一轮
    doc = pdoc(root, pid_)
    ok("F4 存量自愈：一轮 service() 后 final=true（不需人工补 pid.json）",
       doc.get("final") is True and proto.life_terminal(doc) is True, doc)
    ok("F4 只收口不换代：gen/status/exitcode/endedAt 逐字不变 ∧ 未 spawn",
       doc.get("gen") == 1 and doc.get("status") == "stale"
       and doc.get("exitcode") == 127 and doc.get("endedAt") == ended
       and pid_ not in r.popen, doc)
    m1 = os.path.getmtime(proto.pid_path(root, pid_))
    for _ in range(3):
        r.service(pid_)
    ok("F4 幂等：吸收态早退 ⇒ 后续轮次零写入（pid.json mtime 不变）",
       os.path.getmtime(proto.pid_path(root, pid_)) == m1,
       (m1, os.path.getmtime(proto.pid_path(root, pid_))))
    # 收口后终态通知照发（本次运行见证过它非终态 ⇒ 不被重放护栏抑制）
    write_report(adir)
    r.notify_tick()
    got = [e for e in envelopes(proto.position_inbox(root))
           if e[2].get("taskId") == pid_]
    ok("F4 自愈后终态通知补发（stale/127 + 非空 report.md ⇒ event=task_done）",
       len(got) == 1 and got[0][2].get("event") == "task_done", [e[2] for e in got])


# --------------------------------------------------------------- F5（常驻体语义不变）

def f5():
    root = mkroot()
    r = mkrunner(root)
    cmd = "AGENTD_RESIDENT=1 exec sleep 30"
    pid_, _adir, spec = mktask(root, "f5", command=cmd, pid=terminal_doc())
    ok("F5 夹具前提：判为常驻（runner 转调的单点与 scheduler 单点同结论）",
       R.spec_is_resident(spec) is True and S.is_resident(spec) is True, spec)
    for _ in range(3):
        r.service(pid_)
    doc = pdoc(root, pid_)
    ok("F5 常驻体到代终态 ⇒ final 仍为 false（不因代终态被收口）",
       doc.get("final") is not True and proto.life_terminal(doc) is False
       and proto.gen_terminal(doc) is True, doc)
    ok("F5 常驻体不被拉起（policy 非 auto ⇒ 不自愈）∧ 未 spawn",
       doc.get("gen") == 1 and doc.get("status") == "stale" and pid_ not in r.popen, doc)


# --------------------------------------------------------------- F6（auto 自愈判据不变）

def f6():
    root = mkroot()
    r = mkrunner(root)
    pid_, _adir, _spec = mktask(root, "f6", command="sleep 30", policy="auto",
                               pid=terminal_doc(gen=1))
    r.service(pid_)
    doc = pdoc(root, pid_)
    ok("F6 auto 的非常驻体到代终态 ⇒ final 仍为 false（生命周期开放，等自愈）",
       doc is not None and doc.get("final") is not True, doc)
    ok("F6 auto 自愈仍触发：换代 gen=2、新代 running、写 resumed=1",
       doc.get("gen") == 2 and doc.get("status") == "running"
       and doc.get("resumed") == 1, doc)
    r.notify_tick()
    ok("F6 生命周期仍开放 ⇒ 不发终态通知（notify_tick 的门 = life_terminal）",
       not [e for e in envelopes(proto.position_inbox(root))
            if e[1].get("from") == "agentd"],
       envelopes(proto.position_inbox(root)))
    # 收尾：走生产收口路径 stop（杀掉新代，不留测试进程）
    r.do_stop(pid_, {"id": "f6-stop", "from": "task/tester", "action": "stop"},
              pdoc(root, pid_))
    d2 = pdoc(root, pid_)
    h = r.popen.get(pid_)
    deadline = time.time() + 10.0
    while h is not None and h.poll() is None and time.time() < deadline:
        time.sleep(0.05)
    ok("F6 收尾：stop 后 final=true 且新代进程已死（无遗留测试进程）",
       d2.get("final") is True and (h is None or h.poll() is not None),
       (d2.get("final"), None if h is None else h.poll()))


# --------------------------------------------------------------- F7（回归对照）

def f7():
    root = mkroot()
    r = mkrunner(root)
    a, _ad, _s = mktask(root, "f7-oneshot", command="true", policy="one-shot",
                        pid=terminal_doc(status="exited", exitcode=0))
    r.service(a)
    ok("F7 one-shot 非常驻到代终态 ⇒ final（既有行为不回归）",
       pdoc(root, a).get("final") is True, pdoc(root, a))
    b, _bd, _bs = mktask(root, "f7-res-oneshot",
                         command="AGENTD_RESIDENT=1 exec true", policy="one-shot",
                         pid=terminal_doc(status="exited", exitcode=0))
    r.service(b)
    ok("F7 one-shot 的常驻体 ⇒ 仍 final（显式 one-shot = 第一代终止即生命周期终结）",
       pdoc(root, b).get("final") is True, pdoc(root, b))
    c, _cd, _cs = mktask(root, "f7-manual", command="true", policy="manual",
                         pid=terminal_doc(status="killed", exitcode=137))
    r.service(c)
    ok("F7 manual 非常驻 ⇒ final（与缺键同族：不自愈，但仍收口、仍发终态通知）",
       pdoc(root, c).get("final") is True, pdoc(root, c))
    ok("F7 三者都不换代（gen 仍为 1）",
       all(pdoc(root, x).get("gen") == 1 for x in (a, b, c)),
       [pdoc(root, x).get("gen") for x in (a, b, c)])


# --------------------------------------------------------------- F8（两个不得破坏的分支）

def f8():
    root = mkroot()
    r = mkrunner(root)
    pid_, _adir, _spec = mktask(root, "f8-foreign", command="true",
                               host="other-machine", pid=terminal_doc())
    p = proto.pid_path(root, pid_)
    before = (os.path.getmtime(p), open(p).read())
    for _ in range(3):
        r.service(pid_)
    after = (os.path.getmtime(p), open(p).read())
    ok("F8 他机任务只读：路由分支不被破坏（一个字节也不写、final 仍 false）",
       before == after and pdoc(root, pid_).get("final") is not True, after[1])
    pid2, _ad2, _s2 = mktask(root, "f8-final", command="true",
                            pid=terminal_doc(status="exited", exitcode=0, final=True))
    p2 = proto.pid_path(root, pid2)
    b2 = (os.path.getmtime(p2), open(p2).read())
    for _ in range(3):
        r.service(pid2)
    ok("F8 已 final 的吸收态早退：再跑 service() 零写入、不 spawn",
       (os.path.getmtime(p2), open(p2).read()) == b2 and pid2 not in r.popen, b2[1])


def main():
    for fn in (f1, f2, f3, f4, f5, f6, f7, f8):
        fn()
    print("\n==== runner final 语义单测：%d passed, %d failed" % (PASS, FAIL))
    for d in TMP:
        shutil.rmtree(d, ignore_errors=True)
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
