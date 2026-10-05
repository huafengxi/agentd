#!/usr/bin/env python3
"""test_report_slots.py — report.py 排队「卡点」列与「应跑未跑」门禁的**占位上限层次**单测。

裁定（agentfw 域）：饱和判据的上限取**运行态层**（实跑 scheduler 进程 argv 的
`--max-concurrent`，判据单点 = `report.effective_max_concurrent`），⛔ 本体层缺省
`scheduler.DEFAULT_MAX_CONCURRENT`——现网装配声明层（`scheduler-loop.sh` 的
`MAX_CONCURRENT`）覆盖该缺省 ⇒ 拿缺省值判会在 `occ ≥ 缺省` 期间 ① 把真卡点（资源/主机/
缺 host）盖成「等槽位（N/N 满）」② 短路「应跑未跑」采集（用户 2026-08-28 拍板的门禁静默）。
上限不可得（本机无管这棵树的 scheduler ∕ 多枚值不一致）⇒ **不断言「满」**、采集照常跑。
层次用词（本体/装配声明/运行态）同 `lore/library/agentfw/facts/task-book-authoring.md`
「实际落点层次」条的层次候选族。

用例：
  R1 层次错配修复：occ=4（= 本体层缺省）∧ 目标主机不存活 ∧ 上限不可得 ⇒ 卡点列 = 主机层
  R2 反向用例（真饱和）：运行态上限 4 ∧ occ=5 ⇒ 卡点列 = 等槽位（5/4 满·上限=运行态层），
     两数同层，且槽位层仍先于主机层（门禁序 依赖→资源→槽位→主机 未改）
  R3 「应跑未跑」恢复采集：运行态上限 10 ∧ occ=4（≥ 本体层缺省）∧ 四门全满足
     ⇒ 异常区出「🚨 应跑未跑」（改前被 slots_full 短路 ⇒ 不出现）
  R4 降级不断言：上限不可得 ∧ occ=4 ⇒ 任何排队件都不报「等槽位」，采集照常，系统节标「⚠️ 不可得」
  R5 argv 解析：`--max-concurrent N` ∕ `=N` ∕ 未传 ∕ 非整数 ∕ 非正 五格
  R6 进程识别三重判据：真 argv 命中；包装壳（bash -c 提到该路径）∕ 他 root ∕ 缺 --root 排除；
     `--root=` 形态与符号链接 root（realpath 同实体）命中
  R7 effective_max_concurrent 端到端（真进程）：显式值 ∕ argv 未传（落回本体层缺省）∕
     无进程（不可得）∕ 两枚异值（不一致 ⇒ 不可断言）；note 逐格带层次名
  R8 dash/gc-tasks.py 的复用面不破：`ts_to_epoch` 签名与两种时间戳格式的解析值不变

仅标准库。用法：python3 agentd/test_report_slots.py
"""
import datetime
import inspect
import os
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import proto            # noqa: E402
import report as RP     # noqa: E402
import scheduler as S   # noqa: E402

PASS = 0
FAIL = 0
PROCS = []
TMPBASE = None           # 本套唯一工作根（专用临时根；夹具树与存根脚本均在其下）
TMP_PREFIX = "agentd-repslots.%d." % os.getpid()
STUB_SLEEP = 25          # 假 scheduler 存根寿命上界（秒）；用例内另显式 kill（双重上界）
STUB_WAIT_TRIES = 30     # 等存根被 pgrep 看见的轮数（× 0.1s ⇒ 上界 3s）
STUB_SEQ = [0]           # 存根脚本目录序号（多枚并存时不互盖）
HOST_ALIVE = "testhost"
HOST_DEAD = "deadhost"


def ok(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("PASS  %s" % name)
    else:
        FAIL += 1
        print("FAIL  %s  %s" % (name, detail))


# ---- 临时根夹具（工作根 = 专用临时根；生产树只经 sys.path 只读引用） ----

def mkroot():
    """一枚夹具树（在 TMPBASE 下）；返回的 root 即报表与探针的 --root。"""
    root = tempfile.mkdtemp(prefix="root.", dir=TMPBASE)
    os.makedirs(os.path.join(root, "agents", "task"))
    os.makedirs(os.path.join(root, "agents", "run"))
    return root


def mktask(root, tid, host, resources, running=False):
    """合成参与方档案：running=True 写 pid.json（status=running、心跳新鲜）⇒ 计入占位面。"""
    pid_ = "task/" + tid
    adir = proto.agent_dir(root, pid_)
    os.makedirs(os.path.join(adir, "inbox"), exist_ok=True)
    proto.atomic_write_json(proto.spec_path(root, pid_), {
        "name": "repslots-" + tid, "workdir": root, "host": host,
        "command": "true", "resources": resources,
        "createdAt": time.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
    })
    if running:
        proto.atomic_write_json(proto.pid_path(root, pid_), {
            "gen": 1, "pid": 999999, "status": "running", "restarts": 0,
            "startedAt": proto.now_ts(), "lastAliveAt": proto.now_ts(),
            "final": False,
        })
    return pid_


def mkhostlock(root, host):
    """写目标主机探活锁（updatedAt 新鲜 ⇒ scheduler.host_runner_alive 判活）。"""
    proto.atomic_write_json(proto.agentd_lock_path(root, host), {
        "host": host, "pid": os.getpid(), "updatedAt": proto.now_ts()})


def fill_occ(root, n):
    """n 枚占位者（各持独立资源，与候补的资源无交集）。返回 occ 实测值。"""
    for i in range(n):
        mktask(root, "occ%d" % i, HOST_ALIVE, ["repo:repslots-%d" % i], running=True)
    occ, held, _p, _q = S.Scheduler(root, proto.local_hosts(), all_hosts=True)._scan()
    return len(occ), sorted(held)


def spawn_stub_scheduler(root, max_concurrent=None, script_dir=None):
    """起一枚**假 scheduler 进程** = 运行态层夹具（⛔ 真调度器：sleep 存根，不写 enable、
    不动任何树）。argv 形态与生产同族：`python3 <…>/agentd/scheduler.py --root <root>
    [--max-concurrent N] --all-hosts --host testhost`。返回 Popen（调用方 kill；
    另有 STUB_SLEEP 寿命上界兜底）。"""
    STUB_SEQ[0] += 1
    script_dir = script_dir or os.path.join(TMPBASE, "stubbin%d" % STUB_SEQ[0])
    d = os.path.join(script_dir, "agentd")
    os.makedirs(d, exist_ok=True)
    stub = os.path.join(d, "scheduler.py")
    with open(stub, "w", encoding="utf-8") as f:
        f.write("import time\ntime.sleep(%d)\n" % STUB_SLEEP)
    argv = [sys.executable, stub, "--root", root, "--all-hosts", "--host", HOST_ALIVE]
    if max_concurrent is not None:
        argv += ["--max-concurrent", str(max_concurrent)]
    p = subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    PROCS.append(p)
    return p


def wait_probe(root, want):
    """有界轮询等 pgrep 看见存根（上界 = STUB_WAIT_TRIES × 0.1s = 3s）。返回末次读数。"""
    got = (None, "")
    for _i in range(STUB_WAIT_TRIES):
        got = RP.effective_max_concurrent(root)
        if got[0] == want:
            return got
        time.sleep(0.1)
    return got


def dep_cell(root, tid):
    """跑一次 build_report，取该 taskId 在「活跃任务」表的『调度依赖』列（第 8 格）。"""
    text, _tasks = RP.build_report(root, time.time())
    sec = RP.extract_section(text, "active")
    for ln in sec.split("\n"):
        if "`%s`" % tid in ln:
            cells = [c.strip() for c in ln.strip().strip("|").split("|")]
            return cells[7], text
    return None, text


def abn_lines(text):
    return [ln for ln in RP.extract_section(text, "abnormal").split("\n")
            if "应跑未跑" in ln]


def killall():
    for p in PROCS:
        if p.poll() is None:
            p.kill()
        try:
            p.wait(timeout=5)      # 有界等待（⛔ 无条件 wait）
        except subprocess.TimeoutExpired:
            print("WARN  存根未在 5s 内退出（寿命上界 %ds 会自行结束）" % STUB_SLEEP)
    del PROCS[:]


def cleanup():
    """删自家工作根 TMPBASE：realpath 前缀断言（等于/包含/内嵌生产根一律拒删）。
    ⛔ ignore_errors（拒删时不删、打印一行）。"""
    if TMPBASE is None:
        return
    base = os.path.realpath(tempfile.gettempdir())
    prod = os.path.realpath(os.path.dirname(HERE))
    rd = os.path.realpath(TMPBASE)
    if not (rd.startswith(base + os.sep)
            and os.path.basename(rd).startswith("agentd-repslots.")):
        print("REFUSE rmtree（不在临时基目录下 ∨ 非本夹具自建）：%s" % rd)
        return
    if rd == prod or prod.startswith(rd + os.sep) or rd.startswith(prod + os.sep):
        print("REFUSE rmtree（与生产根同形/嵌套）：%s vs %s" % (rd, prod))
        return
    shutil.rmtree(rd)
    if os.path.exists(rd):
        print("WARN  工作根删后仍在场：%s" % rd)


# ---- 用例 ----

def r1():
    """R1 层次错配修复：occ = 本体层缺省 4，真卡点在主机层 ⇒ ⛔ 报「等槽位」。"""
    root = mkroot()
    occ, _held = fill_occ(root, S.DEFAULT_MAX_CONCURRENT)
    tid = mktask(root, "r1-queued", HOST_DEAD, ["repo:repslots-free"])
    cell, text = dep_cell(root, tid)
    ok("R1 前置：occ == 本体层缺省 %d（改前该形态必报「等槽位」）" % S.DEFAULT_MAX_CONCURRENT,
       occ == S.DEFAULT_MAX_CONCURRENT, occ)
    ok("R1 前置：目标主机 %s 确实不存活（真卡点 = 主机层）" % HOST_DEAD,
       S.host_runner_alive(root, [HOST_DEAD])[0] is False)
    ok("R1 卡点列 = 主机层（不再是「等槽位」）", cell == "⛔等主机 `%s` runner 存活" % HOST_DEAD,
       cell)
    ok("R1 卡点列不含「等槽位」", cell is not None and "等槽位" not in cell, cell)
    ok("R1 系统节标注上限不可得（本机无管本树的 scheduler）",
       "占位上限（有效层）：⚠️ 不可得" in text, [l for l in text.split("\n") if "占位上限" in l])


def r2():
    """R2 反向用例：运行态上限 4 ∧ occ=5 ⇒ 报「等槽位（5/4 满·上限=运行态层）」，两数同层。"""
    root = mkroot()
    occ, _held = fill_occ(root, 5)
    tid = mktask(root, "r2-queued", HOST_DEAD, ["repo:repslots-free"])
    spawn_stub_scheduler(root, max_concurrent=4)
    limit, note = wait_probe(root, 4)
    cell, text = dep_cell(root, tid)
    ok("R2 前置：occ=5 ∧ 运行态上限探针取到 4（note 带层次名）",
       occ == 5 and limit == 4 and "运行态层" in note, (occ, limit, note))
    ok("R2 卡点列 = 等槽位（%d/%d 满·上限=运行态层）" % (occ, limit),
       cell == "⛔等槽位（%d/%d 满·上限=运行态层）" % (occ, limit), cell)
    ok("R2 两数同层：格内上限 == 运行态探针值（⛔ 本体层缺省 %d）" % S.DEFAULT_MAX_CONCURRENT,
       cell is not None and ("%d 满" % limit) in cell and limit == 4, cell)
    ok("R2 门禁序未改：槽位层仍先于主机层（该件主机同样不存活）",
       cell is not None and "runner 存活" not in cell, cell)
    ok("R2 系统节给出运行态层取证（pid + argv 键 + 本体层缺省未生效）",
       any("占位上限（有效层）：4 ｜ 运行态层：scheduler pid " in ln
           and "--max-concurrent=4" in ln and "本体层缺省 %d 未生效" % S.DEFAULT_MAX_CONCURRENT in ln
           for ln in text.split("\n")),
       [l for l in text.split("\n") if "占位上限" in l])


def r3():
    """R3 「应跑未跑」恢复采集：运行态上限 10 ∧ occ=4（≥ 本体层缺省）∧ 四门全满足。"""
    root = mkroot()
    mkhostlock(root, HOST_ALIVE)
    occ, _held = fill_occ(root, S.DEFAULT_MAX_CONCURRENT)     # occ=4 = 本体层缺省
    tid = mktask(root, "r3-shouldrun", HOST_ALIVE, ["repo:repslots-free"])
    spawn_stub_scheduler(root, max_concurrent=10)
    limit, note = wait_probe(root, 10)
    _cell, text = dep_cell(root, tid)
    hits = abn_lines(text)
    ok("R3 前置：occ=%d ≥ 本体层缺省 %d ∧ 运行态上限=%d（改前 slots_full=True ⇒ 采集被短路）"
       % (occ, S.DEFAULT_MAX_CONCURRENT, limit),
       occ >= S.DEFAULT_MAX_CONCURRENT and limit == 10 and "运行态层" in note, (occ, limit, note))
    ok("R3 前置：四门全满足（needs 空 ∧ 资源无交集 ∧ 主机存活 ∧ 无 enable/pid）",
       S.host_runner_alive(root, [HOST_ALIVE])[0] is True
       and not os.path.exists(proto.enable_path(root, tid))
       and not os.path.exists(proto.pid_path(root, tid)))
    ok("R3 异常区出现「🚨 应跑未跑」并点名该件（= slots_full 用的是有效上限而非本体层缺省）",
       len(hits) == 1 and tid in hits[0], hits)
    ok("R3 卡点列为空（四门全满足 ⇒ 无可指认卡点，与「应跑未跑」互证）",
       _cell == "-", _cell)


def r4():
    """R4 降级不断言：上限不可得 ∧ occ ≥ 本体层缺省 ⇒ 不报「等槽位」，采集照常。"""
    root = mkroot()
    mkhostlock(root, HOST_ALIVE)
    occ, _held = fill_occ(root, S.DEFAULT_MAX_CONCURRENT + 2)   # occ=6 > 本体层缺省
    t1 = mktask(root, "r4-shouldrun", HOST_ALIVE, ["repo:repslots-free"])
    t2 = mktask(root, "r4-deadhost", HOST_DEAD, ["repo:repslots-free2"])
    limit, note = RP.effective_max_concurrent(root)
    c1, text = dep_cell(root, t1)
    c2, _t = dep_cell(root, t2)
    ok("R4 前置：occ=%d > 本体层缺省 %d ∧ 上限不可得（limit=None）"
       % (occ, S.DEFAULT_MAX_CONCURRENT),
       limit is None and occ > S.DEFAULT_MAX_CONCURRENT, (occ, limit, note))
    ok("R4 任何排队件都不报「等槽位」（主机死的那枚报主机层）",
       "等槽位" not in (c1 or "") and c2 == "⛔等主机 `%s` runner 存活" % HOST_DEAD, (c1, c2))
    ok("R4 「应跑未跑」采集照常跑（不被 slots_full 吞掉）",
       len(abn_lines(text)) == 1 and t1 in abn_lines(text)[0], abn_lines(text))
    ok("R4 系统节明示不可得的原因（读者不会把「无卡点」读成「无上限问题」）",
       any("占位上限（有效层）：⚠️ 不可得 ｜ 本机无 --root 指向本树的 scheduler 进程" in ln
           for ln in text.split("\n")),
       [l for l in text.split("\n") if "占位上限" in l])


def r5():
    """R5 argv 解析五格。"""
    base = ["python3", "/x/agentd/scheduler.py", "--root", "/x"]
    ok("R5 `--max-concurrent 10` → 10", RP._parse_max_concurrent(base + ["--max-concurrent", "10"]) == 10)
    ok("R5 `--max-concurrent=7` → 7", RP._parse_max_concurrent(base + ["--max-concurrent=7"]) == 7)
    ok("R5 未传 → None（语义 = 生效值落回本体层缺省）", RP._parse_max_concurrent(base) is None)
    ok("R5 值非整数 → None", RP._parse_max_concurrent(base + ["--max-concurrent", "abc"]) is None)
    ok("R5 值非正（0 ∕ 负）→ None", RP._parse_max_concurrent(base + ["--max-concurrent", "0"]) is None
       and RP._parse_max_concurrent(base + ["--max-concurrent=-3"]) is None)
    ok("R5 与生产 argv 逐字同形的整串可解析（现网取样形态）",
       RP._parse_max_concurrent(
           ["python3", "/home/u/m/agentd/scheduler.py", "--root", "/home/u/m", "--all-hosts",
            "--host", "samplehost", "--aliases", "sample-alias", "--max-concurrent", "10",
            "--interval", "0.5", "--log-file", "/home/u/m/run/logs/scheduler.log",
            "--log-level", "INFO"]) == 10)


def r6():
    """R6 进程识别三重判据（包装壳 ∕ 他 root ∕ 缺 --root 排除；`--root=` 与符号链接命中）。"""
    root = mkroot()
    real = os.path.realpath(root)
    good = ["python3", "/x/agentd/scheduler.py", "--root", real, "--max-concurrent", "10"]
    ok("R6 真 scheduler argv（同 root）命中", RP._scheduler_argv_matches(good, root) is True)
    ok("R6 `--root=<path>` 形态命中",
       RP._scheduler_argv_matches(
           ["python3", "/x/agentd/scheduler.py", "--root=" + real, "--max-concurrent=10"],
           root) is True)
    ok("R6 包装壳排除（bash -c 的 cmdline 提到该路径；pgrep -f 实测会命中它）",
       RP._scheduler_argv_matches(
           ["/bin/bash", "-c",
            "ps -eo args | grep '[s]cheduler.py'; python3 /x/agentd/scheduler.py --root "
            + real], root) is False)
    ok("R6 他 root 的 scheduler 排除（e2e ∕ 单测临时树 ∪ 人工 --once 调试）",
       RP._scheduler_argv_matches(
           ["python3", "/x/agentd/scheduler.py", "--root", "/tmp/agentd-e2e.123/root"],
           root) is False)
    ok("R6 缺 --root 排除", RP._scheduler_argv_matches(
        ["python3", "/x/agentd/scheduler.py", "--once"], root) is False)
    ok("R6 非 python 解释器排除（argv[0] 判据）",
       RP._scheduler_argv_matches(["/x/agentd/scheduler.py", "--root", real], root) is False)
    # 符号链接 root：realpath 同实体 ⇒ 命中（登记侧 --root 写法差异不得造成假降级）
    link = os.path.join(os.path.dirname(real), "repslots-link-%d" % os.getpid())
    try:
        os.symlink(real, link)
        ok("R6 符号链接 root（realpath 同实体）命中",
           RP._scheduler_argv_matches(
               ["python3", "/x/agentd/scheduler.py", "--root", link], root) is True)
    except OSError as e:
        ok("R6 符号链接 root（realpath 同实体）命中", False, "symlink 失败：%s" % e)
    finally:
        if os.path.islink(link):
            os.unlink(link)     # 只删自己刚建的软链（islink 断言后才动）


def r7():
    """R7 effective_max_concurrent 端到端（真进程）四格 + note 层次名。"""
    root = mkroot()
    limit, note = RP.effective_max_concurrent(root)
    ok("R7 无进程 ⇒ (None, 不可得 note)", limit is None and "本机无 --root 指向本树的 scheduler 进程" in note,
       (limit, note))

    spawn_stub_scheduler(root, max_concurrent=6)
    limit, note = wait_probe(root, 6)
    ok("R7 显式 --max-concurrent 6 ⇒ (6, 运行态层 note 含 pid 与本体层缺省未生效)",
       limit == 6 and "运行态层：scheduler pid " in note and "--max-concurrent=6" in note
       and "本体层缺省 %d 未生效" % S.DEFAULT_MAX_CONCURRENT in note, (limit, note))
    killall()

    root2 = mkroot()
    spawn_stub_scheduler(root2, max_concurrent=None)      # argv 未传 ⇒ 落回本体层缺省
    limit, note = wait_probe(root2, S.DEFAULT_MAX_CONCURRENT)
    ok("R7 argv 未传 ⇒ 生效 = 本体层缺省 %d，note 写明「未传 ⇒ 生效 = 本体层缺省」"
       % S.DEFAULT_MAX_CONCURRENT,
       limit == S.DEFAULT_MAX_CONCURRENT and "argv 未传 --max-concurrent" in note
       and "本体层缺省" in note, (limit, note))
    killall()

    root3 = mkroot()
    spawn_stub_scheduler(root3, max_concurrent=4)
    spawn_stub_scheduler(root3, max_concurrent=6)
    got = (None, "")
    for _i in range(STUB_WAIT_TRIES):
        got = RP.effective_max_concurrent(root3)
        if got[0] is None and "不一致" in got[1]:
            break
        time.sleep(0.1)
    ok("R7 两枚异值（协议禁双调度器的异常形态）⇒ 不可断言 + note 点名不一致与各值",
       got[0] is None and "不一致" in got[1] and "=4" in got[1] and "=6" in got[1], got)
    killall()


def r8():
    """R8 dash/gc-tasks.py 的复用面：ts_to_epoch 签名与解析行为不变。"""
    sig = str(inspect.signature(RP.ts_to_epoch))
    ok("R8 ts_to_epoch 签名仍为单参 (s)（dash/gc-tasks.py:93 复用该单点）", sig == "(s)", sig)
    # 格式一：proto.now_ts（本地时间、点分毫秒）——断言不依赖本机时区
    ok("R8 proto.now_ts 形态按本地时间解析（同秒整值 = mktime，毫秒作小数）",
       RP.ts_to_epoch("2026-10-05-20-00-00.000")
       == time.mktime((2026, 10, 5, 20, 0, 0, 0, 0, -1))
       and abs(RP.ts_to_epoch("2026-10-05-20-00-00.500")
               - RP.ts_to_epoch("2026-10-05-20-00-00.000") - 0.5) < 1e-6)
    # 格式二：spec.createdAt 的 ISO（带偏移）——同一瞬时的两种偏移写法必同值
    ok("R8 ISO 形态按带偏移解析（+08:00 的 20:00 == +00:00 的 12:00，不依赖本机时区）",
       RP.ts_to_epoch("2026-10-05T20:00:00+08:00")
       == RP.ts_to_epoch("2026-10-05T12:00:00+00:00")
       == datetime.datetime(2026, 10, 5, 12, 0, 0,
                            tzinfo=datetime.timezone.utc).timestamp())
    ok("R8 不可解析 → None（半截文件防御不破）",
       RP.ts_to_epoch("") is None and RP.ts_to_epoch(None) is None
       and RP.ts_to_epoch("不是时间戳") is None)


def main():
    global TMPBASE
    TMPBASE = tempfile.mkdtemp(prefix=TMP_PREFIX)
    try:
        for fn in (r1, r2, r3, r4, r5, r6, r7, r8):
            try:
                fn()
            finally:
                killall()
    finally:
        killall()
        cleanup()
    print("\n==== report 占位上限层次单测：%d passed, %d failed" % (PASS, FAIL))
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
