#!/usr/bin/env python3
"""runner.py — 每机一个的中央守护形态 runner（§7.2 合法形态之一，§15.5）。
生产版，自 run2/agent-proto/runner.py 生长（T1-1）。

职责（§7）：写 pid.json（全协议唯一可变档）、写 control/ack/、消费 control/ 请求、
spawn / kill / 判挂死。调度半边是独立模块/独立服务 scheduler.py（
拆出， 彻底去合署， 起 --scheduled 参数
彻底移除）：enable.json 门禁内置、无条件生效（有 spec 还需调度方写 enable.json 才 spawn），
runner 不再实例化/调用调度器；调度方由 scheduler-loop.sh 独立常驻（全局唯一实例）。

判死纪律（计划 §0.b，硬约束）：
  - 本进程 spawn 的进程 → 句柄 poll() 收尾（防线 1，拿得到真死因）；
  - 无句柄（如重启接手）→ (pid,procStart) 二元组探测（防线 2）；
  - **禁止任何裸 waitpid 判死**（会把接手的存活进程误判为已死）。

杀纪律（协议 §5.5，硬约束）：一切基于 pid 的 kill 执行前必过 (pid,procStart)
身份校验，不匹配禁止盲杀、按目标消失处理。

用法：
  python3 runner.py --root <ROOT> [--host NAME] [--aliases a,b]
                    [--interval 0.5]
                    [--log-file PATH] [--log-level DEBUG|INFO|WARNING|ERROR]

  --root       agents/ 树所在根目录（布局 <ROOT>/agents/<participantId>/）
               spawn 前需 enable.json 放行（门禁内置无开关，§14.3；
               调度方在独立服务 scheduler.py，本机守护不合署）
  --log-file   日志文件（缺省输出 stderr）；生产部署约定 ~/m/run/logs/agentd.log
  --log-level  日志级别（缺省 INFO）

仅使用 python3 标准库。
"""
import argparse
import datetime
import hashlib
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import envscrub  # noqa: E402
import proto  # noqa: E402

log = logging.getLogger("agentd.runner")

# ---------- 单实例锁 = 每机一份的探活锁文件 ----------
# 布局：<root>/agents/run/agentd.<host>.lock（host = 规范名，env/host-id 映射；本机 = nv1；
# 路径单点 = proto.agentd_lock_path）。
# agents/ 整树跨机同步（DESIGN-multimachine §2.2）：每机只写自己那份 → 跨机不误撞，
# 且别机可在目录里直接看到各机 agentd 存活情况。历史沿革：原在 agents/.agentd.lock
# （误撞顾虑 迁 <root>/run/agentd.lock），
# 2026-08-27 用户拍板迁 agents/ 顶层，
# 2026-08-31 迁入 agents/run/（全停切换）。
# 内容：JSON 一行 {host, pid, procStart, startedAt, updatedAt}。
# 探活语义（用户简化口径：心跳式定期刷新即探活）：主循环每 5s 重写刷 updatedAt；
# 别机判存活基于 updatedAt 新鲜度而非存在性——同步契约无 --delete（不传播删除），
# 崩溃留死锁、优雅退出也不删文件（删除无意义），停更即自然判死。
# 本机互斥仍是第二道防线（防手工双开/竞态绕过 Makefile pgrep 判重）：
# O_EXCL 创建；已存在 → 按 (pid,procStart) 校验持锁者（杀纪律口径）：
# 活 → 记日志退出（0）；死/解析失败/缺字段 → 视为死锁，删除重建（接管）。
LOCK_REFRESH_INTERVAL = 5.0  # 探活刷新周期（终态该文件跨机同步，勿刷太密，§2.3 备注）

def ts_epoch(ts):
    """now_ts 格式时间戳 → epoch 秒；解析失败返回 None（调用方降级处理）。
    「解析失败」含入参不是字符串（None = 字段缺失、数字等异构写入）：`ts.partition`
    对非 str 抛 AttributeError，必须一并兜住，否则契约（返回 None）不成立、异常逃出
    调用点（修复：`replay_suppressed` 读缺失的 endedAt 曾让 notify_tick 每轮整段
    中断，同轮排在其后的参与方真通知永久饿死）。"""
    try:
        head, _, frac = ts.partition(".")
        return time.mktime(time.strptime(head, "%Y-%m-%d-%H-%M-%S")) + \
            (float("." + frac) if frac else 0.0)
    except (ValueError, OverflowError, TypeError, AttributeError):
        return None


def lock_path(root, host):
    return proto.agentd_lock_path(root, host)


def _lock_doc(host, started_at):
    return {"host": host, "pid": os.getpid(),
            "procStart": proto.proc_starttime(os.getpid()),
            "startedAt": started_at, "updatedAt": proto.now_ts()}


def refresh_lock(lockfile, host, started_at):
    """心跳式重写锁文件刷 updatedAt（startedAt 保持首启时刻；原子落盘，同步通道友好）。"""
    proto.atomic_write_json(lockfile, _lock_doc(host, started_at))


def acquire_single_instance_lock(root, host):
    """O_EXCL 占坑；返回锁文件路径。活锁拒启（日志 + exit 0，对齐旧守护），
    死锁接管（删后重试 1 次），两轮仍失败则 exit 1。"""
    lockfile = lock_path(root, host)
    os.makedirs(os.path.dirname(lockfile), exist_ok=True)
    for _attempt in range(2):
        try:
            fd = os.open(lockfile, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(json.dumps(_lock_doc(host, proto.now_ts()),
                                   ensure_ascii=False) + "\n")
            log.info("single-instance lock acquired: %s (pid %d)",
                     lockfile, os.getpid())
            return lockfile
        except FileExistsError:
            holder_pid = None
            holder_start = None
            try:
                with open(lockfile, encoding="utf-8") as f:
                    doc = json.loads(f.read())
                holder_pid = doc.get("pid")
                holder_start = doc.get("procStart")
            except (OSError, ValueError):
                pass  # 读不到/解析失败按死锁处理（接管）
            # 无 pid → 无法定位持锁者，按死锁处理（接管）。
            # 有 pid → 身份校验（杀纪律口径）：(pid,procStart) 二元组堵 pid 复用；
            # procStart 缺失/为 null（非 Linux 正常形态，本机锁恒如此；或遗留档案）
            # 由 pid_identity_ok 回退裸 kill(pid,0) 探测（
            # 同源修复：修复前 None 身份被当死锁 → 双开防线失效）。
            if holder_pid and proto.pid_identity_ok(holder_pid, holder_start):
                log.error("另一个 agentd runner 已在运行（持锁 pid=%d），本进程退出",
                          holder_pid)
                sys.exit(0)
            try:
                os.unlink(lockfile)
                log.warning("检测到已死持锁进程留下的锁（pid=%s procStart=%s），已接管",
                            holder_pid, holder_start)
            except OSError:
                pass  # 并发删除，重试
    log.error("无法获取单实例锁 %s，退出", lockfile)
    sys.exit(1)


# ---------- 通知通道（终态通知 = reaper 单收件方模型） ----------
# 通知 = inform 信封（from=agentd）。收件面解析单点 = Runner.resolve_reaper（唯一收尾方），
# 登记侧对偶 = core.ts resolveReaper 写 spec.reaper：
#   - `spec.reaper`（路径式 id）= 唯一负责收尾的收件方（读 report 验收、遗留事项进 todo、销账）。
# 回落：reaper 缺失/文法非法/解析不到目录或目录在场但**无消费者**（活性代理 _pid_active）→
# 回落职位信箱 agents/topic/dispatcher/inbox/（系统主题 自 bot/dispatcher/ 移族；
# 订阅者会话的薄 receiver 直订认领注入）并在 body 加 `note` 说明回落原因。职位信箱已退出
# 「任务终态通知」的默认收件面，只作回落面；它保留的写入口 = runner 停滞告警、子端 ask、
# 服务告警、人机/agent 传话。
# 判重完全基于文件（重启不重发），粒度 = **(taskId, 收件方)**：单点标记 = 参与方自家目录内
# notified.json（`to` = 已投收件方**集合**，`complete` 热路径短路位）。写序 = 先信封后标记
# （反序会在崩溃窗口丢通知）；崩溃在两者之间 → 下一轮重发一条重复 inform（可接受：重复的
# 代价远小于常驻一套信箱回落扫描+补写机制）。存量档案（标记缺失/收件人口径变更）的重放
# 由重放护栏独立兜住（见 REPLAY_GRACE_DEFAULT）。
NOTIFY_TO = proto.POSITION_PID     # 回落收件方（职位信箱；reaper 解析不命中时）
NOTIFY_MARK = "notified.json"      # 终态通知判重标记（runner 写，部署层记账，同 pid.log 地位）

# ---------- 终态通知重放护栏（通知面只报「本次运行期间到达终态」的任务） ----------
# 缺陷面：终态档案会一直在盘上（直到 gc）、agents/ 树跨机同步迟到落地、runner 会重启——
# 判重标记（notified.json）只覆盖「本次部署后自己发过的通知」；对标记缺失的历史档案
# （冷启动、新机入网、agentd 启动早于 agents-sync 首轮收敛、信箱路径迁移撞上节点离线），
# 判重集为空 → 本机认领的**全部历史终态任务**被当新事件重放（实测一次 45 条噪音涌进调度员
# 信箱，淹没同时刻的真实通知）。
# 修法：判重 = notified.json 标记 ∩ 重放护栏；护栏两条件**同时成立**才抑制
# （保守方向 = 尽量少抑制，不吞真实通知）：
#   ① 本次进程**从未亲眼见过**该参与方处于非终态（含尚未启动）——终态判定发生在上一次
#      运行，本次运行没有任何新事实要报告。记账点 = service() 读到 pid.json 之后。
#      这一条同时覆盖「启动后 agents-sync 才把旧终态档案 pull 落地」的迟到面
#      （纯启动快照会漏：快照时目录还不在场）。
#   ② pid.json.endedAt 早于**本次进程启动纪元**超过宽限窗（缺省 300s，--replay-grace 可调，
#      0 = 纯启动基线语义：启动前已终态者一律不发）。endedAt **缺失或不可解析**（人工补录/
#      异构写入/被裁剪的同步副本）一律当「远古」→ 抑制：畸形档案只吞它自己，绝不让异常逃出
#      护栏——notify_tick 每轮整段中断会饿死同轮排在其后所有参与方的真通知。
#      基准锚在启动纪元而非 now：真实事件不会因为通知自身延迟（notify_tick 连续异常重试）
#      而被自己的阈值吞掉。宽限窗兜住「上一实例已写 final、还没写信封就被 SIGKILL/掉电」
#      的分钟级窗口。
# 孤儿接管**不受影响**：接管时 pid.json 是 status=running、未 final → 条件①不成立（本次运行
# 见过它非终态）；接管后亲手写 stale/127 + final 属「本次运行期间才判定的终态」→ 照常通知，
# note / warn=no_report / stopReason 一个不丢（terminal_event 的 stale 分支与协议 §5 完成判定）。
# 不落本机状态文件：判据 = pid.json.endedAt（本就落盘且跨机同步）+ 每进程自持的内存观测集，
# 任意时刻可重算；再落一份文件只会多出「文件与事实不一致」的故障面（跨重启陈旧、需清理、
# 多机语义不明）。可观测性走日志：每个被抑制的 id 一条 INFO，进程内去重。
REPLAY_GRACE_DEFAULT = 300.0
REPLAY_GUARD_LOG = "重放护栏"   # 日志锚点（排障：grep 本串 run/logs/agentd.log）


def setup_logging(log_file, level):
    h = logging.FileHandler(log_file, encoding="utf-8") if log_file \
        else logging.StreamHandler()
    h.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [h]
    root.setLevel(getattr(logging, level.upper(), logging.INFO))


class Runner:
    def __init__(self, root, host, aliases, interval, lockfile=None, lock_host=None,
                 replay_grace=REPLAY_GRACE_DEFAULT):
        self.root = os.path.abspath(root)
        self.local_ids = proto.local_hosts(host, aliases)
        self.interval = interval
        # 探活锁文件（主循环定期刷 updatedAt，startedAt 保持首启；优雅退出只停更不删除）
        self.lockfile = lockfile
        self.lock_host = lock_host
        self._lock_refresh_at = time.monotonic() + LOCK_REFRESH_INTERVAL
        self.lock_started_at = proto.now_ts()
        if lockfile:
            doc = proto.read_json(lockfile)
            if isinstance(doc, dict) and doc.get("startedAt"):
                self.lock_started_at = doc["startedAt"]  # 沿用首启时刻，刷新不重置
        # participantId（路径式，如 task/<id>） -> Popen 句柄（只认自己 spawn 的孩子；§11.8 pid 仅 runner 本机自写自读）
        self.popen = {}
        # pid 复用加固（§5.5 杀纪律 / §11.8 防线）：
        # participantId -> 内核启动时刻（内存副本，落盘在 pid.json.procStart）
        self.procstart = {}
        # 缺 host 告警去重（0.5s 轮询防刷屏；内存集，重启重打一次无害）
        self._nohost_warned = set()
        # 终态通知重放护栏（判据见 REPLAY_GRACE_DEFAULT 注释）：本次进程启动纪元 +
        # 宽限窗 + 「亲眼见过非终态」观测集（单调增长，上界 = 本次运行期间曾活跃的本机
        # 认领参与方数——已终态的历史档案不进集合，故与档案总量无关）+ 抑制日志去重集。
        self.run_started_epoch = time.time()
        self.replay_grace = replay_grace
        self._seen_nonterminal = set()
        self._replay_logged = set()
        self.ensure_dispatcher_inbox()

    # ---------- 工具 ----------

    def adir(self, pid_):
        return proto.agent_dir(self.root, pid_)

    def read_spec(self, pid_):
        return proto.read_json(proto.spec_path(self.root, pid_))

    def read_pid(self, pid_):
        return proto.read_json(proto.pid_path(self.root, pid_))

    def write_pid(self, pid_, doc):
        """pid.json 是全协议唯一可变档，runner 独占反复重写（§3.3/§4.2）。"""
        proto.atomic_write_json(proto.pid_path(self.root, pid_), doc)

    def write_ack(self, pid_, req_id, outcome, detail=""):
        """控制回执 control/ack/<id>（runner 写，§4.4）。已有即不重写（不可变档）。"""
        p = os.path.join(proto.control_path(self.root, pid_), "ack", req_id)
        if os.path.exists(p):
            return
        proto.atomic_write_json(p, {"id": req_id, "ts": proto.now_ts(),
                                    "outcome": outcome, "detail": detail})
        log.info("%s ack %s %s %s", pid_, req_id, outcome, detail)

    def pending_reqs(self, pid_):
        """未回执的控制请求，按文件名序（≈到达序）。"""
        cdir = proto.control_path(self.root, pid_)
        if not os.path.isdir(cdir):
            return []
        out = []
        for fn in sorted(os.listdir(cdir)):
            if not fn.endswith(".req"):
                continue
            if os.path.exists(os.path.join(cdir, "ack", fn[:-4])):
                continue  # 已回执 = 已处理（按请求 id 幂等，§11.3）
            req = proto.read_json(os.path.join(cdir, fn))
            if req:
                req.setdefault("id", fn[:-4])
                out.append(req)
        return out

    # ---------- 通知通道（T1-5：终态 → 按登记方分流的收件信箱写 inform） ----------

    def ensure_dispatcher_inbox(self):
        """确保职位信箱目录存在（系统主题：只建 inbox/；topic.md 策展档与 watcher/
        订阅登记属登记流程，不由 runner 代写）。"""
        os.makedirs(proto.position_inbox(self.root), exist_ok=True)

    @staticmethod
    def _bare(tid):
        """id 形态归一化：strip 首段 task/ 前缀取 basename；其它形态原样保留。
        布局迁移（agents/<id> → agents/task/<id>）后存量档案的 id 存在裸形/路径式两种
        形态，判重按裸形比对才不触发重发波（事故 2026-09-03，票 xddi6b）。bot/<name>
        不归一：只剥 task/ 前缀（布局迁移的那一段历史），跨 family 改写会撞名
        （bot/topic 族也入通知面，见 resolve_reaper）。"""
        tid = str(tid)
        return tid[len("task/"):] if tid.startswith("task/") else tid

    def _pid_active(self, pid_, cache=None):
        """直投收件方的**活性代理**：目录在场只证明「地址可写」，不证明「有人会读」——投给
        无人消费的信箱 = 静默丢失（实例形态：已下线 bot 的目录在 gc 前仍在场、pid.json 已 final）。
        三判据任一成立即活：
          ① 进程型：`pid.json` 可读 ∧ 是 dict ∧ `final != True`（会话/进程活着）；
          ② 信箱型/主题型：`watcher/` 有非隐藏条目（有订阅消费者）；
          ③ 被任一参与方 `spec.subscribes` 声明（通道 B 登记期绑定；现网主题主持人多走此形，
            其 topic 目录无 watcher/）。
        ③ 只在 ①② 均不成立时跑；失败方向 = 回落职位信箱 + note（调度员照样看得到，只多一跳），
        不会丢通知。统一适用于所有直投收件方（不限 bot 族）；职位信箱是回落目标，**不做活性判定**
        （回落面必须无条件可写）。
        成本护栏：判据③ 要扫全部参与方的 spec（现网数百份），故 `cache` 为**每轮 tick 一份**的
        备忘录（{参与方 id: 判定结果} + 键 `"*"` = 订阅声明全集）——无它则「每个未排干的终态档案
        × 每个不活收件方」都会重扫一遍全树（比它替代的信箱扫描更贵）。
        @param {dict|None} cache 每轮 tick 的备忘录（None = 不缓存，单次调用面用）
        @returns {(bool, str)} (是否活, 不活原因；活时为空串)"""
        if cache is not None and pid_ in cache:
            return cache[pid_]
        res = self._pid_active_uncached(pid_, cache)
        if cache is not None:
            cache[pid_] = res
        return res

    def _pid_active_uncached(self, pid_, cache=None):
        """活性代理的判定本体（缓存外壳 = `_pid_active`）。"""
        try:
            d = proto.agent_dir(self.root, pid_)
        except ValueError:
            return False, "id 文法非法"
        if not os.path.isdir(d):
            return False, "目录不存在"
        doc = proto.read_json(os.path.join(d, "pid.json"))
        if isinstance(doc, dict) and doc.get("final") is not True:
            return True, ""
        wdir = os.path.join(d, "watcher")
        try:
            if any(not n.startswith(".") for n in os.listdir(wdir)):
                return True, ""
        except OSError:
            pass                     # 无 watcher/ 目录（进程型/未登记订阅）：看下一判据
        if pid_ in self._subscribed_ids(cache):
            return True, ""
        return False, ("目录在场但无消费者（pid.json 缺失或已终态、无 watcher 订阅、"
                       "无 spec.subscribes 声明者）")

    def _subscribed_ids(self, cache=None):
        """被任一参与方 `spec.subscribes` 声明过的 id 全集（活性代理判据③ 的素材；通道 B
        登记期绑定，见协议 §4.1）。一轮 tick 内只扫一次全树（缓存键 `"*"`）。"""
        if cache is not None and "*" in cache:
            return cache["*"]
        out = set()
        for other in proto.list_participants(self.root):
            sp = self.read_spec(other)
            subs = (sp or {}).get("subscribes")
            if isinstance(subs, list):
                out |= set(x for x in subs if isinstance(x, str))
        if cache is not None:
            cache["*"] = out
        return out

    def resolve_reaper(self, spec, cache=None):
        """终态通知的**唯一收件方**解析（单点；登记侧对偶 = core.ts resolveReaper 写
        spec.reaper）。两档：
        ① `spec.reaper` 在场且过文法白名单 → 它（文法非法 → 视作缺字段走 ② + 一行 WARNING：
           登记侧已拒非法值，运行侧只可能是人工补录）；
        ② reaper 不在场/文法非法 → 职位信箱 + note（缺字段属异常形态：登记侧恒写 reaper）；
           目标过活性代理（_pid_active）：不活 → 回落职位信箱 + note（区分「不存在」
           与「无消费者」两种原因，供收件方判断是否需补登记/转投）。
        对 task 与 bot 两族参与方同规则（bot 自身终态——散会 stop 等——同样回流其 reaper）。
        纯判定 + 存在性试探；建目录归发送侧（只建 inbox/，同 ensure_dispatcher_inbox 口径）。
        @param {dict|None} cache 每轮 tick 的活性备忘录（见 _pid_active）
        @returns {dict} {"pid", "inbox", "note": None|str}"""
        spec = spec or {}
        target = None
        raw = spec.get("reaper")
        if isinstance(raw, str) and raw.strip():
            cand = raw.strip()
            if proto.is_valid_participant_id(cand):
                target = cand
            else:
                log.warning("spec.reaper 文法非法（%r），回落职位信箱", raw)
        if target is None:
            return {"pid": NOTIFY_TO, "inbox": proto.position_inbox(self.root),
                    "note": "spec 缺 reaper 字段（或文法非法），回落职位信箱"}
        if target == NOTIFY_TO:
            return {"pid": NOTIFY_TO, "inbox": proto.position_inbox(self.root),
                    "note": None}
        alive, why = self._pid_active(target, cache)
        if alive:
            return {"pid": target, "inbox": proto.inbox_path(self.root, target),
                    "note": None}
        note = ("reaper %s 不存在，回落职位信箱" % target) if why == "目录不存在" \
            else ("reaper %s %s，回落职位信箱" % (target, why))
        return {"pid": NOTIFY_TO, "inbox": proto.position_inbox(self.root),
                "note": note}

    def _ask_inbox_env(self, spec):
        """子端 ask 写侧收件面的 spawn env（项 1）：**复用终态通知的 reaper 解析单点
        `resolve_reaper`**（不在 .ts 侧另立第二套解析），把结果经 env 交子端 `core.writeAskMessage`
        取值——ask 写侧收件面 = 该任务 reaper（与终态通知同源解析），reaper 解析不到目录/无消费者
        → 回落职位信箱并带 note（同款语义）。
        AGENTD_ASK_INBOX = 收件目录绝对路径（reaper 自家信箱 ∨ 回落职位信箱）；
        AGENTD_ASK_NOTE = 回落成因 note（命中 reaper 时不写该枚）。两枚已收进 envscrub.ENV_SCRUB_EXACT
        （身份/信号类，不得继承进孙进程：继承会把它带进被启动的服务，并让嵌套 receiver 误用外层任务的 ask 路由）。
        @returns {dict} 待 env.update 的键值（值恒为 str）"""
        r = self.resolve_reaper(spec)
        env = {"AGENTD_ASK_INBOX": r["inbox"]}
        if r.get("note"):
            env["AGENTD_ASK_NOTE"] = str(r["note"])
        return env

    def notify_mark_path(self, pid_):
        """终态通知判重单点标记路径：参与方自家目录内 notified.json（runner 写，
        部署层记账同 pid.log 地位——不入协议 §8 单写者矩阵）。判重以标记为唯一事实源；
        历史档案的重放由重放护栏兕住（replay_suppressed）。"""
        return os.path.join(self.adir(pid_), NOTIFY_MARK)

    def _mark_doc(self, pid_):
        """读判重标记 notified.json（不存在/坏件 → None），`to` 归一化为**集合**。
        @returns {dict|None} 标记内容副本，其中 `to` 为 set（其余字段原样）"""
        doc = proto.read_json(self.notify_mark_path(pid_))
        if not isinstance(doc, dict):
            return None
        raw = doc.get("to")
        if isinstance(raw, (list, tuple, set)):
            to = set(x for x in raw if isinstance(x, str) and x)
        else:
            to = set()
        out = dict(doc)
        out["to"] = to
        return out

    def _already_notified(self, pid_, to_pid):
        """该参与方的终态通知是否已对**该收件方**发过（幂等判重单点，粒度 =
        (taskId, 收件方)）：自家目录 notified.json 在场且 `to_pid ∈ to 集合` 即已发。
        崩溃窗口（信封已落盘、标记未写）→ 下一轮重发一条重复 inform，接受：
        历史档案的重放由重放护栏独立兕住，不另设信箱回落扫描+补写面。"""
        mark = self._mark_doc(pid_)
        return mark is not None and to_pid in mark["to"]

    def stop_requests(self, pid_):
        """control/ 里全部 action=stop 的请求信封（文件名升序）——取消链路的单一扫描点
        （协议 §5.2/§5.4）：terminal_event 归类与 notify_tick 的取消判定/stopReason 采集
        共用；与 report.py has_stop_request、core.ts hasStopRequest 同语义。"""
        out = []
        cdir = proto.control_path(self.root, pid_)
        try:
            for fn in sorted(os.listdir(cdir)):
                if not fn.endswith(".req"):
                    continue
                req = proto.read_json(os.path.join(cdir, fn))
                if isinstance(req, dict) and req.get("action") == "stop":
                    out.append(req)
        except OSError:
            pass
        return out

    def canceled(self, doc, stop_reqs):
        """调度员主动取消判定（判据单点 = proto.EXITCODE_CANCELED 注释）：
        control/ 有 stop 请求 ∨ exitcode=125。取消不是失败：终态通知不得带
        warn=no_report（否则心跳/调度员每天要为假告警逐条查 control/）。"""
        return doc.get("exitcode") == proto.EXITCODE_CANCELED or bool(stop_reqs)

    def terminal_event(self, doc, pid_):
        """终态事件归类（通知文案用）：
        exited+0 → task_done；有 stop 请求（取消链路）→ task_canceled；
        接管孤儿死因不可得（stale/127）且 report.md 在场 → task_done（退出归属修复
：对齐协议 §5 完成判定 final+report.md；exitcode 记账
        如实保持 127，仅通知分类改判，载荷附 note 注明）；其余 → task_failed。"""
        if doc.get("status") == "exited" and doc.get("exitcode") == 0:
            return "task_done"
        # control/ 存在 stop 请求（无论已回执与否）= 取消链路（优先于孤儿完成改判）
        if self.stop_requests(pid_):
            return "task_canceled"
        # 接管孤儿退出归属：新守护非其父进程、
        # 不能 waitpid，无论正常退出还是崩溃都只能观测「进程消失」记 stale/127。
        # 退出前已写出 report.md = 按 §5 完成判定（final + report.md）本就是完成，
        # 不应被报成失败。风险说明：「伪造 report.md 后杀进程」与旧协议同族——完成判定
        # 本就信任 report.md 在场，报告写于进程退出前是常态，不因此收紧。
        if doc.get("status") == "stale" and doc.get("exitcode") == proto.EXITCODE_STALE \
                and os.path.exists(os.path.join(self.adir(pid_), "report.md")):
            return "task_done"
        return "task_failed"

    def replay_suppressed(self, pid_, doc):
        """终态通知重放护栏（判据见 REPLAY_GRACE_DEFAULT 注释）：True = 历史档案，
        静默抑制（只留一条进程内去重的 INFO 供审计）。两条件**同时成立**才抑制：
        ① 本次运行从未见过它非终态；② endedAt 早于本次启动纪元超过宽限窗。
        endedAt **缺失或**解析失败一律按「远古」处理——final 已置而 endedAt 不可读，终态
        判定必然发生在上一次运行（畸形档案只降级为抑制，绝不让异常逃出本函数瘫痪整轮
        notify_tick：那会饿死同轮排在其后所有参与方的真通知）。纯判定 + 日志，不写任何档案
        （不改 pid.json、不写信封）。"""
        if pid_ in self._seen_nonterminal or self._bare(pid_) in self._seen_nonterminal:
            return False
        ended = ts_epoch(doc.get("endedAt"))
        age = None if ended is None else self.run_started_epoch - ended
        if age is not None and age <= self.replay_grace:
            return False   # 崩溃/重启窗口内的终态：可能上一实例没来得及发，仍按新事件通知
        if pid_ not in self._replay_logged:
            self._replay_logged.add(pid_)
            log.info("%s：抑制历史终态通知 %s（status=%s exitcode=%s endedAt=%s，"
                     "早于本次启动 %s，超宽限窗 %.0fs；本次运行未见证其终态迁移）",
                     REPLAY_GUARD_LOG, pid_, doc.get("status"), doc.get("exitcode"),
                     doc.get("endedAt"),
                     "不可解析" if age is None else "%.0fs" % age, self.replay_grace)
        return True

    def notify_tick(self):
        """每轮扫描：本机认领的 agent 到达生命周期终态（final+代终态）→ 向 **reaper**
        （唯一收件方，resolve_reaper 解析；解析不到/不活 → 回落职位信箱
        topic/dispatcher/inbox/ 并带 note）写一条 inform 终态通知。幂等：判重 =
        自家目录 notified.json 标记（多轮/重启均不重发；崩溃在「信封已写、标记未写」
        窗口 → 重发一条重复 inform，接受）**∩ 重放护栏**（replay_suppressed：本次运行
        未见证其终态迁移的历史档案不当新事件重放，判据见 REPLAY_GRACE_DEFAULT；
        顺序不变 = 判重在前、护栏在后）。
        一期任务均 one-shot，代终态同 tick 置 final；auto 重启服务只在收口（final）后通知一次。"""
        active = {}   # {参与方 id: 活性判定}：本轮活性备忘录（含 "*" = 订阅声明全集）
        for pid_ in proto.list_participants(self.root):
            spec = self.read_spec(pid_)
            if spec is None or not proto.host_matches(spec, self.local_ids):
                continue
            doc = self.read_pid(pid_)
            if not proto.life_terminal(doc):
                continue
            mark = self._mark_doc(pid_)
            if mark is not None and mark.get("complete") is True:
                continue   # 热路径短路：判重已完整 → 零收件方解析、零活性探测
            recipient = self.resolve_reaper(spec, active)
            if self._already_notified(pid_, recipient["pid"]):
                continue
            if self.replay_suppressed(pid_, doc):
                continue   # 历史档案（启动时已终态 / 同步迟到落地），不是本次运行的新事件
            adir = self.adir(pid_)
            report = os.path.join(adir, "report.md")
            event = self.terminal_event(doc, pid_)
            payload = {
                "event": event,
                "taskId": pid_,
                "dir": adir,
                "status": doc.get("status"),
                "exitcode": doc.get("exitcode"),
                "name": spec.get("name") or spec.get("workdir") or "",
            }
            if os.path.exists(report):
                payload["report"] = report
            if recipient.get("note"):
                payload["note"] = recipient["note"]
            # 取消判定先算（下面 warn 抑制与 stopReason 采集共用一次扫描）。
            stop_reqs = self.stop_requests(pid_)
            is_canceled = self.canceled(doc, stop_reqs)
            # 终态缺 report 检测（周复盘 2026-08-31 提案 P5）：
            # exit 0 ∧ 无 report.md ∧ one-shot → 通知载荷加 warn（不改成功判定——
            # 完成判定仍为 final+report.md，交付判定归应用层；通知只是多发一个信号，
            # 供调度员/心跳优先怀疑空跑）。exitcode==0 只可能来自 exited（stale 记 127）。
            # 排除调度员主动取消（自评 R2）：stop 请求与子进程自然 exit 0
            # 竞态时 event 仍为 task_done（exited/0 优先），但缺报告是取消的预期结果而非空跑
            # ——发 warn 只会制造假告警（与 core.ts [已取消]、report.py verdict 取消同判据）。
            if event == "task_done" and doc.get("exitcode") == 0 \
                    and "report" not in payload \
                    and spec.get("restartPolicy") == "one-shot" \
                    and not is_canceled:
                payload["warn"] = "no_report"
            if event == "task_done" and doc.get("status") == "stale":
                payload["note"] = ((payload.get("note") + "；") if payload.get("note") else "") \
                    + ("接管孤儿进程：退出码不可得（stale/127）；"
                       "按完成判定（final + report.md）记为完成")
            # stop 请求的 reason 一并带上（取消场景的上下文）：多个 stop 请求时取首个带 reason 的
            for req in stop_reqs:
                if req.get("reason"):
                    payload["stopReason"] = req["reason"]
                    break
            # 信封落盘（收件目录只建 inbox/，克制口径同 ensure_dispatcher_inbox：
            # 不代写 topic.md/watcher 等登记物）。
            inbox = recipient["inbox"]
            os.makedirs(inbox, exist_ok=True)
            for _ in range(8):  # 文件名撞名重试（同毫秒并发写）
                mid = proto.now_ts() + "-agentd-" + proto.rand_suffix()
                mf = os.path.join(inbox, mid + ".msg")
                if not os.path.exists(mf):
                    break
            proto.atomic_write_json(mf, {
                "id": mid, "from": "agentd", "ts": proto.now_ts(),
                "type": "inform", "body": json.dumps(payload, ensure_ascii=False),
            })
            # 判重单点标记（信封落盘后才写：反向顺序会在崩溃窗口里丢通知）；
            # `complete` 让后续轮次走热路径短路。
            after = self._mark_doc(pid_)
            to = set(after["to"]) if after else set()
            to.add(recipient["pid"])
            mark_doc = dict(after) if after else {}
            mark_doc["to"] = sorted(to)
            mark_doc["ts"] = proto.now_ts()
            mark_doc["event"] = event
            mark_doc["complete"] = True
            proto.atomic_write_json(self.notify_mark_path(pid_), mark_doc)
            log.info("notify %s -> %s%s (%s status=%s exitcode=%s)", pid_,
                     recipient["pid"],
                     "（%s）" % recipient["note"] if recipient.get("note") else "",
                     event, doc.get("status"), doc.get("exitcode"))

    # ---------- spawn / kill（含 pid 复用加固：§5.5 杀纪律 / §11.8 防线） ----------

    def probe_alive(self, pid_, pid, expected_start):
        """存活探测（杀纪律，§5.5：探测同理，不匹配视同进程不存在）。
        防线（§11.8）：本函数处理无句柄场景——(pid,procStart) 二元组校验（防线 2）；
        有句柄场景由调用方直接 poll()（防线 1）。"""
        return proto.pid_identity_ok(pid, expected_start)

    def kill_proc(self, pid_, doc):
        """强杀（§7.1：强制杀只能来自外部）。
        杀纪律（§5.5）：执行前必须校验 (pid, procStart) 身份——不匹配 = 目标已消失
        （pid 被复用），**拒绝盲杀**，返回 False（调用方按「目标已消失」写终态/回执）。
        True = 确有进程被杀。"""
        pid = doc.get("pid")
        if not pid:
            self.popen.pop(pid_, None)
            return False
        if not self.probe_alive(pid_, pid, doc.get("procStart")):
            log.warning("%s kill REFUSED: (pid,procStart) identity mismatch"
                        " (pid reuse) pid %s", pid_, pid)
            self.popen.pop(pid_, None)
            return False
        # setsid 进程组限定爆炸半径：组杀覆盖子进程（如 shell 拉起的真实负载）。
        # 组号直接用 pid：spawn 必带 start_new_session=True（setsid），子进程是新组领导者，
        # pgid ≡ pid 是结构不变式，无需运行时 os.getpgid 查询——旧写法先查再杀多一次系统调用，
        # 且若窗口内 pid 被复用，查到的可能是无关进程的组号（炸到无关进程组）；直接用 pid 则
        # 组杀被限定在「持有该 pid 的进程自己的组」内（攒批，评审 9k1t B-1）。
        # 残余窗口（校验→kill 微秒级）不可完全消除：杀是最后手段（§7.1 外源强杀），
        # 触发需原进程恰在窗口内死亡且内核恰好立即复用同一 pid，概率极小。
        try:
            os.killpg(pid, signal.SIGKILL)
        except OSError:
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        log.info("%s killed pid %d", pid_, pid)
        h = self.popen.pop(pid_, None)
        if h is not None:
            try:
                # 句柄收尾（防线 1 的组成部分：自己 spawn 的孩子，poll/wait 合法且必要）
                h.wait(timeout=2)
            except Exception:
                pass
        return True

    def spawn(self, pid_, spec, gen, resumed=None):
        """spawn 并写 pid.json 新一代（§3.3-1、§9.2）。
        pid 复用加固：记录进程内核启动时刻 procStart（§4.2，与 pid 组成二元身份）。"""
        adir = self.adir(pid_)
        # 环境洗刷（票 1t7e9o）：任务进程不得继承调度方身份族与
        # pi 机密（实证泄漏：PI_WEB_PASSWORD 明文）。口径单点 = envscrub.py，
        # 与 serviced/serviced.py 同源；任务侧 keep 必须空集（无业务豁免）。
        # strip_third_party=True：连第三方 API key 族（*_API_KEY）
        # 一并剥除——子任务不继承宿主用钥；宿主服务自身环境不受影响（洗的是子进程副本）。
        env = envscrub.scrub_env(strip_third_party=True)
        env["AGENT_HOME"] = adir                 # 自家目录（协议容器+应用层家园，§4.1）
        env["AGENT_ROOT"] = self.root
        env["AGENT_SELF"] = pid_                 # 路径式 id（task/<id>）
        # 子任务身份标记（T1-6 防递归）：dispatch 检测到该变量即拒绝分发——
        # 子任务内禁止再分发（对齐旧系统 spawn 注入子任务身份 + 子任务防递归 guard 语义）。
        # 常驻会话不注入（用户拍板调整）：常驻本质 = 调度员身份，
        # 扩展侧三处 AGENTD_TASK 连锁（工厂惰性化/防递归/时长 guard）天然不触发，
        # 扩展代码零改动；识别口径 = spec.command 内嵌 `AGENTD_RESIDENT=1` 前缀。
        if "AGENTD_RESIDENT=1" not in spec.get("command", ""):
            env["AGENTD_TASK"] = "1"
            # 子端 ask 写侧收件面（项 1）：复用 resolve_reaper 单点经 env 交子端取值
            # （ask 收件面 = 该与终态通知同源；reaper 不可达 → 回落职位信箱带 note）。
            # 只任务形态注入（ask-user-child 仅 CHILD_EXTS 任务形态装载；resident/bot 不用 ask_dispatcher）。
            env.update(self._ask_inbox_env(spec))
        # 命令形态约定：spec.command 存裸 bash 命令字符串，
        # 写方不加 bash -c 包装，运行方在此显式以 bash 执行（不依赖 /bin/sh 语义）。
        # 存量旧格式（带 bash -c 的字面值）在此为双重 shell，语义不变，兼容排队存量。
        # spec 路径可移植约定：workdir 位于登记机 $HOME 内者
        # 存 `~/...` 形式（agents 树跨机同步，各机 $HOME 不同，DESIGN-multimachine §2.2）；
        # expanduser 在执行机按本机 $HOME 展开；对绝对路径是 no-op，存量绝对路径行为不变。
        workdir = spec.get("workdir")
        p = subprocess.Popen(["bash", "-c", spec["command"]],
                             cwd=os.path.expanduser(workdir) if workdir else None,
                             env=env, start_new_session=True)
        self.popen[pid_] = p
        start = proto.proc_starttime(p.pid)   # 自开机时钟节拍，不受系统时钟调整影响（§4.2）
        self.procstart[pid_] = start
        doc = {"gen": gen, "pid": p.pid, "status": "running",
               "startedAt": proto.now_ts(), "lastAliveAt": proto.now_ts(),
               "restarts": gen - 1, "final": False}
        if start is not None:
            doc["procStart"] = start
        if resumed is not None:
            doc["resumed"] = resumed
        # 观测端点（票 hegipc）：rpc 封装形态的任务在 run/ 下有透传
        # unix socket（封装脚本 bind），端点路径由 taskId 单点推导（proto.task_sock_path）；
        # 记入 pid.json 供消费方（web）发现——写者仍仅 runner（pid.json 单写者铁律），
        # 存活判定归消费方连接时。非封装形态命令不写此字段。
        if "pi-rpc-wrap.py" in spec.get("command", ""):
            # sock 文件名用二段 name（task id 不含 `/`；路径式 id 取第二段，
            # proto.task_sock_path 口径注记）
            doc["sock"] = proto.task_sock_path(
                self.root, proto.parse_participant_id(pid_)[1])
        self.write_pid(pid_, doc)
        log.info("%s spawned gen %d pid %d procStart %s", pid_, gen, p.pid, start)
        return doc

    def inject_premessage(self, pid_, req):
        """inject 投递（实现定义，§5.3）：以首条预置消息进入新一代——
        换代前向 agent inbox 写一条 inform（from=控制方，body=inject）。"""
        inbox = proto.inbox_path(self.root, pid_)
        # from 可能含 `/`（路径式 id）：文件名段经 fs_safe_id 转写（§2.2 单点）
        mid = proto.now_ts() + "-" + proto.fs_safe_id(req.get("from", "control")) \
            + "-" + proto.rand_suffix()
        proto.atomic_write_json(os.path.join(inbox, mid + ".msg"), {
            "id": mid, "from": req.get("from", "control"), "ts": proto.now_ts(),
            "type": "inform", "body": req["inject"],
        })
        log.info("%s inject pre-message %s", pid_, mid)

    # ---------- 控制三动作（§5.2/§5.4；clear =） ----------

    def do_stop(self, pid_, req, doc):
        if doc is not None and doc.get("final") is True:
            # 已 final：行为性停写，不再碰 pid.json（§13 条 2：重复 stop → noop）
            self.write_ack(pid_, req["id"], "noop", "already final")
            return doc
        if doc is None:
            # spawn 前收到 stop：取消未启动任务（§5.4 规则 4）
            doc = {"gen": 0, "pid": 0, "status": "killed",
                   "exitcode": proto.EXITCODE_KILLED,
                   "startedAt": proto.now_ts(), "endedAt": proto.now_ts(),
                   "lastAliveAt": proto.now_ts(), "restarts": 0, "final": True}
            self.write_pid(pid_, doc)
            self.write_ack(pid_, req["id"], "applied", "canceled before spawn")
            return doc
        if doc.get("status") == "running":
            if self.kill_proc(pid_, doc):
                doc.update(status="killed", exitcode=proto.EXITCODE_KILLED,
                           endedAt=proto.now_ts(), final=True)
                self.write_pid(pid_, doc)
                self.write_ack(pid_, req["id"], "applied", "killed and finalized")
            else:
                # 杀纪律（§5.5）：身份校验不通过 = 目标已消失（pid 被复用），拒绝盲杀。
                # 如实记「消失」事实（死因不可得 → 按 §4.2 约定记 stale）并收口生命周期。
                doc.update(status="stale", exitcode=proto.EXITCODE_STALE,
                           endedAt=proto.now_ts(), final=True)
                self.write_pid(pid_, doc)
                self.write_ack(pid_, req["id"], "noop",
                               "kill refused: (pid,procStart) identity mismatch"
                               " (pid reused), target gone; recorded stale; finalized")
        else:
            # 进程已终态：只置 final 收口（§5.4 规则 2 / §11.1）
            doc["final"] = True
            self.write_pid(pid_, doc)
            self.write_ack(pid_, req["id"], "noop", "already terminal; finalized")
        return doc

    def do_restart(self, pid_, req, spec, doc):
        if doc is not None and doc.get("final") is True:
            self.write_ack(pid_, req["id"], "rejected", "already final")
            return doc
        old_gen = doc["gen"] if doc else 0
        note = ""
        if doc and doc.get("status") == "running":
            if not self.kill_proc(pid_, doc):
                # 杀纪律（§5.5）：旧进程已消失（pid 复用），无杀动作，直接换代。
                note = "(old proc gone: pid reused, kill refused) "
            self.archive_gen(pid_, doc)
        if req.get("inject"):
            self.inject_premessage(pid_, req)
        doc = self.spawn(pid_, spec, old_gen + 1, resumed=old_gen or None)
        self.write_ack(pid_, req["id"], "applied", note + "new gen %d" % doc["gen"])
        return doc

    # ---------- clear（弃历史换代） ----------
    # 语义 = 杀当前进程（杀纪律同 restart）→ 备份会话文件 → 截断到 0 → 换新代空白起步。
    # 与既有动词正交：stop=终态收口、restart=保历史换代、clear=弃历史换代。
    # 只动会话文件与进程换代：不动 spec/enable，inbox/ack 语义不变，pid.json 照常 gen+1。

    BACKUP_DIR = os.path.join("run", "backup")  # 宿主本地运行时区（不入 git、不跨机同步）

    def _claim_origin(self, path):
        """去 replica 标记、升格本机原件（与 sessiond proc.py:_claim_origin 同款 口径）：agents/ 树跨机同步，会话文件可能是远端 pull 落盘的副本——
        本机截断后若仍是 replica 组，会被远端旧副本覆盖。chown 回本进程 uid/gid 去标记。
        本机无 `replica` 组（grp.getgrnam KeyError）或文件不存在/非 replica → 跳过。
        agentd 不 import w 仓代码，接受同构重复。"""

        try:
            import grp
            gr = grp.getgrnam("replica")
        except (ImportError, KeyError):
            return
        try:
            st = os.stat(path)
        except OSError:
            return
        if st.st_gid != gr.gr_gid:
            return
        try:
            os.chown(path, os.getuid(), os.getgid())
            log.info("%s claimed origin (dropped replica tag) for %s", path, path)
        except OSError as e:
            log.error("%s claim origin failed for %s: %s", path, path, e)

    def _backup_and_clear_session(self, pid_):
        """备份会话文件至 <root>/run/backup/<fs_safe_id>-<ts>.jsonl（复制，非 move），
        随后截断原文件到 0 字节并去 replica 标记。返回 (备份路径或 None, 备注串)。
        session 文件不存在 → 无备份、创建空文件（新代从空起步，与截断同构）。"""
        sess = os.path.join(self.adir(pid_), "session", "session.jsonl")
        backup = None
        note = ""
        try:
            if os.path.exists(sess) and os.path.getsize(sess) > 0:
                bdir = os.path.join(self.root, self.BACKUP_DIR)
                os.makedirs(bdir, exist_ok=True)
                backup = os.path.join(
                    bdir, "%s-%s.jsonl" % (proto.fs_safe_id(pid_), proto.now_ts()))
                with open(sess, "rb") as src, open(backup, "wb") as dst:
                    dst.write(src.read())
                note = "backed up to %s; " % backup
            open(sess, "w").close()   # 截断到 0（不存在则创建空文件）
            self._claim_origin(sess)
        except OSError as e:
            log.error("%s clear: backup/truncate failed: %s", pid_, e)
            raise
        return backup, note

    def do_clear(self, pid_, req, spec, doc):
        if doc is not None and doc.get("final") is True:
            self.write_ack(pid_, req["id"], "rejected", "already final")
            return doc
        if doc is None:
            # spawn 前收到 clear：无历史可清（不创建档案），照常等放行拉起（§5.4 规则 4 同源）。
            self.write_ack(pid_, req["id"], "noop", "no session yet; nothing cleared")
            return doc
        note = ""
        st = doc.get("status")
        if st == "running":
            if not self.kill_proc(pid_, doc):
                # 杀纪律（§5.5）：旧进程已消失（pid 复用），拒绝盲杀；目标已不在，
                # 继续备份+清空+换代（与 restart 同口径）。
                note = "(old proc gone: pid reused, kill refused) "
            self.archive_gen(pid_, doc)
        elif st in proto.TERMINAL_STATUSES:
            self.archive_gen(pid_, doc)
        try:
            _backup, bnote = self._backup_and_clear_session(pid_)
        except OSError as e:
            # 备份/清空失败 = 旧历史可能残留：不换新代（拉起来会续旧历史，违背动词语义）。
            self.write_ack(pid_, req["id"], "rejected",
                           "session backup/truncate failed: %s" % e)
            return doc
        # restartPolicy 无关（与 restart 同款：外部控制指令，one-shot 也立即换新代；
        # 新代正常完成即 final）。新代空白起步，不写 resumed 字段（非续跑）。
        doc = self.spawn(pid_, spec, doc["gen"] + 1)
        self.write_ack(pid_, req["id"], "applied",
                       note + bnote + "new gen %d (blank session)" % doc["gen"])
        return doc

    def archive_gen(self, pid_, doc):
        """逐代详史：部署层可选约定（§11.7）——追加 pid.log，不进协议、不参与判定。"""
        try:
            with open(os.path.join(self.adir(pid_), "pid.log"), "a") as f:
                f.write(json.dumps(doc, ensure_ascii=False) + "\n")
        except OSError:
            pass

    def handle_controls(self, pid_, spec, doc):
        for req in self.pending_reqs(pid_):
            action = req.get("action")
            if action == "stop":
                doc = self.do_stop(pid_, req, doc)
            elif action == "restart":
                doc = self.do_restart(pid_, req, spec, doc)
            elif action == "clear":
                doc = self.do_clear(pid_, req, spec, doc)
            else:
                self.write_ack(pid_, req["id"], "rejected", "unknown action %r" % action)
        return doc

    # ---------- 单个参与方一轮 ----------

    def service(self, pid_):
        spec = self.read_spec(pid_)
        if spec is None:
            return  # 无 spec（非进程型参与方）或半截文件下轮重试（§11.6）
        if proto.host_missing(spec):
            # 缺 host = 无人认领：
            # 任何机器都不动手，只留告警；人工补 spec.host 或重新登记后才可执行。
            if pid_ not in self._nohost_warned:
                self._nohost_warned.add(pid_)
                log.warning("%s: spec.host 缺失，无人认领，需人工补 spec.host 或重新登记"
                            "（旧『缺省→本机认领』兜底已废除，防多机重复认领误杀）", pid_)
            return
        if not proto.host_matches(spec, self.local_ids):
            return  # 路由：不归本机，只读视图，一个字节也不写（§15.2）

        doc = self.read_pid(pid_)

        # 重放护栏记账（条件①，判据见 REPLAY_GRACE_DEFAULT）：本次运行亲眼见过它处于
        # 非终态（含尚未启动 doc=None）→ 之后的终态属本次运行期间发生的事件，可通知。
        # 位置在认领判定之后：他机任务只读不记账，不给只读视图留内存。
        if not proto.life_terminal(doc):
            self._seen_nonterminal.add(pid_)

        # 已终结是吸收态（§9.2）：不再 spawn、不再写 pid.json，只幂等回执
        if doc is not None and doc.get("final") is True:
            for req in self.pending_reqs(pid_):
                if req.get("action") == "stop":
                    self.write_ack(pid_, req["id"], "noop", "already final")
                else:
                    self.write_ack(pid_, req["id"], "rejected", "already final")
            self.popen.pop(pid_, None)
            return

        if doc is None:
            # 尚未启动。spawn 前先查控制请求（§5.4 规则 4）
            if self.pending_reqs(pid_):
                # stop → 取消（置 final）；restart → 直接换代拉起。
                # 两条路径都已各自落盘，本轮不再走常规 spawn。
                self.handle_controls(pid_, spec, None)
                return
            # spawn 三检（§11.10）：host（已过）∧ enable 门禁（内置无开关）∧ spec 齐备
            if not os.path.exists(proto.enable_path(self.root, pid_)):
                return  # 排队中：有 spec 无 enable（§9.3/§14.4），不是错误
            self.spawn(pid_, spec, 1)
            return

        # ---- 有 pid.json 且未 final ----
        if doc.get("status") == "running":
            pid = doc.get("pid")
            h = self.popen.get(pid_)
            if h is not None:
                rc = h.poll()  # 防线 1：自己 spawn 的孩子，句柄 poll 收尾，拿得到真死因
                if rc is None:
                    doc["lastAliveAt"] = proto.now_ts()  # 心跳：每轮刷新（§3.3-2）
                    self.write_pid(pid_, doc)
                elif rc < 0:
                    doc.update(status="killed", exitcode=128 + (-rc),
                               endedAt=proto.now_ts())
                    self.write_pid(pid_, doc)
                    self.popen.pop(pid_, None)
                else:
                    doc.update(status="exited", exitcode=rc,
                               endedAt=proto.now_ts())
                    self.write_pid(pid_, doc)
                    self.popen.pop(pid_, None)
            else:
                # 句柄不在手（如 runner 重启后接手档案）：外部存活探测（§7.1-3）。
                # 判死纪律（0.b）：禁裸 waitpid——只走 (pid,procStart) 二元组校验（防线 2）。
                # 杀纪律（§5.5）：不匹配视同进程不存在（进入终态判定路径），
                # 堵住 pid 复用误判活着（§11.8）。
                if self.probe_alive(pid_, pid, doc.get("procStart")):
                    doc["lastAliveAt"] = proto.now_ts()
                    self.write_pid(pid_, doc)
                else:
                    # 进程已消失（或 pid 被复用）而死因不可得：按约定判挂死（§4.2 赋值）
                    doc.update(status="stale", exitcode=proto.EXITCODE_STALE,
                               endedAt=proto.now_ts())
                    self.write_pid(pid_, doc)
                    log.info("%s stale (process gone / pid identity mismatch)%s",
                             pid_,
                             "; report.md present → notify task_done per §5"
                             if os.path.exists(os.path.join(self.adir(pid_),
                                                            "report.md"))
                             else "")
            if doc.get("status") != "running":
                log.info("%s gen %d %s exitcode %s", pid_, doc["gen"],
                         doc["status"], doc.get("exitcode"))
                if spec.get("restartPolicy") == "one-shot":
                    doc["final"] = True  # one-shot：终止即 final（§4.1）
                    self.write_pid(pid_, doc)

        # 消费控制请求（顺序幂等，§5.4）
        doc = self.handle_controls(pid_, spec, doc)

        # 自愈：仅崩溃三态（§4.1）
        if (spec.get("restartPolicy") == "auto"
                and doc.get("final") is not True
                and doc.get("status") in proto.CRASH_STATUSES):
            self.archive_gen(pid_, doc)
            self.spawn(pid_, spec, doc["gen"] + 1, resumed=doc["gen"])

    # ---------- 主循环 ----------

    def run(self):
        log.info("root=%s hosts=%s interval=%s",
                 self.root, sorted(self.local_ids), self.interval)
        while True:
            try:
                for pid_ in proto.list_participants(self.root):
                    try:
                        self.service(pid_)
                    except Exception:
                        # 单个参与方异常不拖垮全局；下轮重试（§11.6 幂等保证安全）
                        log.exception("%s service error (retried next tick)", pid_)
                try:
                    self.notify_tick()  # T1-5 通知通道：终态 → dispatcher inbox（幂等）
                except Exception:
                    log.exception("notify_tick error (retried next tick)")
                if self.lockfile and time.monotonic() >= self._lock_refresh_at:
                    # 探活心跳：定期重写锁文件刷 updatedAt（别机据此判存活）
                    try:
                        refresh_lock(self.lockfile, self.lock_host,
                                     self.lock_started_at)
                    except Exception:
                        log.exception("lock refresh error (retried next tick)")
                    self._lock_refresh_at = time.monotonic() + LOCK_REFRESH_INTERVAL
            except Exception:
                log.exception("loop error (retried next tick)")
            time.sleep(self.interval)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--host", default=None, help="本机身份（缺省用 hostname）")
    ap.add_argument("--aliases", default="", help="本机别名，逗号分隔（§15.3）")
    ap.add_argument("--interval", type=float, default=0.5)
    ap.add_argument("--log-file", default=None,
                    help="日志文件（缺省 stderr；生产约定 ~/m/run/logs/agentd.log）")
    ap.add_argument("--log-level", default="INFO",
                    choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    ap.add_argument("--replay-grace", type=float, default=REPLAY_GRACE_DEFAULT,
                    help="终态通知重放护栏宽限窗（秒，缺省 300）：启动前已终态且 endedAt "
                         "早于本次启动超过此值的任务视为历史档案、不当新通知重放；"
                         "0 = 启动前已终态者一律不发（判据见 REPLAY_GRACE_DEFAULT）")
    a = ap.parse_args()
    setup_logging(a.log_file, a.log_level)
    # 单实例锁：构造 Runner 前获取（活锁拒启/死锁接管）。优雅退出不删文件：
    # 同步契约不传播删除，停更 updatedAt 即别机判死依据（探活锁语义）。
    host_id = a.host or socket.gethostname()
    lockfile = acquire_single_instance_lock(os.path.abspath(a.root), host_id)

    def _on_term(signum, _frame):
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, _on_term)
    r = Runner(a.root, a.host, a.aliases, a.interval,
               lockfile=lockfile, lock_host=host_id,
               replay_grace=a.replay_grace)
    try:
        r.run()
    except (KeyboardInterrupt, SystemExit):
        log.info("bye (lock file %s kept: 停更即判死依据)", lockfile)


if __name__ == "__main__":
    main()
