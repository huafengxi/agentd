#!/usr/bin/env python3
"""scheduler.py — 调度半边（自 runner.py 拆出）。

职责（协议 §14 调度方）：给排队者写 enable.json 放行。runner.py 只保留运行半边，
调度方独立部署（多机阶段 2：用户纠偏彻底拆分，废除合署）：
生产由 scheduler-loop.sh 监督常驻（make scheduler.start），全局唯一实例；
--once 供调试/人工兜底（设计 DESIGN-multimachine §4.1）。

调度模型（DISPATCH.md §3 口径）：
  - spec.json 可选字段 `resources`/`provides`/`needs`（字符串数组）：
      resources 缺省 = ["serial"]（旧系统缺省语义，无参数任务互相串行的手段）；
      provides/needs 缺省 = []。显式 [] = 不占任何资源。
  - 放行条件 = 依赖满足 ∧ 资源可占 ∧ 占位数 < 上限（--max-concurrent，缺省 4）
    ∧ 目标主机存活：
      needs：对每个能力，有成功 provider → 满足；成功口径（用户拍板方案①）：
            生命周期终态 ∧ 任务目录有**非空** report.md（零字节/纯空白视同缺报告）∧
            （exited ∧ exitcode 0 **或** status=stale ∧ exitcode 127——接管孤儿死因不可得，
            §4 约定码）。exit 0 但无非空 report.md = **空跑**（未提交验收报告，口径同
            DISPATCH.md「完成判定」与 report.py verdict「无报告」）→ 不算成功 provider（idle 态）；
            取消（判据单点 proto.EXITCODE_CANCELED）豁免空跑判定：缺报告是取消的预期结果，
            归 canceled 态（不算失败也不算空跑），同样不满足依赖；
            同一能力存在未终态 provider（含排队未落地）→ 等待，且**优先于**同能力的旧 success
            （重发场景：新的真实施覆盖旧的交付，防旧的假成功提前放行依赖者）；
            无 provider 或全无成功者 → 不可满足（不放行；后续出现成功 provider 每轮重评
            自动解锁——沿用旧系统语义）。
      resources：所需资源不得被任何占位者（running 或 已放行未落地）持有；
      占位：本机 status==running 的任务 + 已写 enable 尚无 pid.json 的任务；
      目标主机存活：spec.host（登记时物化 后缺失=无人认领永久排队，
            不进本门禁）对应探活锁（§4.4 机制，
            agents/run/agentd.<host>.lock 的 updatedAt 新鲜度）判活，不存活不放行、
            留在候补队列（不占槽位/资源，排队不是错误，§11.9）；锁缺失/解析失败
            视为不存活（保守，打 WARNING）。阈值与判定细节见 HOST_ALIVE_THRESHOLD。
            已放行任务不回撤（enable 单调，§14.2）：门禁只作用于写 enable 之前。
  - 候补 = 归本机（--all-hosts 全局视图时不按本机身份过滤，为所有机器放行，
    多机阶段 2；占位/资源/能力表本就是全局语义）∧ 有 spec ∧ 无 enable ∧ 无 pid.json，
    按 (spec mtime, 目录名) 升序
    = 到达序；**允许越位**——等资源/能力的候补不阻塞后面条件满足的候补（旧系统同语义）。
  - 常驻豁免（设计 §2.5.2）：spec.command 内嵌 `AGENTD_RESIDENT=1`
    的候补（is_resident）——不计入占位数、不检查资源交集、不受占位上限约束，候补判定与
    host 存活检查保留，即时写 enable 放行（运行期同样不入占位面）。
  - 无参数任务 ⇔ resources=["serial"] ∧ needs=[]：彼此严格 FIFO 串行，行为与一期
    口径逐字节一致（向后兼容第一优先级）。
  - 完全无状态（§14.5）：重启重扫即恢复；阻塞决策只记日志（内存去重仅日志降噪，
    非调度状态，重启丢失无害）。不做能力环检测（拍板：成环任务排队等待，靠日志排查）。

放行依据写进 enable.json 的 note 字段（协议零扩展），决策另打日志（验收可观察）。

用法（独立常驻；监督脚本 scheduler-loop.sh，make scheduler.start/stop/status）：
  python3 scheduler.py --root <ROOT> [--host NAME] [--aliases a,b]
                       [--max-concurrent N] [--all-hosts] [--once] [--interval 0.5]
                       [--log-file PATH] [--log-level DEBUG|INFO|WARNING|ERROR]
  --once       只跑一轮即退出（调试/人工兜底）；不带 --once 即常驻循环（外部监督）
  --all-hosts  全局视图：候补不按本机身份过滤（多机终态：本机调度器为所有机器放行）；
               缺省关 = 只放行归本机的任务（单机/未切换形态，行为不变）

仅使用 python3 标准库。
"""
import argparse
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import proto  # noqa: E402
import runner  # noqa: E402  # 复用探活锁心跳周期口径（LOCK_REFRESH_INTERVAL）

log = logging.getLogger("agentd.scheduler")

SCHEDULER_ID = "agentd-scheduler"      # enable.json 的 by 字段（调度方标识，§14.2）
DEFAULT_RESOURCES = ["serial"]         # resources 缺省语义（旧系统缺省，串行手段）
DEFAULT_MAX_CONCURRENT = 4             # 全局占位上限（缺省，拍板 2026-08-27）

# ---- 放行门禁第四条件：目标主机存活判定 ----
# 数据源 = 每机探活锁 <root>/agents/run/agentd.<host>.lock（proto.agentd_lock_path 单点，
# DESIGN-multimachine §4.4：runner 心跳式定期刷新 updatedAt，别机按新鲜度判存活，
# 停更即自然判死；锁经星型同步汇聚到全局树，允许 ≤1 周期滞后）。
# 阈值推导（复用现网既有口径，不新造标准）：
#   心跳周期 = runner.LOCK_REFRESH_INTERVAL（5s）；
#   同步滞后 ≤1 周期 = 轮询兜底 interval 15s（ssh-sync 缺省）+ rsync 耗时（DESIGN §2.3）；
#   DESIGN §4.4 口径「判活阈值（以分钟计）」；
#   60s ≥ 数个心跳周期 + 一整同步周期 + rsync 耗时，另吸收中等跨机时钟偏差
#   （updatedAt 用写方本机时钟写、读方本机时钟比，余量由阈值承担）。
HOST_ALIVE_THRESHOLD = 60.0  # 秒：updatedAt 陈旧度超过该值 → 判目标机无存活 runner


def host_lock_path(root, host):
    """探活锁路径（单点 = proto.agentd_lock_path，任务）。"""
    return proto.agentd_lock_path(root, host)


def _parse_ts(ts):
    """解析 proto.now_ts 格式时间戳（%Y-%m-%d-%H-%M-%S[.mmm]，写方本机时钟）→ epoch。
    与 now_ts 的 strftime localtime 口径对称（mktime 按本机时钟解释）；
    解析失败/非字符串 → None（按不存活证据处理）。"""
    if not isinstance(ts, str):
        return None
    try:
        return time.mktime(time.strptime(ts[:19], "%Y-%m-%d-%H-%M-%S"))
    except ValueError:
        return None


def host_runner_alive(root, host_names, threshold=HOST_ALIVE_THRESHOLD):
    """目标主机存活判定（放行门禁第四条件）。

    host_names: 候补锁名集合（规范名/别名；任一锁新鲜即存活——覆盖「别名登记、
                规范名持锁」的错配）。返回 (alive, reason, warn)：
      alive=True  → reason = 判活依据（日志/审计用）；
      alive=False → reason ∈ lock stale / lock missing / lock unreadable，
                    warn=True 当 missing/unreadable（存量异常，需求：保守+WARNING）。
    证据优先级（多候补锁时）：有可解析者取最新一条（最新仍陈旧 → stale）；
    无可解析但有文件 → unreadable；全缺 → missing。"""
    stale_best = None      # (age, host, updatedAt)：可解析但陈旧者中最新的一条
    saw_file = False
    for h in host_names:
        p = host_lock_path(root, h)
        if not os.path.exists(p):
            continue
        saw_file = True
        doc = proto.read_json(p)
        epoch = _parse_ts(doc.get("updatedAt")) if isinstance(doc, dict) else None
        if epoch is None:
            continue  # 半截/坏 JSON/缺字段：证据不足，继续找其他候补锁
        age = time.time() - epoch
        if age <= threshold:
            return True, "host %s lock fresh (age %.0fs)" % (h, age), False
        if stale_best is None or age < stale_best[0]:
            stale_best = (age, h, doc.get("updatedAt"))
    if stale_best is not None:
        age, h, upd = stale_best
        return False, "lock stale: host=%s updatedAt=%s age=%.0fs" % (h, upd, age), False
    if saw_file:
        return False, "lock unreadable: %s" % ",".join(
            host_lock_path(root, h) for h in host_names), True
    return False, "lock missing: no agentd.<host>.lock for %s" % ",".join(host_names), True


# ---------- spec 调度字段规范化 ----------

def _str_list(v):
    return [x for x in v if isinstance(x, str)] if isinstance(v, list) else []


def is_resident(spec):
    """常驻候补识别（设计 §2.5.2）：spec.command 内嵌 `AGENTD_RESIDENT=1`
    env 前缀（命令串匹配口径，零字段扩展）。常驻者不占并发槽位、
    免互斥资源即时放行（不占槽/免资源判定见 _scan/tick）。"""
    return isinstance(spec, dict) and "AGENTD_RESIDENT=1" in spec.get("command", "")


def normalize_sched_fields(spec):
    """从 spec 取调度三元组（缺失按缺省语义）。返回 {resources, provides, needs}。"""
    spec = spec if isinstance(spec, dict) else {}
    resources = spec.get("resources")
    return {
        "resources": _str_list(resources) if resources is not None
        else list(DEFAULT_RESOURCES),
        "provides": _str_list(spec.get("provides")),
        "needs": _str_list(spec.get("needs")),
    }


# 报告非空判定的首块取样字节数：区分「零字节/纯空白」与真交付足够（首块全空白而文件
# 更长的病态形态由 report_nonempty 的整文件兜底覆盖）。
REPORT_HEAD_BYTES = 4096


def report_nonempty(path):
    """report.md 是否**非空在场**（完成判定自证件的判据单点，DISPATCH.md「完成判定」；
    硬化 = 🟡3：零字节 report.md 曾让 exit0 空跑仍被判 success，依赖它的
    评审照样被提前放行）。存在 ∧ 去空白后有内容；读失败/缺失 → False（保守）。
    跨机半截文件取「有任一非空白字节即算在场」的保守方向：agents-sync 正在搬运的半截
    报告不被误判为空跑（误判方向 = 把真交付说成空跑、冤压依赖者，比漏判更贵）。
    report.py 的 has_report 复用本函数（呈现面与放行判据同源，不另写一份逻辑）。"""
    try:
        if os.path.getsize(path) <= 0:
            return False
        with open(path, "rb") as f:
            if f.read(REPORT_HEAD_BYTES).strip():
                return True
    except OSError:
        return False
    try:                      # 首块全空白但文件更长：整文件兜底，不因取样窗口误判
        with open(path, "rb") as f:
            return bool(f.read().strip())
    except OSError:
        return False


def _report_present(root, pid_):
    """任务目录 report.md 是否非空在场（判据 = report_nonempty 单点）。
    非法参与方 id（agent_dir 单点文法校验抛 ValueError）→ 保守当无报告。"""
    try:
        adir = proto.agent_dir(root, pid_)
    except ValueError:
        return False
    return report_nonempty(os.path.join(adir, "report.md"))


def stop_requested(root, pid_):
    """control/ 是否存在 action=stop 请求（取消判据的一半）：口径同 runner.stop_requests /
    report.has_stop_request / core.ts hasStopRequest，判据单点注释 = proto.EXITCODE_CANCELED。
    只读；目录缺失/半截文件当「没有」。"""
    cdir = proto.control_path(root, pid_)
    try:
        names = os.listdir(cdir)
    except OSError:
        return False
    for fn in sorted(names):
        if not fn.endswith(".req"):
            continue
        req = proto.read_json(os.path.join(cdir, fn))
        if isinstance(req, dict) and req.get("action") == "stop":
            return True
    return False


def provider_canceled(doc, root=None, pid_=None):
    """provider「调度员主动取消」判定（DISPATCH.md「完成判定」豁免面）：
    exitcode == proto.EXITCODE_CANCELED ∨ control/ 有 action=stop 请求。
    取消不是失败、也不是空跑：缺 report.md 是取消的预期结果（不得判 idle/failed；
    告警侧豁免在 runner.notify_tick 不发 warn=no_report）。
    注意序：本判定在 report.md 检查**之后**用——取消但已交付报告者仍算 success
    （与 report.py verdict「exit0∧report → 成功」同序，口径不漂移）。"""
    if not isinstance(doc, dict):
        return False
    if doc.get("exitcode") == proto.EXITCODE_CANCELED:
        return True
    return root is not None and pid_ is not None and stop_requested(root, pid_)


def provider_idle(doc, root=None, pid_=None):
    """空跑判定：生命周期终态 ∧ exited ∧ exitcode 0 ∧ 无非空 report.md ∧ 非取消。
    形态 = 模型 provider 连续 `Connection error.` 后进程 exit 0、零改动、未提交验收报告
    （事故： 空跑被算成功 provider → 依赖它的评审任务两次空转白烧 token）。
    root/pid_ 缺省 None → 无任务目录上下文，不判空跑（False）。只读，不写任务目录。"""
    if not proto.life_terminal(doc) or root is None or pid_ is None:
        return False
    if doc.get("status") != "exited" or doc.get("exitcode") != 0:
        return False
    return not _report_present(root, pid_) and not provider_canceled(doc, root, pid_)


def provider_success(doc, root, pid_):
    """provider 成功判定（用户拍板方案①；空跑收紧 =）：
      ① 生命周期终态 ∧ exited ∧ exitcode 0 ∧ 任务目录存在**非空** report.md；或
      ② 生命周期终态 ∧ status=="stale" ∧ exitcode 127（stale：守护重启接手孤儿，
         死因不可得，§4 约定码）∧ 任务目录存在**非空** report.md（子进程实际完成的自证：
         固定指令要求全部验收 ✅ 才写报告且报告先于退出落盘；口径同 report.py 统计——
         runner 终态通知分类属信号面，仍按文件在场判）。收紧：必须 stale，
         真实 exited/127（如 command not found）不再误放行。
    非空 report.md 在场是两分支的共同前提 = DISPATCH.md「完成判定」（final + 非空 report.md）
    在调度面的落地：exit 0 但无（非空）报告 = 空跑（provider_idle），不算成功 provider，
    其能力不被满足。仍判失败：真实非 0 退出码、非 stale 态的 127、127 无报告、killed/137。
    取消（provider_canceled）不算失败也不算空跑，但同样不满足依赖（canceled 态）。
    root/pid_ 为**必需参数**（⚪1 收紧）：报告检查是成功判据的必要条件，
    没有任务目录上下文就无法判——漏传即 TypeError（开发期暴露），不留「静默回到旧宽口径
    （exited∧0 直接算成功）」的退化分支。
    无状态语义不变：判定只读文件，重启重扫即恢复（§14.5）。"""
    if not proto.life_terminal(doc):
        return False
    if doc.get("status") == "exited" and doc.get("exitcode") == 0:
        return _report_present(root, pid_)
    if doc.get("status") == "stale" and doc.get("exitcode") == 127:
        return _report_present(root, pid_)
    return False


# eval_needs 的 wait 原因串标记：同能力已有 success 但仍按 pending 等待（日志/审计锚点）
PENDING_OVER_SUCCESS_MARK = "ignoring older success provider"


def caps_pending_over_success(needs, providers_by_cap):
    """needs 中「同能力既有未终态 provider 又有旧 success」的 cap = eval_needs 判 wait 且
    PENDING_OVER_SUCCESS_MARK 成立的形态（判据单点：tick 据此升 WARNING、report.py 据此
    呈现卡点与恢复路径，三处不再靠解析 reason 串对齐）。
    返回 [(cap, pending_ids, success_ids)]；无命中返回 []。纯函数、只读。"""
    out = []
    for cap in needs or []:
        provs = providers_by_cap.get(cap) or []
        pending = [t for t, st in provs if st == "pending"]
        ok = [t for t, st in provs if st == "success"]
        if pending and ok:
            out.append((cap, pending, ok))
    return out


def eval_needs(needs, providers_by_cap):
    """能力依赖判定（三态语义沿用旧系统 evalNeeds；provider 态扩面 + pending 优先 =）。

    providers_by_cap: {cap: [(taskId, state)]}，state ∈
      {"success","pending","failed","canceled","idle"}（后两态见 _scan：canceled = 调度员
      主动取消，idle = 空跑 exit0∧无报告；两者都不满足依赖，但都不叫「失败」——
      日志/报表文案分开呈现，避免把取消与空跑说成实施失败）。
    返回 ("ok"|"wait"|"unsat", reasons[])：
      ok    = 每个能力都有成功 provider 且无未终态 provider；
      wait  = 某能力存在未终态 provider（等待其完成）——**优先于**同能力的旧 success：
              重发场景下新的真实施覆盖旧的交付，防「旧的假成功」提前放行依赖者
              （恢复路径：把 pending provider 取消/落地终态，即回到 success 判定）；
      unsat = 某能力无 provider，或 provider 全部终态且无成功者（failed/canceled/idle 任意组合）
              （不放行，成功 provider 出现后每轮重评自动解锁）。
    """
    reasons = []
    if not needs:
        return "ok", reasons
    waiting = False
    unsat = False
    for cap in needs:
        providers = providers_by_cap.get(cap, [])
        ok = [t for t, st in providers if st == "success"]
        pending = ["%s[%s]" % (t, st) for t, st in providers if st == "pending"]
        if pending:
            waiting = True
            reasons.append("cap %s: waiting provider %s%s" % (
                cap, ",".join(pending),
                (" (%s %s)" % (PENDING_OVER_SUCCESS_MARK, ",".join(ok))) if ok else ""))
            continue
        if ok:
            reasons.append("cap %s: provided by %s" % (cap, ok[0]))
            continue
        unsat = True
        reasons.append("cap %s: unsatisfiable (%s)" % (
            cap, "no provider" if not providers else
            ",".join("%s[%s]" % (t, st) for t, st in providers)))
    return ("unsat" if unsat else "wait" if waiting else "ok"), reasons


class Scheduler:
    """调度方（无状态，每轮重扫 §14.5）。独立常驻服务（scheduler-loop.sh 监督，
    多机阶段 2 去合署）或 --once 单次使用。"""

    def __init__(self, root, local_ids, max_concurrent=DEFAULT_MAX_CONCURRENT,
                 all_hosts=False):
        self.root = os.path.abspath(root)
        self.local_ids = local_ids
        self.max_concurrent = max_concurrent
        # 全局视图（多机阶段 2，设计 §3.4）：为真时候补不按本机身份过滤，
        # 为所有机器的任务放行；缺省假 = 按本机过滤的现行行为（逐字节不变）。
        self.all_hosts = all_hosts
        # 日志降噪：participantId -> 上次阻塞原因（仅日志去重，非调度状态）
        self._last_block = {}
        # 缺 host 周期性告警：participantId -> 上次告警时刻（非调度状态）
        self._nohost_warned_at = {}
        # 空跑 provider 告警去重：已 WARNING 过的 provider 集合（非调度状态，重启重打一次无害）
        self._idle_warned = set()

    # ---------- 扫描 ----------

    def _scan(self):
        """一遍扫描收集调度视图：占位者/能力表/候补队列。
        返回 (occupants, held_resources, providers_by_cap, queue)。
          occupants: [participantId]（running 或 已放行未落地，占位数口径）
          held_resources: 占位者持有的资源集合（含缺省 ["serial"]）
          providers_by_cap: {cap: [(taskId, "success"|"pending"|"failed"|
                                           "canceled"|"idle")]}（态语义见 eval_needs）
          queue: [(spec_mtime, participantId)] 候补（无 enable ∧ 无 pid.json）"""
        occupants = []
        held = set()
        providers_by_cap = {}
        queue = []
        for pid_ in proto.list_participants(self.root):
            spec = proto.read_json(proto.spec_path(self.root, pid_))
            if spec is None:
                continue
            # 缺 host 任务不受本机身份过滤（两种模式下都进队列视图，供告警；
            # 永不放行由 tick 把关 ）
            if (not self.all_hosts and not proto.host_missing(spec)
                    and not proto.host_matches(spec, self.local_ids)):
                continue
            fields = normalize_sched_fields(spec)
            doc = proto.read_json(proto.pid_path(self.root, pid_))
            enabled = os.path.exists(proto.enable_path(self.root, pid_))
            running = doc is not None and doc.get("status") == "running"
            if running or (enabled and doc is None):
                if not is_resident(spec):
                    # 常驻豁免（设计 §2.5.2）：常驻者不计入占位数、
                    # 不持有互斥资源——永久占槽会压死普通任务放行。
                    occupants.append(pid_)
                    held.update(fields["resources"])
            for cap in fields["provides"]:
                # provider 态判定优先序：success（有报告）> pending（未终态）
                # > canceled（取消豁免：不算失败也不算空跑）> idle（空跑 exit0∧无报告）> failed
                if provider_success(doc, self.root, pid_):
                    state = "success"
                elif not proto.life_terminal(doc):
                    state = "pending"  # 未终态（含排队未落地/运行中）
                elif provider_canceled(doc, self.root, pid_):
                    state = "canceled"
                elif provider_idle(doc, self.root, pid_):
                    state = "idle"   # 空跑：告警在 tick 打（_scan 被 report.py 复用，不喷日志）
                else:
                    state = "failed"
                providers_by_cap.setdefault(cap, []).append((pid_, state))
            if not enabled and doc is None:
                try:
                    mtime = os.path.getmtime(proto.spec_path(self.root, pid_))
                except OSError:
                    continue
                queue.append((mtime, pid_))
        return occupants, held, providers_by_cap, queue

    # ---------- 决策日志（去重） ----------

    def _block(self, pid_, reason, warn=False):
        """阻塞日志（去重降噪：原因变化才打，未变化降 DEBUG）。warn=True 时首次/变化
        打 WARNING（用于锁缺失/解析失败等存量异常口径）。"""
        prev = self._last_block.get(pid_)
        if prev != reason:
            if warn:
                log.warning("scheduler: blocked %s — %s", pid_, reason)
            else:
                log.info("scheduler: blocked %s — %s", pid_, reason)
            self._last_block[pid_] = reason
        else:
            log.debug("scheduler: blocked %s — %s (unchanged)", pid_, reason)

    # ---------- 一轮调度 ----------

    def _gate_target_host(self, spec, alive_cache):
        """放行门禁第四条件：目标主机存活。返回 (alive, reason, warn)。
        前提：spec.host 在场（缺 host 已被 tick 前置拦截为永久排队，
        不会到达本门禁）。候补锁名 = [spec.host]；命中本机身份集合时扩展为全集，
        防「别名登记、规范名持锁」错配误杀。同轮相同候补集合缓存结果（每锁每轮只读一次盘）。"""
        host = spec.get("host")
        names = sorted(set(self.local_ids)) if host in self.local_ids else [host]
        key = tuple(names)
        if key not in alive_cache:
            alive_cache[key] = host_runner_alive(self.root, names)
        return alive_cache[key]

    def tick(self):
        """无状态重扫放行：依赖满足 ∧ 资源可占 ∧ 占位 < 上限 ∧ 目标主机存活；
        允许越位。幂等。"""
        occupants, held, providers_by_cap, queue = self._scan()
        # 空跑 provider 告警（日志面）：只在 tick 打，_scan 保持无副作用
        # （report.py 复用 _scan 采集报表，不得因渲染而喷 WARNING）。
        for cap, lst in providers_by_cap.items():
            for prov, st in lst:
                if st == "idle":
                    self._warn_idle(prov, cap)
        self._idle_warned &= set(p for lst in providers_by_cap.values()
                                 for p, st in lst if st == "idle")
        queue.sort()
        seen = set()
        alive_cache = {}
        for _, pid_ in queue:
            spec = proto.read_json(proto.spec_path(self.root, pid_))
            if spec is None:
                continue
            if proto.host_missing(spec):
                # 缺 host = 无人认领（认领侧同口径）：永久排队，不放行，周期性告警。
                self._warn_nohost(pid_)
                continue
            fields = normalize_sched_fields(spec)
            resident = is_resident(spec)
            verdict, reasons = eval_needs(fields["needs"], providers_by_cap)
            if verdict == "unsat":
                self._block(pid_, "needs unsatisfiable: " + "; ".join(
                    r for r in reasons if "unsatisfiable" in r))
                continue
            if verdict == "wait":
                # pending 压住同能力旧 success 时升 WARNING（可观测：旧交付被忽略是异常形态，
                # 需调度员确认该 pending provider 是否该取消；恢复路径见 eval_needs 注释。
                # 判据 = caps_pending_over_success 单点，报表异常区同源呈现）
                self._block(pid_, "needs waiting: " + "; ".join(reasons),
                            warn=bool(caps_pending_over_success(
                                fields["needs"], providers_by_cap)))
                continue
            clash = sorted(set(fields["resources"]) & held)
            if clash and not resident:
                self._block(pid_, "resources held: %s" % ",".join(clash))
                continue
            if len(occupants) >= self.max_concurrent and not resident:
                self._block(pid_, "slots full: %d/%d" % (
                    len(occupants), self.max_concurrent))
                continue
            # resident 豁免（设计 §2.5.2）：常驻候补不检查资源交集、
            # 不受占位上限约束，候补判定与 host 存活检查保留，即时写 enable 放行。
            alive, detail, warn = self._gate_target_host(spec, alive_cache)
            if not alive:
                # 目标机无存活 runner：不放行、留候补队列（不占槽位/资源，与未放行
                # 语义一致；排队不是错误 §11.9）。：host=mac 无
                # runner 被放行占住 serial 槽位，阻塞全队列约 9 小时——门禁根治。
                self._block(pid_, "host %s runner not alive (%s)" % (
                    spec.get("host"), detail), warn=warn)
                continue
            seen.add(pid_)
            waiting = len(queue) - len(seen)
            if not fields["needs"] and fields["resources"] == DEFAULT_RESOURCES:
                note = "fifo-serial: %d waiting" % waiting   # 一期口径原文案
            else:
                note = ("dag: deps ok%s; resources free(%s); slots %d/%d; %d waiting"
                        % ("(" + ",".join(fields["needs"]) + ")" if fields["needs"] else "",
                           ",".join(fields["resources"]) or "-",
                           len(occupants) + 1, self.max_concurrent, waiting))
            proto.atomic_write_json(proto.enable_path(self.root, pid_),
                                    {"ts": proto.now_ts(), "by": SCHEDULER_ID,
                                     "note": note})
            self._last_block.pop(pid_, None)
            # 占位与资源即时生效（防同轮重复放行/双占，旧系统 started 同语义）；
            # resident 不入占位面（永久豁免）。
            if not resident:
                occupants.append(pid_)
                held.update(fields["resources"])
            log.info("scheduler: enabled %s (%s)", pid_, note)
        # 清理已消失者的去重残留（防无限增长）
        queued = set(queue_item[1] for queue_item in queue)
        for pid_ in [p for p in self._last_block if p not in queued]:
            self._last_block.pop(pid_, None)
        for pid_ in [p for p in self._nohost_warned_at if p not in queued]:
            self._nohost_warned_at.pop(pid_, None)

    # ---------- 空跑 provider 告警（日志面） ----------

    def _warn_idle(self, pid_, cap):
        """空跑 provider 首次发现打一次 WARNING（内存去重 = 非调度状态，重启重打无害）。
        通知面既有信号 = runner 终态通知载荷 warn=no_report（投职位信箱 topic/dispatcher）。"""
        if pid_ in self._idle_warned:
            return
        self._idle_warned.add(pid_)
        log.warning("scheduler: provider %s 空跑（exitcode=0 ∧ 无非空 report.md ∧ 非取消）——"
                    "不算成功 provider，cap %s 不被满足，依赖者不放行；"
                    "处置 = 重发实现任务或取消依赖者（DISPATCH.md §11）", pid_, cap)

    # ---------- 缺 host 周期告警 ----------

    NOHOST_WARN_INTERVAL = 1800.0  # 30 分钟重打一次（巡检可发现；非常驻刷屏）

    def _warn_nohost(self, pid_):
        """缺 host 任务视为永久排队：周期性 WARNING（首次 + 每 30 分钟），
        提示人工补 spec.host 或重新登记。"""

        now = time.monotonic()
        last = self._nohost_warned_at.get(pid_)
        if last is None or now - last >= self.NOHOST_WARN_INTERVAL:
            self._nohost_warned_at[pid_] = now
            log.warning("scheduler: %s spec.host 缺失，无人认领，永久排队不放行；"
                        "需人工补 spec.host 或重新登记", pid_)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True)
    ap.add_argument("--host", default=None)
    ap.add_argument("--aliases", default="")
    ap.add_argument("--max-concurrent", type=int, default=DEFAULT_MAX_CONCURRENT)
    ap.add_argument("--all-hosts", action="store_true",
                    help="全局视图：候补不按本机身份过滤，为所有机器放行（多机阶段 2）；"
                         "缺省关 = 只放行归本机的任务（行为不变）")
    ap.add_argument("--once", action="store_true", help="只跑一轮即退出")
    ap.add_argument("--interval", type=float, default=0.5)
    ap.add_argument("--log-file", default=None)
    ap.add_argument("--log-level", default="INFO",
                    choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    a = ap.parse_args()
    h = logging.FileHandler(a.log_file, encoding="utf-8") if a.log_file \
        else logging.StreamHandler()
    h.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [h]
    root.setLevel(getattr(logging, a.log_level.upper(), logging.INFO))

    local_ids = proto.local_hosts(a.host, a.aliases)
    sched = Scheduler(a.root, local_ids, a.max_concurrent, all_hosts=a.all_hosts)
    if a.once:
        sched.tick()
        return
    while True:
        try:
            sched.tick()
        except Exception:
            log.exception("scheduler tick error (retried next tick)")
        time.sleep(a.interval)


if __name__ == "__main__":
    main()
