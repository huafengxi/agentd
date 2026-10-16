"""Read-only task observations, scheduling projections and gate decisions."""
from dataclasses import dataclass, field
import os
import time

import proto
import task_state


DEFAULT_RESOURCES = ["serial"]         # resources 缺省语义（旧系统缺省，串行手段）
DEFAULT_MAX_CONCURRENT = 4             # 全局占位上限（缺省，拍板 2026-08-27）

# ---- 放行门禁第四条件：目标主机存活判定 ----
# 数据源 = 每机探活锁 <root>/agents/run/agentd.<host>.lock（proto.agentd_lock_path 单点，
# DESIGN-multimachine §4.4：runner 心跳式定期刷新 updatedAt，别机按新鲜度判存活，
# 停更即自然判死；锁经星型同步汇聚到全局树，允许 ≤1 周期滞后）。
# 阈值推导（复用现网既有口径，不新造标准）：
#   心跳周期 = runner.py 的 LOCK_REFRESH_INTERVAL（现值 5s；⛔ 为取该常量 import runner——
#   模块级 import 会执行它的模块体，把本不相关的运行半边拉成本服务的运行时依赖）；
#   同步滞后 ≤1 周期 = 轮询兜底 interval 15s（ssh-sync 缺省）+ rsync 耗时（DESIGN §2.3）；
#   DESIGN §4.4 口径「判活阈值（以分钟计）」；
#   60s ≥ 数个心跳周期 + 一整同步周期 + rsync 耗时，另吸收中等跨机时钟偏差
#   （updatedAt 用写方本机时钟写、读方本机时钟比，余量由阈值承担）。
HOST_ALIVE_THRESHOLD = 60.0  # 秒：updatedAt 陈旧度超过该值 → 判目标机无存活 runner


def host_lock_path(root, host):
    """探活锁路径（单点 = proto.agentd_lock_path，任务）。"""
    return proto.agentd_lock_path(root, host)


def parse_host_ts(ts):
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
        epoch = parse_host_ts(doc.get("updatedAt")) if isinstance(doc, dict) else None
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


# eval_needs 的 wait 原因串标记：同能力已有 success 但仍按 pending 等待（日志/审计锚点）
PENDING_OVER_SUCCESS_MARK = "ignoring older success provider"


@dataclass(frozen=True)
class Dependency:
    cap: str
    state: str
    providers: tuple

    @property
    def pending(self):
        return tuple(pid for pid, state in self.providers if state == "pending")

    @property
    def success(self):
        return tuple(pid for pid, state in self.providers if state == "success")


def dependency_details(needs, providers_by_cap):
    result = []
    for cap in needs or []:
        providers = tuple(providers_by_cap.get(cap) or [])
        states = {state for _, state in providers}
        state = "wait" if "pending" in states else "ok" if "success" in states else "unsat"
        result.append(Dependency(cap, state, providers))
    return tuple(result)


def caps_pending_over_success(needs, providers_by_cap):
    return [(d.cap, list(d.pending), list(d.success))
            for d in dependency_details(needs, providers_by_cap)
            if d.pending and d.success]


def eval_needs(needs, providers_by_cap):
    """Compatibility text view of structured dependency results."""
    details = dependency_details(needs, providers_by_cap)
    reasons = []
    for d in details:
        if d.state == "wait":
            pending = ",".join("%s[pending]" % pid for pid in d.pending)
            reasons.append("cap %s: waiting provider %s%s" % (
                d.cap, pending, (" (%s %s)" % (PENDING_OVER_SUCCESS_MARK,
                                             ",".join(d.success))) if d.success else ""))
        elif d.state == "ok":
            reasons.append("cap %s: provided by %s" % (d.cap, d.success[0]))
        else:
            providers = ",".join("%s[%s]" % pair for pair in d.providers)
            reasons.append("cap %s: unsatisfiable (%s)" % (d.cap, providers or "no provider"))
    states = {d.state for d in details}
    return ("unsat" if "unsat" in states else "wait" if "wait" in states else "ok"), reasons


@dataclass(frozen=True)
class TaskObservation:
    id: str
    spec: object = None
    pid: object = None
    spec_exists: bool = False
    pid_exists: bool = False
    enabled: bool = False
    spec_mtime: object = None
    has_report: bool = False
    stopped: bool = False
    error: object = None


def observe_tasks(root):
    """Read each participant's scheduling facts once; retain unreadable-file presence."""
    records = {}
    for pid in proto.list_participants(root):
        try:
            sp, pp = proto.spec_path(root, pid), proto.pid_path(root, pid)
            spec_exists, pid_exists = os.path.exists(sp), os.path.exists(pp)
            spec, doc = proto.read_json(sp), proto.read_json(pp)
            try:
                mtime = os.path.getmtime(sp)
            except OSError:
                mtime = None
            records[pid] = TaskObservation(
                pid, spec if isinstance(spec, dict) else None,
                doc if isinstance(doc, dict) else None, spec_exists, pid_exists,
                os.path.exists(proto.enable_path(root, pid)), mtime,
                task_state.report_nonempty(os.path.join(proto.agent_dir(root, pid), "report.md")),
                task_state.stop_requested(root, pid))
        except (OSError, ValueError) as error:
            records[pid] = TaskObservation(pid, error=str(error))
    return records


@dataclass
class SchedulingView:
    records: dict
    occupants: list = field(default_factory=list)
    held_resources: set = field(default_factory=set)
    providers_by_cap: dict = field(default_factory=dict)
    queue: list = field(default_factory=list)
    resource_holders: dict = field(default_factory=dict)


def scheduling_view(root, local_ids=(), all_hosts=True, records=None):
    """Project observations into occupancy, dependency and candidate indexes."""
    records = observe_tasks(root) if records is None else records
    view = SchedulingView(records)
    for pid, record in records.items():
        if record.error:
            raise ValueError(record.error)
        spec, doc = record.spec, record.pid
        if spec is None:
            continue
        if not all_hosts and not proto.host_missing(spec) and not proto.host_matches(spec, local_ids):
            continue
        fields = normalize_sched_fields(spec)
        running = doc is not None and doc.get("status") == "running"
        if (running or (record.enabled and doc is None)) and not task_state.is_resident(spec):
            view.occupants.append(pid)
            view.held_resources.update(fields["resources"])
            for resource in fields["resources"]:
                view.resource_holders.setdefault(resource, []).append(pid)
        state = task_state.classify_provider(doc, record.has_report, record.stopped)
        for cap in fields["provides"]:
            view.providers_by_cap.setdefault(cap, []).append((pid, state))
        if not record.enabled and doc is None and record.spec_mtime is not None:
            view.queue.append((record.spec_mtime, pid))
    return view


def probe_target_host(root, spec, local_ids, cache):
    """Read each candidate host-set once per view, including local aliases."""
    host = spec.get("host")
    names = sorted(set(local_ids)) if host in local_ids else [host]
    key = tuple(names)
    if key not in cache:
        cache[key] = host_runner_alive(root, names)
    return cache[key]


@dataclass(frozen=True)
class GateDecision:
    code: str
    reason: str = ""
    warn: bool = False
    resources: tuple = ()
    dependencies: tuple = ()

    @property
    def ready(self):
        return self.code == "ready"


def evaluate_gate(spec, occupants, held, providers_by_cap, max_concurrent, host_probe):
    """Apply the scheduler's gate order; probe hosts only after earlier gates pass."""
    if proto.host_missing(spec):
        return GateDecision("missing_host", "spec.host missing")
    fields = normalize_sched_fields(spec)
    dependencies = dependency_details(fields["needs"], providers_by_cap)
    verdict, reasons = eval_needs(fields["needs"], providers_by_cap)
    if verdict == "unsat":
        return GateDecision("needs_unsat", "needs unsatisfiable: " + "; ".join(
            reason for reason, dependency in zip(reasons, dependencies) if dependency.state == "unsat"),
                            dependencies=dependencies)
    if verdict == "wait":
        return GateDecision("needs_wait", "needs waiting: " + "; ".join(reasons),
                            warn=bool(caps_pending_over_success(fields["needs"], providers_by_cap)),
                            dependencies=dependencies)
    resident = task_state.is_resident(spec)
    clash = tuple(sorted(set(fields["resources"]) & held))
    if clash and not resident:
        return GateDecision("resources", "resources held: %s" % ",".join(clash), resources=clash)
    if max_concurrent is not None and len(occupants) >= max_concurrent and not resident:
        return GateDecision("slots", "slots full: %d/%d" % (len(occupants), max_concurrent))
    alive, detail, warn = host_probe(spec)
    if not alive:
        return GateDecision("host", "host %s runner not alive (%s)" % (spec.get("host"), detail), warn)
    return GateDecision("ready")
