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
from task_state import (is_resident, report_nonempty, stop_requested,
                        provider_canceled, provider_idle, provider_success)

log = logging.getLogger("agentd.scheduler")

SCHEDULER_ID = "agentd-scheduler"      # enable.json 的 by 字段（调度方标识，§14.2）
# Public re-exports keep existing CLI consumers on the shared implementation.
from scheduling import (DEFAULT_RESOURCES, DEFAULT_MAX_CONCURRENT,
                        HOST_ALIVE_THRESHOLD, PENDING_OVER_SUCCESS_MARK,
                        host_lock_path, host_runner_alive, parse_host_ts as _parse_ts,
                        normalize_sched_fields, eval_needs, caps_pending_over_success,
                        scheduling_view, evaluate_gate, probe_target_host)


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
        return probe_target_host(self.root, spec, self.local_ids, alive_cache)

    def tick(self):
        """无状态重扫放行：依赖满足 ∧ 资源可占 ∧ 占位 < 上限 ∧ 目标主机存活；
        允许越位。幂等。"""
        view = scheduling_view(self.root, self.local_ids, self.all_hosts)
        occupants, held = view.occupants, view.held_resources
        providers_by_cap, queue = view.providers_by_cap, view.queue
        # Idle warnings belong to the writer; the shared observation layer stays read-only.
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
            fields = normalize_sched_fields(spec)
            resident = is_resident(spec)
            decision = evaluate_gate(spec, occupants, held, providers_by_cap,
                                     self.max_concurrent,
                                     lambda candidate: self._gate_target_host(candidate, alive_cache))
            if decision.code == "missing_host":
                self._warn_nohost(pid_)
                continue
            if not decision.ready:
                self._block(pid_, decision.reason, warn=decision.warn)
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
        通知面既有信号 = runner 终态通知载荷 warn=no_report（投职位信箱 = proto.POSITION_PID）。"""
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
