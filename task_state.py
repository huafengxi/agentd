"""Shared task lifecycle and delivery predicates; no scheduler or CLI dependency."""
import os

import proto


def is_resident(spec):
    """Explicit supervision lifecycle, independent of command text and persona."""
    return isinstance(spec, dict) and spec.get("resident") is True


# 分块扫描报告，遇到非空白即返回，长空白前缀无需重开或整文件读入。
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
            for chunk in iter(lambda: f.read(REPORT_HEAD_BYTES), b""):
                if chunk.strip():
                    return True
    except OSError:
        return False
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
    return classify_provider(doc, _report_present(root, pid_)) == "success"


def classify_provider(doc, has_report, stopped=False):
    """Classify a provider from observed facts; no filesystem access."""
    if not proto.life_terminal(doc):
        return "pending"
    status, code = doc.get("status"), doc.get("exitcode")
    if has_report and ((status == "exited" and code == 0) or
                       (status == "stale" and code == proto.EXITCODE_STALE)):
        return "success"
    if stopped or provider_canceled(doc):
        return "canceled"
    if status == "exited" and code == 0 and not has_report:
        return "idle"
    return "failed"
