"""Agent business predicates; execution authority belongs to rund."""
import os
REPORT_HEAD_BYTES=4096


def is_resident(spec):
    """Explicit supervision lifecycle, independent of command text and persona."""
    return isinstance(spec, dict) and spec.get("resident") is True


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
