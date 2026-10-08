#!/usr/bin/env python3
"""safe_delete.py — 删除类操作的**身份断言前置**单点。

删除类命令（`rm -rf` ∕ `shutil.rmtree` ∕ 批量 unlink）的失败形态不是「删不掉」而是
「删掉了不该删的」：目标路径由变量拼出来时，同一个变量在不同分支上可能分别指向**自建临时根**与
**生产根**，而路径字符串「看起来不像真的」不构成证据。本模块把「先证身份、后动手」做成机制，
使调用方无法把两步写反。

# 调用姿势

    import tempfile
    from safe_delete import safe_rmtree

    BASE = tempfile.mkdtemp(prefix="myjob.")      # 授权根 = 本次自建的临时基目录
    ...                                            # 在 BASE 内造工作树
    deleted, why = safe_rmtree(os.path.join(BASE, "work"), authorized_root=BASE)
    # deleted=True  ⇒ 已删；deleted=False ⇒ **一行拒绝理由已印到 stdout、目标一个字节未动**

批量删文件的形态 = 把待删文件所在的**自建子树整棵**交给 `safe_rmtree`（⛔ 逐枚 unlink：
那会把「身份断言」退化成每枚一次、且漏一枚就静默残留）。本模块**只此一个删除入口**。

# 判据（`authorize_target` 五道闸，按序判、命中即返回）

1. 目标不存在 ⇒ 拒绝（存在性由调用方判；⛔ 在此吞成放行，否则「删了个空气」会被记成成功）。
2. 目标 == 授权根 ⇒ 拒绝（授权根是清理面的**锚**、不是清理对象）。
3. 目标是授权根的**祖先** ⇒ 拒绝。
4. 目标不在授权根**内部** ⇒ 拒绝。
5. **第二道闸**（与 2–4 的授权根前缀判定独立）：目标不在系统临时目录
   （`tempfile.gettempdir()`）内、且不在调用方显式白名单 `allow_outside_temp` 里 ⇒ 拒绝。
   授权根自身在生产树相邻处时，这一道是唯一的挡面。

授权根**必须由调用方显式传入**（无缺省值）：缺省落到任何常量都等于把生产树写进机制。

# ⛔ 面

- ⛔ `shutil.rmtree(..., ignore_errors=True)`：吞错即失去防线（残留半个树却报成功）。
- ⛔ 内部 `try/except: pass`：`OSError`（权限 ∕ 非空 ∕ 目标被换成符号链接）一律上抛给调用方。
- ⛔ 在本模块写死主机名 ∕ 机器清单 ∕ 仓名单 ∕ 内网端点 ∕ 凭据面路径（收录判据 = 只住机制，
  判据 = 改一处部署不得产生本仓的 diff）。

单测 = 同目录 `test_safe_delete.py`（扁平形态，与本仓其余 `test_*.py` 同款）。
"""

import os
import shutil
import sys
import tempfile

__all__ = ["safe_rmtree", "authorize_target"]


def _norm(path):
    """解到实体再判：`~` 展开 ∕ 相对路径按 cwd 解 ∕ 符号链接与 `..` 一律解穿。"""
    return os.path.realpath(os.path.abspath(os.path.expanduser(str(path))))


def authorize_target(target, authorized_root, allow_outside_temp=()):
    """删除前的身份/前缀判定（**纯函数、零副作用** ⇒ 可单测、可被删除入口复用）。

    返回 `(True, "")` = 放行 ∨ `(False, <一行拒绝理由>)` = 拒绝。五道闸见模块 docstring。"""
    t = _norm(target)
    a = _norm(authorized_root)
    if not os.path.exists(t):
        return False, "目标不存在（%s）⇒ 不删：存在性由调用方判，⛔ 在此吞成放行" % t
    if t == a:
        return False, "目标 == 授权根（%s）⇒ 不删：授权根是清理面的锚、不是清理对象" % t
    if a.startswith(t + os.sep):
        return False, "目标是授权根的祖先（%s ⊃ %s）⇒ 不删" % (t, a)
    if not t.startswith(a + os.sep):
        return False, "目标不在授权根内部（%s ∉ %s）⇒ 不删" % (t, a)
    tmp = _norm(tempfile.gettempdir())
    if not t.startswith(tmp + os.sep):
        wl = tuple(_norm(x) for x in (allow_outside_temp or ()))
        if not any(t == w or t.startswith(w + os.sep) for w in wl):
            return False, ("目标不在系统临时目录内（%s ∉ %s）且不在显式白名单 %s ⇒ 不删"
                           % (t, tmp, list(wl) or "（空）"))
    return True, ""


def safe_rmtree(target, authorized_root, allow_outside_temp=()):
    """**唯一的删除入口**：身份断言在前、`shutil.rmtree` 在后（⛔ 反序）。

    返回 `(True, "")` = 已删 ∨ `(False, <理由>)` = 未删且理由已印到 stdout。
    断言与删除之间若目标被换成符号链接 ⇒ `shutil.rmtree` 自身拒绝（它不对 symlink 递归），
    异常上抛、⛔ 本函数吞。"""
    allowed, why = authorize_target(target, authorized_root, allow_outside_temp)
    if not allowed:
        sys.stdout.write("safe_delete 拒绝：%s\n" % why)
        return False, why
    shutil.rmtree(_norm(target))
    return True, ""
