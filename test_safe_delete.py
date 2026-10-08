#!/usr/bin/env python3
"""test_safe_delete.py — `safe_delete.py` 单测（D 系列）：删除前的身份断言面。

判据（口径正文 = `safe_delete.py` 的模块 docstring，⛔ 此处复述第二份）：五道闸按序判、
命中即拒绝且**不删**；授权根由调用方显式传入；⛔ `ignore_errors` ∕ ⛔ 内部吞异常。

**验「拒删」一律用无害诱饵**：所有目标都在本文件自建的临时根内（`tempfile.mkdtemp`），
⛔ 对任何生产根跑删除类命令——守卫失效时那一跑就删掉整仓含未追踪文件、git 恢复不了。
「拒绝」性质的核法 = 断言返回值 ∧ 断言目标**逐字节仍在场**（inode 号 + 内容 md5 前后同值），
⛔ 只看返回码。

用例：
  D1 授权根内自建子树 ⇒ 放行并真删
  D2 目标 == 授权根 ⇒ 拒绝、授权根仍在场
  D3 目标是授权根的祖先 ⇒ 拒绝、祖先树仍在场
  D4 目标不在授权根内部（同基目录的兄弟子树）⇒ 拒绝、兄弟树仍在场
  D5 第二道闸：目标在系统临时目录外 ⇒ 拒绝；显式白名单覆盖 ⇒ 放行
  D6 目标不存在 ⇒ 拒绝（⛔ 吞成放行）
  D7 顺序核：放行路径 = authorize 在 rmtree **之前**；四条拒绝路径 = rmtree 零调用
  D8 源码面非执行核证：`safe_rmtree` 体内 authorize 的调用行号 < rmtree 的调用行号，
     且模块源码里 `ignore_errors` ∕ `except` 零命中（⛔ 吞错即失去防线）
  D9 符号链接形态：授权根内的 symlink 指向授权根外 ⇒ realpath 解穿后按 D3 拒绝

仅标准库（本仓测试面无 pytest）。用法：python3 agentd/test_safe_delete.py
"""
import ast
import hashlib
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import safe_delete as SD        # noqa: E402

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


def mkroot(case):
    d = tempfile.mkdtemp(prefix="safe-delete-test.")
    TMP.append(d)
    r = os.path.join(d, case)
    os.makedirs(r)
    return d, r


def tree(root, name, files=("a.txt", "sub/b.txt")):
    """在 root 内造一枚同形诱饵子树，返回其路径。"""
    t = os.path.join(root, name)
    for rel in files:
        p = os.path.join(t, rel)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            f.write("诱饵正文 %s\n" % rel)
    return t


def ident(path):
    """在场性指纹 = (inode, 逐文件 md5 字典)；⛔ 只看「路径不存在」当拒绝的证据。"""
    if not os.path.exists(path):
        return None
    dig = {}
    for dirpath, _dirnames, filenames in os.walk(path):
        for fn in sorted(filenames):
            p = os.path.join(dirpath, fn)
            with open(p, "rb") as f:
                dig[os.path.relpath(p, path)] = hashlib.md5(f.read()).hexdigest()
    return (os.stat(path).st_ino, dig)


def cleanup():
    """清理面自带身份断言（在 rmtree 之前）；⛔ `ignore_errors=True`。"""
    tmp = os.path.realpath(tempfile.gettempdir())
    for d in TMP:
        real = os.path.realpath(d)
        if not real.startswith(tmp + os.sep) or not os.path.basename(real).startswith(
                "safe-delete-test."):
            print("拒绝清理：%s 不是本文件自建的临时根（tempdir=%s）" % (real, tmp))
            continue
        shutil.rmtree(real)


def spy_rmtree():
    """把模块内的 `shutil.rmtree` 换成计数间谍（返回 (log, restore)）。"""
    log = []
    real = SD.shutil.rmtree

    def spy(path, *a, **kw):
        log.append(("rmtree", path))
        return real(path, *a, **kw)

    SD.shutil.rmtree = spy

    def restore():
        SD.shutil.rmtree = real

    return log, restore


def spy_authz(log):
    real = SD.authorize_target

    def spy(*a, **kw):
        log.append(("authorize",))
        return real(*a, **kw)

    SD.authorize_target = spy
    return real


def d1():
    base, root = mkroot("d1")
    t = tree(root, "work")
    before = ident(t)
    deleted, why = SD.safe_rmtree(t, authorized_root=base)
    ok("D1 授权根内自建子树 ⇒ 放行并真删（返回 (True, '')、目标已不在场、授权根仍在场）",
       deleted is True and why == "" and not os.path.exists(t) and os.path.isdir(base),
       (deleted, why, before is not None))


def d2():
    base, root = mkroot("d2")
    tree(root, "keep")
    before = ident(base)
    deleted, why = SD.safe_rmtree(base, authorized_root=base)
    ok("D2 目标 == 授权根 ⇒ 拒绝且授权根**逐字节仍在场**（inode + 内容 md5 同值）",
       deleted is False and "授权根" in why and ident(base) == before, (deleted, why))


def d3():
    base, root = mkroot("d3")
    deep = os.path.join(root, "a", "b")
    os.makedirs(deep)
    tree(root, "a")
    before = ident(os.path.join(root, "a"))
    deleted, why = SD.safe_rmtree(os.path.join(root, "a"), authorized_root=deep)
    ok("D3 目标是授权根的祖先 ⇒ 拒绝且祖先树逐字节仍在场",
       deleted is False and "祖先" in why and ident(os.path.join(root, "a")) == before,
       (deleted, why))


def d4():
    base, root = mkroot("d4")
    auth = tree(root, "auth")
    sib = tree(root, "sibling")
    before = ident(sib)
    deleted, why = SD.safe_rmtree(sib, authorized_root=auth)
    ok("D4 目标不在授权根内部（兄弟子树）⇒ 拒绝且兄弟树逐字节仍在场",
       deleted is False and "不在授权根内部" in why and ident(sib) == before, (deleted, why))


def d5():
    base, root = mkroot("d5")
    t = tree(root, "outside")
    before = ident(t)
    real_gettempdir = tempfile.gettempdir
    fake = os.path.join(base, "not-the-tempdir")
    os.makedirs(fake)
    try:
        SD.tempfile.gettempdir = lambda: fake          # 让 t 落到「系统临时目录外」
        deleted, why = SD.safe_rmtree(t, authorized_root=base)
        ok("D5a 第二道闸：目标在系统临时目录外 ⇒ 拒绝且目标逐字节仍在场",
           deleted is False and "系统临时目录" in why and ident(t) == before, (deleted, why))
        deleted2, why2 = SD.safe_rmtree(t, authorized_root=base, allow_outside_temp=(base,))
        ok("D5b 调用方显式白名单覆盖第二道闸 ⇒ 放行并真删",
           deleted2 is True and why2 == "" and not os.path.exists(t), (deleted2, why2))
    finally:
        SD.tempfile.gettempdir = real_gettempdir


def d6():
    base, root = mkroot("d6")
    ghost = os.path.join(root, "ghost")
    deleted, why = SD.safe_rmtree(ghost, authorized_root=base)
    ok("D6 目标不存在 ⇒ 拒绝（⛔ 吞成放行：「删了个空气」不得记成成功）",
       deleted is False and "不存在" in why, (deleted, why))


def d7():
    base, root = mkroot("d7")
    t = tree(root, "work")
    log, restore = spy_rmtree()
    real_auth = spy_authz(log)
    try:
        SD.safe_rmtree(t, authorized_root=base)
        ok("D7a 放行路径的调用顺序 = authorize **在** rmtree 之前（⛔ 反序）",
           log == [("authorize",), ("rmtree", os.path.realpath(t))], log)
        log[:] = []
        keep = tree(root, "keep")
        keep2 = tree(root, "keep2")
        deep = os.path.join(keep2, "sub")
        SD.safe_rmtree(keep, authorized_root=keep)            # 闸 2：目标 == 授权根
        SD.safe_rmtree(keep2, authorized_root=deep)           # 闸 3：目标是授权根的祖先
        SD.safe_rmtree(keep2, authorized_root=keep)           # 闸 4：不在授权根内部
        SD.safe_rmtree(os.path.join(root, "ghost"), authorized_root=base)   # 闸 1：不存在
        ok("D7b 四条拒绝路径 ⇒ `shutil.rmtree` **零调用**（间谍只见到 authorize）且两棵诱饵树在场",
           log == [("authorize",)] * 4 and os.path.isdir(keep) and os.path.isdir(keep2), log)
    finally:
        SD.authorize_target = real_auth
        restore()


def d8():
    src_path = os.path.join(HERE, "safe_delete.py")
    with open(src_path, encoding="utf-8") as f:
        src = f.read()
    mod = ast.parse(src)
    fn = next(n for n in mod.body if isinstance(n, ast.FunctionDef) and n.name == "safe_rmtree")
    lines = {"authorize_target": [], "rmtree": []}
    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            f_ = node.func
            name = (f_.attr if isinstance(f_, ast.Attribute) else
                    f_.id if isinstance(f_, ast.Name) else None)
            if name in lines:
                lines[name].append(node.lineno)
    ok("D8a 源码面：`safe_rmtree` 体内 `authorize_target` 的调用行号 < `shutil.rmtree` 的调用行号"
       "（断言在删除之前，非执行面核证）",
       len(lines["authorize_target"]) == 1 and len(lines["rmtree"]) == 1
       and max(lines["authorize_target"]) < min(lines["rmtree"]), lines)
    ok("D8b 源码面：模块内无 `ast.Try` 节点、无 `ignore_errors` 实参（⛔ 吞错即失去防线；"
       "核法走 AST ⇒ docstring 里写「⛔ try/except」这几个字不会自伤）",
       not [n for n in ast.walk(mod) if isinstance(n, ast.Try)]
       and not [k for n in ast.walk(mod) if isinstance(n, ast.Call) for k in n.keywords
                if k.arg == "ignore_errors"],
       (len([n for n in ast.walk(mod) if isinstance(n, ast.Try)]),))
    ok("D8c 源码面：只暴露一个删除入口（`__all__` 里的删除动词恰一枚 = `safe_rmtree`）",
       SD.__all__.count("safe_rmtree") == 1
       and not [n for n in SD.__all__ if n != "safe_rmtree" and n.startswith("safe_")],
       SD.__all__)


def d9():
    base, root = mkroot("d9")
    tree(root, "real")
    link = os.path.join(root, "link")
    os.symlink(os.path.join(root, "real"), link)               # 授权根内的 symlink → 根内真树
    before = ident(os.path.join(root, "real"))
    deleted, why = SD.safe_rmtree(link, authorized_root=link)  # 解穿后 target == 授权根
    ok("D9 符号链接形态：realpath 解穿后按「目标 == 授权根」拒绝、真树逐字节仍在场"
       "（⛔ 按路径字面判 ⇒ 会放行）",
       deleted is False and "授权根" in why
       and ident(os.path.join(root, "real")) == before, (deleted, why))


def main():
    for fn in (d1, d2, d3, d4, d5, d6, d7, d8, d9):
        fn()
    print("\n==== safe_delete 单测：%d passed, %d failed" % (PASS, FAIL))
    cleanup()
    left = [d for d in TMP if os.path.exists(os.path.realpath(d))]
    if left:
        print("清理残留：%s" % left)
    sys.exit(1 if FAIL or left else 0)


if __name__ == "__main__":
    main()
