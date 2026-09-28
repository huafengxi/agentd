#!/usr/bin/env python3
"""fakeagent.py — e2e 测试用的伪 agent 负载（短命脚本进程）。

由 runner 按 spec.command 拉起；环境变量（runner 注入）：
  AGENT_HOME  自家目录 agents/<id>/
  AGENT_ROOT  agents/ 树根
  AGENT_SELF  自己的参与方标识

行为（每 0.2s 一轮）：
  1. 启动时若自家目录存在 `crashflag` 文件 → 立即以码 3 退出（模拟崩溃，供自愈测试）；
  2. 读 inbox 未 ack 消息（按文件名序）：
     - type=ask    → 向对方（路径式 id 直落）写 reply（ref 回引），
                     然后写自家 inbox/ack/<id>（§6.3/§6.4）；
     - type=inform → ack；body 以 "CMD:" 开头则执行指令：
         CMD:exit      → 正常退出（码 0）
         CMD:crash     → 模拟崩溃（码 3）
       其余 inform（含 restart inject 的预置消息）追加进 session/memory.txt
       （跨代记忆体验证，§3.4）。
仅使用 python3 标准库。
"""
import json
import os
import sys
import time

HOME = os.environ["AGENT_HOME"]
ROOT = os.environ["AGENT_ROOT"]
SELF = os.environ["AGENT_SELF"]


def atomic_write(path, text):
    d = os.path.dirname(path)
    os.makedirs(d, exist_ok=True)
    tmp = path + ".tmp%d" % os.getpid()
    with open(tmp, "w") as f:
        f.write(text)
    os.rename(tmp, path)


def send(to, msg):
    # 路径式 id 直落（同 proto.agent_dir）：to = <family>/<name>
    family, _, name = to.partition("/")
    if family not in ("task", "bot") or not name or "/" in name:
        return  # 非法寻址：不投递（测试负载宽容处置）
    inbox = os.path.join(ROOT, "agents", family, name, "inbox")
    # from 进文件名：`/` → `.` 转写（§2.2 fs_safe_id 口径）
    mid = time.strftime("%Y-%m-%d-%H-%M-%S") + "-" + SELF.replace("/", ".") + "-" + \
        os.urandom(2).hex()
    msg = {"id": mid, "from": SELF, **msg}
    atomic_write(os.path.join(inbox, mid + ".msg"),
                 json.dumps(msg, ensure_ascii=False) + "\n")


def acked_set():
    d = os.path.join(HOME, "inbox", "ack")
    return set(os.listdir(d)) if os.path.isdir(d) else set()


def main():
    if os.path.exists(os.path.join(HOME, "crashflag")):
        sys.exit(3)  # 模拟崩溃
    rounds = 0
    while True:
        rounds += 1
        if os.path.exists(os.path.join(HOME, "crashflag")):
            sys.exit(3)  # 运行期出现旗 → 模拟崩溃（供自愈测试在存活后触发）
        acked = acked_set()
        idir = os.path.join(HOME, "inbox")
        msgs = sorted(f for f in os.listdir(idir)) if os.path.isdir(idir) else []
        for fn in msgs:
            if not fn.endswith(".msg"):
                continue
            mid = fn[:-4]
            if mid in acked:
                continue  # ack 判重（§11.5）
            try:
                with open(os.path.join(idir, fn)) as f:
                    m = json.load(f)
            except ValueError:
                continue  # 半截文件下轮重试（§11.6）
            if m.get("type") == "ask":
                # 语义闭环：reply 写回对方 inbox，ref 回引（§6.3/§6.4）
                send(m["from"], {"ts": time.strftime("%Y-%m-%d-%H-%M-%S"),
                                 "type": "reply", "ref": mid,
                                 "body": "echo:" + str(m.get("body", ""))})
                # reply 后同样要 ack 原消息（传输层与会话层分离，§6.4）
            elif m.get("type") == "inform":
                body = str(m.get("body", ""))
                if body.startswith("CMD:exit"):
                    atomic_write(os.path.join(HOME, "inbox", "ack", mid),
                                 json.dumps({"id": mid, "ts": "now"}) + "\n")
                    sys.exit(0)
                if body.startswith("CMD:crash"):
                    sys.exit(3)  # 不 ack：崩溃前不确认，测试重投递语义
                os.makedirs(os.path.join(HOME, "session"), exist_ok=True)
                with open(os.path.join(HOME, "session", "memory.txt"), "a") as f:
                    f.write(body + "\n")
            # 写传输层确认（§4.6）
            atomic_write(os.path.join(HOME, "inbox", "ack", mid),
                         json.dumps({"id": mid,
                                     "ts": time.strftime("%Y-%m-%d-%H-%M-%S")}) + "\n")
        max_loops = os.environ.get("FAKE_MAX_LOOPS")
        if max_loops and rounds >= int(max_loops):
            sys.exit(0)
        time.sleep(0.2)


if __name__ == "__main__":
    main()
