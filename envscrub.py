#!/usr/bin/env python3
"""envscrub.py — spawn 环境洗刷口径单点（票 1t7e9o）。

服务/任务进程从调度上下文（派发任务、调度员会话）spawn 时，必须剥掉
任务/会话身份族，防止身份投毒与机密泄漏：
  - svc/svc.py 启动服务 → scrub_env()（keep 空集：历史上唯一豁免
    PI_WEB_PASSWORD 已改为解密后在洗刷之外注入）；
  - agentd/runner.py spawn 任务进程 → scrub_env()（keep 空集——任务进程
    无业务需要保留任何 PI_/调度身份变量）。

名单单一事实源在本模块：三个调用方 import 同一份 ENV_SCRUB_EXACT /
ENV_SCRUB_PREFIXES，禁止各自复制名单漂移：
  - svc/svc.py（服务启动，同工作区经 sys.path 共享）；
  - agentd/runner.py（任务进程 spawn，strip_third_party=True）；
  - w/ext/sessiond/proc.py（web 会话进程 spawn 收敛：
    原独立拷贝漂移后改同源；keep 空集、不剥第三方 key 族）。
第三方 API key 族（ENV_SCRUB_SUFFIXES）仅 runner 侧经
strip_third_party=True 启用（服务/会话启动路径不剥）。

历史：口径诞生于 2026-08-27 pi-web 投毒事故后的 svc.py clean_env 补 SESSIOND_SESSION_FILE + PI_ 前缀族；2026-08-31 实证
runner spawn 零洗刷（任务进程 environ 可见 PI_WEB_PASSWORD 明文），
抽为本模块双方共用（票 1t7e9o）。仅使用 python3 标准库。
"""

import os

ENV_SCRUB_EXACT = {"AGENTD_TASK", "AGENT_SELF", "AGENT_HOME", "AGENT_ROOT",
                   "DISPATCH_TASK_ID",
                   # sessiond host marker (w/ext/sessiond/proc.py HOST_MARKER):
                   # if a child inherits it, web's stale-host sweep
                   # (_clear_stale_proc) can SIGTERM the child as if it were
                   # an orphaned session host (2026-08-31 scheduler kill
                   # incident; scrub hardening)
                   "SESSIOND_SESSION_FILE",
                   # resident markers (spec.command env prefix):
                   # they must only ever come from the spec.command string itself;
                   # inheriting them would mis-tag children as resident sessions
                   # (一行名单加固，零副作用)
                   "AGENTD_RESIDENT", "AGENTD_SESSION_NAME",
                   # persona input: DISPATCH_PROFILE (a SINGLE profile name; the
                   # capability chain syntax is retired) must only ever come from
                   # the spec.command prefix — resident bots declare it there, and
                   # task form carries it only when an explicit persona was
                   # registered (the `executor` baseline capability is prepended by
                   # the assembler itself, pi-wrap/pi-rpc-wrap.py, not via this env).
                   # Inheriting it would load the PARENT's persona into every child
                   # (poisoned runner -> every task runs with the dispatcher
                   # profile). Same family/reason as AGENTD_RESIDENT above. No
                   # caller relies on inheritance (repo-wide grep: only
                   # spec.command, pi-rpc-wrap.py's reader and test fixtures).
                   "DISPATCH_PROFILE",
                   # readiness-handshake signal paths (🔴1):
                   # pi-wrap/pi-rpc-wrap.py::spawn_pi() hands the child pi the ABSOLUTE
                   # paths of its own two ready marks (<root>/run/agentd/<id>.init-ok /
                   # .recv-armed) via these two env vars, set explicitly AFTER any
                   # scrubbing. They are identity/signal class exactly like
                   # AGENTD_RESIDENT above: they must only ever come from the writer
                   # itself. Inherited copies leak into every grandchild (bash tool ->
                   # node/python/make), which (a) made svc/clean-make.py refuse to run
                   # in ANY subtask (its LEAK_PREFIXES self-check sees AGENT* it cannot
                   # scrub -> not even read-only `svc.status`), and (b) let a nested
                   # receiver-child trust an OUTER task's marks. No caller relies on
                   # inheritance (repo-wide: only spawn_pi's explicit assignment,
                   # fakepi_rpc.py fixtures and ext tests, all explicit).
                   "AGENTD_WRAP_INIT_OK", "AGENTD_WRAP_RECV_ARMED",
                   # ask 写侧收件面信号 env（项 1）：runner.spawn 复用 resolve_reaper 单点
                   # 解析出该任务的 reaper 信箱，经这两枚 env 交子端 core.writeAskMessage 取值
                   # （AGENTD_ASK_INBOX=收件目录、AGENTD_ASK_NOTE=回落成因 note），set explicitly AFTER
                   # scrubbing。与 AGENTD_WRAP_* 同族同理：身份/信号类，必须只来自写者本身——继承进孙进程
                   # （bash→make）会让 svc/clean-make.py 的 LEAK_PREFIXES 自检（"AGENT" 前缀）洗不掉而拒绝
                   # 执行，且嵌套 receiver 会误用外层任务的 ask 路由。No caller relies on inheritance。
                   "AGENTD_ASK_INBOX", "AGENTD_ASK_NOTE",
                   # heartbeat-session marker (spec.command env prefix):
                   # it must only ever come from the spec.command string of the
                   # heartbeat task itself (registered by assistant/heartbeat.sh as
                   # `DISPATCH_HEARTBEAT=1 exec ...`; convention = agent-file-protocol.md
                   # "命令前缀标记惯例"). Inherited copies leak into every grandchild
                   # (bash tool -> make -> svc/svc.py), which (a) made svc/clean-make.py
                   # refuse to run in ANY heartbeat session (its LEAK_PREFIXES self-check
                   # sees the DISPATCH* prefix it cannot scrub -> not even read-only
                   # `svc.status`), and (b) if the started service is agentd, EVERY task
                   # it later spawns inherits the recursion-guard heartbeat exemption
                   # (core.ts recursionGuardReason) -> the leaf-no-redispatch guard fails
                   # silently, system-wide. The heartbeat session itself is unaffected:
                   # runner.spawn scrubs FIRST, then runs spec.command via bash -c, so the
                   # prefix injects the marker AFTER scrubbing, and pi-rpc-wrap.py does
                   # not scrub (spawn_pi passes os.environ through) -> the pi child still
                   # sees it. Same family/reason as AGENTD_RESIDENT above (also
                   # spec.command-prefix-only). No caller relies on inheritance
                   # (repo-wide: only heartbeat.sh's spec.command, the TS guard readers
                   # in core.ts/index.ts, wrap's convergence-branch note and tests).
                   "DISPATCH_HEARTBEAT",
                   # harness marker set by pi itself at entry (dist/cli.js,
                   # dist/rpc-entry.js: process.env.AI_AGENT = "pi"): children
                   # that need it re-set it, so inheriting only mis-tags services
                   # /task processes as agent sessions. The sibling marker
                   # PI_CODING_AGENT is already covered by the PI_ prefix family
                   #.
                   "AI_AGENT"}
# 第三方 API key 族（评审攒批）：子任务进程不得继承宿主侧第三方 key
# （GEMINI_API_KEY/DASHSCOPE_API_KEY/OPENAI_API_KEY/BRAVE_API_KEY/TOKENFLOW_API_KEY 等，
# 按后缀族覆盖）——机密最小化：任务进程用键走自身配置/文件解密路径（如 pi models.json
# 的 !command 自解密），无需继承宿主 env。仅 runner spawn 侧启用（scrub_env
# strip_third_party=True）；svc 服务启动路径保持不剥——宿主服务与其会话的用钥不受影响。
ENV_SCRUB_SUFFIXES = ("_API_KEY",)
ENV_SCRUB_PREFIXES = ("DISPATCH_TASK_",
                      # pi harness family (PI_SESSION_FILE, PI_SESSION_ID,
                      # PI_MODEL, ...): no child needs them inherited
                      # (建议修② originally scrubbed only
                      # PI_SESSION_FILE — prefix now covers the whole family,
                      #)
                      "PI_")


def scrub_env(base=None, keep=frozenset(), strip_third_party=False):
    """返回剥除身份族后的环境字典（不改入参）。

    base 缺省 = 当前进程 os.environ；keep = 前缀族豁免名单（调用方参数化；
    当前三个调用方都传空集）。strip_third_party = 是否连第三方 API key 族
    （ENV_SCRUB_SUFFIXES）一并剥除：仅 runner spawn 任务进程启用；服务启动
    路径（svc/svc.py）保持 False，宿主服务用钥不受影响。
    """
    src = os.environ if base is None else base
    return {k: v for k, v in src.items()
            if k not in ENV_SCRUB_EXACT
            and (k in keep
                 or not any(k.startswith(p) for p in ENV_SCRUB_PREFIXES))
            and not (strip_third_party
                     and any(k.endswith(s) for s in ENV_SCRUB_SUFFIXES))}
