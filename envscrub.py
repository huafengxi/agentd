#!/usr/bin/env python3
"""Process identity scrubbing for agent execution and session adapters. Callers inject task identity explicitly; inherited scheduler identity is removed."""

import os

ENV_SCRUB_EXACT = {"AGENTD_TASK", "AGENT_SELF", "AGENT_HOME", "AGENT_ROOT",
                   "DISPATCH_TASK_ID",
                   # sessiond host marker (w/ext/sessiond/sessions.py HOST_MARKER):
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
                   # the assembler itself, agentd/pi-rpc-wrap.py, not via this env).
                   # Inheriting it would load the PARENT's persona into every child
                   # (poisoned runner -> every task runs with the dispatcher
                   # profile). Same family/reason as AGENTD_RESIDENT above. No
                   # caller relies on inheritance (repo-wide grep: only
                   # spec.command, pi-rpc-wrap.py's reader and test fixtures).
                   "DISPATCH_PROFILE",
                   # readiness-handshake signal paths (🔴1):
                   # agentd/pi-rpc-wrap.py::spawn_pi() hands the child pi the ABSOLUTE
                   # paths of its own two ready marks (<root>/run/agentd/<id>.init-ok /
                   # .recv-armed) via these two env vars, set explicitly AFTER any
                   # scrubbing. They are identity/signal class exactly like
                   # AGENTD_RESIDENT above: they must only ever come from the writer
                   # itself. Inherited copies leak into every grandchild (bash tool ->
                   # node/python/make -> serviced/serviced.py -> the service), and let a
                   # nested receiver-child trust an OUTER task's marks. No caller relies
                   # on inheritance (repo-wide: only spawn_pi's explicit assignment,
                   # fakepi_rpc.py fixtures and ext tests, all explicit).
                   "AGENTD_WRAP_INIT_OK", "AGENTD_WRAP_RECV_ARMED",
                   # ask 写侧收件面信号 env（项 1）：runner.spawn 复用 resolve_reaper 单点
                   # 解析出该任务的 reaper 信箱，经这两枚 env 交子端 core.writeAskMessage 取值
                   # （AGENTD_ASK_INBOX=收件目录、AGENTD_ASK_NOTE=回落成因 note），set explicitly AFTER
                   # scrubbing。与 AGENTD_WRAP_* 同族同理：身份/信号类，必须只来自写者本身——继承进孙进程
                   # （bash→make→serviced.py）会把它带进被启动的服务，且嵌套 receiver 会误用外层任务的 ask
                   # 路由。No caller relies on inheritance。
                   "AGENTD_ASK_INBOX", "AGENTD_ASK_NOTE",
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
# strip_third_party=True）；服务启动路径保持不剥——宿主服务与其会话的用钥不受影响。
ENV_SCRUB_SUFFIXES = ("_API_KEY",)
ENV_SCRUB_PREFIXES = ("DISPATCH_TASK_",
                      # pi harness family (PI_SESSION_FILE, PI_SESSION_ID,
                      # PI_MODEL, ...): no child needs them inherited
                      # (建议修② originally scrubbed only
                      # PI_SESSION_FILE — prefix now covers the whole family,
                      #)
                      "PI_")

# 本名单是**枚名制**（不是身份前缀制：同前缀族里住着大量配置旋钮——AGENTD_WRAP_* 超时族、
# AGENTD_DIR、SESSIOND_* 五枚——按前缀洗会静默把旋钮洗成缺省值）⇒ 一枚新增的身份标记若漏进
# 名单，会静默继承进被启动的服务（无运行时守卫拦它）。兜底面 = **提交期钉桩**：
# agentd/test_wrap.py T47 扫 spec.command 的 env 前缀键（bots/daemon/*/spec.json、
# heartbeats/register.py），逐枚断言 scrub_env
# 真洗掉；漏列 ⇒ 测试红（在提交前，不在事故里）。新增身份标记的姿势 = 同批把它加进
# ENV_SCRUB_EXACT（旋钮类则不必：它本该继承）。


def scrub_env(base=None, keep=frozenset(), strip_third_party=False):
    """返回剥除身份族后的环境字典（不改入参）。

    base 缺省 = 当前进程 os.environ；keep = 前缀族豁免名单（调用方参数化；
    当前三个调用方都传空集）。strip_third_party = 是否连第三方 API key 族
    （ENV_SCRUB_SUFFIXES）一并剥除：仅 runner spawn 任务进程启用；服务启动
    路径（serviced/serviced.py）保持 False，宿主服务用钥不受影响。
    """
    src = os.environ if base is None else base
    return {k: v for k, v in src.items()
            if k not in ENV_SCRUB_EXACT
            and (k in keep
                 or not any(k.startswith(p) for p in ENV_SCRUB_PREFIXES))
            and not (strip_third_party
                     and any(k.endswith(s) for s in ENV_SCRUB_SUFFIXES))}
