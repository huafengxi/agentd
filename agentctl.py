#!/usr/bin/env python3
"""agentctl.py — 协议原型 CLI。

子命令（对应协议章节）：
  create        写 spec.json 创建进程型参与方（§5.1/§2.2/§4.1；`--description` → spec `name`、
                `--reaper` → spec `reaper`，恒写 `createdAt`，与 dispatch 登记面对齐）
  create-bot    创建信箱型 bot（bot/<名字>/，只建 inbox/，无 spec）
  bot register  登记进程型 bot spec：`--subscribes topic/<id>[,…]`（通道 B，§4.1）
                + `--description`（→ spec `name`）/`--reaper`（→ spec `reaper`）
  topic init    创建 topic 标准布局（topic.md 骨架 + inbox/ + watcher/ 订阅登记，
                设计稿 dispatch/docs/design/topic-design.md §7/§8）
  send          向参与方 inbox 写消息：ask|inform|reply 信封（§6）；`--deliver` 投递方式（§6.6）、
                `--body-file -` 走 stdin（长正文不经 shell 断词）；type=ask 自动带 via 标记（§4.5）
  ack           收件方写传输层确认 inbox/ack/<id>（§3.1/§6.4）
  control       写控制请求 stop|restart|clear [--inject][--reason]（§5.2/§4.3）
  answer        答复目标任务的未答 ask（自动找 askId、继承其 via、回执点名所答条目）
  cancel        取消参与方（= control stop + 存活前置门）
  update        改未放行排队任务的调度字段 resources/provides/needs（§14）
  enable        调度方写 enable.json 放行（调度扩展，§14.2）
  status        读 pid.json，输出两层谓词判定（§10）
  list          列出全部参与方与状态摘要

**动词分两档**：协议动词（create/create-bot/bot register/topic init/send/ack/control/enable）
只负责把布局与信封落盘，对不存在的收件方、已终态的参与方同样合法（目录即队列 §11.4，
收尾清理也要能写 stop）；用例动词（answer/cancel/update）代表一个**意图**，带前置门
（存在 / 非生命周期终态 / 意图可成立），意图落空当场报错而不静默写无人消费的件。
写信封与写控制请求的 `from` 归属一律走 `resolve_sender` 单点（显式 --from ＞ 环境
AGENT_SELF ＞ 回落职位信箱），不按动词分叉。

通用：--root <ROOT>（**工作区根**，如 ~/m —— 不是 ~/m/agents；<ROOT> 的 basename 为
`agents` ∨ <ROOT>/agents 不在场，两者任一即拒绝且不静默建目录；<ROOT>/env/host-id 缺失
只告警、登记不失败，口径同 `local_canonical_host`）。**缺省 = 本脚本所在仓的父目录**
（现场发现，不依赖 cwd 与 env ⇒ 工作区内任何目录直接
`python3 <WS>/agentd/agentctl.py <动词> …` 即可）；显式 --root
只用于另一棵树（如测试临时树）。仅使用 python3 标准库。

**删除铁律**：本 CLI 只做创建/登记，不提供任何删除动作——agents/ 内目录清理一律走
`agents-sync/gc.py add` 删除清单通道（bot/ 族还需 --force 审计旁路），绝不直接 rm。
"""
import argparse
import json
import os
import socket
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import proto  # noqa: E402


def parse_str_array(v, opt):
    """调度字段（resources/provides/needs）：JSON 字符串数组，元素限定字符串。"""
    try:
        arr = json.loads(v)
    except ValueError:
        die("%s 需为 JSON 字符串数组，如 '[\"gpu\"]'：%r" % (opt, v))
    if not isinstance(arr, list) or not all(isinstance(x, str) for x in arr):
        die("%s 元素必须全部为字符串：%r" % (opt, v))
    return arr


def local_canonical_host(root):
    """登记机规范名（口径 @agentd#routing）：<root>/env/host-id 映射文件（每行 `<hostname> <空白> <规范名>`，# 注释；
    入库一份全机器共用），按本机 hostname 查表；文件缺失/不可读/未命中回退本机
    hostname（登记不失败，返回回退标记供提示）。"""
    hn = socket.gethostname()
    try:
        with open(os.path.join(root, "env", "host-id")) as f:
            for line in f:
                t = line.strip()
                if not t or t.startswith("#"):
                    continue
                parts = t.split()
                if parts[0] == hn and len(parts) > 1:
                    return parts[1], False
    except OSError:
        pass
    return hn, True


def require_workspace_root(root):
    """--root 前置校验（错 root 可见性硬化）：root 必须是**工作区根**（如 ~/m）。

    硬前置（任一命中即 die，**绝不静默建目录**）：
      ① basename(realpath(root)) == "agents" —— root 传成了 agents 树本身。本条**不依赖
         嵌套残骸是否在场**：残骸（`<工作区根>/agents/agents/`，即错 root 误用的产物）恰好
         满足 ②，只留 ② 会被它自我击穿（同类误用只报一行软 WARN 就放行、写侧 makedirs
         继续往嵌套树里建目录与落信封）。
      ② <root>/agents 不在场 —— proto.* 一律 join(root, "agents", …) 且写侧
         makedirs(recursive=True)，所以错 root 会静默建出一棵无人消费的嵌套树：control
         请求写进去、重启不发生、残骸要两条 gc 台账条目才收得掉（实例 2026-09-15：两个
         任务各误投一枚 control req 到 agents/agents/bot/<名>/control/）。

    判据射程只到「root 是不是工作区根」，**不收窄 `<root>/agents/` 下条目的形状**：
    depth-1 legacy 条目继续合法（权威 = lore/library/agentfw/facts/not-doing.md「gc 台账
    不收窄 depth-1 legacy 条目」）⇒ 不得改写成「第一段必须是 task/bot/topic/gc」一类全称
    判据。残骸清理不在本校验射程（一律走 agents-sync/gc.py add 通道，绝不直接 rm）。

    软前置（只告警、不改 rc）：<root>/env/host-id —— 既有裁定「映射文件缺失 → 回退
    hostname 本身，不阻塞守护启动与任务登记」（本文件头注与 env/host-id 头注、
    agentd/README「路由字段」节同源，e2e S22④/S27③ 钉住）⇒ 不得升为硬拒绝；
    但错 root 也表现为该文件缺失，故补一行指向 root 语义的 WARN（与 `local_canonical_host`
    回退时的「补一行」告警各管一面：那行说机器映射，本行说 root 是不是工作区根）。

    返回归一化后的绝对路径（expanduser + abspath；对既有绝对路径入参是 no-op）。"""
    r = os.path.abspath(os.path.expanduser(root))
    if os.path.basename(os.path.realpath(r)) == "agents":
        die("--root %r 不是工作区根：你传的是 agents 树本身（basename=agents）。"
            "--root 须为工作区根（如 ~/m），不是 ~/m/agents；本次未创建任何目录/文件。"
            % root, code=2)
    agents = os.path.join(r, "agents")
    if not os.path.isdir(agents):
        die("--root %r 不是工作区根：%s 不在场。--root 须为工作区根（如 ~/m），"
            "不是 ~/m/agents；本次未创建任何目录/文件。"
            % (root, agents), code=2)
    if not os.path.exists(os.path.join(r, "env", "host-id")):
        print("agentctl: WARN env/host-id 不在场（root=%s）：--root 须为工作区根"
              "（工作区根含 agents/ 与 env/host-id），若不是请改正；登记照旧进行，"
              "登记机规范名回退本机 hostname" % r, file=sys.stderr)
    return r


def default_root():
    """缺省工作区根 = 本仓所在根（布局固定为 `<root>/agentd/agentctl.py` ⇒ 上两级）。

    现场发现、不依赖 cwd 与 env：会话里从任何工作目录调用都解析到同一个根（收录判据 ①
    的「现场发现 ∨ 调用方注入」两档里的前者）。本仓单独 checkout（父目录无 `agents/`）时
    该值过不了 `require_workspace_root` 的硬前置 ⇒ 报「不是工作区根」，此时显式传 `--root`。"""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def portable_path(p):
    """spec 路径可移植归一化（口径 @agentd#sync-channel）：agents 树跨机同步、
    各机 $HOME 不同——位于 $HOME 内的路径一律存 `~/...`
    形式，非 home 路径（如 /data/...）保持绝对路径；消费方（runner）以 expanduser
    展开（对绝对路径是 no-op，存量绝对路径行为不变）。入参须为已规范化的绝对路径。"""
    home = os.path.expanduser("~")
    if p == home:
        return "~"
    if p.startswith(home + os.sep):
        return "~" + p[len(home):]
    return p


def beijing_iso():
    """登记时刻（spec.createdAt）：北京时区 ISO 秒级，与 dispatch 侧 core.ts toBeijingISO
    同格式（`YYYY-MM-DDTHH:MM:SS+08:00`，与登记机时区无关）。"""
    return datetime.now(timezone(timedelta(hours=8))).replace(microsecond=0).isoformat()


def cmd_create(a):
    # 两个可选字段的校验先于任何 makedirs/落盘（拒后零副作用，口径同 cmd_bot_register）
    reaper = check_reaper(getattr(a, "reaper", None))
    description = check_description(getattr(a, "description", None))
    if a.name:
        if "/" in a.name or a.name.startswith("."):
            die("非法目录名：%r（作为路径组件合法即可，§2.2）" % a.name)
        # 文件名禁用 *?[]（用户拍板）：属组闸门 pull 保护清单是模式语义，
        # 含 glob 元字符的文件名会被当通配符（现行为=拒绝入清单+告警，该文件失去保护），
        # 故在登记源头禁用（口径见 @agentd#sync-channel）。自动任务名（auto_name）本就安全。
        if set("*?[]") & set(a.name):
            die("非法文件名：%r 含 glob 元字符 *?[]（属组闸门文件名禁用约定，"
                "@agentd#sync-channel）" % a.name)
        name = a.name
        # 自定义名（生产唯一使用者 = 每日心跳）→ task/<名字>/。
        adir = proto.task_dir(a.root, name)
    else:
        name = proto.auto_name()
        # 自动名→ task/<rand6>
        adir = proto.task_dir(a.root, name)
    # 撞名检查覆盖全部布局（协议 §2.2 创建方义务；同名跨布局亦拒绝）
    for cand in (adir, proto.task_dir(a.root, name),
                 proto.bot_dir(a.root, name)):
        if os.path.exists(cand):
            die("拒绝创建：%s 已存在" % cand, code=2)
    os.makedirs(adir, exist_ok=True)
    os.makedirs(os.path.join(adir, "inbox"), exist_ok=True)
    # --workdir：先 expanduser（接受 ~/ 形式）再规范化，落盘时归一化为可移植形式
    spec = {"command": a.command,
            "workdir": portable_path(os.path.abspath(os.path.expanduser(a.workdir))),
            "creator": a.creator}
    if a.restart_policy:
        if a.restart_policy not in ("manual", "auto", "one-shot"):
            die("restartPolicy 取值须为 manual|auto|one-shot")
        spec["restartPolicy"] = a.restart_policy
    # §4.1 可选字段（口径同 bot register）：给了才写键；
    # createdAt 恒写（与 dispatch 登记面 core.ts dispatchTask2 对齐，创建时刻跨机可比）。
    if description:
        spec["name"] = description        # §4.1 `name` = 人类可读描述（非目录名）
    if reaper:
        spec["reaper"] = reaper           # §4.1 终态通知唯一收件面（缺失 → 运行时回落职位信箱带 note）
    spec["createdAt"] = beijing_iso()
    # 路由字段（多机阶段 0，设计 §3.2）：「spec.host 缺省=
    # 登记机」在登记时刻物化落盘（B-2 修复）——不传 --host 自动写本机规范名，
    # heartbeat 等既有调用方零改动即获得显式路由；显式 --host 他机时 createdByHost 仍登记机。
    canonical, fallback = local_canonical_host(a.root)
    if fallback:
        print("warning: env/host-id 缺失或无本机（%s）映射条目，登记机规范名回退为本机 hostname：%s"
              "（请到 env/host-id 补一行「%s <规范名>」；登记正常进行）"
              % (socket.gethostname(), canonical, socket.gethostname()), file=sys.stderr)
    host = a.host or canonical
    created_by = a.created_by_host or canonical
    # 最终非空兜底：解析链
    # （映射表 → hostname）任一环产出空值 → 写 $(hostname) 原文，绝不留空 host；
    # 认领侧已无「缺省→本机」兜底，空 host = 全网无人认领。
    if not host or not created_by:
        hn = socket.gethostname()
        host = host or hn
        created_by = created_by or hn
        print("warning: host 解析链产出空值，最终兜底写 hostname：%r" % hn,
              file=sys.stderr)
    if not host:
        host = "unknown"  # hostname 也空的病态情形：绝不留空
    if not created_by:
        created_by = host
    spec["host"] = host
    spec["createdByHost"] = created_by
    # 调度字段（：DAG 调度）：不传则不写入，
    # 调度侧按缺省语义处理（resources→["serial"]，provides/needs→[]）。
    for opt, key in (("resources", "resources"), ("provides", "provides"),
                     ("needs", "needs")):
        v = getattr(a, opt)
        if v is not None:
            spec[key] = parse_str_array(v, "--" + opt)
    proto.atomic_write_json(os.path.join(adir, "spec.json"), spec)  # temp+rename（§5.1）
    print(name)


def cmd_create_bot(a):
    """创建信箱型 bot（bot 布局，设计计划 §1.2）：
    agents/bot/<名字>/，只建 inbox/（信箱型目录内容），无 spec.json
    （进程型 bot 由 channel 登记等机制写 spec）。人指定名，唯一性是创建方义务。"""
    check_name_segment(a.name, "bot 名")
    adir = proto.bot_dir(a.root, a.name)
    for cand in (adir, proto.task_dir(a.root, a.name)):
        if os.path.exists(cand):
            die("拒绝创建：%s 已存在" % cand, code=2)
    os.makedirs(os.path.join(adir, "inbox"), exist_ok=True)
    print(a.name)


def check_name_segment(name, what="名字"):
    """目录名/条目名合法性单点（走寻址单点 proto，§2.1/§2.2）：段白名单 +
    glob 元字符禁用（属组闸门保护清单是模式语义）+ 布局容器保留名拒绝 +
    **登记侧点名收紧**（拒 '.' 开头与名内连续点）。

    收紧只在本写入口（create-bot / bot register / topic init / watcher 登记），
    **不动寻址侧** proto.is_valid_name_segment：存量病态名仍可被寻址/投递/清理
    （失败方向安全，口径同 core.ts subscriberAckNs 的「拒绑定面不拒寻址」）。
    task 登记侧（cmd_create）同款拒 '.' 开头已在其内联校验中。"""
    if not name or not proto.is_valid_name_segment(name):
        die("非法 %s：%r（白名单 [A-Za-z0-9._-]+，禁止空/'.'/'..'，§2.1）" % (what, name))
    if set("*?[]") & set(name):
        die("非法 %s：%r 含 glob 元字符 *?[]（属组闸门文件名禁用约定，"
            "@agentd#sync-channel）" % (what, name))
    if name in proto.LAYOUT_DIRS:
        die("保留名：%r 是布局容器目录名" % name, code=2)
    # 与运行时侧对称（建议项 1 + isSafeAckNs）：
    # ① '.' 开头 → agents/ 扫描面（core.ts 枚举参与方/profile 链解析）一律跳过隐藏
    #    条目 → 登记成功即造了个系统看不见的参与方（profile 名同款：登记侧放行
    #    但 wrap 拒 → 告警裸启动，承诺与实际不一致）；
    # ② 名内连续点 → fsSafeId 转写后的 ack 命名空间段含 '..' → 运行时 isSafeAckNs
    #    判不安全 → topic 绑定面整体不启用（subscriberAckNs refusal，静默少投）。
    # 两者都是「登记成功却半残」→ 写入口直接拒，错误文案指明合法命名规则。
    if name.startswith(".") or ".." in name:
        die("非法 %s：%r——合法命名 = [A-Za-z0-9._-]+ 且不以 '.' 开头、不含连续点 '..'"
            "（'.' 开头被 agents/ 扫描面当隐藏条目跳过；名内 '..' 使 ack 命名空间段不安全"
            " → topic 绑定面不启用；§2.1/§2.2）" % (what, name))
    return name


def parse_subscribes(raw):
    """`--subscribes` 解析（通道 B 写入口，协议 §4.1 `spec.subscribes`）：逗号分隔的
    路径式 id 列表 → 去重保序。硬校验与实现层 receiver 同口径（core.specSubscribes）：
    **只接受 topic/ 族**——task/bot 信箱的收件归属是会话名的纯函数，不得被第三方会话
    绑定（防抢收终态通知/控制面消息）。空串 = 显式清空（返回 []，落盘时删字段）。
    登记侧拒绝非法项（而非像 receiver 那样静默跳过）：写入口不留脏声明。"""
    items = [x.strip() for x in (raw or "").split(",")]
    out = []
    for s in items:
        if not s:
            continue
        if not proto.is_valid_participant_id(s):
            die("非法订阅项：%r——须为两段路径式 id，name ∈ [A-Za-z0-9._-]+" % s)
        family, _name = proto.parse_participant_id(s)
        if family != proto.TOPIC_DIR:
            die("拒绝订阅 %r：只有 topic/ 族信箱可被绑定（通道 B 白名单，防抢收——"
                "task/bot 信箱收件归属 = 会话名的纯函数，§4.1）" % s)
        if s not in out:
            out.append(s)
    return out


def cmd_topic_init(a):
    """建 topic 标准布局（agents/topic/<id>/，设计稿 topic-design.md §7/§8）：
    topic.md 骨架（frontmatter `when:` 占位 + 议题/已决/未决三小节）、inbox/（全量日志）、
    watcher/（订阅注册表）；`--watcher <会话裸名>` 可重复 = 建通道 A 订阅条目（存在即订阅、
    删条目即退订，实现层 receiver 每轮实时重扫）。
    **只创建不删除**：散会/归档走 `agents-sync/gc.py add topic/<id>/` 通道（铁律）。"""
    check_name_segment(a.id, "topic id")
    tdir = proto.topic_dir(a.root, a.id)
    # 撞名检查覆盖全部布局（协议 §2.2 创建方义务）：同名 bot/task 在场即拒——防「职位信箱型
    # bot」这类数据容器/进程载体杂交概念复活（移族的教训）；
    # `bot register` 反向不查 topic 同名（主持人 bot 与其主题成对出现，无落盘冲突）。
    for cand in (tdir, proto.bot_dir(a.root, a.id), proto.task_dir(a.root, a.id)):
        if os.path.exists(cand):
            die("拒绝创建：%s 已存在（topic 不覆盖既有目录；删除走 agents-sync/gc.py 通道）"
                % cand, code=2)
    watchers = []
    for w in (a.watcher or []):
        check_name_segment(w, "watcher 会话裸名")
        if w not in watchers:
            watchers.append(w)
    os.makedirs(os.path.join(tdir, "inbox"), exist_ok=True)
    wdir = os.path.join(tdir, "watcher")
    os.makedirs(wdir, exist_ok=True)
    proto.atomic_write(os.path.join(tdir, "topic.md"),
                       topic_md_skeleton(a.id, a.title or a.id))
    for w in watchers:
        proto.atomic_write(os.path.join(wdir, w),
                           "# 订阅声明（通道 A）：%s 会话 receiver 直订本主题信箱"
                           "（agentctl topic init 登记）。删除本条目 = 即时退订。\n" % w)
    print("topic/%s" % a.id)
    print("  dir      %s" % tdir)
    print("  layout   topic.md + inbox/ + watcher/"
          + ("（订阅条目：%s）" % ", ".join(watchers) if watchers else ""))
    print("  下一步   策展 topic.md（单写者=owner/主持人）；登记主持人 bot 用 "
          "`agentctl bot register`；删除走 `agents-sync/gc.py add topic/%s/`" % a.id)


def topic_md_skeleton(tid, title):
    """topic.md 骨架（策展文档，单写者 = owner/主持人）：frontmatter `when:` 占位（知识域
    入册约定见 bots/README.md「知识库规范」，索引由 bots/kb_index.py 派生）+ 议题/已决/未决。
    刻意保持小 = 简报不是叙事；全量日志归 inbox/（机制自动过账，勿手工双写）。"""
    return """---
when: "（占位：何时该来读本主题文档——一句话；入册后由 bots/kb_index.py 聚合进知识清单）"
---

# {title}

> topic = `{tid}`（协作容器：本文策展 + `inbox/` 全量日志 + `watcher/` 订阅注册表）。
> **单写者** = owner/主持人；日志由机制自动过账（`agentctl send` 投递即落盘），绝不手工双写。
> 机制权威：`dispatch/docs/design/topic-design.md`（§5.1 主持人载体、§7 双件套）；消息与收件口径：`dispatch/DISPATCH.md` §4/§8。
> 删除/归档本主题一律走 `agents-sync/gc.py add topic/{tid}/` 通道（铁律：不得直接 rm）。

## 议题

（要决什么、判据是什么、预算多少轮；议题漂移时显式改写并记原因与时间）

## 已决

（决策 + 时间 + 依据；检查点式追加）

## 未决

（待决项 / 待用户或调度员拍板 / 行动清单：谁、做什么、何时）
""".format(title=title, tid=tid)


def check_reaper(raw, what="reaper"):
    """`--reaper` 文法校验（终态通知唯一收件面，协议 §4.1 `spec.reaper`）：复用寻址
    单点 `proto.is_valid_participant_id`（两段路径式 `<family>/<name>`，family ∈
    proto.FAMILIES，name 过段白名单），非法即 `die`——与 `resolve_sender`/`require_pid`
    同一口径，不新增 proto 逻辑。缺失（None）= 不写该键 → 运行时 `runner.resolve_reaper`
    回落职位信箱 `topic/dispatcher` 并带 note，属既有语义，此处不代填。"""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if not proto.is_valid_participant_id(s):
        die("非法 %s：%r——须为两段路径式 <family>/<name>，family ∈ {%s}，"
            "name ∈ [A-Za-z0-9._-]+（禁 ./../空段、裸名；§2.1/§4.1）"
            % (what, raw, ", ".join(proto.FAMILIES)))
    return s


def check_description(raw, what="description"):
    """`--description` 校验（写入 spec 的 `name` 字段 = 人类可读描述，协议 §4.1）：
    只拒不可落盘的形态（含 NUL）；缺失/空串 = 不写该键（spec 与现行为逐字一致）。
    长度上界属调用方口径（8080 表单侧限 ≤200 字符，见 w/ext/sessiond/proc.py），
    CLI 不代设——人工登记长描述是合法用法。"""
    if raw is None:
        return None
    s = str(raw)
    if not s.strip():
        return None
    if "\x00" in s:
        die("非法 %s：含 NUL 字节，不可落 JSON 档案" % what)
    return s


def cmd_bot_register(a):
    """登记进程型 bot（bot 族 spec.json）：给协议 §4.1 的可选扩展字段 `subscribes`
    （通道 B = 登记期订阅意图）一个合法写入口，消灭手写不可变档（S2 结论：
    字段留在 spec.json，登记为可选扩展字段）。`--description`/`--reaper`
    把 §4.1 另两个可选字段 `name`（人类可读描述）与 `reaper`（终态通知收件面）也纳入
    同一写入口——8080 表单登记（`op=create_bot`）经本命令落盘，不另开写盘路径。

    两态：
      - spec.json **已在场** → 只改 `subscribes` 一个字段，其余字段逐字保留（spec 是协议
        不可变档；运行期增删订阅应走通道 A = `topic/<id>/watcher/<裸名>` 条目）；
        `--subscribes ''` = 显式清空（删字段，回到零声明现状）。`--description`/`--reaper`
        在本分支**不生效**（改字段 = 人工编辑 spec.json 后 `control restart`），给了只提示。
      - spec.json **不在场** → 需要 `--command/--workdir/--creator` 三件（与 `create` 同纪律）
        才创建进程型 bot spec（+ inbox/）；只想要信箱用 `create-bot`，守护型 bot 走被追踪
        声明源 `bots/daemon/<名字>/spec.json` + `make bots.seed`（fresh clone 自愈）。

    实现层 receiver 每轮实时重读 spec（通道 B 与通道 A 取并集），故订阅改动对在跑会话
    动态生效；但 spec 只承载**登记期意图**（口径见 topic-design.md §5.1）。"""
    check_name_segment(a.name, "bot 名")
    # 同名跨布局撞名亦拒（协议 §2.2 创建方义务）；bot 目录本身在场 = 正常登记情形。
    # 撞名面只查 task/bot（同 create-bot）：**topic 同名不拒**——两者不同族目录无落盘冲突，
    # 且主持人 bot 与其主题本就成对出现（topic-design.md §5.1 命名约定）；反向不对称：
    # `topic init` 会拒 bot/task 同名（防止再造「职位信箱型 bot」这类杂交概念）。
    tdir_same = proto.task_dir(a.root, a.name)
    if os.path.exists(tdir_same):
        die("拒绝创建：%s 已存在（同名跨布局撞名，§2.2）" % tdir_same, code=2)
    subs = parse_subscribes(a.subscribes) if a.subscribes is not None else None
    # 两个新可选字段的校验先于任何 makedirs/落盘（拒后零副作用，口径同 parse_subscribes）
    reaper = check_reaper(getattr(a, "reaper", None))
    description = check_description(getattr(a, "description", None))
    bdir = proto.bot_dir(a.root, a.name)
    spath = os.path.join(bdir, "spec.json")
    os.makedirs(os.path.join(bdir, "inbox"), exist_ok=True)
    if os.path.exists(spath):
        if reaper or description:
            print("warning: spec.json 已在场 ⇒ --description/--reaper 不生效"
                  "（本命令只改 subscribes；改这两字段 = 人工编辑 %s 后 "
                  "`agentctl control restart bot/%s`）" % (spath, a.name),
                  file=sys.stderr)
        if subs is None:
            die("bot/%s 的 spec.json 已在场：本命令只改 subscribes 字段（其余字段不可变），"
                "请传 --subscribes（清空传 --subscribes ''）" % a.name, code=2)
        doc = proto.read_json(spath)
        if not isinstance(doc, dict):
            die("拒绝改写 %s：不可读/半截/非 JSON 对象（人工核对后再登记）" % spath, code=2)
        if subs:
            doc["subscribes"] = subs
        else:
            doc.pop("subscribes", None)   # 显式清空 = 回到「字段缺失 = 零声明」现状
        proto.atomic_write_json(spath, doc)
        print("bot/%s" % a.name)
        print("  spec     %s（只改 subscribes，其余字段保留）" % spath)
        print("  subscribes %s" % (json.dumps(subs, ensure_ascii=False) if subs
                                   else "（已清空 = 字段移除）"))
        return
    # 新建 spec：command/workdir/creator 三件必备（与 cmd_create 同纪律）
    if not a.command or not a.workdir or not a.creator:
        die("bot/%s 无 spec.json：新建进程型 bot spec 需 --command/--workdir/--creator 三件"
            "（只要信箱用 `create-bot`；守护型 bot 走被追踪声明源 bots/daemon/<名字>/spec.json"
            " + `make bots.seed`；只想改订阅而 spec 应在场 = 先核对路径 --root）" % a.name,
            code=2)
    spec = {"command": a.command,
            "workdir": portable_path(os.path.abspath(os.path.expanduser(a.workdir))),
            "creator": a.creator}
    if a.restart_policy:
        if a.restart_policy not in ("manual", "auto", "one-shot"):
            die("restartPolicy 取值须为 manual|auto|one-shot")
        spec["restartPolicy"] = a.restart_policy
    canonical, fallback = local_canonical_host(a.root)
    if fallback:
        print("warning: env/host-id 缺失或无本机（%s）映射条目，登记机规范名回退为本机 "
              "hostname：%s" % (socket.gethostname(), canonical), file=sys.stderr)
    spec["host"] = a.host or canonical or socket.gethostname() or "unknown"
    spec["createdByHost"] = a.created_by_host or canonical or spec["host"]
    if subs:
        spec["subscribes"] = subs
    # 两个可选字段：给了才写键 ⇒ 缺省时 spec 与现行为逐字一致
    if description:
        spec["name"] = description        # §4.1 `name` = 人类可读描述（非目录名）
    if reaper:
        spec["reaper"] = reaper           # §4.1 终态通知唯一收件面（缺失 → 回落职位信箱）
    proto.atomic_write_json(spath, spec)  # temp+rename（§5.1）
    print("bot/%s" % a.name)
    print("  spec     %s（新建）" % spath)
    print("  subscribes %s" % (json.dumps(subs, ensure_ascii=False) if subs else "（未声明）"))
    print("  name     %s" % (description if description else "（未声明）"))
    print("  reaper   %s" % (reaper if reaper
                             else "（未声明 → 终态通知回落职位信箱 %s）" % proto.POSITION_PID))
    print("  下一步   放行由调度方写 enable.json（或 `agentctl enable bot/%s --by who`）；"
          "人格装载靠 spec.command 前置 DISPATCH_PROFILE=<profile 名>（薄清单 "
          "bots/profiles/<名>.json 的 caps 决定注入哪些能力）" % a.name)


def require_pid(s, what="参与方"):
    """路径式参与方 id 硬校验（两段文法，§2.1）：非法即报错退出。"""
    if not proto.is_valid_participant_id(s):
        die("%s id 非法：%r——须为两段路径式 <family>/<name>，"
            "family ∈ {%s}，name ∈ [A-Za-z0-9._-]+（禁 ./../空段、裸名）"
            % (what, s, ", ".join(proto.FAMILIES)))
    return s


def resolve_sender(explicit, what="发送方"):
    """信封 `from` 归属解析单点：与 TS 侧 `core.resolveCreatorPid`
    **同源优先级** —— ① 显式 `--from`（非法即拒，调用方写错不得静默降级）；
    ② 环境 `AGENT_SELF`（过路径式 id 文法白名单，非法/缺失则静默落下一档，
    同 TS 侧对非法 env 的处置）；③ 回落职位信箱 `proto.POSITION_PID`。
    **不接受裸名**：裸名违 §2.2 两段路径式文法，且把非调度员会话（领域会话/
    主持人）的取消记成调度员职位 = 审计与权威归属双失真。文法校验复用
    `proto.is_valid_participant_id`（既有函数，不新增 proto 逻辑）。
    适用面 = 全部写信封/写控制请求的动词（`send` / `answer` / `cancel` / `control`）：
    归属口径全链单一，不得按动词分叉。"""
    if explicit is not None and str(explicit).strip():
        s = str(explicit).strip()
        if not proto.is_valid_participant_id(s):
            die("%s身份（--from）非法：%r——须为两段路径式 <family>/<name>，"
                "family ∈ {%s}（§2.2；不接受裸名）；缺省时取环境 AGENT_SELF，"
                "再缺省回落职位信箱 %s"
                % (what, explicit, ", ".join(proto.FAMILIES), proto.POSITION_PID))
        return s
    env_self = os.environ.get("AGENT_SELF")
    s = env_self.strip() if isinstance(env_self, str) else ""
    if s and proto.is_valid_participant_id(s):
        return s
    return proto.POSITION_PID


def require_not_retired(s, what="参与方"):
    """退役地址护栅：命中 `proto.RETIRED_MAILBOXES`
    → **拒绝 + 回执新地址**；该表与 TS 侧 `core.RETIRED_MAILBOXES`（core.ts）跨语言同源
    同口径（单点声明见 proto.py，钉桩见 e2e.py `s41`）。

    调用位置硬约束：必须在任何路径计算 / `os.makedirs` / `atomic_write_json` **之前**
    （否则 `inbox_path` + 原子写会先把僵尸信箱的父目录建出来）；且**独立于目录是否在
    场**（纯查表）——旧目录 gc 前仍在场、有 pid.json，能过 `require_pid`，只有本护栅拦得住。
    只加在**接受路径式地址且会写盘**的子命令（send/ack/control/enable）；只读的
    status/list 不加（保留对退役目录的考古能力，零副作用）。"""
    moved = proto.retired_move(s)
    if moved:
        die("%s %s 已退役（职位信箱移族，任务）——请改用 %s"
            "（调度员职位信箱 = 系统主题，订阅者会话 receiver 直订注入）。"
            "本次未写任何文件/目录。" % (what, s, moved), code=2)
    return s


def body_of(a):
    """正文取值单点（`--body` XOR `--body-file`，后者 `-` = stdin）；trim 后为空即拒。

    长正文/含引号与换行的正文走 `--body-file -` + heredoc，不经 shell 断词与转义（CLI 形态
    相对工具形态的唯一真实代价 = 参数经 shell，故给文件/stdin 通道）。校验先于一切落盘
    （拒 = 零副作用，§4.5 `body` 必填）。"""
    raw, fp = getattr(a, "body", None), getattr(a, "body_file", None)
    if raw is not None and fp is not None:
        die("--body 与 --body-file 二选一（同时给 = 歧义，不猜优先级）")
    if fp is not None:
        try:
            raw = sys.stdin.read() if fp == "-" else open(fp, encoding="utf-8").read()
        except OSError as e:
            die("--body-file 读取失败：%s" % e)
    if raw is None:
        die("缺正文：--body <文本> ∨ --body-file <路径|->（`-` = stdin）")
    body = raw.strip()
    if not body:
        die("正文为空（trim 后）：拒绝投递，本次未写任何文件（§4.5 body 必填）")
    return body


def cmd_send(a):
    require_pid(a.to, "收件方")
    require_not_retired(a.to, "收件方")
    body = body_of(a)                       # 收件方可先于其余一切存在：目录本身就是队列（§11.4）
    if a.type == "reply" and not a.ref:
        die("type=reply 必须带 --ref（无主答复无效，§6.2）")
    sender = resolve_sender(a.sender, "发送方")
    env = {"from": sender, "ts": proto.now_ts(), "type": a.type, "body": body}
    if a.ref:
        env["ref"] = a.ref                  # reply 必带；非 reply 带 ref 宽容透传（协议未禁）
    if a.deliver:
        env["deliver"] = a.deliver          # 可选字段：只在显式指定时落盘（缺省 = followUp，§6.6）
    if a.type == "ask":
        # 非阻塞征询的来源标记（§4.5 `via`）：其 reply 由 `answer` 继承该标记 → 收件侧 drain
        # 白名单据此放行消费；不带 via 的 ask = 阻塞征询，reply 归阻塞面单一消费。
        env["via"] = proto.ASK_VIA_SEND_MESSAGE
    mid, _p = proto.write_message(proto.inbox_path(a.root, a.to), env)
    print(mid)


def cmd_ack(a):
    require_pid(a.participant)
    require_not_retired(a.participant)
    p = os.path.join(proto.inbox_path(a.root, a.participant), "ack", a.msgid)
    if os.path.exists(p):
        print("already-acked")
        return
    proto.atomic_write_json(p, {"id": a.msgid, "ts": proto.now_ts()})
    print("acked")


def write_control_req(root, pid_, action, sender, reason=None, inject=None):
    """写控制请求信封（§4.3）单点：`control` 与 `cancel` 共用（id/文件名/字段集不分叉）。
    杀进程与置 final 归 runner（§5.4）；本层只落盘请求。返回请求 id。"""
    rid = proto.now_ts() + "-" + proto.fs_safe_id(sender) + "-" + proto.rand_suffix()
    req = {"id": rid, "from": sender, "ts": proto.now_ts(), "action": action}
    if inject:
        req["inject"] = inject
    if reason:
        req["reason"] = reason
    proto.atomic_write_json(os.path.join(proto.control_path(root, pid_), rid + ".req"), req)
    return rid


def cmd_control(a):
    require_pid(a.participant)
    require_not_retired(a.participant)
    if a.inject and a.action != "restart":
        die("inject 仅对 restart 有意义（§4.3）")
    # from = 真实控制方（解析单点 resolve_sender）
    print(write_control_req(a.root, a.participant, a.action,
                            resolve_sender(a.sender, "控制方"),
                            reason=a.reason, inject=a.inject))


def require_live_participant(root, pid_, verb):
    """用例动词的前置门（`answer` / `cancel` / `update` 共用）：有 spec.json ∧ 非生命周期终态。

    协议动词（`send` / `control`）不过本门：它们只负责把信封/请求落盘，对不存在的收件方与
    已终态的参与方同样合法（目录即队列 §11.4；收尾清理也要能写 stop）。用例动词代表
    一个**意图**（答复它 / 取消它 / 改它的调度字段），意图落空必须当场报错而不是静默写个
    无人消费的件。返回 spec 文档。"""
    spec = proto.read_json(proto.spec_path(root, pid_))
    if spec is None:
        die("%s 失败：参与方不存在（无 spec.json）：%s" % (verb, pid_))
    pid_doc = proto.read_json(proto.pid_path(root, pid_))
    if proto.life_terminal(pid_doc):
        die("%s 失败：%s 已生命周期终态（final，status=%s）"
            % (verb, pid_, (pid_doc or {}).get("status", "?")), code=2)
    return spec


def cmd_answer(a):
    """答复目标任务的**最早一条未答 ask**（§4.5/§6.4）：写 reply 信封到它的自家 inbox。

    与裸 `send --type reply --ref <id>` 的差别（也是本动词存在的理由）：① 不需调用方自己
    找 askId（扫描面 = 职位信箱 ∪ spec.reaper 自家信箱，单点 `proto.ask_scan_inboxes`）；
    ② 三道前置门（存在 / 非 final / 确有未答 ask）；③ **继承所答 ask 的 `via`**——非阻塞
    征询（`send --type ask`）的 reply 带 via 才会被收件侧 drain 放行消费，丢了该标记 = 答复
    静默悬空（§4.5）。回执把「答的是哪条、它是不是阻塞态」当场可见（最早未答 ask 未必是
    调用方以为在解的那条）。"""
    require_pid(a.participant, "目标任务")
    require_not_retired(a.participant, "目标任务")
    body = body_of(a)
    require_live_participant(a.root, a.participant, "answer")
    ask = proto.find_pending_ask(a.root, a.participant)
    if ask is None:
        die("answer 失败：%s 没有未答的 ask（无需答复；反问由对方会话发起）" % a.participant)
    env = {"from": resolve_sender(a.sender, "答复方"), "ts": proto.now_ts(),
           "type": "reply", "ref": ask.get("id"), "body": body}
    if ask.get("via"):
        env["via"] = ask["via"]             # 继承来源标记（阻塞 ask 无 via → reply 也不带）
    mid, _p = proto.write_message(proto.inbox_path(a.root, a.participant), env)
    q = " ".join(str(proto.ask_summary(ask) or "").split())
    if len(q) > 120:
        q = q[:120] + "…"
    print(mid)
    print("  askId=%s" % ask.get("id"))
    print("  blocking=%s" % ("no" if ask.get("via") else "yes"))
    print("  via=%s" % (ask.get("via") or "-"))
    print("  question=%s" % q)


def cmd_cancel(a):
    """取消参与方（用例动词）：前置门（存在 / 非 final）+ 写 `control/stop` 请求。

    与裸 `control <p> stop` 的差别就是那道前置门：已终态的参与方无需取消，静默写一个
    无人消费的 stop 请求会让调用方误以为「我取消了它」。杀与置 final 归 runner（§5.4）；
    未启动任务的取消由 runner「spawn 前查控制请求」原生承载。"""
    require_pid(a.participant, "目标参与方")
    require_not_retired(a.participant, "目标参与方")
    require_live_participant(a.root, a.participant, "cancel")
    rid = write_control_req(a.root, a.participant, "stop",
                            resolve_sender(a.sender, "控制方"),
                            reason=(a.reason or "").strip() or "（未给出原因）")
    print(rid)


def cmd_update(a):
    """改**未放行**排队任务的调度字段（resources/provides/needs，§14）。

    调度器无状态重扫（每轮读 spec 判依赖/资源）⇒ 放行前直写下一轮即生效。五道硬校验
    （任一不满足即拒且零落盘）：① 三字段至少传一个；② 传入值必须是字符串数组（`[]` =
    显式清空）；③ 任务存在；④ 非生命周期终态；⑤ **未放行**（无 enable.json）——已放行
    任务的依赖判定已被调度器消费，改字段无意义且易误读（要改需求走干预：inbox inform ∨
    cancel + 重发）。只覆盖传入字段，其余 spec 字段原样不动。"""
    require_pid(a.participant, "目标任务")
    require_not_retired(a.participant, "目标任务")
    keys = (("resources", a.resources), ("provides", a.provides), ("needs", a.needs))
    given = [(k, v) for k, v in keys if v is not None]
    if not given:
        die("update：--resources/--provides/--needs 至少传一个（传 '[]' = 显式清空）")
    parsed = [(k, parse_str_array(v, "--" + k)) for k, v in given]
    require_live_participant(a.root, a.participant, "update")
    if os.path.exists(proto.enable_path(a.root, a.participant)):
        die("update 失败：%s 已放行（存在 enable.json），其依赖/资源判定已被调度器消费，"
            "拒绝修改调度字段。已放行/运行中任务要改需求请走干预手段（send --type inform "
            "∨ cancel + 重发）" % a.participant, code=2)
    spec = proto.read_json(proto.spec_path(a.root, a.participant))
    for k, v in parsed:
        spec[k] = v
    proto.atomic_write_json(proto.spec_path(a.root, a.participant), spec)
    print("updated %s: %s" % (a.participant,
                               json.dumps(dict(parsed), ensure_ascii=False)))


def cmd_enable(a):
    require_pid(a.participant)
    require_not_retired(a.participant)
    p = proto.enable_path(a.root, a.participant)
    if os.path.exists(p):
        die("enable.json 已存在——只进不退，不可重复写定（§14.2）", code=2)
    proto.atomic_write_json(p, {"ts": proto.now_ts(), "by": a.by,
                                **({"note": a.note} if a.note else {})})
    print("enabled")


def cmd_status(a):
    require_pid(a.participant)
    doc = proto.read_json(proto.pid_path(a.root, a.participant))
    if doc is None:
        if os.path.exists(proto.spec_path(a.root, a.participant)):
            st = "排队/未启动（有 spec 无 pid.json）"
            if not os.path.exists(proto.enable_path(a.root, a.participant)):
                st += "；无 enable.json（排队中，等调度方/手工放行，§9.3/§14）"
        else:
            st = "无 spec（信箱型 bot 或不存在）"
        print("participant=%s\nstate=%s\n代终态=未知　生命周期终态=未知"
              % (a.participant, st))
        return
    gt = proto.gen_terminal(doc)
    lt = proto.life_terminal(doc)
    print("participant=%s" % a.participant)
    for k in ("gen", "pid", "procStart", "status", "exitcode", "startedAt",
              "endedAt", "lastAliveAt", "resumed", "restarts", "final"):
        if k in doc:
            print("%s=%s" % (k, doc[k]))
    print("代终态=%s　生命周期终态=%s" % ("真" if gt else "假", "真" if lt else "假"))


def cmd_list(a):
    for pid_ in proto.list_participants(a.root):
        doc = proto.read_json(proto.pid_path(a.root, pid_))
        if doc is None:
            s = "queued" if os.path.exists(proto.spec_path(a.root, pid_)) else "inbox-only"
        else:
            s = doc.get("status", "?")
            if doc.get("final"):
                s += ",final"
            s += ",gen%s" % doc.get("gen")
        print("%-40s %s" % (pid_, s))


def die(msg, code=1):
    print("agentctl: " + msg, file=sys.stderr)
    sys.exit(code)


def main():
    ap = argparse.ArgumentParser(prog="agentctl")
    ap.add_argument("--root", default=None,
                    help="工作区根（如 ~/m），不是 ~/m/agents：basename 为 agents ∨ "
                         "<root>/agents 不在场即拒绝（不静默建目录）；<root>/env/host-id "
                         "不在场只告警（登记不失败）。"
                         "缺省 = 本脚本所在仓的父目录（现场发现，不依赖 cwd/env）")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("create", help="写 spec.json 创建参与方")
    p.add_argument("--name", help="自定义目录名；缺省自动生成（§2.2）")
    p.add_argument("--command", required=True)
    p.add_argument("--workdir", required=True)
    p.add_argument("--creator", required=True)
    p.add_argument("--description", default=None,
                   help="写入 spec `name` 字段（人类可读描述，§4.1）；缺省不写该键")
    p.add_argument("--reaper", default=None,
                   help="写入 spec `reaper` 字段（终态通知唯一收件面，§4.1）："
                        "两段路径式 <family>/<name>，family ∈ {%s}；非法即拒。"
                        "缺省不写该键 → 运行时回落职位信箱 %s 带 note"
                        % (", ".join(proto.FAMILIES), proto.POSITION_PID))
    p.add_argument("--restart-policy", choices=["", "manual", "auto", "one-shot"],
                   default="")
    p.add_argument("--host")
    p.add_argument("--created-by-host")
    p.add_argument("--resources", default=None,
                   help="调度：互斥资源名数组（JSON，如 '[\"gpu\"]'；缺省=[\"serial\"]，[]=不占）")
    p.add_argument("--provides", default=None,
                   help="调度：本任务成功后提供的能力名数组（JSON）")
    p.add_argument("--needs", default=None,
                   help="调度：依赖的能力名数组（JSON；provider 成功后才放行）")
    p.set_defaults(fn=cmd_create)

    p = sub.add_parser("create-bot",
                       help="创建信箱型 bot（bot/<名字>/，只建 inbox/，无 spec）")
    p.add_argument("--name", required=True, help="人指定的 bot 名（§2.1 段白名单）")
    p.set_defaults(fn=cmd_create_bot)

    # bot 组：登记进程型 bot spec（subscribes = 通道 B 写入口，协议 §4.1）
    pb = sub.add_parser(
        "bot", help="bot 登记（子命令：register）——删除仍走 agents-sync/gc.py 通道")
    bsub = pb.add_subparsers(dest="subcmd", required=True)
    p = bsub.add_parser(
        "register",
        help="登记进程型 bot spec：--subscribes topic/<id>[,…]（通道 B）；"
             "spec 已在场时只改 subscribes（删除仍走 agents-sync/gc.py）")
    p.add_argument("--name", required=True, help="bot 裸名（§2.1 段白名单）")
    p.add_argument("--subscribes", default=None,
                   help="逗号分隔的 topic/<id> 列表（只有 topic/ 族可绑定，防抢收）；"
                        "空串 = 显式清空（删字段）")
    p.add_argument("--command", help="新建 spec 必备：启动命令（同 create）")
    p.add_argument("--workdir", help="新建 spec 必备：工作目录（~/ 可，落盘归一化可移植）")
    p.add_argument("--creator", help="新建 spec 必备：登记方（路径式 id 或任务名）")
    p.add_argument("--restart-policy", choices=["", "manual", "auto", "one-shot"],
                   default="")
    p.add_argument("--host")
    p.add_argument("--created-by-host")
    p.add_argument("--description", default=None,
                   help="新建 spec 时写入 `name` 字段（人类可读描述，§4.1）；缺省不写该键。"
                        "spec 已在场时不生效（本命令只改 subscribes）")
    p.add_argument("--reaper", default=None,
                   help="新建 spec 时写入 `reaper` 字段（终态通知唯一收件面，§4.1）："
                        "两段路径式 <family>/<name>，family ∈ {%s}；非法即拒。"
                        "缺省不写该键 → 运行时回落职位信箱 %s。"
                        "spec 已在场时不生效"
                        % (", ".join(proto.FAMILIES), proto.POSITION_PID))
    p.set_defaults(fn=cmd_bot_register)

    # topic 组：协作容器脚手架（设计稿 dispatch/docs/design/topic-design.md §7/§8）
    pt = sub.add_parser(
        "topic", help="topic 协作容器（子命令：init）——只创建；删除/归档铁律走 "
                      "agents-sync/gc.py add topic/<id>/ 通道，绝不直接 rm")
    tsub = pt.add_subparsers(dest="subcmd", required=True)
    p = tsub.add_parser(
        "init",
        help="建 agents/topic/<id>/ 标准布局：topic.md 骨架（frontmatter when: 占位 + "
             "议题/已决/未决）+ inbox/ + watcher/；已存在目录拒绝")
    p.add_argument("id", help="topic id（§2.1 段白名单）")
    p.add_argument("--title", help="topic.md 标题（缺省 = id）")
    p.add_argument("--watcher", action="append",
                   help="订阅登记（通道 A）：会话裸名，建 watcher/<裸名> 条目（存在即订阅、"
                        "删条目即退订）；可重复")
    p.set_defaults(fn=cmd_topic_init)

    p = sub.add_parser("send", help="写 inbox 消息信封（协议动词：不做存活/意图校验）")
    p.add_argument("to")
    p.add_argument("--type", choices=list(proto.MSG_TYPES), default="inform",
                   help="会话语用类型（§6.2）；缺省 inform。ask = **非阻塞**征询（信封自动带"
                        " via=%s，其 reply 由 answer 继承该标记才会被收件侧消费）；"
                        "reply 必带 --ref" % proto.ASK_VIA_SEND_MESSAGE)
    p.add_argument("--body", help="正文（与 --body-file 二选一；trim 后空即拒）")
    p.add_argument("--body-file", dest="body_file",
                   help="正文取自文件（长文/含引号换行走此道，不经 shell 断词）；`-` = stdin")
    p.add_argument("--ref", help="reply 必填：所答 ask 的 id")
    p.add_argument("--deliver", choices=list(proto.DELIVER_MODES), default=None,
                   help="投递方式（§6.6，与 type 正交）：steer = 立即介入运行中会话的当前轮；"
                        "缺省不写字段 = followUp（排队等当前轮结束）")
    p.add_argument("--from", dest="sender",
                   help="发送方身份（两段路径式 id）；缺省取环境 AGENT_SELF，再缺省回落职位"
                        "信箱 %s（不接受裸名）" % proto.POSITION_PID)
    p.set_defaults(fn=cmd_send)

    # 用例动词（与协议动词分档）：代表一个意图，故带前置门（存在 / 非 final / 意图可成立），
    # 意图落空当场报错而不静默写无人消费的件。判据单点 = require_live_participant。
    p = sub.add_parser("answer", help="答复目标任务的未答 ask（写 reply 信封，继承其 via）")
    p.add_argument("participant", help="目标任务（task/<id>）")
    p.add_argument("--body", help="答复正文（与 --body-file 二选一）")
    p.add_argument("--body-file", dest="body_file", help="答复正文取自文件；`-` = stdin")
    p.add_argument("--from", dest="sender", help="答复方身份（同 send --from）")
    p.set_defaults(fn=cmd_answer)

    p = sub.add_parser("cancel", help="取消参与方（= control stop + 存活前置门）")
    p.add_argument("participant")
    p.add_argument("--reason", help="取消原因（入请求信封，审计面；缺省写「未给出原因」）")
    p.add_argument("--from", dest="sender", help="控制方身份（同 send --from）")
    p.set_defaults(fn=cmd_cancel)

    p = sub.add_parser("update", help="改未放行排队任务的调度字段（已放行即拒）")
    p.add_argument("participant")
    p.add_argument("--resources", default=None,
                   help="互斥资源名数组（JSON，如 '[\"gpu\"]'；'[]' = 显式清空）")
    p.add_argument("--provides", default=None, help="成功后提供的能力名数组（JSON）")
    p.add_argument("--needs", default=None, help="依赖的能力名数组（JSON）")
    p.set_defaults(fn=cmd_update)

    p = sub.add_parser("ack", help="写 inbox/ack/<id>")
    p.add_argument("participant")
    p.add_argument("msgid")
    p.set_defaults(fn=cmd_ack)

    p = sub.add_parser("control", help="写控制请求")
    p.add_argument("participant")
    p.add_argument("action", choices=["stop", "restart", "clear"])
    p.add_argument("--inject", help="仅 restart：进新一代启动上下文（§5.3）")
    p.add_argument("--reason")
    p.add_argument("--from", dest="sender",
                   help="控制方身份（两段路径式 id，如 bot/<名>、task/<id>）；缺省取环境"
                        " AGENT_SELF，再缺省回落职位信箱 topic/dispatcher（不接受裸名）")
    p.set_defaults(fn=cmd_control)

    p = sub.add_parser("enable", help="写 enable.json 放行（正常由调度方写；手工兑底）")
    p.add_argument("participant")
    p.add_argument("--by", required=True)
    p.add_argument("--note")
    p.set_defaults(fn=cmd_enable)

    p = sub.add_parser("status", help="读 pid.json 输出谓词判定")
    p.add_argument("participant")
    p.set_defaults(fn=cmd_status)

    p = sub.add_parser("list", help="列出参与方")
    p.set_defaults(fn=cmd_list)

    a = ap.parse_args()
    # 错 root 前置拒绝（不静默建嵌套树）；缺省值 = 现场发现（本仓父目录）
    a.root = require_workspace_root(a.root or default_root())
    a.fn(a)


if __name__ == "__main__":
    main()
