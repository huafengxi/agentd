"""proto.py — Agent 文件通信协议（草案 v1.0）公共库（生产版，自 run2/agent-proto 生长）。

协议权威文档：/home/alice/m/agentd/agent-file-protocol.md
本文件实现其中的公共机制：
  - 原子落盘（§5.1：本机 temp + rename）
  - 时间戳与自动名（§2.2 / §4：格式实现约定，同一参与方内可比先后）
  - 两层判定谓词（§10）
  - 机器身份匹配（§15.3：hostname + 别名）
  - pid 身份与杀纪律（§5.5/§11.8：procStart 读取、(pid,procStart) 校验）
仅使用 python3 标准库。
"""
import json
import os
import random
import re
import socket
import string
import time

TERMINAL_STATUSES = ("exited", "killed", "stale")  # 代终态集合（§10）
CRASH_STATUSES = ("exited", "killed", "stale")     # 崩溃三态（§4.1 自愈只拉这三态）

# 约定赋值（§4.2：killed/stale 由 runner 按约定赋值；对照 DISPATCH 现状口径）
EXITCODE_KILLED = 137
EXITCODE_STALE = 127
# 「调度员主动取消」判据的单一事实源（自评 R2）：现网 runner 的 stop 杀记
# EXITCODE_KILLED/137，125 是旧系统 CANCELED 的约定值（§12.1 概念映射），仅作兼容入参保留。
# 取消 = control/ 存在 action=stop 请求 ∨ exitcode == EXITCODE_CANCELED —— runner（终态通知
# 不发 no_report warn）、report.py（verdict 判「取消」）、core.ts（列表标 [已取消]）三处同判据。
EXITCODE_CANCELED = 125


def now_ts() -> str:
    """时间戳字符串，格式实现约定；同一参与方内可比先后（字典序即时间序）。"""
    return time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime()) + \
        (".%03d" % (int(time.time() * 1000) % 1000))


def rand_suffix(n: int = 4) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def auto_name() -> str:
    """自动生成的参与方标识（§2.2a，taskId 用途）：`<rand6>`（6 个小写字母+数字随机字符）。

    不带日期（2026 用户拍板，覆盖原 `MMDD-HHMM-<rand4>` 格式）；
    36^6 空间区分度足够，撞名由既有重试逻辑吸收。旧扁平任务目录已于 2026-08-31
    经 GC delete-list 清理；
    消息/控制信封 id（file_id/now_ts）仍用完整时间戳，不受影响。"""
    return rand_suffix(6)


def file_id() -> str:
    """消息/控制请求文件名主体：<ts>-<from>-<rand> 中的 <ts>-...-<rand> 部分。"""
    return now_ts() + "-" + rand_suffix()


def atomic_write(path: str, data: str) -> None:
    """一次写定：同目录 temp 文件 + rename 原子落盘（§5.1）。"""
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    tmp = os.path.join(d, ".tmp-%s-%s" % (os.getpid(), rand_suffix(6)))
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.rename(tmp, path)


def atomic_write_json(path: str, obj) -> None:
    atomic_write(path, json.dumps(obj, ensure_ascii=False, indent=1) + "\n")


def read_json(path: str):
    """读 JSON；不存在或半截（解析失败）返回 None —— 下轮重试（§11.6）。"""
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


# ---- pid 身份与杀纪律（§5.5 / §11.8） ----
#
# pid 只是可被操作系统随时复用的编号，不是身份。身份 = (pid, procStart) 二元组：
# procStart = spawn 时记录的内核启动时刻（/proc/<pid>/stat 第 22 字段 starttime，
# 自开机时钟节拍，不受系统时钟调整影响）。任何基于 pid 的探测与 kill 前必须校验。

def proc_starttime(pid):
    """读 /proc/<pid>/stat 第 22 字段 starttime（自开机时钟节拍）。
    非 Linux / 进程不存在 / 读解析失败 → None（= 进程不存在口径）。"""
    try:
        with open("/proc/%d/stat" % pid) as f:
            data = f.read()
    except OSError:
        return None
    # comm（第 2 字段）可含空格与括号：取最后一个 ')' 之后的部分 = 第 3..N 字段
    try:
        rest = data.rsplit(")", 1)[1].split()
        return int(rest[22 - 3])  # 第 22 字段 → rest[19]
    except (IndexError, ValueError):
        return None


def pid_identity_ok(pid, expected_start):
    """(pid, procStart) 二元组校验（§5.5 杀纪律）。
    True  = 身份一致，可探测/可发信号；
    False = 目标已消失（/proc 不存在，或 pid 已被复用——不匹配即按消失处理）。
    无记录身份（expected_start=None，非 Linux/遗留档案）时回退裸 kill(pid,0) 探测。

    顺序即语义：
    必须先判 expected_start——修复前「/proc 不可读（非 Linux）→ 恒 False」的早退
    使裸探测回退分支永远走不到，macOS 上孤儿接管把存活进程误判为死（stale/127）。
    裸探测的已知取舍：无记录身份时不防 pid 复用（文档既定语义；非 Linux 无更优手段）。"""
    if not pid:
        return False
    if expected_start is None:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False
    live = proc_starttime(pid)
    if live is None:
        return False
    return live == int(expected_start)


# ---- 目录布局（§3 + 2026-08-31 类型分层；bot 族） ----
#
# agents/ 按类型分层（设计计划 = agents/task/65ijbb/plan.md，用户 2026-09-03 批准；
# topic 族，设计稿 = dispatch/docs/design/topic-design.md）：
#   agents/task/<id>/     临时任务族（自动名 <rand6>，自定义名如 heartbeat-* 同落此处）
#   agents/bot/<名字>/     稳定名长驻体族（吸收原 participant + channel；信箱型 =
#                         inbox-only，分发型/进程型 = 另含 watcher/ + spec.json 等）
#   agents/topic/<id>/    协作容器族（topic.md 策展文档 + inbox/ 全量日志 +
#                         watcher/ owner 登记；非进程型，不入扫描面/不被监督）
#   agents/queue/<名字>/  无状态请求处理站族（信箱 inbox/ 与处理进程档案同目录；
#                         ⛔ 非 LLM 会话载体，无 watcher/ 订阅注册表——活性由其进程档案直供）
# 寻址 = 路径式 id 直落（§2.2）：id = <family>/<name>，family ∈ {task, bot, topic, queue}
# 白名单封闭集，按第一段直落 agents/<family>/<name>/，不做存在性试探、无回退。
# 落盘一律由写侧显式走 task_dir()/bot_dir()/queue_dir() 创建入口。

TASK_DIR = "task"
BOT_DIR = "bot"
TOPIC_DIR = "topic"
QUEUE_DIR = "queue"
LAYOUT_DIRS = (TASK_DIR, BOT_DIR, TOPIC_DIR, QUEUE_DIR)  # 布局容器目录名（非参与方）
FAMILIES = (TASK_DIR, BOT_DIR, TOPIC_DIR, QUEUE_DIR)      # 参与方族白名单（封闭集，加族 = 改常量）

# 段白名单（协议 §2.1）：字母数字与 `._-`；`.`/`..`/空段单独拒（相对路径分量）。
NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def is_valid_name_segment(s) -> bool:
    """单个名字段合法性（白名单 + 拒相对路径分量）。"""
    return isinstance(s, str) and bool(NAME_RE.match(s)) \
        and s not in (".", "..")


def parse_participant_id(s):
    """路径式参与方 id 解析（§2.1 文法单点，双侧同口径）：
    恰好一个 `/`，前段 ∈ 族白名单，后段过段白名单 → (family, name)；非法抛 ValueError。
    限两段：三段及以上拒绝；裸名（无 `/`）拒绝。"""
    if not isinstance(s, str) or s.count("/") != 1:
        raise ValueError(
            "非法参与方 id：%r——须为两段路径式 <family>/<name>，"
            "family ∈ {%s}，name ∈ [A-Za-z0-9._-]+（禁 ./../空段）"
            % (s, ", ".join(FAMILIES)))
    family, name = s.split("/")
    if family not in FAMILIES:
        raise ValueError("非法参与方 id：%r——未知族 %r（白名单 %s）"
                         % (s, family, "/".join(FAMILIES)))
    if not is_valid_name_segment(name):
        raise ValueError("非法参与方 id：%r——name 段须 ∈ [A-Za-z0-9._-]+，"
                         "禁止空/'.'/'..'" % (s,))
    return family, name


def is_valid_participant_id(s) -> bool:
    """路径式参与方 id 合法性判定（不抛异常版）。"""
    try:
        parse_participant_id(s)
        return True
    except ValueError:
        return False


def fs_safe_id(pid_: str) -> str:
    """`/` 进文件名的转写单点（§2.2）：`/` → `.`（如 `task/65ijbb` → `task.65ijbb`）。
    一切拼信封/事件文件名处强制过。转写不可逆（段内含 `.` 时），但文件名只用于唯一性
    与 ack 对齐——信封/事件 JSON 内字段为权威，判重按整串相等，闭环不破。"""
    return pid_.replace("/", ".")


def task_dir(root: str, pid_: str) -> str:
    """任务目录（写侧创建用）：agents/task/<id>/。"""
    return os.path.join(root, "agents", TASK_DIR, pid_)


def bot_dir(root: str, name: str) -> str:
    """bot 目录（写侧创建用）：agents/bot/<名字>/（稳定名长驻体族，吸收原
    participant + channel 语义；信箱型 = inbox-only，进程型另含 spec.json 等）。"""
    return os.path.join(root, "agents", BOT_DIR, name)


def bot_inbox(root: str, name: str) -> str:
    """bot 收件目录：agents/bot/<名字>/inbox/（写侧创建用）。显式入口，
    不经寻址解析器（固定参与方的落点单点）。"""
    return os.path.join(bot_dir(root, name), "inbox")


def topic_dir(root: str, name: str) -> str:
    """topic 目录（写侧创建用）：agents/topic/<id>/（协作容器族，非进程型）。"""
    return os.path.join(root, "agents", TOPIC_DIR, name)


def topic_inbox(root: str, name: str) -> str:
    """topic 全量日志信箱：agents/topic/<id>/inbox/（写侧创建用）。"""
    return os.path.join(topic_dir(root, name), "inbox")


def queue_dir(root: str, name: str) -> str:
    """queue 目录（写侧创建用）：agents/queue/<名字>/（无状态请求处理站族：
    信箱与处理进程档案同目录，⛔ 非 LLM 会话载体）。"""
    return os.path.join(root, "agents", QUEUE_DIR, name)


def queue_inbox(root: str, name: str) -> str:
    """queue 收件目录：agents/queue/<名字>/inbox/（写侧创建用）。"""
    return os.path.join(queue_dir(root, name), "inbox")


# ---- 职位信箱（queue 族参与方：信箱与 watcher 处理进程同目录）----
# 调度通知流（runner 终态通知 + 子端 ask + 人机/服务传话）的写入口 =
# agents/queue/dispatcher/inbox/；消费 = 同目录的 watcher 进程（轮询并派一次性 handler），
# 单消费者 ⇒ ack 扁平（⛔ 命名空间）。
# 应用层语义权威 = dispatch/DISPATCH.md §1/§8；与 TS 侧 core.ts positionInbox 同源。
# 常量名沿用 POSITION_TOPIC（历史：职位信箱曾是系统主题）；现值语义 = position 名，
# 它同时是遗留 topic tombstone 的名字段（两族同名、同号保护）。
POSITION_TOPIC = "dispatcher"
POSITION_PID = QUEUE_DIR + "/" + POSITION_TOPIC
# 系统主题豁免名单（单一事实源 攒批 5）：按设计无策展 owner 的系统主题
# （报表侧不出主持人链接；gc 侧不可删）。agents-sync/gc.py 的 PROTECTED_SYSTEM_PATHS 是同一
# 名单的 hub 侧副本（该脚本刻意保持 stdlib-only、不 import 本模块），两侧由 e2e S41 的
# 同源断言钉住——将来新增系统主题只改一侧即变红。
PROTECTED_SYSTEM_TOPICS = (POSITION_TOPIC,)
# 职位信箱的两个载体地址（同一信箱在两个族里的落点：现役 queue 参与方 + 遗留 topic
# tombstone）：两个都不可删——回落面必须无条件可写，而 tombstone 一旦删除会被冻结节点
# （老代码仍把它当回落目标）的 send 侧 ensure-inbox 复活并经网状同步推回。
# gc.py 与 ssh-sync.py 的 hub/消费侧副本同值（e2e S41 + test_gc.py 钉）。
PROTECTED_SYSTEM_PATHS = tuple(TOPIC_DIR + "/" + t for t in PROTECTED_SYSTEM_TOPICS) + (
    POSITION_PID,)


def position_inbox(root: str) -> str:
    """职位信箱：agents/queue/dispatcher/inbox/。"""
    return queue_inbox(root, POSITION_TOPIC)


# ---- 已退役信箱地址表（移族；Python 侧护栏 = 建议修 2） ----
# **单点口径（跨语言双实现）**：本表与 TS 侧 agentd 扩展 `core.ts` 的
# `export const RETIRED_MAILBOXES`（定位 = 在调用方工作区 `git grep -n RETIRED_MAILBOXES`；
# 扩展根由调用方的 pi 装载面决定，本仓不钉它的路径）**同源同口径——改一处必须同步另一处**
# （键 = 退役的路径式地址，值 = 新地址）。
# 语义：写侧命中即**显式拒绝并回执新地址**——不静默重定向（地址口径单一，投递方按回执
# 自愈重投），更不得落旧地址（`os.makedirs` 会重建无人消费的僵尸信箱 = 静默丢信；
# 旧目录 gc 删除后尤其如此）。
# 判定**纯查表、与目录是否在场无关**：旧目录在 gc 前仍在场且有 pid.json（能过
# agentctl.require_pid 的文法/身份校验），只有查表能拦住它。
# 键值是部署面的具体参与方名（与 POSITION_TOPIC 同类）：本仓收录判据的既有例外面，
# 加条目沿用同款形态（两语言同批 + e2e S41 钉相等）。
RETIRED_MAILBOXES = {
    BOT_DIR + "/" + POSITION_TOPIC: POSITION_PID,
    TOPIC_DIR + "/" + POSITION_TOPIC: POSITION_PID,
    BOT_DIR + "/notify-user": QUEUE_DIR + "/notify-user",
    # position 信箱与其 watcher 进程合并成单一 queue 参与方（信箱与处理进程同目录）
    # ⇒ 两类旧地址都进表：前者有存量投递方，后者只防陈旧文档驱动的写侧重建僵尸信箱。
    BOT_DIR + "/work-lead": QUEUE_DIR + "/work-lead",
    BOT_DIR + "/agentfw-lead": QUEUE_DIR + "/agentfw-lead",
    BOT_DIR + "/work-lead-watcher": QUEUE_DIR + "/work-lead",
    BOT_DIR + "/agentfw-lead-watcher": QUEUE_DIR + "/agentfw-lead",
}


def retired_move(pid_):
    """退役地址查表（对端 = core.ts 的 `RETIRED_MAILBOXES[to]`）：命中 → 新地址；
    否则 None。纯函数、不触盘（不做存在性试探，§2.2 口径）。"""
    return RETIRED_MAILBOXES.get(pid_)


def agent_dir(root: str, pid_: str) -> str:
    """参与方目录解析器（路径式 id 直落，§2.2）：
    id = <family>/<name> → agents/<family>/<name>/，不做存在性试探、无回退。
    未知族/非法文法 → ValueError（写侧报错、读侧当不存在由调用方处置）。"""
    family, name = parse_participant_id(pid_)
    return os.path.join(root, "agents", family, name)


def list_participants(root: str):
    """参与方扫描面 = task/* ∪ bot/* ∪ queue/*（进程型族入扫描：监督与自愈靠它枚举）。
    返回**路径式 id**（`task/<id>`、`bot/<name>`、`queue/<name>`）；无 spec 的信箱型目录对
    runner/调度器天然无事可做（与「无 spec 不入任务列表」同机制，零新增过滤）。
    topic 族**不入扫描面**：协作容器是文件参与方、无进程语义，
    寻址可直落（agent_dir），但不需要 runner/调度器监督。"""
    base = os.path.join(root, "agents")
    out = []
    for family in (TASK_DIR, BOT_DIR, QUEUE_DIR):
        fdir = os.path.join(base, family)
        if not os.path.isdir(fdir):
            continue
        out.extend(family + "/" + d for d in os.listdir(fdir)
                   if os.path.isdir(os.path.join(fdir, d))
                   and not d.startswith("."))
    return sorted(out)


def spec_path(root, pid_):     return os.path.join(agent_dir(root, pid_), "spec.json")
def pid_path(root, pid_):      return os.path.join(agent_dir(root, pid_), "pid.json")
def inbox_path(root, pid_):    return os.path.join(agent_dir(root, pid_), "inbox")
def control_path(root, pid_):  return os.path.join(agent_dir(root, pid_), "control")
def enable_path(root, pid_):   return os.path.join(agent_dir(root, pid_), "enable.json")


def task_sock_path(root: str, pid_: str) -> str:
    """任务观测 socket 端点（单点定义，任务 / 票 hegipc）：<root>/run/agentd/<id>.sock。
    入参 = 二段 name（task id 本身不含 `/`；路径式 id 的调用方先取第二段传入 口径注记）。
    平台约束（取证；措辞校正，实测 Python 3.13/Linux 与
    3.14/macOS）：端点是 AF_UNIX 路径，受 sun_path 上限约束——可 bind 的路径总长
    ≤107 字节（Linux，sun_path 108 含结尾 NUL）/ ≤103（macOS，104）。超限时
    CPython 在下调 bind(2) 前自行预检长度，抛 OSError("AF_UNIX path too long")
    且 errno=None（**不是**内核 ENAMETOOLONG）；wrap 的超限判定因此取 errno 与
    报文两信号并集，以 sock_bind_failed 诊断退出。生产 root=<workspace-root> 不受影响，深根
    临时树自避。
    run/ = 宿主本地运行时区（不入 git、不进 agents/ 跨机同步树——实测 rsync -a 会复制死
    socket 节点，用户拍板绝不落 agents/）。写者：封装脚本 pi-wrap/pi-rpc-wrap.py（bind/
    chmod/清理）；runner 只把本路径写进 pid.json.sock 字段（spawn 时）；消费方（web
    sessiond）连接时判活。伴生档 <sock>.pid 记 {pid, procStart} 供陈旧节点身份接管。"""

    return os.path.join(root, "run", "agentd", pid_ + ".sock")


TASK_READY_KINDS = ("init-ok", "recv-armed")


def task_ready_path(root: str, pid_: str, kind: str) -> str:
    """任务**就绪握手标记**路径（单点定义）：<root>/run/agentd/<id>.<kind>。

    kind 两态（各自单写者，两个文件而非一个 = 避免双写者竞态）：
      - `init-ok`    写者 = 封装脚本 pi-wrap/pi-rpc-wrap.py：初始 prompt 已被 pi 接受
                     （或幂等跳过 / resident 裸启动）后落盘 → 子端收件扩展
                     （agentd 扩展的 receiver-child.ts）据此开「就绪门」
                     才开始 drain 自家 inbox。缺它 = 子端在 session_start 抢跑注入 →
                     pi 侧「Agent is already processing」拒收初始 prompt（exit 1 秒死）
                     或注入轮先跑完触发 agent_settled → wrap 误判收敛（exit 0 假成功、
                     session.jsonl 永不落盘）。
      - `recv-armed` 写者 = 子端收件扩展：就绪门开、首次补扫完成后落盘 → wrap 有界等待
                     它之后才进收敛监督（防「首轮瞬时结束」时排队中的注入被收敛吞掉）。

    与 sock 同族同目录（run/ = 宿主本地运行时区，不入 git、不进 agents/ 跨机同步树），
    故不存在跨机陈旧副本；每代 spawn 前由 wrap 删除上一代残留（陈旧标记不得骗开本代门）。
    入参 = 二段 name（同 task_sock_path 口径）。"""
    if kind not in TASK_READY_KINDS:
        raise ValueError("task_ready_path: 非法 kind %r（允许 %s）"
                         % (kind, "/".join(TASK_READY_KINDS)))
    return os.path.join(root, "run", "agentd", "%s.%s" % (pid_, kind))


def agentd_lock_path(root: str, host: str) -> str:
    """每机探活锁路径（单点定义）：<root>/agents/run/agentd.<host>.lock。
    runner.py（写：本机互斥锁 + 心跳刷 updatedAt）与 scheduler.py / report.py
    （读：updatedAt 新鲜度判活）一律经本函数，勿在他处另拼路径。
    历史：原在 <root>/agents/ 顶层，
    2026-08-31 迁入 agents/run/（全停切换）。"""
    return os.path.join(root, "agents", "run", "agentd.%s.lock" % host)


# ---- 判定谓词（§10：唯一定义的两个判定） ----

def gen_terminal(pid_doc) -> bool:
    """代终态 ≡ status ∈ {exited, killed, stale}。"""
    return bool(pid_doc) and pid_doc.get("status") in TERMINAL_STATUSES


def life_terminal(pid_doc) -> bool:
    """生命周期终态 ≡ final == true 且 status ∈ 终态。"""
    return bool(pid_doc) and pid_doc.get("final") is True and gen_terminal(pid_doc)


# ---- 消费判据（§6.4 两层确认：ack 键） ----
#
# 「信封已投、是否被消费」的唯一证据是终态 `ack/<id>`，不是信封在场。收件侧（TS 扩展）的在飞态
# **只在进程内存**（认领 = 记内存表，注入文本逐字落会话 jsonl 后才写终态 ack；不判丢 ⇒ 无重投上界、
# 无认亏态），故盘上没有「已认领未确认」的中间形态可读 ⇒ Python 侧判据只有终态 ack 一个面。


def ack_key(env, fn) -> str:
    """信封 → ack 键（单点）：优先信封 `id`；缺 id（信封违规）或畸形 id（含路径分隔符或
    `..`，用作 ack 文件名会逃逸 ack/ 目录）→ 回退文件名主体（去 `.msg` 后缀）。
    与 TS 侧 `core.ts::messageAckId` 同口径。非 dict 信封不抛（回退文件名）。"""
    raw = env.get("id") if isinstance(env, dict) else None
    raw = raw if isinstance(raw, str) else ""
    if raw and "/" not in raw and "\\" not in raw and ".." not in raw:
        return raw
    return fn[:-4] if isinstance(fn, str) and fn.endswith(".msg") else (fn or "")


# ---- 消息信封读写（§4.5/§6.2/§6.4） ----
#
# 写侧单点：信封的字段集、id/文件名形状、原子落盘与撞名重试均住本层，CLI 只组信封。
# 读侧对**半截件**宽容（§11.6）：JSON 不可解析即跳过，下轮重试——写侧是 tmp+rename，
# 正常链路读不到半截件，该宽容只对付非本协议写者的残留物。

MSG_TYPES = ("ask", "inform", "reply")        # §6.2 会话语用三分法（封闭集）
DELIVER_MODES = ("steer", "followUp")          # §6.6 投递方式（与 type 正交；缺省不写 = followUp）
ASK_VIA_SEND_MESSAGE = "send_message"          # §4.5 `via`：非阻塞征询的 ask 来源标记
SYSTEM_SENDER = "agentd"                       # §2.2 系统发送方（裸名保留特例）


def list_messages(inbox_dir):
    """信箱内全部信封（按文件名字典序 = 时间序），每条补 `_file`/`_name`。
    目录不存在 → 空表；半截件跳过（§11.6）。"""
    try:
        names = sorted(f for f in os.listdir(inbox_dir) if f.endswith(".msg"))
    except OSError:
        return []
    out = []
    for fn in names:
        doc = read_json(os.path.join(inbox_dir, fn))
        if not isinstance(doc, dict):
            continue
        doc["_file"] = os.path.join(inbox_dir, fn)
        doc["_name"] = fn
        out.append(doc)
    return out


def write_message(inbox_dir, envelope_no_id, retries=8):
    """写一枚信封（§4.5）：补 `id`（= 文件名主体 `<ts>-<from转写>-<rand>`）后 tmp+rename 原子落盘。
    同名撞车（同毫秒同 rand）换 rand 重试；耗尽抛 RuntimeError。返回 `(mid, path)`。"""
    os.makedirs(inbox_dir, exist_ok=True)
    sender = envelope_no_id.get("from") or ""
    for _ in range(retries):
        mid = now_ts() + "-" + fs_safe_id(sender) + "-" + rand_suffix()
        path = os.path.join(inbox_dir, mid + ".msg")
        if os.path.exists(path):
            continue
        doc = dict(envelope_no_id)
        doc["id"] = mid
        atomic_write_json(path, doc)
        return mid, path
    raise RuntimeError("write_message：连续 %d 次文件名冲突，放弃" % retries)


def ask_scan_inboxes(root, task_pid):
    """ask 扫描面（§4.5 写侧收件面 = 该任务的 reaper）：**职位信箱 ∪ `spec.reaper` 自家信箱**。

    读侧对写侧落点不可知（落点由 runner spawn 时的活性解析决定），故未答 ask 判定一律扫
    **并集**覆盖所有候选落点。**取值非二次解析**：只读登记侧写定的 `spec.reaper` 字段并拼
    其信箱路径，不做活性判定、不做「选哪个」的回落决策（那是写侧 `runner.resolve_reaper`
    的职责，本层不另立第二套）。缺字段 / 文法非法 / spec 不可读 → 只职位信箱。
    返回去重后的 inbox 目录列表（至少含职位信箱）。"""
    out = [position_inbox(root)]
    spec = read_json(spec_path(root, task_pid))
    reaper = (spec or {}).get("reaper")
    reaper = reaper.strip() if isinstance(reaper, str) else ""
    if reaper and is_valid_participant_id(reaper):
        cand = inbox_path(root, reaper)
        if cand not in out:
            out.append(cand)
    return out


def list_task_asks(root, task_pid):
    """该任务发过的全部 ask（扫描面 = `ask_scan_inboxes`）：跨信箱按 id 去重，并按 id 字典序
    归并（id 前缀 = 时间戳 ⇒ 字典序即时间序）→「最早未答 ask」语义跨信箱稳定。
    排序用**码元序**（不用 locale 感知比较：它会重排标点/大小写 → 与「最早」序漂移）。"""
    out, seen = [], set()
    for ib in ask_scan_inboxes(root, task_pid):
        for m in list_messages(ib):
            if m.get("type") == "ask" and m.get("from") == task_pid \
                    and m.get("id") not in seen:
                seen.add(m.get("id"))
                out.append(m)
    out.sort(key=lambda m: str(m.get("id")))
    return out


def find_pending_ask(root, pid_):
    """目标任务的**最早一条未答 ask**：扫描面中 `from == <task 路径式 id>` 的 ask，且目标任务
    `inbox/` 中不存在 `ref == ask.id` 的 reply（§6.4：每个 ask 至多一个 reply）。无则 None。
    消费证据是 reply 信封在场，**不是 ack**（ack 是收件侧传输层判重，与「已答」无关）。"""
    asks = list_task_asks(root, pid_)
    if not asks:
        return None
    answered = {r.get("ref") for r in list_messages(inbox_path(root, pid_))
                if r.get("type") == "reply"}
    for a in asks:
        if a.get("id") not in answered:
            return a
    return None


def ask_question_text(ask):
    """ask 信封 body 里的 question 原文（body 约定 = JSON 字符串 `{question,…}`，§4.5；
    解析失败按原文）。"""
    body = (ask or {}).get("body")
    if isinstance(body, str):
        try:
            doc = json.loads(body)
        except ValueError:
            return body
        if isinstance(doc, dict) and isinstance(doc.get("question"), str):
            return doc["question"]
        return body
    return "" if body is None else str(body)


def ask_summary(ask, limit=200):
    """ask 的展示摘要（question 优先；非 JSON body 折叠空白后截断）。"""
    body = (ask or {}).get("body")
    if isinstance(body, str):
        try:
            doc = json.loads(body)
            if isinstance(doc, dict) and isinstance(doc.get("question"), str):
                return doc["question"]
        except ValueError:
            pass
        t = " ".join(body.split())
        return t[:limit] + "…" if len(t) > limit else t
    return "" if body is None else str(body)


# ---- 机器身份（§15.3：hostname + 别名） ----

def local_hosts(host_arg=None, aliases_arg=None):
    """本机身份集合：显式 --host + 别名 + 本机 hostname。"""
    s = set()
    if host_arg:
        s.add(host_arg)
    for a in (aliases_arg or "").split(","):
        a = a.strip()
        if a:
            s.add(a)
    s.add(socket.gethostname())
    return s


def host_matches(spec, local_ids) -> bool:
    """认领规则（§15.2）：spec.host 须命中本机身份集合才认领。

    **缺失/空 = 无人认领**：
    旧语义「缺省 → 本机认领」是 B-2 切换期的存量兼容兜底，多机网格下会让每台机器
    同时认领同一任务（误监督/误杀），窗口关闭后废除。登记侧（core.ts/agentctl）
    一律在登记时刻物化 spec.host，缺 host 目录属人工补录错误或遗留档案，
    正确处置是无人认领 + 告警（人工补 spec.host 或重新登记）。"""
    h = spec.get("host")
    if not h:
        return False
    return h in local_ids


def host_missing(spec) -> bool:
    """spec.host 缺失/空（无人认领标志；告警判定用，与 host_matches 同一口径）。"""
    return not (spec or {}).get("host")
