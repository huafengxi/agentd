#!/usr/bin/env python3
"""report.py — <workspace-root>/agents/ 任务目录 markdown 报表（只读扫描，绝不写任务目录）。

用途：反复刷新生成物，接近实时地观察全链路任务状态（用户 2026-08-28 需求）。与 agentctl.py 并列的单文件脚本，仅 python3 标准库。

状态判定口径与 agentctl status 的两层判定（协议 §10）保持一致：
  - 无 pid.json：有 enable.json=「已放行待拉起」，否则「排队中」
  - pid.json final=true：成功（exitcode=0 且有**非空** report.md）/ 失败（exitcode≠0）/
    取消（exitcode=125 或 control/ 有 stop 请求；判据单点 = proto.EXITCODE_CANCELED 注释，
    与 core.ts 的 [已取消] 标记、runner 不发 no_report warn 同判据——取消优先于「无报告」：
    stop 请求与子进程自然 exit 0 竞态时缺报告是取消的预期结果，不是空跑）/
    无报告（final、缺**非空** report.md（零字节/纯空白视同缺报告，口径单点 =
    scheduler.report_nonempty，与调度器放行判据同源）且非取消；
    多为探针/预期无报告任务，中性显示、不进异常区，用户 2026-08-28 拍板）/
    成功（接管丢退出码）：exitcode=127 且有非空 report.md —— 孤儿接管导致退出码丢失的已知机制，
    任务实际成功（与心跳对账口径一致），不算失败、不进异常区
  - final=false 且 status=running：运行中；lastAliveAt 距今 >5 分钟 → ⚠️心跳停滞

异常区排队类口径（用户 2026-08-28 拍板）：
① 「应跑未跑」——未启动任务（无 enable.json ∧ 无 pid.json）
镜像调度器放行门禁（复用同目录 scheduler/proto 谓词）计算「应跑」：needs 均有成功
provider ∧ 资源与占位者无交集 ∧ 占位数未饱和 ∧ 目标主机存活；四者皆满足却仍未放行 →
🚨 应跑未跑（提示调度器可能卡住）。等待依赖/等待资源/等待槽位/主机不存活均属正常调度状态，
不报（取代旧「排队超 30 分钟」规则）。
② 「needs 不可满足」（用户第二条排队类规则）——未启动任务直接复用
scheduler.eval_needs（import 复用，口径零漂移）判三态：unsat（某能力无 provider 或 provider
全部终态非成功）→ 🚨 needs 不可满足（调度器永不放行，需人工干预）；wait（仍有未终态
provider）属正常等待，不报。
③ 「pending 压住旧 success」（🟡2）——wait 里唯一需人工看一眼的子形态：某能力
同时有未终态 provider 与旧 success provider → 调度器按 pending 优先判 wait、旧交付被忽略
（判据单点 = scheduler.caps_pending_over_success，与 tick 升 WARNING 同源）→ ⚠️ 条目点名
cap/在途 provider/被忽略的旧交付 + 恢复路径（等其落地终态或取消它，DISPATCH.md §11）。
严重度低于 ②（不是永不放行）故用 ⚠️ 不用 🚨。provider 五态与 pending 优先的完整口径
不复述，权威 = agentd/README.md「调度」节。

顶部「## 系统」小节（用户 2026-08-28 要求：先看调度基础设施是否正常，再看任务）：生成时间行之后、统计之前，展示 ① 各 host（nv1/nv2/dev/mac）
runner 存活（agents/run/agentd.<host>.lock 的 updatedAt 新鲜度，复用调度器判活口径与 60s
阈值）；② scheduler 存活（dev 本地 pgrep）；③ agents-sync 链路间接指标（远端锁到达
dev 的新鲜度 = 远端 runner + 同步链路双活证明；dev 为 hub 无本地链路，注明即可）。
（2026-08-31 三期 rehome 前 hub 为 nv1，票 7t0ufv；本文件随一/二期迁 dev。）
任一异常同步进异常区「🚨 基础设施」条目。

健壮性：任务目录可能被跨机同步写入（半截文件常见）——任何 JSON 解析失败/文件
缺失只跳过该字段并在该行标注「⚠️读取不完整」，绝不整体崩溃。

活跃任务表「调度依赖」列（用户 2026-08-31 拍板）：needs 反查 provider
任务名与当前状态（`需:cap←taskId(运行中/排队中/已成功待消费)`，无 provider 标注）、
provides 原样展示；**展示序 = scheduler.eval_needs 的裁决序**（pending 优先于旧 success🟡2）——同 cap 两者并存时点名在途 provider 并附「忽略旧成功 task/<id>」，
让需:列指出真正压住放行的那一个（否则呈现为「已成功待消费」却不放行 = 像调度器卡死）。
排队中任务追加 ⛔ 卡点（等资源点名占位者/等槽位/缺 host/等主机，
门禁顺序与 scheduler.tick 一致，复用 eval_needs 三态与 _scan 反查，一次扫描零重扫）。

缺省输出末尾含「最近完成」区块（用户 2026-09-01 要求）：最近 10 个终态任务，
完成时间降序，列 = 完成时间/taskId/名称/结果/耗时/报告有无；与 --finalized 完整终态表并存。

task/bot 两族分区呈现（用户 2026-09-03 拍板）：既有统计/活跃/最近完成/终态各节
只含 task 族；bot/<名字> 常驻进程单独成节「## bot 常驻进程」（状态语义按族区分：
auto 策略 final=被停用而非完成；one-shot final=完成），统计行两族分别计数，异常检测对两族
分别生效且文案带族别。只改呈现与采集字段（补 restartPolicy/stop_request），不改调度/
扫描/判活语义。

会话观测面链接口径（扩展 的「会话」列）：活跃/最近完成/终态三表的
taskId 与名称单元格本身即链接，bot 表对**会话型 bot**（spec.command 含 pi-rpc-wrap.py 或
pid.json 有 sock）的 bot 名与用途名同样给链；脚本型 bot/转发器与无 host 的任务保持纯文本
（绝不造假链接）。链接一律 md 形式、指向 /<host>/agents/<族>/<名>/spec.json?v=chat（机器
前缀经反代路由到宿主机）；为何不用 raw HTML 见 md_link / chat_url 上方注。
 之后活跃表独立的「会话」列（`[观测](…)`）与 taskId/名称两格三处同链冗余，已整列
删除（表头/分隔行/行渲染同步收窄）——其余三表（最近完成/终态/bot）本就无
独立「会话/观测」列，口径天然统一。

主题节「主持人」列（设计稿 dispatch/docs/design/topic-design.md §5.1/§8.1）：主题
表第 2 列给出主持人的 `?v=chat` 会话链接（同一套 chat_url/md_link，host 取该 bot 的
spec.host → 跨机经反代路由到会话宿主机）。判据 = topic_hosts（宽口径：订阅该 topic 的
会话型 bot；系统主题按设计无策展 owner → 恒 `-`）；已单列为主持人的裸名从 watcher/owner
列剔除（同一名字不在两格重复，反冗余口径同上）。

参与方展示字段全集单点 = blank_participant：collect_task 的采集初值与
build_report 的「采集异常兜底」共用同一工厂，杜绝两处手写字典漂移（兜底曾缺
restarts/gen 等键 → bot 表渲染 KeyError，且整份报表先 build 再切片，任一小节都出不来）。

用法示例：
  python3 agentd/report.py                 # 打印到 stdout
  watch -n 5 python3 agentd/report.py     # 反复刷新观察
  python3 agentd/report.py --out /tmp/agents-report.md   # 原子写文件
  python3 agentd/report.py --finalized     # 显示终态表（缺省不显示；显示最近 20 条）
  python3 agentd/report.py --all           # 终态表显示全部（隐含 --finalized）
  python3 agentd/report.py --root <workspace-root>   # 指定工作区根（扫描 <root>/agents/）
  python3 agentd/report.py -s active         # 只输出单个小节的正文（无标题/无生成时间行）
"""



import argparse
import datetime
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.parse

# 复用同目录协议库、调度门禁谓词与 CLI 的缺省根单点（三者均仅标准库），保证「应跑」判定与
# scheduler.tick 放行条件零漂移、`--root` 缺省与 agentctl 同源（现场发现，⛔ 不写死部署值）。
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import agentctl  # noqa: E402
import proto  # noqa: E402
import scheduler  # noqa: E402

HEARTBEAT_STALE_SECS = 5 * 60      # 运行中任务心跳停滞阈值

# scheduler 判活有限重试：抗 pgrep 瞬时假阴性。总耗时上限 ≤1.5s。
SCHED_ALIVE_TRIES = 3
SCHED_ALIVE_RETRY_DELAY = 0.4      # 重试间隔（秒）

# 「## 系统」小节：runner 判活锁固定舰队。判活口径与阈值完全
# 复用调度器放行门禁第四条件（scheduler.host_runner_alive / HOST_ALIVE_THRESHOLD），零漂移。
SYSTEM_HOSTS = ("nv1", "nv2", "dev", "mac")
TERMINAL_LIMIT_DEFAULT = 20        # 终态表缺省条数（--finalized）
RECENT_DONE_LIMIT = 10             # 缺省输出「最近完成」区块条数（用户 2026-09-01）
PROGRESS_SUMMARY_CHARS = 80        # 最新进展摘要截断

PROTO_TS_RE = re.compile(
    r"^(\d{4})-(\d{2})-(\d{2})-(\d{2})-(\d{2})-(\d{2})(\.\d+)?$")


# ---- 时间解析 ---------------------------------------------------------------
# 两种共存格式：spec.createdAt 为 ISO（2026-08-28T16:00:21+08:00）；
# pid.json 各时间戳为 proto.now_ts 格式（2026-08-28-16-00-23.303，本地时间）。
# 统一转 epoch 秒便于比较与算时长。

def ts_to_epoch(s):
    """时间戳字符串 → epoch 秒；解析失败返回 None（半截文件防御）。"""
    if not isinstance(s, str) or not s.strip():
        return None
    s = s.strip()
    try:
        dt = datetime.datetime.fromisoformat(s)
    except ValueError:
        dt = None
    if dt is not None:
        if dt.tzinfo is not None:
            return dt.timestamp()
        return time.mktime(dt.timetuple()) + dt.microsecond / 1e6
    m = PROTO_TS_RE.match(s)
    if not m:
        return None
    try:
        y, mo, d, h, mi, se = (int(x) for x in m.groups()[:6])
        frac = float(m.group(7) or 0)
        return time.mktime((y, mo, d, h, mi, se, 0, 0, -1)) + frac
    except (ValueError, OverflowError):
        return None


def fmt_ts(epoch):
    """epoch 秒 → `MM-DD HH:MM`（报表紧凑显示）；None → `-`。"""
    if epoch is None:
        return "-"
    return time.strftime("%m-%d %H:%M", time.localtime(epoch))


def fmt_dur(secs):
    """秒数 → 紧凑时长（35s / 12m / 1h05m / 2d3h）；None/负 → `-`。"""
    if secs is None or secs < 0:
        return "-"
    secs = int(secs)
    if secs < 60:
        return "%ds" % secs
    if secs < 3600:
        return "%dm" % (secs // 60)
    if secs < 86400:
        return "%dh%02dm" % (secs // 3600, secs % 3600 // 60)
    return "%dd%dh" % (secs // 86400, secs % 86400 // 3600)


def md_escape(s):
    """markdown 表格单元格转义（| 与换行会破坏表格）。"""
    return str(s).replace("|", "\\|").replace("\n", " ")


def truncate(s, n):
    s = str(s)
    return s if len(s) <= n else s[:n - 1] + "…"


# ---- 会话观测面链接 ------------------------------------------
# 渲染器核实（w/ext/markdown/view/markdown.html，marked v12.0.2）：
# ① raw HTML `<a>` 能透传，但页面 renderMarkdown 前的 underscore_escape_filter
#    把代码块/行内码以外的下划线一律转义 → `target="_blank"` 会变成
#    `target="\_blank"`（成了普通命名窗口，不是 _blank 关键字）；
# ② 页面 head 已有 `<base target="_blank">`，普通 md 链接天然在新标签页打开。
# 结论：一律用 md 链接、不嵌 raw HTML —— 同样得到「点击弹出观测窗（新标签页
# 聊天视图）」效果，且不受下划线过滤影响，也不必改渲染框架。

def _q(seg):
    """URL path 段编码（quote 视 _ 为安全字不转，这里补上）。"""
    return urllib.parse.quote(str(seg), safe="").replace("_", "%5F")


def chat_url(participant_id, host):
    """登记会话 ?v=chat 观测面 URL：/<host>/agents/<族>/<名>/spec.json?v=chat。
    机器前缀让任意入口经反代路由到会话宿主机（跨机同理，口径同原「会话」列）。
    host 缺失或族不在 task/bot 白名单 → ""（不给假链接）。"""
    if not host or "/" not in participant_id:
        return ""
    family, name = participant_id.split("/", 1)
    if family not in (proto.TASK_DIR, proto.BOT_DIR) or not name:
        return ""
    # quote 后额外把 _ 转成 %5F：页面 underscore_escape_filter 会把代码块/行内码
    # 以外的下划线一律转义（包括链接 URL 里的），预编码可免被改成 \_ 而断链。
    return "/%s/agents/%s/%s/spec.json?v=chat" % (
        _q(host), family, _q(name))


def md_link(text, url):
    """表格单元格链接；url 为空 → 纯文本（绝不造假链接）。
    文本 = 调用方先 truncate，这里 md_escape（|/换行）后再转义 [ ]：未转义的 |
    会切断单元格，未转义的 [ ] 会让 md 链接解析歧义。"""
    label = md_escape(text).replace("[", "\\[").replace("]", "\\]")
    return "[%s](%s)" % (label, url) if url else label


def is_session_bot(b):
    """bot 族是否有会话观测面：pi-rpc-wrap 常驻会话（spec.command 含 pi-rpc-wrap.py，
    或 pid.json 有 sock 字段）。脚本型 bot 与 channel 转发器无会话 → 纯文本
    （哪些 bot 属哪一族不在本仓复述：名单住调用方的声明面）。取代 旧口径
    「bot 族一律无会话观测面」（会话型 bot 上线后已过时）。"""
    return "pi-rpc-wrap.py" in str(b.get("command") or "") or bool(b.get("has_sock"))


# ---- 只读取件 ----------------------------------------------------------------

def read_json_safe(path):
    """读 JSON；缺失/半截/非 dict → (None, ok_flag)。"""
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
        if isinstance(doc, dict):
            return doc, True
    except (OSError, ValueError):
        pass
    return None, False


def latest_progress_line(path):
    """progress.md 最新一条进展（新条目置顶约定：取首条 `- ` 开头的行）。"""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                t = line.strip()
                if t.startswith("-"):
                    return t.lstrip("- ").strip()
    except OSError:
        pass
    return ""


# provider 终态非成功态的呈现文案（态语义单点 = scheduler.eval_needs / _scan）：
# idle = 空跑（exit0 ∧ 无 report.md ∧ 非取消，不满足依赖）、canceled = 调度员主动取消
# （不算失败也不算空跑，同样不满足依赖）——两者分开呈现，不把取消/空跑说成实施失败。
PROVIDER_STATE_ZH = {"idle": "空跑（exit0 无报告）", "canceled": "已取消",
                     "failed": "已失败"}


def _provider_states_zh(detail):
    """scheduler.eval_needs 的 unsat 明细串 `task/a[idle],task/b[failed]` → 可读文案。
    未识别态原样保留（保守，不因新态丢信息）。"""
    out = []
    for item in detail.split(","):
        m = re.match(r"^(.*)\[(\w+)\]$", item.strip())
        if not m:
            out.append(item.strip())
            continue
        pid_, st = m.group(1), m.group(2)
        out.append("%s（%s）" % (pid_, PROVIDER_STATE_ZH.get(st, st)))
    return "、".join(x for x in out if x)


def has_stop_request(adir):
    """control/ 存在 action=stop 的请求 = 取消链路（协议 §5.2/§5.4）。"""
    cdir = os.path.join(adir, "control")
    try:
        names = os.listdir(cdir)
    except OSError:
        return False
    for n in names:
        if not n.endswith(".req"):
            continue
        doc, _ = read_json_safe(os.path.join(cdir, n))
        if doc and doc.get("action") == "stop":
            return True
    return False


# ---- 单任务采集 ---------------------------------------------------------------

def blank_participant(pid_, adir=None):
    """参与方展示字段初值全集（**单一事实源**）：collect_task 的采集初值
    与 build_report 的采集异常兜底共用本工厂，避免两处手写字典漂移——兜底缺键会让渲染
    路径 KeyError（bot 表的 restarts/gen），而整份报表是先 build 再按小节切片，一处炸
    = 全部小节出不来。
    adir 给定 → enabled/has_report/has_progress/has_result 就地探测；不给（兜底路径可能
    连目录都解析不出来）→ 一律 False。"""
    def _ex(fn):
        return adir is not None and os.path.exists(os.path.join(adir, fn))

    def _rep():
        # 完成判定自证件与放行判据同源（scheduler.report_nonempty 单点🟡3）：
        # 零字节/纯空白 report.md 不算交付 → verdict 落「无报告」、依赖列显示空跑，
        # 不出现「报表 ✅ 成功、调度器判 idle 不放行」的不可解释形态。
        return adir is not None and scheduler.report_nonempty(
            os.path.join(adir, "report.md"))

    return {"id": pid_, "name": pid_, "workdir": "", "host": "",
            "createdAt": None, "creator": "", "sched": "",
            "has_pid": False, "final": False, "status": "", "exitcode": None,
            "startedAt": None, "endedAt": None, "lastAliveAt": None,
            "restarts": None, "gen": None,
            # bot 族字段：restartPolicy=manual|auto|one-shot；
            # stop_request = control/ 有 action=stop（区分「被停用」与「意外终态」）
            "restartPolicy": "", "stop_request": False,
            # 会话观测面判据：spec.command（pi-rpc-wrap 会话型 bot）
            # 与 pid.json 的 sock（封装已就绪 = 可直播）
            "command": "", "has_sock": False,
            # 登记期订阅声明（协议 §4.1 通道 B）：主持人判据用
            "subscribes": [],
            "enabled": _ex("enable.json"),
            "has_report": _rep(),
            "has_progress": _ex("progress.md"),
            "has_result": _ex("result.md"),
            "progress": "", "incomplete": False, "verdict": "?", "verdict_icon": "?",
            "heartbeat_stale": False, "canceled": False, "takeover127": False,
            # 调度三元组（口径与调度器一致；spec 缺失时取调度缺省语义）
            "needs": [], "resources": list(scheduler.DEFAULT_RESOURCES),
            "provides": []}


def collect_task(root, task_id, now):
    """采集一个任务目录的全部展示字段。任何读取失败只置 incomplete，不抛。
    task_id = 路径式 id（task/<id> 等），寻址经 proto.agent_dir 直落。"""
    adir = proto.agent_dir(root, task_id)
    t = blank_participant(task_id, adir)
    try:
        spec, ok = read_json_safe(os.path.join(adir, "spec.json"))
        if spec is None and not os.path.exists(os.path.join(adir, "spec.json")):
            return None  # 无 spec 无 pid 的纯 inbox 目录（如信箱型 bot）不算任务
        if spec is None:
            t["incomplete"] = True
        else:
            t["name"] = spec.get("name") or task_id
            t["restartPolicy"] = str(spec.get("restartPolicy") or "")
            t["command"] = str(spec.get("command") or "")
            subs = spec.get("subscribes")
            t["subscribes"] = [s for s in subs if isinstance(s, str)] \
                if isinstance(subs, list) else []
            t["workdir"] = spec.get("workdir") or ""
            t["host"] = spec.get("host") or ""
            t["creator"] = spec.get("creator") or ""
            t["createdAt"] = ts_to_epoch(spec.get("createdAt"))
            res, prov, need = (spec.get("resources"), spec.get("provides"),
                               spec.get("needs"))
            bits = []
            if res not in (None, []):
                bits.append("res:" + ",".join(map(str, res or [])))
            if need:
                bits.append("needs:" + ",".join(map(str, need)))
            if prov:
                bits.append("provides:" + ",".join(map(str, prov)))
            t["sched"] = " ".join(bits)
            sf = scheduler.normalize_sched_fields(spec)
            t["needs"], t["resources"], t["provides"] = (
                sf["needs"], sf["resources"], sf["provides"])

        pid_doc, pid_ok = read_json_safe(os.path.join(adir, "pid.json"))
        if os.path.exists(os.path.join(adir, "pid.json")):
            if pid_doc is None:
                t["incomplete"] = True  # pid.json 存在但半截/坏：不可判定
            else:
                t["has_pid"] = True
                t["final"] = pid_doc.get("final") is True
                t["status"] = str(pid_doc.get("status") or "")
                ec = pid_doc.get("exitcode")
                t["exitcode"] = ec if isinstance(ec, int) else None
                if t["exitcode"] is None and ec is not None:
                    t["incomplete"] = True  # exitcode 字段本身半截
                for k in ("startedAt", "endedAt", "lastAliveAt"):
                    v = pid_doc.get(k)
                    if v is not None:
                        e = ts_to_epoch(v)
                        t[k] = e
                        if e is None:
                            t["incomplete"] = True
                t["restarts"] = pid_doc.get("restarts")
                t["gen"] = pid_doc.get("gen")
                t["has_sock"] = bool(pid_doc.get("sock"))
        t["progress"] = latest_progress_line(os.path.join(adir, "progress.md"))
        t["stop_request"] = has_stop_request(adir)
    except Exception:
        t["incomplete"] = True  # 兜底：采集层任何意外都不炸整体

    judge(t, adir, now)
    return t


def judge(t, adir, now):
    """状态判定（口径见文件头注；与 agentctl status 两层判定一致）。"""
    if t["incomplete"] and not t["has_pid"] and \
            os.path.exists(os.path.join(adir, "pid.json")):
        t["verdict"], t["verdict_icon"] = "不可判定（pid.json 读取不完整）", "⚠️"
        return
    if not t["has_pid"]:
        if t["enabled"]:
            t["verdict"], t["verdict_icon"] = "已放行待拉起", "⏳"
        else:
            t["verdict"], t["verdict_icon"] = "排队中", "⏳"
        return
    if t["final"]:
        # 取消判据单点（proto.EXITCODE_CANCELED 注释）：exitcode=125 ∨ control/ 有 stop 请求。
        # 三处呈现共用此判据（本函数 verdict、core.ts 列表 [已取消]、runner 终态通知不发
        # warn=no_report）——取消不是失败，也不该被当成「无报告」空跑。
        canceled = t["exitcode"] == proto.EXITCODE_CANCELED or has_stop_request(adir)
        if t["exitcode"] == 0:
            if t["has_report"]:
                t["verdict"], t["verdict_icon"] = "成功", "✅"
            elif canceled:
                # stop 请求与子进程自然 exit 0 的竞态（runner 侧 exited/0 优先 → 通知仍
                # task_done）：缺报告是取消的预期结果 → 判取消，与 [已取消] 标记一致。
                t["verdict"], t["verdict_icon"] = "取消", "🚫"
                t["canceled"] = True
            else:
                # 终态无报告：多为探针/预期无报告任务，噪音大于信号——不进异常区、
                # 中性标记（用户 2026-08-28 拍板）。
                t["verdict"], t["verdict_icon"] = "无报告", "·"
        else:
            if t["exitcode"] == 127 and t["has_report"]:
                # 孤儿接管退出码丢失的已知机制：任务实际成功（有 report.md）。
                # 口径与心跳对账一致：不算失败。
                t["verdict"], t["verdict_icon"] = "成功（接管丢退出码）", "✅"
                t["takeover127"] = True
            elif canceled:
                t["verdict"], t["verdict_icon"] = "取消", "🚫"
                t["canceled"] = True
            else:
                t["verdict"], t["verdict_icon"] = "失败", "❌"
        return
    if t["status"] == "running":
        t["verdict"], t["verdict_icon"] = "运行中", "🏃"
        if t["lastAliveAt"] is not None and now - t["lastAliveAt"] > HEARTBEAT_STALE_SECS:
            t["heartbeat_stale"] = True
    else:
        t["verdict"], t["verdict_icon"] = t["status"] or "未知(非终态)", "❓"


# ---- 系统小节（调度基础设施健康度） ----------------------

def _pgrep_once():
    try:
        rc = subprocess.run(["pgrep", "-f", "agentd/scheduler.py"],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL).returncode
    except OSError:
        return False
    return rc == 0


def scheduler_process_alive():
    """scheduler 判活：本机（report.py 现网跑在 dev，即 scheduler 所在机）存在匹配
    `agentd/scheduler.py` 的进程即活。pgrep 天然排除自身；report.py 自身 cmdline 不含
    该模式，无误判。pgrep 缺失/执行失败按不存活处理（保守，宁可报 🚨）。
    单次 pgrep 有瞬时假阴性（进程在跑但偶发无匹配），改为有限重试：任一次成功即
    判活，全部失败才报死；总耗时上限 ≤1.5s。"""
    for i in range(SCHED_ALIVE_TRIES):
        if _pgrep_once():
            return True
        if i < SCHED_ALIVE_TRIES - 1:
            time.sleep(SCHED_ALIVE_RETRY_DELAY)
    return False


def collect_system_health(root, now):
    """采集调度基础设施健康度。返回 (rows, sched_ok, infra_abn)：
      rows      —— 每 host 一行（锁存在性/心跳 elapsed/到达 mtime elapsed/判活结论）；
      sched_ok  —— scheduler 进程是否在；
      infra_abn —— 异常区 markdown 行（「🚨 基础设施」系列）。
    口径：① runner 存活 = 锁内 updatedAt 新鲜度，解析路径/时间戳格式/阈值全部复用
    scheduler（host_lock_path/_parse_ts/HOST_ALIVE_THRESHOLD），与调度门禁零漂移；
    ② agents-sync 链路为间接口径——远端锁由远端 runner 写、经同步链路到达 dev，
    内容新鲜即同时证明「远端 runner 活 + 同步链路通」；dev 为 hub 无本地链路，注明即可。
    任何读取失败只降级标注，不炸整体。"""
    rows = []
    abn = []
    for host in SYSTEM_HOSTS:
        p = scheduler.host_lock_path(root, host)
        mtime_epoch = None
        try:
            mtime_epoch = os.path.getmtime(p)
        except OSError:
            pass
        doc = proto.read_json(p)
        upd_epoch = scheduler._parse_ts(doc.get("updatedAt")) \
            if isinstance(doc, dict) else None
        row = {"host": host, "exists": mtime_epoch is not None,
               "upd_age": (now - upd_epoch) if upd_epoch is not None else None,
               "mtime_age": (now - mtime_epoch) if mtime_epoch is not None else None,
               "readable": upd_epoch is not None}
        row["runner_ok"] = row["readable"] and \
            row["upd_age"] <= scheduler.HOST_ALIVE_THRESHOLD
        rows.append(row)
        if not row["exists"]:
            abn.append("- 🚨 **基础设施** `%s` runner 锁缺失（无 %s）"
                       % (host, md_escape(os.path.basename(p))))
        elif not row["runner_ok"]:
            detail = ("最近心跳 %s 前" % fmt_dur(row["upd_age"])) \
                if row["readable"] else "锁不可读（半截/坏 JSON）"
            abn.append("- 🚨 **基础设施** `%s` runner stale：%s（阈值 %ds）"
                       % (host, detail, int(scheduler.HOST_ALIVE_THRESHOLD)))
    sched_ok = scheduler_process_alive()
    if not sched_ok:
        abn.append("- 🚨 **基础设施** scheduler 不在（pgrep -f 'agentd/scheduler.py' "
                   "无匹配；查 run/logs/scheduler.log 与 make scheduler.status）")
    return rows, sched_ok, abn


def bot_state(b, now):
    """bot 族状态渲染：bot 为常驻/一次性进程，终态语义与 task 族不同——
    auto 策略 final=被停用/意外死亡而非完成。返回 (状态, 备注)。"""
    if b["incomplete"]:
        return "⚠️ 不可判定", "读取不完整"
    if not b["has_pid"]:
        st = "⏳ 已放行待拉起" if b["enabled"] else "⏳ 未启动"
        if b["restartPolicy"] == "auto":
            return st, "auto 预期常驻"
        return st, ""
    if not b["final"]:
        if b["heartbeat_stale"]:
            return "🏃 运行中 ⚠️心跳停滞", ""
        return "🏃 运行中", ""
    pol = b["restartPolicy"] or "-"
    if pol == "auto":
        if b["stop_request"]:
            return "🚫 已停用", "人工停用（stop 请求），非常规完成"
        return "❌ 意外终态", "auto 却 final 且无 stop 请求"
    if pol == "one-shot":
        if b["canceled"]:
            return "🚫 取消", "exitcode=%s" % b["exitcode"]
        if b["exitcode"] == 0:
            return "✅ 完成", ""
        return "❌ 失败", "exitcode=%s" % b["exitcode"]
    return b["verdict_icon"] + " " + b["verdict"], "exitcode=%s" % b["exitcode"]


# ---- topic 协作容器采集（设计稿 dispatch/docs/design/topic-design.md） ----

def topic_title(adir):
    """主题标题 = topic.md 首个非空正文行（剥 `#` 前缀）；缺失/不可读 → 空串。
    跳过 YAML frontmatter（首行 `---` 至下一个 `---`）：topic.md 骨架自带 `when:` 占位
    （知识域入册约定，见 bots/README.md「知识库规范」），不当标题。"""
    try:
        with open(os.path.join(adir, "topic.md"), encoding="utf-8",
                  errors="replace") as f:
            in_fm = False
            for i, line in enumerate(f):
                t = line.strip()
                if i == 0 and t == "---":
                    in_fm = True
                    continue
                if in_fm:
                    if t == "---":
                        in_fm = False
                    continue
                if t:
                    return t.lstrip("#").strip()
    except OSError:
        pass
    return ""


def collect_topic(adir):
    """采集一个 topic 目录的展示字段（只读，任何失败置缺省不抛）：
    消息数 = inbox/*.msg 计数（不含 ack/ 子目录）；最新消息 = 信封 ts 字段最大值
    （ts 缺失/解析失败的信封跳过）；watcher = watcher/ 下条目名（owner 登记）。"""
    info = {"msgs": 0, "latest": None, "watchers": []}
    inbox = os.path.join(adir, "inbox")
    try:
        names = [n for n in os.listdir(inbox) if n.endswith(".msg")]
    except OSError:
        names = []
    info["msgs"] = len(names)
    for n in names:
        doc, _ = read_json_safe(os.path.join(inbox, n))
        ts = (doc or {}).get("ts")
        if isinstance(ts, str) and (info["latest"] is None or ts > info["latest"]):
            info["latest"] = ts
    try:
        info["watchers"] = sorted(
            w for w in os.listdir(os.path.join(adir, "watcher"))
            if proto.is_valid_name_segment(w))
    except OSError:
        pass
    return info


def _bot_view(root, bots, now):
    """主持人判据的 bot 族数据源（**只此一处**，避免 bot 族被扫两遍造成口径分叉）：
    build_report 传入已采集的 bots（复用同一次 list_participants 扫描）；独立调用方
    （e2e/直接调 collect_topics）不传 → 就地只读扫 agents/bot/*，仍走同一个 collect_task
    采集器（字段与兜底口径与报表主体逐字一致）。"""
    if bots is not None:
        return bots
    base = os.path.join(root, "agents", proto.BOT_DIR)
    try:
        names = sorted(d for d in os.listdir(base) if not d.startswith("."))
    except OSError:
        return []
    out = []
    for n in names:
        try:
            b = collect_task(root, proto.BOT_DIR + "/" + n, now)
        except Exception:
            b = None          # 怪名目录（agent_dir 抛 ValueError）等：跳过不炸
        if b is not None:
            out.append(b)
    return out


def topic_hosts(topic_id, watchers, bots):
    """主题的主持人集合（宽口径，设计稿 §5.1 主持人载体 + §8.1 tasks tab 口径）：
    满足 ①∧② 的 bot 族参与者 ——
      ① 订阅该 topic：通道 B = `spec.subscribes` 含 `topic/<id>`；**或** 通道 A =
        `topic/<id>/watcher/<裸名>` 条目在场且该 bot 目录在场（轻场景：调度员临时兼主持
        只经 topic 侧开 watcher 条目，不会为自己补登记 spec.subscribes）；
      ② `is_session_bot()` 为真（有 `?v=chat` 会话面；脚本型 bot 无）。
    **不以 `DISPATCH_PROFILE=moderator` 作判据**（profile 是人格资产不是身份判据）。
    系统主题（名单单一事实源 = `proto.PROTECTED_SYSTEM_TOPICS`，与 agents-sync/gc.py 的
    PROTECTED_SYSTEM_PATHS 由 e2e S41 同源断言钉住 攒批 5）按设计无策展
    owner（日志全由机制过账）→ 判据入口即早返回空集合（豁免只做一次，不在渲染层散落
    判断）。返回按 id 升序的采集字典列表；零主持人 → []。"""
    if topic_id in proto.PROTECTED_SYSTEM_TOPICS:
        return []
    pid_ = proto.TOPIC_DIR + "/" + topic_id
    wl = set(watchers or ())
    out = []
    for b in bots:
        bid = str(b.get("id") or "")
        name = bid.split("/", 1)[1] if "/" in bid else ""
        if not name or not is_session_bot(b):
            continue
        if pid_ in (b.get("subscribes") or []) or name in wl:
            out.append(b)
    return sorted(out, key=lambda b: str(b.get("id") or ""))


def collect_topics(root, bots=None, now=None):
    """枚举 agents/topic/*（不经 list_participants：topic 族不入进程扫描面）。
    返回按 id 升序的 [{id, title, msgs, latest, watchers, hosts}]；无主题 → 空表。
    hosts = 主持人（bot 采集字典列表，判据见 topic_hosts）；bot 族数据源见 _bot_view。"""
    base = os.path.join(root, "agents", proto.TOPIC_DIR)
    try:
        names = sorted(d for d in os.listdir(base)
                       if not d.startswith(".")
                       and os.path.isdir(os.path.join(base, d)))
    except OSError:
        return []
    if not names:
        return []
    bview = _bot_view(root, bots, time.time() if now is None else now)
    out = []
    for n in names:
        adir = os.path.join(base, n)
        info = collect_topic(adir)
        info["hosts"] = topic_hosts(n, info["watchers"], bview)
        out.append({"id": n, "title": topic_title(adir), **info})
    return out


# ---- 报表渲染 ----------------------------------------------------------------

def build_report(root, now, show_terminal=False, all_terminal=False):
    try:
        # 扫描面 = task/* ∪ bot/*（路径式 id）；无 spec 的信箱型目录
        # 经 collect_task 返回 None 自然不进报表。
        # 异常时同旧行为返回错误页。
        names = proto.list_participants(root)
    except OSError as e:
        return "# agents report\n\n❌ 无法读取 %s/agents：%s\n" % (root, e), []
    collected = []
    for name in names:
        try:
            t = collect_task(root, name, now)
        except Exception:
            # 采集异常兜底：字段全集走 blank_participant（与 collect_task 同源，绝不缺键，
            #）。真实触发面 = 目录名不过 proto 段白名单（list_participants 不校验
            # 名字，agent_dir → parse_participant_id 抛 ValueError），如手工/他工具落的怪名目录。
            t = blank_participant(name)
            t["incomplete"] = True
            t["verdict"], t["verdict_icon"] = "不可判定", "⚠️"
        if t is not None:
            collected.append(t)

    # 按 id 族拆分：task 族走既有各节；bot 族单独成节（见下方）。
    # 统计/异常/活跃/终态各表只算 task 族，避免「任务数」被常驻进程稀释。
    tasks = [t for t in collected if t["id"].startswith(proto.TASK_DIR + "/")]
    bots = [t for t in collected if t["id"].startswith(proto.BOT_DIR + "/")]

    running = [t for t in tasks if not t["final"] and t["has_pid"]]
    queued = [t for t in tasks if not t["has_pid"]]
    finals = [t for t in tasks if t["final"]]
    ok = [t for t in finals if t["verdict"] == "成功"]
    ok127 = [t for t in finals if t["takeover127"]]
    failed = [t for t in finals if t["verdict"] == "失败"]
    canceled = [t for t in finals if t["verdict"] == "取消"]
    stale = [t for t in running if t["heartbeat_stale"]]
    incomplete = [t for t in tasks if t["incomplete"]]

    # 「应跑未跑」门禁（用户 2026-08-28 拍板）：镜像调度器
    # tick 放行条件——needs 均有成功 provider ∧ 资源与占位者持有资源无交集 ∧
    # 占位数未饱和 ∧ 目标主机存活；全部满足却仍未启动（无 enable ∧ 无 pid.json）
    # 才进异常区。等待依赖/资源/槽位/主机均属正常调度状态，不报（取代旧「排队超 30 分钟」）。
    should_run = []
    occ, held, providers_by_cap, slots_full = [], set(), None, False
    try:
        occ, held, providers_by_cap, _q = scheduler.Scheduler(
            root, proto.local_hosts(), all_hosts=True)._scan()
        slots_full = len(occ) >= scheduler.DEFAULT_MAX_CONCURRENT
        gate_ok = True
    except Exception:
        gate_ok = False  # 门禁信息采集失败 → 保守不报两条排队类规则，其余报表照常
    host_alive_cache = {}
    if gate_ok and not slots_full:
        for t in queued:
            if t["verdict"] != "排队中" or t["incomplete"]:
                continue  # 已放行/不可判定者不在本口径（半截文件保守跳过）
            if scheduler.eval_needs(t["needs"], providers_by_cap)[0] != "ok":
                continue  # wait=正常等待不报；unsat 由下方「needs 不可满足」规则单报
            if set(t["resources"]) & held:
                continue  # 资源被占位者持有：正常等待，不报
            if proto.host_missing({"host": t["host"]}):
                continue  # 缺 host = 无人认领永久排队（调度器既定语义），不报
            if t["host"] not in host_alive_cache:
                host_alive_cache[t["host"]] = scheduler.host_runner_alive(
                    root, [t["host"]])[0]
            if not host_alive_cache[t["host"]]:
                continue  # 目标主机不存活：调度门禁不放行，正常等待，不报
            should_run.append(t)

    # 「needs 不可满足」（用户第二条排队类规则，2026-08-28 拍板）：
    # 未启动任务直接复用 scheduler.eval_needs 三态语义（import 复用，口径零漂移）——
    # unsat（某能力无 provider 或 provider 全部终态非成功）→ 异常区；
    # wait（仍有未终态 provider）属正常等待，不报。与槽位是否饱和无关（能力问题）。
    needs_unsat = []
    if gate_ok:
        for t in queued:
            if t["verdict"] != "排队中" or t["incomplete"]:
                continue  # 已放行/不可判定者不在本口径（半截文件保守跳过）
            verdict, reasons = scheduler.eval_needs(t["needs"], providers_by_cap)
            if verdict != "unsat":
                continue  # wait=正常等待不报；ok 交「应跑未跑」门禁继续判
            # reasons 形如 "cap X: unsatisfiable (no provider|task/a[idle],task/b[failed])"
            caps = []
            for r in reasons:
                m = re.match(r"cap (\S+): unsatisfiable \((.*)\)$", r)
                if not m:
                    continue
                why = ("无 provider" if m.group(2) == "no provider"
                       else "provider 无成功者：" + _provider_states_zh(m.group(2)))
                caps.append("`%s`（%s）" % (m.group(1), why))
            needs_unsat.append((t, "、".join(caps) if caps else "（原因见 scheduler 日志）"))

    # 「pending 压住旧 success」（🟡2）：同能力既有在途 provider 又有旧 success
    # → eval_needs 判 wait（pending 优先）、旧交付被忽略，调度器不放行。判据单点 =
    # scheduler.caps_pending_over_success（tick 升 WARNING 同源）；呈现面补恢复路径，
    # 免得调度员为「看起来像卡死」去翻 run/logs/scheduler.log。
    pending_over = []
    if gate_ok:
        for t in queued:
            if t["verdict"] != "排队中" or t["incomplete"]:
                continue  # 已放行/不可判定者不在本口径（enable 单调不回撤）
            hits = scheduler.caps_pending_over_success(t["needs"], providers_by_cap)
            if not hits:
                continue
            pending_over.append((t, "、".join(
                "`%s`（在途 %s，忽略旧交付 %s）" % (cap, "、".join(p), "、".join(s))
                for cap, p, s in hits)))

    # ---- 调度依赖展示：活跃任务的 needs/provides 可见 ----
    # 反查表一次构建：providers_by_cap 来自上方 scheduler._scan（与调度门禁同口径、
    # 零漂移、零重扫）；task_by_id/res_holders 由已采集 tasks 列表就地构建。
    task_by_id = {t["id"]: t for t in tasks}
    # 资源持有者反查（口径同 scheduler._scan 占位者：运行中或已放行待拉起）
    res_holders = {}
    for t in tasks:
        if (t["has_pid"] and not t["final"]) or (t["enabled"] and not t["has_pid"]):
            for r in t["resources"]:
                res_holders.setdefault(r, []).append(t["id"])

    def provider_disp(cap):
        """能力 → `cap←taskId(状态)`；无 _scan 结果 → 原样显示；无 provider → 标注。"""
        if providers_by_cap is None:
            return cap
        provs = providers_by_cap.get(cap) or []
        pick = None
        # 展示序 = scheduler.eval_needs 的裁决序（pending 优先于旧 success🟡2），
        # 再展终态非成功者：需:列点名的必须是真正决定放行/不放行的那个 provider。
        for want in ("pending", "success", "idle", "canceled", "failed"):
            for pid_, st in provs:
                if st == want:
                    pick = (pid_, st)
                    break
            if pick:
                break
        if pick is None:
            return "%s（无 provider）" % cap
        pid_, st = pick
        tt = task_by_id.get(pid_)
        if st == "success":
            disp = "已成功待消费"
        elif tt is not None and tt["has_pid"] and not tt["final"]:
            disp = "运行中"
        elif tt is not None and tt["enabled"]:
            disp = "已放行"
        elif st in PROVIDER_STATE_ZH:
            disp = PROVIDER_STATE_ZH[st]
        else:
            disp = "排队中"
        if st == "pending":
            # 同 cap 另有旧 success = 被忽略的交付（调度器判 wait），就地说明成因
            ignored = [p for p, s in provs if s == "success"]
            if ignored:
                return "%s←%s(%s，忽略旧成功 %s)" % (
                    cap, pid_, disp, "、".join(ignored))
        return "%s←%s(%s)" % (cap, pid_, disp)

    def wait_note(t):
        """排队任务卡点（门禁顺序同 scheduler.tick：依赖→资源→槽位→主机）。
        等能力无需追加（需:列已点名 provider 及其状态）；无可指认返回 ''。"""
        v, _r = scheduler.eval_needs(t["needs"], providers_by_cap)
        if v != "ok":
            return ""  # wait/unsat：卡点在能力，需:列可见（unsat 另进异常区）
        blocked = sorted(set(t["resources"]) & held)
        if blocked:
            items = []
            for r in blocked:
                hs = res_holders.get(r, [])
                items.append("%s←%s" % (r, ",".join(hs)) if hs else r)
            return "等资源:" + "、".join(items)
        if slots_full:
            return "等槽位（%d/%d 满）" % (len(occ), scheduler.DEFAULT_MAX_CONCURRENT)
        if proto.host_missing({"host": t["host"]}):
            return "缺 host 无人认领"
        if t["host"] not in host_alive_cache:
            try:
                host_alive_cache[t["host"]] = scheduler.host_runner_alive(
                    root, [t["host"]])[0]
            except Exception:
                return ""
        if not host_alive_cache[t["host"]]:
            return "等主机 `%s` runner 存活" % t["host"]
        return ""

    def dep_cell(t):
        """调度依赖列：需:cap←provider(状态) ／ 供:cap；排队任务追加 ⛔卡点。
        无 needs/provides → `-`。"""
        parts = []
        if t["needs"]:
            parts.append("需:" + "、".join(provider_disp(c) for c in t["needs"]))
        if t["provides"]:
            parts.append("供:" + ",".join(t["provides"]))
        if (t["verdict"] == "排队中" and not t["incomplete"]
                and providers_by_cap is not None):
            note = wait_note(t)
            if note:
                parts.append("⛔" + note)
        return " ｜ ".join(parts) if parts else "-"

    L = []
    L.append("# agents 全链路任务报表")
    L.append("")
    L.append("> 生成时间：%s ｜ 根目录：`%s` ｜ 扫描 %d 个参与方目录（task %d + bot %d）"
             % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(now)),
                root, len(tasks) + len(bots), len(tasks), len(bots)))
    L.append("")

    # 系统小节（用户 08-28 要求：先看调度基础设施是否正常，再看任务；
    #）。异常进异常区「🚨 基础设施」条目（见下方异常区）。
    sys_rows, sched_ok, infra_abn = collect_system_health(root, now)
    L.append("## 系统")
    L.append("")
    L.append("| host | runner | 最近心跳 elapsed | 锁到达 mtime elapsed | agents-sync 链路 |")
    L.append("|---|---|---|---|---|")
    for r in sys_rows:
        if not r["exists"]:
            runner, hb, mt = "⚠️ 锁缺失", "-", "-"
        else:
            hb = fmt_dur(r["upd_age"]) if r["readable"] else "⚠️ 不可读"
            mt = fmt_dur(r["mtime_age"])
            runner = "✅" if r["runner_ok"] else "⚠️ stale"
        if r["host"] == "dev":
            link = "—（hub，无本地链路）"
        elif not r["exists"] or not r["readable"]:
            link = "⚠️ 无证据"
        elif r["runner_ok"]:
            link = "✅ 双活"
        else:
            link = "⚠️ 无法证明"
        L.append("| %s | %s | %s | %s | %s |" % (r["host"], runner, hb, mt, link))
    L.append("")
    L.append("scheduler（dev）：%s" % ("✅ 在跑" if sched_ok else "🚨 不在"))
    L.append("")

    # 统计头
    L.append("## 统计")
    L.append("")
    # 「终态无报告」不单列统计（不进异常区、不醒目，用户 08-28 拍板），
    # 终态表中该类任务以中性标记「· 无报告」显示。
    # 两族分别计数：上表只算 task 族，避免「任务数」被 bot 稀释；
    # bot 族另列一行独立口径（见下）。
    L.append("| 族 | 总数 | 运行中 | 排队 | 成功 | 成功(接管127) | 失败 | 取消 | 读取不完整 |")
    L.append("|---|---|---|---|---|---|---|---|---|")
    stale_note = "（%d ⚠️心跳停滞）" % len(stale) if stale else ""
    L.append("| task | %d | %d%s | %d | %d | %d | %d | %d | %d |"
             % (len(tasks), len(running), stale_note, len(queued),
                len(ok), len(ok127), len(failed), len(canceled),
                len(incomplete)))
    bot_running_n = len([b for b in bots if b["has_pid"] and not b["final"]])
    bot_nostart_n = len([b for b in bots if not b["has_pid"]])
    bot_final_n = len([b for b in bots if b["final"]])
    bot_stale_n = len([b for b in bots if b["has_pid"] and not b["final"]
                       and b["heartbeat_stale"]])
    bot_inc_n = len([b for b in bots if b["incomplete"]])
    bot_stale_note = "（%d ⚠️心跳停滞）" % bot_stale_n if bot_stale_n else ""
    L.append("| bot | %d | %d%s | %d(未启动) | %d(终态) | — | — | — | %d |"
             % (len(bots), bot_running_n, bot_stale_note, bot_nostart_n,
                bot_final_n, bot_inc_n))
    L.append("")

    # 异常区
    L.append("## ⚠️ 异常区")
    L.append("")
    abn = list(infra_abn)  # 「🚨 基础设施」条目置顶：先看设施再看任务
    # 注：「终态无报告」不进异常区（用户 2026-08-28 拍板：探针/预期无报告任务，
    # 噪音大于信号）；终态表中以中性标记显示。
    for t in failed:
        abn.append("- ❌ **失败** `[task] %s`（%s）：exitcode=%s"
                   % (t["id"], md_escape(t["name"]), t["exitcode"]))
    for t in stale:
        ago = fmt_dur(now - t["lastAliveAt"]) if t["lastAliveAt"] else "?"
        abn.append("- 💓 **心跳停滞** `[task] %s`（%s）：最近心跳 %s（%s 前）"
                   % (t["id"], md_escape(t["name"]), fmt_ts(t["lastAliveAt"]), ago))
    for t in should_run:
        abn.append("- 🚨 **应跑未跑** `[task] %s`（%s）：依赖/资源/槽位/主机放行条件皆满足"
                   "却仍未放行（登记于 %s），调度器可能卡住（查 run/logs/scheduler.log）"
                   % (t["id"], md_escape(t["name"]), fmt_ts(t["createdAt"])))
    for t, caps in needs_unsat:
        abn.append("- 🚨 **needs 不可满足** `[task] %s`（%s）：缺能力 %s，调度器永不放行"
                   % (t["id"], md_escape(t["name"]), caps))
    for t, caps in pending_over:
        abn.append("- ⚠️ **pending 压住旧 success** `[task] %s`（%s）：能力 %s——调度器按在途"
                   " provider 等待、旧交付暂不放行（非永不放行）；恢复 = 等在途 provider 落地"
                   "终态或取消它（DISPATCH.md §11）"
                   % (t["id"], md_escape(t["name"]), caps))
    for t in incomplete:
        abn.append("- ⚠️ **读取不完整** `[task] %s`：跨机同步半截文件，部分字段不可判定"
                   % t["id"])
    # bot 族异常：与 task 族同区呈现，文案带族别。
    # ① 运行中心跳停滞；② 进程缺失（有 spec 无活进程且 restartPolicy=auto 预期常驻）；
    # ③ 意外终态（auto 策略却 final 且无 stop 请求=非停用性死亡）；
    # ④ one-shot 失败（退出码≠0 且非取消）。auto+stop 请求=人工停用属预期，不报。
    for b in bots:
        if b["incomplete"]:
            abn.append("- ⚠️ **读取不完整** `[bot] %s`：跨机同步半截文件，部分字段不可判定"
                       % b["id"])
            continue
        if b["has_pid"] and not b["final"] and b["heartbeat_stale"]:
            ago = fmt_dur(now - b["lastAliveAt"]) if b["lastAliveAt"] else "?"
            abn.append("- 💓 **心跳停滞** `[bot] %s`（%s）：最近心跳 %s（%s 前）"
                       % (b["id"], md_escape(b["name"]),
                          fmt_ts(b["lastAliveAt"]), ago))
        elif not b["has_pid"] and b["restartPolicy"] == "auto":
            abn.append("- 🚨 **进程缺失** `[bot] %s`（%s）：有 spec 无活进程，"
                       "restartPolicy=auto 预期常驻（守护监督可能不在）"
                       % (b["id"], md_escape(b["name"])))
        elif b["final"] and b["restartPolicy"] == "auto" and not b["stop_request"]:
            abn.append("- 🚨 **意外终态** `[bot] %s`（%s）：restartPolicy=auto 却 final"
                       "（exitcode=%s）且无 stop 请求——非常规停用，需人工确认"
                       % (b["id"], md_escape(b["name"]), b["exitcode"]))
        elif b["final"] and b["restartPolicy"] == "one-shot" \
                and b["verdict"] == "失败":
            abn.append("- ❌ **失败** `[bot] %s`（%s）：exitcode=%s"
                       % (b["id"], md_escape(b["name"]), b["exitcode"]))
    L.extend(abn if abn else ["（无异常）"])
    L.append("")

    # 活跃任务表（未终态：运行中 + 排队/已放行待拉起）：运行中（含心跳停滞）
    # 排最前，其次已放行待拉起，最后排队中；组内按登记时间升序（用户 08-28 要求，
    #）。
    def active_priority(t):
        if t["has_pid"]:            # 运行中（含心跳停滞）
            return 0
        return 1 if t["enabled"] else 2   # 已放行待拉起 / 排队中

    active = sorted(running + queued,
                    key=lambda t: (active_priority(t),
                                   t["createdAt"] or t["startedAt"] or 0))
    L.append("## 活跃任务（%d）" % len(active))
    L.append("")
    if active:
        # 无独立「会话」列（删）：taskId 与名称两格本身即 ?v=chat 观测链接，
        # 再加一列 `[观测](同链)` 是三处同链冗余（加列 → 格内给链后失去理由）。
        L.append("| taskId | 名称 | 状态 | host | workdir | 已运行 | 最近心跳 | 调度依赖 | 最新进展 |")
        L.append("|---|---|---|---|---|---|---|---|---|")
        for t in active:
            state = t["verdict_icon"] + " " + t["verdict"]
            if t["heartbeat_stale"]:
                state += " ⚠️心跳停滞"
            if t["incomplete"]:
                state += " ⚠️读取不完整"
            runfor = fmt_dur(now - t["startedAt"]) if t["startedAt"] else "-"
            hb = fmt_ts(t["lastAliveAt"]) if t["has_pid"] else "-"
            prog = truncate(t["progress"], PROGRESS_SUMMARY_CHARS) or "-"
            # 会话观测链接：任务会话 = 普通会话，聊天窗直开；
            # 带 /<host>/ 机器前缀 → 任意入口经反代路由到任务宿主机；新形态（有 sock）
            # = 直播+可介入，旧形态运行中 = 服务端拒绝并指引，终态 = 复活续聊。
            # taskId 与名称单元格同链（：点击任务名即弹出观测窗，md 链接
            # 靠页面 <base target="_blank"> 新标签页打开）；无 host → 纯文本（chat_url 返空）。
            url = chat_url(t["id"], t["host"])
            L.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s |"
                     % (md_link("`%s`" % t["id"], url),
                        md_link(truncate(t["name"], 32), url),
                        md_escape(state), md_escape(t["host"] or "-"),
                        md_escape(truncate(t["workdir"] or "-", 36)),
                        runfor, hb, md_escape(dep_cell(t)), md_escape(prog)))
    else:
        L.append("（无活跃任务）")
    L.append("")

    # bot 族单独成节：常驻进程与任务混在一张表里会稀释任务口径；
    # bot 状态语义见 bot_state（auto 策略 final=被停用/意外死亡，非完成）。
    bots_sorted = sorted(bots, key=lambda b: b["id"])
    L.append("## bot 常驻进程（%d）" % len(bots_sorted))
    L.append("")
    if bots_sorted:
        L.append("| bot | 用途名 | 状态 | restartPolicy | restarts | gen | 已运行 | 最近心跳 | host | 备注 |")
        L.append("|---|---|---|---|---|---|---|---|---|---|")
        for b in bots_sorted:
            bstate, bnote = bot_state(b, now)
            runfor = fmt_dur(now - b["startedAt"]) if b["startedAt"] else "-"
            hb = fmt_ts(b["lastAliveAt"]) if b["lastAliveAt"] else "-"
            # bot 族会话观测面（修正 旧口径）：pi-rpc-wrap 常驻
            # 会话型 bot（`bot/<名>`，pid.json 有 sock）与 task 族同权，bot 名
            # 与用途名均可点击弹出 ?v=chat 观测窗；脚本型 bot 与 channel
            # 转发器无会话 → 保持纯文本（族别名单住调用方的声明面）。
            burl = chat_url(b["id"], b["host"]) if is_session_bot(b) else ""
            L.append("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |"
                     % (md_link("`%s`" % b["id"].split("/", 1)[1], burl),
                        md_link(truncate(b["name"], 40), burl),
                        md_escape(bstate),
                        md_escape(b["restartPolicy"] or "-"),
                        b["restarts"] if b["restarts"] is not None else "-",
                        b["gen"] if b["gen"] is not None else "-",
                        runfor, hb, md_escape(b["host"] or "-"),
                        md_escape(truncate(bnote, 40) or "-")))
    else:
        L.append("（无 bot 族进程目录）")
    L.append("")

    # topic 协作容器小节：直扫 agents/topic/*（非进程型、不入
    # list_participants 扫描面）；每主题一行：id/主持人/标题/消息数/最新消息/watcher。
    # 主持人列：判据见 topic_hosts（系统主题豁免）；链接 host 取该 bot
    # spec.host（跨机经反代路由到会话宿主机），零主持人 → 纯文本 `-`（绝不造假链接）。
    # bots 直接复用上方已采集的同一份数据（bot 族不二次扫描，口径不分叉）。
    topics = collect_topics(root, bots=bots, now=now)
    L.append("## 主题（%d）" % len(topics))
    L.append("")
    if topics:
        L.append("| topic | 主持人 | 标题 | 消息数 | 最新消息 | watcher/owner |")
        L.append("|---|---|---|---|---|---|")
        for tp in topics:
            cells, host_names = [], set()
            for b in tp["hosts"]:
                nm = str(b["id"]).split("/", 1)[1]
                host_names.add(nm)
                cells.append(md_link(nm, chat_url(b["id"], b["host"])))
            # 反冗余（口径同→「三处同链整列删」）：已单列为主持人的裸名
            # 从 watcher/owner 列剔除，同一名字不在两格重复；剔空后为 `-`。
            rest = [w for w in tp["watchers"] if w not in host_names]
            L.append("| `%s` | %s | %s | %d | %s | %s |"
                     % (tp["id"],
                        ", ".join(cells) or "-",
                        md_escape(truncate(tp["title"] or "-", 40)),
                        tp["msgs"],
                        md_escape(tp["latest"] or "-"),
                        md_escape(", ".join(rest) or "-")))
    else:
        L.append("（无主题）")
    L.append("")

    # 「最近完成」区块（用户 2026-09-01 要求）：缺省输出即含——
    # 最近 10 个终态任务，完成时间降序；与 --finalized 的完整终态表并存不冲突。
    # 无 endedAt 的排最后（口径同下方终态表）；区块整体 ≤~15 行，保持缺省输出简洁。
    recent_done = sorted(finals, key=lambda t: t["endedAt"] or 0,
                         reverse=True)[:RECENT_DONE_LIMIT]
    L.append("## 最近完成（最近 %d 个终态任务）" % RECENT_DONE_LIMIT)
    L.append("")
    if recent_done:
        L.append("| 完成时间 | taskId | 名称 | 结果 | 耗时 | 报告 |")
        L.append("|---|---|---|---|---|---|")
        for t in recent_done:
            concl = t["verdict_icon"] + " " + t["verdict"]
            if t["incomplete"]:
                concl += " ⚠️读取不完整"
            dur = fmt_dur(t["endedAt"] - t["startedAt"]) \
                if t["endedAt"] is not None and t["startedAt"] is not None else "-"
            rep = "有" if t["has_report"] else "· 无报告"
            # 终态任务链接语义与「观测」列一致（终态 = 复活续聊，服务端已有处理）
            url = chat_url(t["id"], t["host"])
            L.append("| %s | %s | %s | %s | %s | %s |"
                     % (fmt_ts(t["endedAt"]), md_link("`%s`" % t["id"], url),
                        md_link(truncate(t["name"], 32), url),
                        md_escape(concl), dur, rep))
    else:
        L.append("（无终态任务）")
    L.append("")

    # 终态任务表：默认不渲染（用户 08-28 拍板：status.md 保持简洁）；
    # --finalized 显示最近 20 条、--all 显示全部，均按完成时间倒序。
    if show_terminal:
        finals_sorted = sorted(finals, key=lambda t: t["endedAt"] or 0, reverse=True)
        shown = finals_sorted if all_terminal else finals_sorted[:TERMINAL_LIMIT_DEFAULT]
        more = len(finals_sorted) - len(shown)
        title = "## 终态任务（显示 %d/%d%s）" % (
            len(shown), len(finals_sorted), "" if all_terminal else "，--all 显示全部")
        L.append(title)
        L.append("")
        if shown:
            L.append("| taskId | 名称 | 结论 | exitcode | host | 完成时间 |")
            L.append("|---|---|---|---|---|---|")
            for t in shown:
                concl = t["verdict_icon"] + " " + t["verdict"]
                if t["incomplete"]:
                    concl += " ⚠️读取不完整"
                url = chat_url(t["id"], t["host"])
                L.append("| %s | %s | %s | %s | %s | %s |"
                         % (md_link("`%s`" % t["id"], url),
                            md_link(truncate(t["name"], 32), url),
                            md_escape(concl),
                            t["exitcode"] if t["exitcode"] is not None else "-",
                            md_escape(t["host"] or "-"), fmt_ts(t["endedAt"])))
            if more > 0:
                L.append("")
                L.append("（另有 %d 条更早的终态任务未显示，--all 显示全部）" % more)
        else:
            L.append("（无终态任务）")
        L.append("")
    return "\n".join(L) + "\n", tasks


# ---- 小节切片（--section：tasks 页原子块 widget 化） ----------------
# 纯后处理、零侵入：build_report 的采集与渲染一行不动；先拿全文，再按行切出单个
# `## ` 小节（`活跃任务`/`bot 常驻进程`/`最近完成`/`终态任务` 带括号计数，用前缀匹配）。
# 输出只含小节正文：`## ` 标题行本身被去掉，也不补生成时间行（标题/时间由 widget 承担）。
SECTION_TITLES = {
    "system": "## 系统",
    "stats": "## 统计",
    "abnormal": "## ⚠️ 异常区",
    "active": "## 活跃任务",
    "bots": "## bot 常驻进程",
    "topics": "## 主题",
    "recent": "## 最近完成",
    "terminal": "## 终态任务",
}


def extract_section(text, key):
    """从全文切出 key 对应小节的正文（节头之后到下一个 `^## ` 或 EOF）；
    去掉 `## ` 标题行本身，也不补生成时间行（标题/时间由 widget 端承担）。
    key 必须已在 SECTION_TITLES（argparse choices 已保证）。"""
    title = SECTION_TITLES[key]
    lines = text.split("\n")
    start = None
    for i, ln in enumerate(lines):
        if ln.startswith("## ") and ln.startswith(title):
            start = i
            break
    if start is None:
        return "（报表中未找到小节 `%s`）\n" % title
    end = len(lines)
    for j in range(start + 1, len(lines)):
        if lines[j].startswith("## "):
            end = j
            break
    frag = lines[start + 1:end]
    while frag and frag[0] == "":
        frag.pop(0)
    while frag and frag[-1] == "":
        frag.pop()
    return ("\n".join(frag) + "\n") if frag else "\n"


def atomic_write_text(path, text):
    """写临时文件再 rename（防观察者读到半截）；临时文件与目标同目录。"""
    d = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".report-tmp-", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.rename(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def main():
    ap = argparse.ArgumentParser(
        prog="report.py",
        description="<workspace-root>/agents/ 任务目录 markdown 报表（只读扫描；跨机同步半截"
                    "文件自动降级标注，绝不整体崩溃）。",
        epilog="用法示例：\n"
               "  python3 agentd/report.py                # 打印到 stdout\n"
               "  watch -n 5 python3 agentd/report.py     # 反复刷新观察全链路状态\n"
               "  python3 agentd/report.py --out /tmp/agents-report.md\n"
               "  python3 agentd/report.py --finalized     # 显示终态表（缺省隐藏；最近 20 条）\n"
               "  python3 agentd/report.py --all           # 终态表不截断（隐含 --finalized）\n"
               "  python3 agentd/report.py --root <workspace-root>   # 指定工作区根\n"
               "  python3 agentd/report.py -s active         # 只输出单个小节正文（无标题/时间行）",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=agentctl.default_root(),
                    help="工作区根目录（扫描 <root>/agents/；缺省 = 本脚本所在仓的父目录，"
                         "现场发现，不依赖 cwd/env）")
    ap.add_argument("--out", metavar="PATH",
                    help="写文件而非 stdout（临时文件+rename 原子写）")
    ap.add_argument("--finalized", action="store_true",
                    help="显示终态任务表（缺省不显示；显示最近 %d 条）"
                         % TERMINAL_LIMIT_DEFAULT)
    ap.add_argument("--all", action="store_true",
                    help="终态表显示全部（隐含 --finalized）")
    ap.add_argument("-s", "--section", metavar="KEY", choices=list(SECTION_TITLES),
                    help="只输出单个小节的正文，不含标题行与生成时间行（纯后处理切片；"
                         "tasks.md widget 消费，任务）。"
                         "KEY ∈ %s；terminal 隐含终态表可见" % "/".join(SECTION_TITLES))
    a = ap.parse_args()

    root = os.path.abspath(os.path.expanduser(a.root))
    now = time.time()
    # --section terminal 需终态表在场：等价于隐含 --finalized（--all 仍按显式参数）
    show_terminal = a.finalized or a.all or a.section == "terminal"
    text, _tasks = build_report(root, now, show_terminal, a.all)
    if a.section:
        text = extract_section(text, a.section)
    if a.out:
        atomic_write_text(a.out, text)
        print("written: %s" % os.path.abspath(a.out))
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
