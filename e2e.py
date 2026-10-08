#!/usr/bin/env python3
"""e2e.py — agentd 端到端测试（生产版，自 run2/agent-proto/e2e.py 生长）。

在 /tmp/ 下搭临时 agents/ 树，runner 以子进程前台运行（短周期 0.2s，日志落
临时文件），逐场景断言并打印 PASS/FAIL。结束后杀掉全部测试进程并清理临时树。

场景（S1–S8/S10 自原型移植，S11–S12 为 T1-1/T1-3 新增，S14–S15 为 T1-5 新增）：
  S1 创建→正常运行→正常退出→两层谓词判定
  S2 崩溃 + restartPolicy=auto 自愈（只拉崩溃三态）
  S3 pause 已退役（runner rejected unknown action、零写入；restart 健在）
  S4 stop→final→restart/已退役动作被 rejected
  S5 ask/reply 握手（ack 判重 + reply.ref 闭环）
  S6 自定义目录名创建与存在性拒绝
  S7 enable.json 门禁（内置无开关；无合署调度，手工放行才 spawn）
  S8 路由：spec.host != 本机 → 不认领
  （S9 序号不属业务场景：仅用于收尾检查「无遗留测试进程」——宣称用例数按实际 check 数 注）
  S10 伪造 pid 复用：探活判消失 + kill 被拒，无关进程无恙（§5.5/§11.8）
  S11 杀 runner 重启后接手存活子进程不误判 stale（计划 0.b 判死纪律）
  S12 FIFO 串行调度骨架：独立调度方一次只放行一个
  S14 终态通知：done/failed/canceled 三态 + 字段齐备 + 幂等（T1-5）
  S15 runner 重启不重发终态通知（判重基于文件，T1-5）
  S16 探活锁文件：双开拒绝 + 死锁接管 + 优雅退出不删文件 + updatedAt 周期刷新
  S17 子进程环境注入：AGENTD_TASK=1 防递归标记（T1-6）+ 第三方 *_API_KEY 族洗刷
  S18 DAG 资源互斥：同资源串行、异资源并发
  S19 DAG provides/needs：成功放行 + 失败不放行 + 成功 provider 解锁（同任务）
  S20 全局占位上限 + 越位：--max-concurrent 限流，阻塞者不阻塞后续满足者（同任务）
  S21 接管孤儿退出归属
  S22 登记写 host：缺省物化登记机规范名（host-id 映射查表）/显式他机≠createdByHost/未命中与缺失回退（多机阶段 0；映射口径）
  S23 调度器全局视图 --all-hosts：为他机任务放行/缺省只放本机（多机阶段 2 去合署）
  S24 命令形态约定：裸命令（含引号/变量展开）正确执行 + 旧格式（带 bash -c 字面值）双壳兼容
  S25 spec 路径跨 home 可移植：登记侧 ~/ 归一化（HOME 覆写）+ 执行侧 expanduser（登记/执行机 home 不同）+ 命令引用 $AGENT_HOME/$AGENT_ROOT 正确展开
  S26 无身份档案（pid.json procStart 缺失）的存活孤儿接管：不误判 stale，心跳继续刷新
  S27 缺 host 无人认领：双伪节点扫描均不认领+告警；调度器不放行+告警；登记侧映射缺失回退 hostname 非空；带 host 任务认领不回归
  S29 needs provider 127 容忍：stale/127+report.md 放行 / 127 无报告、真实 exited/127、取消 137 与非 0 退出码不放行
  S30 stale 锁回归（评审 3k8n 建议修）：探活锁 updatedAt 陈旧 → 判死不放行不占位；锁刷新恢复放行；本机任务不回归（S23 新鲜锁的对偶）
  S31 agentctl --resources/--provides/--needs CLI 解析与拒绝分支（评审 7fbn/3zyd 建议修，同任务）
  S32 bot 布局（设计计划 §1/§2）：自动名落 task/ 端到端（终态通知 taskId 路径式）+
      id 文法（两段/拒三段与穿越/裸名拒绝）+ agent_dir 直落 + 扫描面 task/*∪bot/* +
      create-bot（信箱型，撞名拒绝、不入任务面）+ bot inbox 投递（文件名 fs_safe_id 转写）
  S33 rpc 封装全链路（票 hegipc）：pid.json.sock 字段 + agents/ 树零 socket +
      wrap 收敛 exited/0 + final + 通知 task_done + sock/.pid 清理 + result.md 退役
  S34 runner 重启不杀任务：wrap 自持管道 → 杀 runner 任务存活 → 孤儿接管续监督 → stop 组杀收敛
  S37 常驻能力（设计 期 1）：resident spawn 不注入 AGENTD_TASK（扩展侧
      三处连锁天然不触发，三件套照旧）+
      调度豁免（槽满放行 + 不计占位/不持资源，普通任务不受挤压）
  S38 control/clear（弃历史换代）：running → 杀+备份+截断+空白新代（新进程可正常
      收件）+ 备份落位可解 + one-shot 立即换新代 + final 拒绝 +
      spawn 前 noop；reload 正交语义由 restart 既有场景（S3/S4）覆盖
  S39 topic 协作容器（设计稿 dispatch/docs/design/topic-design.md）：寻址四族（文法/直落/
      扫描面隔离）+ agentctl send 投递闭环（目录自动创建/信封字段/文件名格式）+
      GC 删除清单通道接受 topic/ 与 bot/（bot 不朽铁律 2026-09-06 经用户拍板移除，；
      topic/dispatcher 受 PROTECTED_SYSTEM_PATHS 缺省拒删、--force 审计旁路可删）
  S40 agentctl 脚手架：topic init 标准布局（topic.md 骨架 + inbox/ + watcher/
      订阅登记）与拒绝面 + bot register --subscribes（协议 §4.1 通道 B 写入口：新建/只改一字段/
      清空/他族拒绝）+ bot register --description/--reaper（：§4.1 两个可选字段
      写入口——带两 flag → spec 含两键 / 缺省 → 两键不在场 / 非法 reaper → rc≠0 且零落盘 /
      既在场分支两 flag 不生效）+ report.py 标题跳 frontmatter（知识域入册约定，见 bots/README.md）
      + report.py 主题节「主持人」列：宽口径判据（通道 A∨B ∧ 会话型 bot）/
      跨机链接前缀取 spec.host/脚本型与信箱型不出链/watcher 列剔重（剔空 → -）/
      系统主题豁免恒 -
  S41 退役地址护栏：proto.RETIRED_MAILBOXES 与 TS 侧
      core.ts 同源同口径 + agentctl 四个写侧子命令（send/ack/control/enable）拒绝并回执新址 +
      拒后零落盘副作用 + 护栏独立于目录是否在场（gc 后仍拦得住）+ 正常地址/文法校验零回归
  S42 取消任务不再误报「无报告」（自评 R2）：调度员主动取消（control/ 有 stop
      请求，或 exitcode=125）缺报告是预期结果 → 终态通知不带 warn=no_report（含 stop 请求与
      子进程自然 exit 0 的竞态）+ 非取消缺报告 one-shot 仍带 warn（P5 对照）+ report.py
      verdict 判「取消」不判「无报告」（与 core.ts [已取消] 标记同判据，扩展侧单测覆盖）
  S43 空跑 provider 不算成功（缺陷来源 = 🟡1）：exit0 ∧ 无 report.md
      ∧ 非取消 = 空跑（idle 态）不满足依赖、依赖者不放行（WARNING + 报表点名）；exit0 ∧ 有
      report.md 照常放行；取消（stop 请求 ∨ exitcode=125）豁免空跑判定（canceled 态：不判失败、
      不发 no_report 告警）但同样不满足依赖；同 cap 下 pending provider 优先于旧 success
      （重发场景防旧的假成功提前放行），取消 pending provider 即回到旧 success 放行；
      report.md 零字节/纯空白 = 无内容不算交付（同归空跑🟡3）；报表面跟随裁决
      （需:列点名在途 provider 并附被忽略的旧交付 + 异常区 ⚠️ pending 压住旧 success 含恢复
      路径，🟡2）；夹具 provider 未被拉起按 pdoc 为 None 真验（⚪5）
  S44 终态通知重放护栏（缺陷实例 = 2026-09-07 nv1/nv2 冷启动重放 45 条历史终态通知）：
      隔离树（S44ROOT，本地职位信箱零信封）复现旧判重集为空的冷启动现场 —— 25 个历史终态档案
      零重放（逐条 INFO 留痕）+ 迟到落地档案（runner 在跑时才 pull 到位的旧终态目录）零重放 +
      本次运行内新终态通知一次且载荷齐备 + 孤儿接管 stale/127∧report.md 在场仍通知（note 在）+
      取消不回归（task_canceled、无 warn、stopReason 在；125/stop 请求两判据）+ 旧代终态档案本次
      运行内 stop 收口（do_stop 不刷新 endedAt）仍通知 + 宽限窗
      （endedAt 距启动 5s 仍通知；--replay-grace 0 则抑制，且不关掉真终态通知与 warn 信号）
  S49 子任务自家信箱推送收件面：用 node 驱动**真** receiver-child.ts + core.ts
      对本 e2e 临时树跑一次子端收件（写入侧 = 真 agentctl send + 真 runner 终态通知）：
      自家 inform 被认领注入 + 落盘确认后写扁平 ack / deliver 两形态（steer 与缺省
      followUp）/ 0828 回归——职位信箱的终态通知零 ack、零注入、原件留存（子任务绕不
      drain 非自家信箱；收件面锁死单点 = core.taskSelfMailbox）
  S50 子端收件面 P0 spawn 竞态（🔴0）：**带未读信封
      spawn 的子任务必须有 session/session.jsonl**（判据口径订正 🟡3：
      session.jsonl 在场只是**必要非充分**——抢跑形态下注入轮先起，真 pi 会落出含 user+assistant
      的 session.jsonl，但初始 prompt 从未进会话树；「实质执行过」的硬判据 = **会话树含初始
      prompt 的 user 事件**（或 report.md 在场），S53 已按此断言）——健康路径（就绪门生效：prompt 被接受早于
      子端开门、注入落树、信封全 ack、reply 不被认领、标记退出即清）/ 形态① exit 1 秒死
      （diagnosis stage=prompt_rejected ∧ session.jsonl 不在场）/ 形态② **exit 0 假成功**
      （零执行、session.jsonl 不在场、却报 task_done，只剩 warn=no_report 一张网）/ 变异对照
      （两形态各加 FAKE_CHILD_HONOR_GATE=1 → 均回健康路径 = 就绪门就是区分因子）/ 陈旧标记
      spawn 前必清（不得骗开本代门）/ arm 有界等待（子端永不 arm → WARN 后照常收敛，不假活）
  S53 真 pi live 覆盖（测试要求 ③； report 遗留 5 自认未做 live 验证）：
      真 `pi --mode rpc`（localhost 桩供应商 + PI_CODING_AGENT_DIR 合成配置，不碰真 ~/.pi）+
      真 runner/wrap/receiver-child.ts —— 真 inotify（fs.watch）认领、真 steer/followUp 队列
      注入落会话树、真落盘确认（注入文本进 jsonl → 写终态 ack；在飞态只在进程内存）；并交叉验证 🔴0 就绪门
      （初始 prompt 真被处理 = **会话树含初始 prompt 的 user 事件**，不是「有 user+assistant」——
      抢跑形态下后者也成立 订正）与 🔴2（reply 零认领零注入）。
      pi 不在 PATH → 显式 SKIP（platform_skip 记账，不静默、不计失败）
  S45 终态通知收件面（reaper 单收件方）与回落面：活性 reaper 直投自家信箱 + notified.json
      集合语义标记（唯一判重事实源）；reaper 缺失/文法非法 → 回落职位信箱 + note 点名成因
      （creator 回落档已裁）；reaper 目录不在场 **或在场但无消费者**（活性代理：pid.json 非
      final ∨ watcher/ 有条目 ∨ 被 spec.subscribes 声明）→ 回落 + note 点名成因；信箱里的
      存量终态信封**不参与判重**（无回落扫描/补写面：无标记的档案允许重发一条，历史档案的
      重放由 S44 重放护栏独立兜住）；多轮 tick/杀重启不重发（粒度 = (taskId,收件方)）
  S54 终态通知 reaper 主模型：收件面 = {reaper}（watchers 旁观面已裁，spec 残留字段零作用）；
      reaper 直投自家信箱 → 职位信箱零份（已退出默认收件面）；载荷不含 role/reaperPid；
      reaper 不存在/无消费者 → 回落职位信箱带 note；(taskId,收件方) 判重 → 多轮 tick +
      杀重启后只一份、标记 to = [reaper]
  S55 bot 族自身终态（「task 与 bot 两族同规则」的回归面）：进程型 bot（spec.reaper、
      restartPolicy=auto）真跑起来 → control stop → 通知落 reaper 信箱（event=task_canceled）+
      标记落位 + 职位信箱零份；对照 = 无 reaper 字段者回落职位信箱带 note
  S57 控制信封 `from` 归属单点化：`agentctl control` 的 `from` 与文件名前缀
      不再硬编码一个裸名缺省值，改走与 TS 侧 `core.resolveCreatorPid` 同源的优先级——
      显式 `--from`（非法即拒 + 零落盘副作用）＞ 环境 `AGENT_SELF`（过 §2.2 文法白名单，
      非法/缺失静默落下一档）＞ 回落职位信箱 `queue/dispatcher`；非调度员会话（领域会话/
      主持人）的取消因此记成它自己（审计与权威归属不再失真）
  S58 写侧动词两档（协议动词 send/ack/control/enable vs 用例动词 answer/cancel/update）：
      send 的 type 缺省 inform、`--deliver` 显式才落盘、`--body-file -` 逐字保真、
      `type=ask` 自动带 via、from 三档归属、六种拒分支零落盘；answer 在扫描面并集
      （职位信箱 ∪ spec.reaper 自家信箱）里找**最早一条未答 ask**、reply 继承其 via
      （阻塞 ask 无 via → reply 也不带）、回执点名所答条目与阻塞态，`--deliver` 与 send
      同形（steer 逐字落盘 / 缺省 = 信封不写该键而非写 followUp / 枚举外值 argparse 拒
      rc=2），三种意图落空
      （无未答 ask / 已 final / 无 spec）全拒；cancel 带存活前置门而 control 不过门（对照）；
      update 五道硬校验（含「已放行即拒」）且只覆盖传入字段；**跨语言钉桩** = TS 收件侧
      core.ts 的 `ASK_VIA_SEND_MESSAGE`/`DELIVER_MODES`/消息型枚举与本仓 proto 常量
      逐字相等（不一致 = Python 写的答复 TS drain 不认 → 静默悬空）；`--root` 缺省 = 现场发现
      （本仓父目录；仓内/工作区根/根上层三种 cwd 同根，只读），显式传错 root 仍前置拒绝
  S59 `--root` 前置校验的自我击穿回归：错 root（= agents 树本身）在**嵌套残骸已在场**
      （残骸恰好满足「`<root>/agents` 在场」这条硬前置）时仍 die（rc=2）且零新建目录/文件
      （写侧 send 与只读 status 两面各钉一次）；零回归 = 正常工作区根的只读动词照常 rc=0、
      **残骸在场也放行**（判据射程只到「root 是不是工作区根」，不收窄 `<root>/agents/` 下
      条目的形状 ⇒ 不得写成全称判据）；`env/host-id` 软前置未被升硬（缺该文件只 WARN、
      rc=0，登记回退 hostname 非空；S22④/S27③ 同族）
  S60 `agentctl create --profile`（人格装载的登记侧写入口；消掉「手写 DISPATCH_PROFILE
      前缀、漏写即静默回落缺省档」的绕行）四格：拼前缀（逐字 = 手写形态、spec 不长新键）/
      非法名被拒（白名单 = 现场枚举 `<root>/bots/profiles/*.json`、枚举根按 `--root` 解析、
      枚举根不在场也拒 ⇒ 反写死名单钉；rc=2 + 打印可选名单 + 零落盘）/ 前缀冲突被拒
      （异值 rc=2 且两侧值都在报文里、同值幂等不重复拼）/ 缺省一行 WARN 且照建
      （command 逐字原值；自带前缀不打 = 存量手写形态零回归）+ `create --help` 覆盖三格
  S61 `agentctl create --prompt-file`（任务书与登记同批落盘；消掉「`prompt.md` 必须与
      `create` 写在同一个 shell 调用里」这条竞态绕行规则）七格：可读文件 ⇒ `prompt.md`
      逐字在场（⛔ 渲染 ∨ 转义 ∨ 补尾换行）且 stdout 仍是 taskId 裸串 / **写点顺序钉**
      （AST 取 `cmd_create` 里两个原子写点的行号：prompt < spec ⇒ 调度方看见 spec 时
      prompt 必已在场，竞态窗结构上闭合）/ 拒分支 ×5（不存在 ∨ 是目录 ∨ 零字节 ∨
      纯空白 ∨ 非 UTF-8）rc=2 + 零落盘（⛔ 建了目录却没 prompt 的半成品）/ 缺省不传
      旗标行为逐字不变（⛔ 写 `prompt.md`）/ 已在场目录仍拒且不覆写既有任务书 /
      `create --help` 覆盖三格 / 与 `--profile` 同用不互斥

场景前置依赖（单跑部分场景时注意，否则会把缺夹具的 FAIL 误读成回归）：S15 读 S14 的通知产物、
S37 会清场前序遗留的非终态参与方；S44 自建隔离树（S44ROOT），
S45/S54/S55 自建合成收件方夹具（`_synth_bot`/`_synth_proc_bot`）→ **可单跑**；
S57 自建控制信封夹具（不起进程、收尾自清）→ **可单跑**；
S58 自建写信封夹具（不起进程；职位信箱只删本场景写的件）→ **可单跑**；
S59 自建临时工作区根夹具（含残骸；不起进程、不读主树 ROOT，收尾自清）→ **可单跑**；
S60 自建临时工作区根夹具（含 `bots/profiles/` 白名单枚举根；不起进程、不读主树 ROOT，
收尾自清）→ **可单跑**；
S61 自建临时工作区根夹具（含任务书正文与四类非法文件夹具；不起进程、不读主树 ROOT，
收尾自清）→ **可单跑**。

平台兼容（mac）：① Linux-only 依赖走平台感知——/proc/<pid>/environ 只在
Linux 在场，且 macOS 无等价替代（ps -Eww/eww 不暴露他进程环境，SIP；等价手段需 ctypes
KERN_PROCARGS2，改动面大于收益），故 S17 的「runner 宿主 environ 保留假 key」子断言在非
Linux 显式 SKIP（platform_skip 记账，汇总行报 skip 处数，不静默、不计失败），该场景其余
断言（子进程注入/洗刷）照常跑；procStart 断言（S1/S10）按平台分支断言各自正常形态。
② 竞态加固：runner 记 status=running 与子进程实际落盘之间无同步点（Popen 即记），读子进程
刚写的文件前一律先 wait_file 等存在（可选等写入落定），超时才判失败。

多机阶段 2（去合署）：调度侧场景（S12/S18–S20/S23）的调度方均为
独立的 scheduler.py 进程（常驻助手 start_scheduler 或 --once 助手 tick_scheduler）。
runner 的 enable.json 门禁内置无开关（--scheduled 参数已移除）：
非调度场景经 create() 内置的手工放行（agentctl enable）拉起；S7 同时证明无调度器时
runner 不自写 enable。

仅使用 python3 标准库（例外：S49 以子进程调 `node` 驱动真 TS 扩展——node 是 pi 本体的
运行时，四机必然在场；缺失则断言失败并点名原因，不静默跳过）。
"""
import ast
import glob
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
# pi-rpc-wrap.py（与它的替身 fakepi_rpc.py、验证矩阵 test_wrap.py）住在兄弟仓
# pi-wrap/；proto.py 反过来是那个仓从本仓 import 的单点。PI_WRAP_DIR 可改指。
PI_WRAP_DIR = os.environ.get("PI_WRAP_DIR") or os.path.join(
    os.path.dirname(HERE), "pi-wrap")
WRAP = os.path.join(PI_WRAP_DIR, "pi-rpc-wrap.py")
# 调用方扩展目录（相对工作区根）：agentd 扩展住调用方 pi 的**全局装载面**，本仓 ⛔ 不逐处钉它的
# 路径 ⇒ 单点在此，可由 AGENTD_EXT_REL 注入（与 pi-wrap/pi-rpc-wrap.py 的 EXT_DIR_REL 同名同口径）。
EXT_REL = os.environ.get("AGENTD_EXT_REL") or "pi-core/agent/extensions/agentd"
EXT_PARTS = tuple(EXT_REL.split("/"))
EXT_DIR = os.path.abspath(os.path.join(HERE, "..", *EXT_PARTS))
# 继承面洗刷（t-zqm0）：名单单一事实源 = 同目录 envscrub.py（与 runner.py /
# serviced/serviced.py / w/ext/sessiond/proc.py 同源），本文件 ⛔ 不复制第二份名单。
# 入口就地洗 os.environ（_scrub_inherited_env），其后所有子进程 env 一律经 scrub_env()
# 取（幂等：入口洗过后再洗零增量），⛔ 不再直接 dict(os.environ) 继承宿主身份族。
# 放在 PI_WRAP_DIR/EXT_REL 取值之后 ⇒ 那两个注入口（PI_ 前缀族）不受洗刷影响。
sys.path.insert(0, HERE)
import proto  # noqa: E402  地址单点（职位信箱等）在夹具里按常量派生，⛔ 硬编码族/名字
from envscrub import scrub_env  # noqa: E402

TMPBASE = tempfile.mkdtemp(prefix="agentd-e2e.%d." % os.getpid())


def _sock_endpoint_ok(base):
    """wrap 观测端点投影不超平台限：端点 = <root>/run/agentd/<name>.sock
    是 AF_UNIX 路径，受内核 sun_path 上限（Linux 108 / macOS 104 字节）约束。
    mkdtemp 跟随 TMPDIR——macOS 缺省 /var/folders/.../T/ 太深，端点超长 →
    wrap 启动即炸（CPython 在 bind(2) 前预检长度、抛 OSError "AF_UNIX path
    too long"，errno=None，非内核 ENAMETOOLONG；S33-35 族在 mac 全挂， 取证）。
    投影用最坏场景名（最长显式名 22B），阈值 100B 留平台余量。"""
    worst = os.path.join(base, "root", "run", "agentd", "s26-noid-takeover.sock")
    return len(worst.encode()) <= 100


if not _sock_endpoint_ok(TMPBASE):
    # 深 TMPDIR 平台（mac）回退短根：/tmp（Linux/mac 均在场；mac 的 /tmp 符号
    # 链接无碍——bind 计的是传入路径串长度，不解引用后重计）。
    shutil.rmtree(TMPBASE, ignore_errors=True)
    TMPBASE = tempfile.mkdtemp(prefix="agentd-e2e.%d." % os.getpid(), dir="/tmp")
    assert _sock_endpoint_ok(TMPBASE), \
        "TMPBASE 过深，AF_UNIX 端点将超平台限：%s" % TMPBASE
ROOT = os.path.join(TMPBASE, "root")
RUNNER_LOG = os.path.join(TMPBASE, "runner.log")
SCHED_LOG = os.path.join(TMPBASE, "scheduler.log")
INTERVAL = "0.2"
IS_LINUX = sys.platform.startswith("linux")
RESULTS = []
PLATFORM_SKIPS = []   # [(标签, 原因)]：平台不适用而显式跳过的断言（不计失败，汇总行报数）
# 场景过滤（argv 位置参数，子串匹配场景名，如 `e2e.py S45 S14`）：空 = 全量。
# 只改涉及场景时的快通道；被过滤掉的场景打 SKIP 且不入 RESULTS（不计成也不计败）。
# 注意：部分场景依赖前序产物（如 S15 读 S14 的通知），选取时自带前置场景。
ONLY = {a.upper() for a in sys.argv[1:] if not a.startswith("-")}
RUNNER = None
SCHEDULER = None


def ctl(*args, expect_rc=0):
    r = subprocess.run([sys.executable, os.path.join(HERE, "agentctl.py"),
                        "--root", ROOT, *args], capture_output=True, text=True)
    if expect_rc is not None and r.returncode != expect_rc:
        raise AssertionError("agentctl %s rc=%d (want %d): %s"
                             % (" ".join(args), r.returncode, expect_rc, r.stderr))
    return r


def pdoc(pid_):
    p = os.path.join(adir_of(pid_),  "pid.json")
    if not os.path.exists(p):
        return None
    with open(p) as f:
        return json.load(f)


def sdoc(pid_):
    p = os.path.join(adir_of(pid_),  "spec.json")
    if not os.path.exists(p):
        return None
    with open(p) as f:
        return json.load(f)


def wait_until(fn, desc, timeout=8.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            v = fn()
            if v:
                return v
        except Exception:
            pass
        time.sleep(0.1)
    raise AssertionError("timeout waiting: " + desc)


def platform_skip(label, reason):
    """平台不适用的断言：显式记账 + 立即打印 SKIP 行（不静默、不计失败）。
    汇总行按「N 项中 M 项含平台 skip（共 K 处断言）」报数，见 main()。"""
    PLATFORM_SKIPS.append((label, reason))
    print("SKIP  %s —— %s（平台 %s）" % (label, reason, sys.platform), flush=True)


def wait_file(path, desc, timeout=8.0, settled=False):
    """竞态加固：读/utime/清理「子进程刚写的文件」之前先等它落盘。
    runner 在 Popen 后即记 status=running，与子进程实际创建/写完文件之间没有同步点，
    只等 running 就直接读会撞 FileNotFoundError 或读到半截内容（mac 取证：S37a 的
    envprobe、S38a 的 session.jsonl）。
    settled=True 再等「大小连续两次观察不变」，用于随后要断言全文/要 aging mtime 的场合；
    超时才判失败（上界 timeout，绝不无条件等）。"""
    wait_until(lambda: os.path.exists(path) or None,
               "%s：文件存在 %s" % (desc, path), timeout=timeout)
    if not settled:
        return path
    t0 = time.time()
    last = -1
    while time.time() - t0 < timeout:
        sz = os.path.getsize(path)
        if sz == last:
            return path
        last = sz
        time.sleep(0.1)
    raise AssertionError("timeout waiting: %s：写入落定 %s" % (desc, path))


def _lock_doc(path):
    """探活锁文件内容（S16）：不存在/半截 JSON 回 None，供 wait_until 轮询。"""
    if not os.path.exists(path):
        return None
    try:
        with open(path) as f:
            return json.loads(f.read())
    except ValueError:
        return None


def check(name, fn):
    if ONLY and not any(tok in name.upper() for tok in ONLY):
        print("SKIP  %s（场景过滤：%s）" % (name, ",".join(sorted(ONLY))), flush=True)
        return
    try:
        fn()
        RESULTS.append((name, True, ""))
        print("PASS  %s" % name, flush=True)
    except Exception as e:
        RESULTS.append((name, False, str(e)))
        print("FAIL  %s —— %s" % (name, e), flush=True)


def start_runner(extra=None, env_extra=None):
    global RUNNER
    env = None
    if env_extra:
        env = dict(scrub_env(), **env_extra)
    RUNNER = subprocess.Popen(
        [sys.executable, os.path.join(HERE, "runner.py"), "--root", ROOT,
         "--host", "e2ehost", "--aliases", "e2e-alias",
         "--interval", INTERVAL, "--log-file", RUNNER_LOG, "--log-level", "DEBUG"]
        + list(extra or []),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env)
    time.sleep(0.3)


def start_scheduler(max_concurrent=None, all_hosts=False):
    """独立调度方常驻子进程（多机阶段 2 去合署后的调度侧形态）。"""
    global SCHEDULER
    SCHEDULER = subprocess.Popen(
        [sys.executable, os.path.join(HERE, "scheduler.py"), "--root", ROOT,
         "--host", "e2ehost", "--aliases", "e2e-alias",
         "--interval", INTERVAL, "--log-file", SCHED_LOG, "--log-level", "DEBUG"]
        + (["--max-concurrent", str(max_concurrent)] if max_concurrent is not None else [])
        + (["--all-hosts"] if all_hosts else []),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.3)


def stop_scheduler():
    global SCHEDULER
    if SCHEDULER:
        SCHEDULER.send_signal(signal.SIGTERM)
        try:
            SCHEDULER.wait(timeout=3)
        except subprocess.TimeoutExpired:
            SCHEDULER.kill()
        SCHEDULER = None


def tick_scheduler(*extra):
    """scheduler.py --once 单次放行（调试/人工兜底入口，同生产人工兜底口径）。"""
    r = subprocess.run(
        [sys.executable, os.path.join(HERE, "scheduler.py"), "--root", ROOT,
         "--host", "e2ehost", "--aliases", "e2e-alias", "--once"] + list(extra),
        capture_output=True, text=True)
    assert r.returncode == 0, "scheduler --once rc=%d: %s" % (r.returncode, r.stderr)


def stop_runner():
    global RUNNER
    if RUNNER:
        RUNNER.send_signal(signal.SIGTERM)
        try:
            RUNNER.wait(timeout=3)
        except subprocess.TimeoutExpired:
            RUNNER.kill()
        RUNNER = None


def kill_runner_hard():
    """SIGKILL 掉 runner（模拟崩溃/被杀，供 S11 接手场景）。"""
    global RUNNER
    if RUNNER:
        RUNNER.kill()
        RUNNER.wait(timeout=3)
        RUNNER = None


# ROOT 作为 argv 传入（fakeagent 忽略它）——使命令行可被 pkill/pgrep -f 精确定位，
# 残留检查才不失真。
FAKE_CMD = "python3 %s %s"


def adir_of(pid_):
    """路径式 id 直落（同 proto.agent_dir 口径）：
    `task/<id>`/`bot/<名>` → agents/<family>/<name>/；裸名兼容回落 task/（仅测试夹具旧写法）。"""
    if "/" in pid_:
        return os.path.join(ROOT, "agents", pid_)
    return os.path.join(ROOT, "agents", "task", pid_)


def create(name_args, policy=None, host=None, command=None, gate=True):
    args = ["create",
            "--command", command or (FAKE_CMD % (os.path.join(HERE, "fakeagent.py"), ROOT)),
            "--workdir", ROOT, "--creator", "tester"]
    if policy:
        args += ["--restart-policy", policy]
    if host:
        args += ["--host", host]
    args += name_args
    name = ctl(*args).stdout.strip()
    pid_ = "task/" + name  # 路径式参与方 id
    # 登记侧恒写 reaper（core.ts resolveReaper 缺省推导面）；agentctl 夹具侧等价补写：
    # reaper=职位信箱 → resolve_reaper 直落回落面、不带 note（终态载荷与生产同构）
    set_sched_fields(pid_, reaper="queue/dispatcher")
    if gate:
        # enable.json 门禁内置无开关（--scheduled 已移除）：非调度场景手工放行，
        # 语义等价旧无门禁模式的登记即拉起；调度侧场景传 gate=False 由调度方放行。
        ctl("enable", pid_, "--by", "e2e")
    return pid_


def inbox_msgs(pid_):
    d = os.path.join(adir_of(pid_),  "inbox")
    out = []
    for fn in sorted(glob.glob(os.path.join(d, "*.msg"))):
        with open(fn) as f:
            out.append(json.load(f))
    return out


def ack_of(pid_, reqid, kind="control"):
    return os.path.exists(os.path.join(adir_of(pid_),  kind, "ack", reqid))


def ack_json(pid_, reqid):
    p = os.path.join(adir_of(pid_),  "control", "ack", reqid)
    if not os.path.exists(p):
        return None
    with open(p) as f:
        return json.load(f)


def pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def set_sched_fields(pid_, **fields):
    """测试用：登记后、放行前直写 spec.json 的调度字段（resources/provides/needs）。"""
    p = os.path.join(adir_of(pid_),  "spec.json")
    with open(p) as f:
        spec = json.load(f)
    spec.update(fields)
    tmp = p + ".tmp"
    with open(tmp, "w") as f:
        json.dump(spec, f)
    os.rename(tmp, p)


def enable_doc(pid_):
    p = os.path.join(adir_of(pid_),  "enable.json")
    if not os.path.exists(p):
        return None
    with open(p) as f:
        return json.load(f)


def enable_task(pid_, by="e2e"):
    """放行一个任务，**容忍常驻调度方抢先写 `enable.json`**：放行凭证只进不退（§14.2），重复写
    被 agentctl 拒（rc=2）→ 已在场即视为已放行（谁写的不影响调用方的观察点）。用于「调度方常驻
    ∧ 需要该任务立刻在途」的场景：手工放行与调度方 tick 抢同一枚凭证，胜负随机器负载漂移
    （runner 侧热路径变快后调度方 tick 更容易抢先，S43④ 实测）。"""
    r = ctl("enable", pid_, "--by", by, expect_rc=None)
    if r.returncode != 0:
        assert enable_doc(pid_) is not None, \
            "放行失败且无 enable.json（rc=%d）：%s" % (r.returncode, r.stderr)
    return r


def notifs_in(inbox_dir, taskid=None):
    """读一个信箱目录中 runner 发的终态通知（from=agentd 的 inform，body=JSON 字符串）；
    传 taskid（路径式）则只回该任务的。返回 [(信封, 载荷)]。分流后两路收件面
    （职位信箱 / bot 型登记方自家信箱）共用本 reader，信封字段逐字同构。"""
    out = []
    for fn in sorted(glob.glob(os.path.join(inbox_dir, "*.msg"))):
        try:
            with open(fn) as f:
                env = json.load(f)
        except ValueError:
            continue
        if not isinstance(env, dict) or env.get("from") != "agentd" \
                or env.get("type") != "inform":
            continue
        try:
            payload = json.loads(env["body"])
        except (ValueError, KeyError):
            continue
        if taskid and payload.get("taskId") != taskid:
            continue
        out.append((env, payload))
    return out


def disp_notifs(taskid=None):
    """职位信箱 agents/queue/dispatcher/inbox/（系统主题 自 bot/dispatcher/
    移族）的终态通知 = 分流后的**回落收件面**（登记方非 bot 型时落此处）。"""
    return notifs_in(proto.position_inbox(ROOT), taskid)


def bot_notifs(name, taskid=None):
    """bot/<名> 自家信箱的终态通知（bot 型 reaper 的直投面）。"""
    return notifs_in(os.path.join(ROOT, "agents", "bot", name, "inbox"), taskid)


# ---------------------------------------------------------------- S1

def s1():
    a = create([])                      # 自动生成名
    # 自动生成名新格式：<rand6>（6 个小写字母+数字随机字符，不带日期；撞名由既有重试逻辑吸收，
    # 2026 用户拍板，覆盖旧格式 MMDD-HHMM-<rand4>）；
    # 消息/控制信封 id 仍为完整时间戳格式，不受影响。
    assert re.fullmatch(r"task/[a-z0-9]{6}", a), \
        "自动生成的参与方 id 应为路径式 task/<rand6>：%r" % a
    wait_until(lambda: (pdoc(a) or {}).get("status") == "running", "S1 running")
    ctl("send", a, "--type", "inform", "--body", "CMD:exit", "--from", "task/tester")
    wait_until(lambda: (pdoc(a) or {}).get("status") == "exited", "S1 exited")
    # 两层谓词的新口径：`create` 缺省不写 `restartPolicy` 键 ⇒ 不自愈，但**非常驻参与方到
    # 代终态即收口**（final 与 restartPolicy 无关；判据 = README「约定赋值与实现口径」，
    # 单测 agentd/test_runner_final.py F1/F4）。中间态「代终态 ∧ 生命周期开放」仍存在，
    # 载体 = 常驻体与 auto（F5/F6 钉）。
    doc = wait_until(lambda: pdoc(a) if (pdoc(a) or {}).get("final") is True else None,
                     "S1 非常驻到代终态即 final")
    assert doc["exitcode"] == 0, doc
    if IS_LINUX:
        assert isinstance(doc.get("procStart"), int), \
            "spawn 应记录进程内核启动时刻 procStart（§4.2 杀纪律）：%r" % doc
    else:
        # 非 Linux 无 /proc：procStart 不可得，档案缺失/None 是正常形态；
        # 身份校验回退裸 kill(pid,0) 探测（proto.pid_identity_ok）
        assert doc.get("procStart") is None, \
            "非 Linux：procStart 应为缺失/None：%r" % doc
    sys.path.insert(0, HERE)
    import proto
    assert proto.gen_terminal(doc) is True, "代终态应为真"
    assert proto.life_terminal(doc) is True, "生命周期终态应为真（缺键 = 不自愈但仍收口）"
    st = ctl("status", a).stdout
    assert "代终态=真" in st and "生命周期终态=真" in st, st
    # 吸收态幂等：已 final 后再 stop → 回执 noop、不再写盘、不得有新代
    #（收尾兼防泄漏：带 enable 的未 final 者会计占调度方全局占位上限，阻塞后续场景如 S23）
    rid = ctl("control", a, "stop", "--from", "task/tester").stdout.strip()
    wait_until(lambda: ack_of(a, rid), "S1 stop ack")
    assert ack_json(a, rid)["outcome"] == "noop", ack_json(a, rid)
    gen = pdoc(a)["gen"]
    time.sleep(0.6)
    assert pdoc(a)["gen"] == gen and pdoc(a)["final"] is True, "final 后不得有新代"


# ---------------------------------------------------------------- S2

def s2():
    a = create(["--name", "s2-auto"], policy="auto")
    wait_until(lambda: (pdoc(a) or {}).get("status") == "running", "S2 running")
    open(os.path.join(adir_of(a),  "crashflag"), "w").close()
    wait_until(lambda: (pdoc(a) or {}).get("restarts", 0) >= 2, "S2 自愈≥2次")
    doc = pdoc(a)
    assert doc["gen"] >= 3 and doc["gen"] == doc["restarts"] + 1, doc
    os.remove(os.path.join(adir_of(a),  "crashflag"))

    def stable_running():
        # 去旗瞬间可能有一代已读过旗仍将退出；要求连续观察窗内同代存活才算稳定
        d = pdoc(a)
        if not d or d["status"] != "running":
            return None
        g = d["gen"]
        time.sleep(0.7)
        d2 = pdoc(a)
        return d2 if d2 and d2["status"] == "running" and d2["gen"] == g else None

    doc = wait_until(stable_running, "S2 去旗后存活（稳定窗）", timeout=12.0)
    time.sleep(0.5)
    assert pdoc(a)["gen"] == doc["gen"] and pdoc(a)["status"] == "running", \
        "去旗后不应再重启"
    ctl("control", a, "stop", "--from", "task/tester")
    wait_until(lambda: (pdoc(a) or {}).get("final") is True, "S2 stop 收尾")


# ---------------------------------------------------------------- S3

def s3():
    """pause 已退役（控制面动作枚举 = stop/restart/clear）：agentctl 客户端拒绝该动作；
    runner 收到盘上遗留的 action=pause 请求 → rejected unknown action（零状态写入，
    任务照常运行，restart 仍可换代）。"""
    a = create(["--name", "s3-nopause"], policy="auto")
    wait_until(lambda: (pdoc(a) or {}).get("status") == "running", "S3 running")
    # 客户端枚举门：agentctl control pause → argparse choices 拒绝（rc=2），零落盘
    ctl("control", a, "pause", "--from", "task/tester", expect_rc=2)
    cdir = os.path.join(adir_of(a), "control")
    assert not os.path.exists(cdir) or \
        not any(f.endswith(".req") for f in os.listdir(cdir)), "客户端拒绝必须零落盘"
    # runner 侧：直接落一枚 action=pause 请求（模拟存量/异构写入）→ rejected unknown action
    import proto
    os.makedirs(cdir, exist_ok=True)
    rid = proto.now_ts() + "-tester-" + proto.rand_suffix()
    proto.atomic_write_json(os.path.join(cdir, rid + ".req"), {
        "id": rid, "from": "task/tester", "ts": proto.now_ts(), "action": "pause"})
    wait_until(lambda: ack_of(a, rid), "S3 pause ack")
    ack = ack_json(a, rid)
    assert ack["outcome"] == "rejected" and "unknown action" in ack["detail"], ack
    d = pdoc(a)
    assert d["status"] == "running" and d.get("final") is not True, \
        "pause 请求不得产生任何状态写入：%r" % d
    # restart 仍可换代（可逆干预路径健在）
    gen = d["gen"]
    rid2 = ctl("control", a, "restart", "--from", "task/tester").stdout.strip()
    wait_until(lambda: ack_of(a, rid2), "S3 restart ack")

    def resumed():
        d2 = pdoc(a)
        return d2 if d2 and d2["status"] == "running" and d2["gen"] == gen + 1 else None
    d3 = wait_until(resumed, "S3 restart 新代")
    assert d3.get("resumed") == gen, "resumed 应指回旧代"
    ctl("control", a, "stop", "--from", "task/tester")
    wait_until(lambda: (pdoc(a) or {}).get("final") is True, "S3 stop 收尾")


# ---------------------------------------------------------------- S4

def s4():
    a = create(["--name", "s4-stop"])
    wait_until(lambda: (pdoc(a) or {}).get("status") == "running", "S4 running")
    rid = ctl("control", a, "stop", "--from", "task/tester").stdout.strip()
    wait_until(lambda: ack_of(a, rid), "S4 stop ack")
    assert ack_json(a, rid)["outcome"] == "applied"
    doc = wait_until(lambda: pdoc(a) if (pdoc(a) or {}).get("final") else None,
                     "S4 final")
    assert doc["status"] == "killed", doc
    import proto
    assert proto.life_terminal(doc) is True
    rid_r = ctl("control", a, "restart", "--from", "task/tester").stdout.strip()
    wait_until(lambda: ack_of(a, rid_r), "S4 restart ack")
    assert ack_json(a, rid_r)["outcome"] == "rejected", ack_json(a, rid_r)
    import proto as _proto
    cdir4 = os.path.join(adir_of(a), "control")
    rid_p = _proto.now_ts() + "-tester-" + _proto.rand_suffix()
    _proto.atomic_write_json(os.path.join(cdir4, rid_p + ".req"), {
        "id": rid_p, "from": "task/tester", "ts": _proto.now_ts(), "action": "pause"})
    wait_until(lambda: ack_of(a, rid_p), "S4 已退役 pause ack")
    assert ack_json(a, rid_p)["outcome"] == "rejected", ack_json(a, rid_p)
    gen = pdoc(a)["gen"]
    time.sleep(0.6)
    assert pdoc(a)["gen"] == gen and pdoc(a)["final"] is True, "final 后不得有新代"


# ---------------------------------------------------------------- S5

def s5():
    a = create(["--name", "s5-ask"])
    wait_until(lambda: (pdoc(a) or {}).get("status") == "running", "S5 running")
    ask_id = ctl("send", a, "--type", "ask", "--body", "ping?", "--from", "bot/tester") \
        .stdout.strip()

    def replied():
        for m in inbox_msgs("bot/tester"):
            if m.get("type") == "reply" and m.get("ref") == ask_id:
                return m
        return None
    rep = wait_until(replied, "S5 reply 到达 tester inbox")
    assert rep["from"] == a, rep   # from = 路径式 id（AGENT_SELF）
    # fakeagent 先写 reply 再写自家 ack（两步无原子性）：等 ack 落盘再断言，别抢跑
    ackp = os.path.join(adir_of(a),  "inbox", "ack", ask_id)
    wait_until(lambda: os.path.exists(ackp) or None, "S5 agent ack 该 ask 落盘")
    assert os.path.exists(ackp), "agent 应已 ack 该 ask"
    n_replies = len([m for m in inbox_msgs("bot/tester") if m.get("ref") == ask_id])
    assert n_replies == 1, "每个 ask 至多一个 reply（幂等）"
    # ack 判重：同 id 消息重投（at-least-once）不得引发二次答复
    src = os.path.join(adir_of(a),  "inbox", ask_id + ".msg")
    tmp = src + ".dup"
    shutil.copy(src, tmp)
    os.remove(src)
    os.rename(tmp, src)
    time.sleep(1.0)
    n2 = len([m for m in inbox_msgs("bot/tester") if m.get("ref") == ask_id])
    assert n2 == 1, "重投同 id 消息被重复处理（ack 判重失效）"
    ctl("control", a, "stop", "--from", "task/tester")
    wait_until(lambda: (pdoc(a) or {}).get("final") is True, "S5 stop 收尾")


# ---------------------------------------------------------------- S6

def s6():
    n1 = create(["--name", "svc.demo"])
    assert n1 == "task/svc.demo", n1
    r = ctl("create", "--name", "svc.demo",
            "--command", "true", "--workdir", ROOT, "--creator", "tester",
            expect_rc=2)
    assert "已存在" in r.stderr
    ctl("control", "task/svc.demo", "stop", "--from", "task/tester")  # 收尾（spawn 前取消）
    wait_until(lambda: (pdoc("task/svc.demo") or {}).get("final") is True,
               "S6 spawn 前 stop 取消")


# ---------------------------------------------------------------- S7

def s7():
    stop_runner()
    start_runner()       # enable.json 门禁内置无开关（--scheduled 已移除）：
    a = create(["--name", "s7-gate"], gate=False)  # runner 自己永不写 enable；无 enable 绝不出现 pid。
    ep = os.path.join(adir_of(a),  "enable.json")
    pp = os.path.join(adir_of(a),  "pid.json")
    time.sleep(1.5)                    # 观察窗：无独立调度器 → 任务排队（去合署证明）
    assert not os.path.exists(ep), "去合署：无调度器时 runner 不得自写 enable"
    assert not os.path.exists(pp), "未放行不得出现 pid.json"
    ctl("enable", a, "--by", "e2e-scheduler")   # 手工/调度方放行后才 spawn（门禁不变量）
    wait_until(lambda: (pdoc(a) or {}).get("status") == "running", "S7 放行后 spawn")
    r = ctl("enable", a, "--by", "e2e-scheduler", expect_rc=2)  # 单调：不可重复写定
    assert "已存在" in r.stderr
    ctl("control", a, "stop", "--from", "task/tester")
    wait_until(lambda: (pdoc(a) or {}).get("final") is True, "S7 stop 收尾")
    stop_runner()
    start_runner()


# ---------------------------------------------------------------- S8

def s8():
    # 它机任务不被本机认领：不预放行（避免带 enable 无 pid 永久计占调度方占位上限）
    a = create(["--name", "s8-foreign"], host="other-machine", gate=False)
    time.sleep(1.0)
    assert pdoc(a) is None, "spec.host 指向它机，本机 runner 不得认领"
    rid = ctl("control", a, "stop", "--from", "task/tester").stdout.strip()
    time.sleep(0.8)
    assert not ack_of(a, rid), "非本机 agent 的 control 不应被消费"
    assert not os.path.exists(os.path.join(adir_of(a),  "pid.json"))


# ---------------------------------------------------------------- S10

def s10():
    """伪造 pid 复用（§5.5 杀纪律 / §11.8）：篡改 pid.json 指向长期存活的无关进程，
    但 (pid,procStart) 身份不匹配——断言：探活判消失（终态路径生效）、
    控制 kill 被拒绝而不伤及无关进程、回执如实。"""
    sys.path.insert(0, HERE)
    import proto as _proto
    import runner as _runner_mod
    # 无关长存进程（「pid 复用受害者」）：命令行不含 ROOT 标记，与测试树无涉
    victim = subprocess.Popen(["sleep", "3600"])
    try:
        vstart = _proto.proc_starttime(victim.pid)
        if vstart is None:
            # 非 Linux 无 /proc：procStart 不可读，用伪值构造「期望身份不匹配」，
            # 语义不变：期望身份非 None 而实际不可验证 → 按消失处理（不伤及无关进程）
            vstart = 999999999

        # 案例 A（探活）：篡改档案指向无关进程 + procStart 不匹配；
        # runner 重启后句柄不在手 → 二元组校验不通过 = 目标消失 → stale 终态，受害者无恙。
        a = create(["--name", "s10-reuse"])
        wait_until(lambda: (pdoc(a) or {}).get("status") == "running", "S10A running")
        stop_runner()
        doc = pdoc(a)
        doc["pid"] = victim.pid
        doc["procStart"] = vstart + 12345   # pid 存在但身份不匹配 = 模拟 pid 已被复用
        with open(os.path.join(adir_of(a),  "pid.json"), "w") as f:
            json.dump(doc, f)
        start_runner()
        d = wait_until(lambda: pdoc(a) if (pdoc(a) or {}).get("status") == "stale"
                       else None, "S10A 探活判消失→stale")
        assert d["exitcode"] == 127, d
        assert victim.poll() is None, "无关进程不得被误伤（探活路径）"
        rid = ctl("control", a, "stop", "--from", "task/tester").stdout.strip()
        wait_until(lambda: ack_of(a, rid), "S10A stop ack")
        assert (pdoc(a) or {}).get("final") is True, "终态后 stop 应置 final 收口"
        assert victim.poll() is None, "无关进程不得被误伤（收尾路径）"
        assert ack_json(a, rid)["outcome"] == "noop", ack_json(a, rid)

        # 案例 B（杀纪律）：篡改身份的 running 档案遭遇 stop → kill 被拒：
        # 不执行盲杀，写「目标已消失」终态（死因不可得→stale 约定）+ 如实回执。
        rootb = os.path.join(TMPBASE, "root-s10")
        kg = "task/s10-killguard"   # 路径式 id
        adir = os.path.join(rootb, "agents", "task", "s10-killguard")
        os.makedirs(os.path.join(adir, "inbox"), exist_ok=True)
        os.makedirs(os.path.join(adir, "control"), exist_ok=True)
        _proto.atomic_write_json(os.path.join(adir, "spec.json"),
                                 {"command": "true", "workdir": rootb,
                                  "creator": "tester"})
        ridb = "s10-forged-stop"
        _proto.atomic_write_json(os.path.join(adir, "control", ridb + ".req"),
                                 {"id": ridb, "from": "task/tester",
                                  "ts": _proto.now_ts(), "action": "stop"})
        _proto.atomic_write_json(os.path.join(adir, "pid.json"),
                                 {"gen": 1, "pid": victim.pid,
                                  "procStart": vstart + 777,
                                  "status": "running",
                                  "startedAt": _proto.now_ts(),
                                  "lastAliveAt": _proto.now_ts(),
                                  "restarts": 0, "final": False})
        r = _runner_mod.Runner(rootb, "e2ehost", "", 0.2)
        r.do_stop(kg, {"id": ridb, "from": "task/tester", "action": "stop"},
                  r.read_pid(kg))
        d2 = r.read_pid(kg)
        assert d2["status"] == "stale" and d2["final"] is True, d2
        with open(os.path.join(adir, "control", "ack", ridb)) as f:
            ack = json.load(f)
        assert ack["outcome"] == "noop", ack
        assert "refused" in ack["detail"] and "pid reused" in ack["detail"], ack
        assert victim.poll() is None, "kill 被拒后无关进程必须仍存活"
        shutil.rmtree(rootb, ignore_errors=True)
    finally:
        try:
            victim.kill()
            victim.wait(timeout=3)
        except Exception:
            pass


# ---------------------------------------------------------------- S11（新增，0.b）

def s11():
    """杀 runner 重启后接手存活子进程不误判 stale（计划 §0.b 判死纪律）。
    杀纪律接手路径：无句柄 → (pid,procStart) 身份校验通过 → 刷心跳，存活进程不得被误判；
    进程真死后（句柄不在手、真死因不可得）按约定判 stale/127（§4.2）。

    场景卡注记（任务 lt4v 攒批）：终态通知含 no_report warn → 优先怀疑任务空跑
    （进程没产出 report.md 即退出）。"""
    a = create(["--name", "s11-takeover"])
    doc = wait_until(lambda: (pdoc(a) or {}).get("status") == "running" and pdoc(a),
                     "S11 running")
    child_pid = doc["pid"]
    alive_before = doc["lastAliveAt"]
    assert pid_alive(child_pid), "子进程应在跑"

    kill_runner_hard()                       # SIGKILL：模拟 runner 崩溃/被杀
    time.sleep(0.5)
    assert pid_alive(child_pid), "runner 被杀不得殃及子进程（setsid 独立会话）"

    start_runner()                           # 重启：句柄不在手，走接手路径
    def taken_over():
        d = pdoc(a)
        if not d or d.get("status") != "running":
            return None
        # 接手后心跳应继续刷新（lastAliveAt 前进），且 pid 不变、进程仍活
        if d["lastAliveAt"] > alive_before and d["pid"] == child_pid \
                and pid_alive(child_pid):
            return d
        return None
    d2 = wait_until(taken_over, "S11 接手存活进程：保持 running、心跳刷新", timeout=6.0)
    time.sleep(1.0)                          # 多轮观察窗：持续不得误判 stale
    d3 = pdoc(a)
    assert d3["status"] == "running" and d3["pid"] == child_pid, \
        "接手后存活进程被误判：%r" % d3
    assert pid_alive(child_pid), "接手路径不得伤害存活进程"

    # 进程真死（CMD:exit）：无句柄探活路径判死，死因不可得 → 按约定 stale/127
    ctl("send", a, "--type", "inform", "--body", "CMD:exit", "--from", "task/tester")
    d4 = wait_until(lambda: pdoc(a) if (pdoc(a) or {}).get("status") == "stale"
                    else None, "S11 进程真死后判死")
    assert d4["exitcode"] == 127, d4
    assert not pid_alive(child_pid), "子进程应已退出"
    time.sleep(0.6)
    assert pdoc(a)["status"] == "stale", "判死后不得复活/改写"
    ctl("control", a, "stop", "--from", "task/tester")   # 收尾收口
    wait_until(lambda: (pdoc(a) or {}).get("final") is True, "S11 stop 收尾")


# ---------------------------------------------------------------- S12（新增）

def s12():
    """FIFO 串行调度骨架（独立调度方，多机阶段 2 去合署后形态）：同一时刻至多放行一个。
    a1（one-shot 短命）先创建先放行；a1 未终结前 a2 排队（无 enable）；
    a1 终结后 a2 自动获 enable 并启动。"""
    stop_runner()
    start_runner()
    start_scheduler()
    try:
        # a1：one-shot，FAKE_MAX_LOOPS=15 → 约 3s 后正常退出即 final（留出 a2 排队观察窗）
        cmd1 = (FAKE_CMD % (os.path.join(HERE, "fakeagent.py"), ROOT)) \
            .replace("python3", "FAKE_MAX_LOOPS=15 python3", 1)
        a1 = create(["--name", "s12-first"], policy="one-shot", command=cmd1, gate=False)
        wait_until(lambda: os.path.exists(os.path.join(adir_of(a1), 
                                                       "enable.json")),
                   "S12 a1 被调度方放行")
        with open(os.path.join(adir_of(a1),  "enable.json")) as f:
            en = json.load(f)
        assert en["by"] == "agentd-scheduler", en
        wait_until(lambda: (pdoc(a1) or {}).get("status") == "running",
                   "S12 a1 running")

        a2 = create(["--name", "s12-second"], gate=False)
        time.sleep(1.0)   # 观察窗：a1 占位期间 a2 必须排队
        assert not os.path.exists(os.path.join(adir_of(a2),  "enable.json")), \
            "串行纪律：已有 running 任务时不得放行第二个"
        assert pdoc(a2) is None, "未放行不得 spawn"

        wait_until(lambda: (pdoc(a1) or {}).get("final") is True,
                   "S12 a1 one-shot 终结", timeout=10.0)
        wait_until(lambda: os.path.exists(os.path.join(adir_of(a2), 
                                                       "enable.json")),
                   "S12 a1 终结后放行 a2（FIFO）", timeout=6.0)
        wait_until(lambda: (pdoc(a2) or {}).get("status") == "running",
                   "S12 a2 running")
        ctl("control", a2, "stop", "--from", "task/tester")
        wait_until(lambda: (pdoc(a2) or {}).get("final") is True, "S12 stop 收尾")
    finally:
        stop_scheduler()
        stop_runner()
        start_runner()


# ---------------------------------------------------------------- S14（新增，T1-5）

def s14():
    """终态通知通道（T1-5）：生命周期终态 → 职位信箱 queue/dispatcher/inbox/ 一条 inform。
    三态文案区分（done/failed/canceled）+ 字段齐备 + 幂等（多轮扫描只发一次）。"""
    # ① 正常完成 → task_done
    a = create(["--name", "s14-done"], policy="one-shot")
    wait_until(lambda: (pdoc(a) or {}).get("status") == "running", "S14 running")
    ctl("send", a, "--type", "inform", "--body", "CMD:exit", "--from", "task/tester")
    wait_until(lambda: (pdoc(a) or {}).get("final") is True, "S14 final")
    hit = wait_until(lambda: disp_notifs(a)[0] if disp_notifs(a) else None,
                     "S14 终态通知到达职位信箱")
    env, payload = hit
    assert env["from"] == "agentd" and env["type"] == "inform", env
    assert payload["event"] == "task_done", payload
    assert payload["taskId"] == a and payload["status"] == "exited" \
        and payload["exitcode"] == 0, payload
    assert payload["dir"] == adir_of(a), payload
    # spec.name 属应用层附加字段（dispatch 写入）；agentctl 建的 spec 无 name →
    # 通知摘要回落 workdir（name/spec 摘要字段在场即可）
    assert payload["name"] == ROOT, payload
    assert "report" not in payload, "无 report.md 时不得带 report 字段"
    assert payload.get("warn") == "no_report", \
        "exit 0 无 report 的 one-shot 任务终态通知应含 no_report warn（P5）：%r" % payload
    time.sleep(1.0)  # 多轮扫描观察窗：不得重复发
    assert len(disp_notifs(a)) == 1, "幂等失效：同一终态多次通知"
    msg_file = os.path.join(proto.position_inbox(ROOT), env["id"] + ".msg")
    assert os.path.exists(msg_file), "文件名应为 <id>.msg"

    # ② 崩溃（exitcode≠0）→ task_failed，report.md 在场则带路径
    b = create(["--name", "s14-fail"], policy="one-shot")
    wait_until(lambda: (pdoc(b) or {}).get("status") == "running", "S14b running")
    with open(os.path.join(adir_of(b),  "report.md"), "w") as f:
        f.write("# 报告\n")
    ctl("send", b, "--type", "inform", "--body", "CMD:crash", "--from", "task/tester")
    wait_until(lambda: (pdoc(b) or {}).get("final") is True, "S14b final")
    _e2, p2 = wait_until(lambda: disp_notifs(b)[0] if disp_notifs(b) else None,
                         "S14b 通知")
    assert p2["event"] == "task_failed" and p2["exitcode"] == 3, p2
    assert p2["report"] == os.path.join(adir_of(b),  "report.md"), p2

    # ③ stop 取消 → task_canceled
    c = create(["--name", "s14-cancel"], policy="one-shot")
    wait_until(lambda: (pdoc(c) or {}).get("status") == "running", "S14c running")
    ctl("control", c, "stop", "--from", "task/tester")
    wait_until(lambda: (pdoc(c) or {}).get("final") is True, "S14c final")
    _e3, p3 = wait_until(lambda: disp_notifs(c)[0] if disp_notifs(c) else None,
                         "S14c 通知")
    assert p3["event"] == "task_canceled" and p3["status"] == "killed", p3

    # ④ exit 0 且有 report.md → task_done 不带 warn（P5 对照：有报告的正常完成）
    d = create(["--name", "s14-done-report"], policy="one-shot")
    wait_until(lambda: (pdoc(d) or {}).get("status") == "running", "S14d running")
    with open(os.path.join(adir_of(d),  "report.md"), "w") as f:
        f.write("# 报告\n")
    ctl("send", d, "--type", "inform", "--body", "CMD:exit", "--from", "task/tester")
    wait_until(lambda: (pdoc(d) or {}).get("final") is True, "S14d final")
    _e4, p4 = wait_until(lambda: disp_notifs(d)[0] if disp_notifs(d) else None,
                         "S14d 通知")
    assert p4["event"] == "task_done" and p4["exitcode"] == 0, p4
    assert p4.get("report") == os.path.join(adir_of(d),  "report.md"), p4
    assert "warn" not in p4, "有 report.md 的正常完成不得带 no_report warn：%r" % p4


# ---------------------------------------------------------------- S15（新增，T1-5）

def s15():
    """判重基于文件而非内存：杀重启 runner 后不得重发既有终态通知。"""
    before = [(e["id"], p["taskId"]) for e, p in disp_notifs()]
    assert before, "S15 前置：应已有终态通知（S14 产物）"
    kill_runner_hard()
    start_runner()
    time.sleep(1.5)  # 多轮扫描观察窗
    after = [(e["id"], p["taskId"]) for e, p in disp_notifs()]
    assert after == before, "runner 重启后重发了终态通知：%r vs %r" % (before, after)


# ---------------------------------------------------------------- S16（新增，T1-6）

def s16():
    """单实例锁 = 每机一份的探活锁文件。
    布局 <root>/agents/run/agentd.<host>.lock（树内、跨机同步可见，每机只写自己那份）；
    JSON 一行 {host, pid, procStart, startedAt, updatedAt}；主循环定期刷 updatedAt。
    ① 双开：活锁在持时第二实例秒退（rc=0），锁不被抢、原实例无恙；
    ② 死锁接管：SIGKILL 后死锁残留，新实例按 (pid,procStart) 校验接管重建；
    ③ 优雅退出（SIGTERM）→ 不删文件（同步契约不传播删除），只停更，
       再启走接管路径重建；④ updatedAt 周期刷新（探活心跳）。"""
    lockfile = os.path.join(ROOT, "agents", "run", "agentd.e2ehost.lock")
    # 锁由 runner 子进程自写自刷（start_runner 只有 0.3s 兜底 sleep）：等它落到当前实例
    wait_file(lockfile, "S16 在跑 runner 应持有探活锁文件（agents/run/ 下）")
    wait_until(lambda: (_lock_doc(lockfile) or {}).get("pid") == RUNNER.pid,
               "S16 锁内容刷到当前 runner pid")
    doc = _lock_doc(lockfile)
    assert doc["host"] == "e2ehost", doc
    assert doc["pid"] == RUNNER.pid, "锁内容应含当前 runner pid：%r" % doc
    assert "procStart" in doc and doc["startedAt"] and doc["updatedAt"], doc

    # ① 双开拒绝：第二活实例启动即退出（不接管活锁）
    r2 = subprocess.Popen(
        [sys.executable, os.path.join(HERE, "runner.py"), "--root", ROOT,
         "--host", "e2ehost", "--interval", INTERVAL,
         "--log-file", os.path.join(TMPBASE, "runner2.log")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    rc = r2.wait(timeout=10)
    assert rc == 0, "双开应退出码 0（对齐旧守护），实得 rc=%d" % rc
    assert RUNNER.poll() is None, "双开不得影响原实例"
    with open(lockfile) as f:
        assert json.loads(f.read())["pid"] == RUNNER.pid, "锁不得被双开者抢走"
    with open(os.path.join(TMPBASE, "runner2.log")) as f:
        assert "已在运行" in f.read(), "双开退出应有日志说明"
    kill_runner_hard()

    # ② 死锁接管：SIGKILL 后锁残留（死 pid），新实例接管并重建锁内容
    assert os.path.exists(lockfile), "SIGKILL 后应残留死锁"
    start_runner()
    assert RUNNER.poll() is None, "死锁应被接管而非拒启"
    wait_until(lambda: (_lock_doc(lockfile) or {}).get("pid") == RUNNER.pid,
               "S16② 接管后锁 pid 更新为新实例")
    content2 = _lock_doc(lockfile)
    assert content2["pid"] == RUNNER.pid, \
        "接管后锁 pid 应更新为新实例：%r" % content2

    # ③ 优雅退出不删文件：SIGTERM → 文件保留（停更即判死依据），再启接管重建
    old_pid = RUNNER.pid
    stop_runner()
    assert os.path.exists(lockfile), "优雅退出不得删除探活锁文件（判死靠新鲜度）"
    with open(lockfile) as f:
        dead_doc = json.loads(f.read())
    assert dead_doc["pid"] == old_pid, "优雅退出后文件应保持死持锁者内容"
    start_runner()
    wait_until(lambda: (_lock_doc(lockfile) or {}).get("pid") == RUNNER.pid,
               "S16③ 再启接管重建（新实例持锁）")
    content3 = _lock_doc(lockfile)
    assert content3["pid"] == RUNNER.pid, "再启应接管重建（新实例持锁）"
    assert RUNNER.poll() is None

    # ④ 探活心跳：updatedAt 周期刷新（等过一个刷新周期）
    u1 = _lock_doc(lockfile)["updatedAt"]
    time.sleep(6.0)
    u2 = _lock_doc(lockfile)["updatedAt"]
    assert u2 != u1, "updatedAt 应周期刷新（%r vs %r）" % (u1, u2)


# ---------------------------------------------------------------- S17（新增，T1-6）

def s17():
    """子进程环境注入（T1-6 防递归）：runner spawn 注入 AGENTD_TASK=1
    （dispatch 检测即拒分发）与既有三件套（AGENT_HOME/AGENT_ROOT/AGENT_SELF）。
    另验第三方 API key 族洗刷：runner 带假 *_API_KEY 启动，
    子任务环境已剥、runner 宿主自身环境保留（洗的是子进程副本）。"""
    a = create(["--name", "s17-env"], policy="one-shot",
               command='echo "$AGENTD_TASK|$AGENT_SELF" > "$AGENT_HOME/envprobe"')
    wait_until(lambda: (pdoc(a) or {}).get("final") is True, "S17 final")
    doc = pdoc(a)
    assert doc["status"] == "exited" and doc["exitcode"] == 0, doc
    with open(os.path.join(adir_of(a),  "envprobe")) as f:
        probe = f.read().strip()
    assert probe == "1|%s" % a, "AGENTD_TASK/AGENT_SELF 注入不符：%r" % probe

    # 第三方 API key 族洗刷：假 key 注入 runner env，验子任务继承面已洗。
    stop_runner()
    fake_keys = {"GEMINI_API_KEY": "e2e-fake-gemini",
                 "DASHSCOPE_API_KEY": "e2e-fake-dashscope",
                 "OPENAI_API_KEY": "e2e-fake-openai",
                 "BRAVE_API_KEY": "e2e-fake-brave"}
    try:
        start_runner(env_extra=fake_keys)
        b = create(["--name", "s17-scrub"], policy="one-shot",
                   command='env | cut -d= -f1 | sort > "$AGENT_HOME/envkeys"')
        wait_until(lambda: (pdoc(b) or {}).get("final") is True, "S17b final")
        doc = pdoc(b)
        assert doc["status"] == "exited" and doc["exitcode"] == 0, doc
        with open(os.path.join(adir_of(b), "envkeys")) as f:
            keys = [l.strip() for l in f if l.strip()]
        leaked = [k for k in keys if k.endswith("_API_KEY")]
        assert not leaked, "子任务环境继承第三方 key：%r" % leaked
        assert "AGENTD_TASK" in keys, "子任务环境缺 AGENTD_TASK（洗刷误伤三件套）"
        # 宿主服务不受影响：runner 自身 environ 保留全部假 key（洗的是子进程副本）
        # 读本进程外的 environ 只有 Linux /proc 一条路：非 Linux 显式 SKIP
        if IS_LINUX:
            with open("/proc/%d/environ" % RUNNER.pid, "rb") as f:
                renv = f.read().decode(errors="replace").split("\0")
            for k, v in fake_keys.items():
                assert "%s=%s" % (k, v) in renv, "runner 宿主 env 丢失 %s" % k
        else:
            platform_skip(
                "S17 runner 宿主 environ 保留假 key",
                "无 /proc/<pid>/environ（Linux-only）；macOS ps -Eww/eww 实测不暴露他进程"
                "环境（SIP），等价替代需 ctypes KERN_PROCARGS2（改动面大、非标准库稳定面）"
                "——故本子断言平台跳过；S17 其余断言（子进程 AGENTD_TASK 注入 + key 族洗刷）"
                "已照常执行并在本平台生效")
    finally:
        stop_runner()
        start_runner()


# ---------------------------------------------------------------- S18（DAG 新增）

def s18():
    """DAG 资源互斥：同资源（gpu）任务互斥串行；异资源（nvme）任务可与 gpu 任务并发。
    g1（gpu，短命）+ n1（nvme）同时 running；g2（gpu）在 g1 占位期间无 enable，
    g1 终结后 g2 放行。"""
    stop_runner()
    try:
        cmd_short = (FAKE_CMD % (os.path.join(HERE, "fakeagent.py"), ROOT)) \
            .replace("python3", "FAKE_MAX_LOOPS=15 python3", 1)
        g1 = create(["--name", "s18-gpu1"], policy="one-shot", command=cmd_short, gate=False)
        set_sched_fields(g1, resources=["gpu"])
        g2 = create(["--name", "s18-gpu2"], gate=False)
        set_sched_fields(g2, resources=["gpu"])
        n1 = create(["--name", "s18-nvme"], gate=False)
        set_sched_fields(n1, resources=["nvme"])
        start_runner()
        start_scheduler()

        wait_until(lambda: (pdoc(g1) or {}).get("status") == "running", "S18 g1 running")
        wait_until(lambda: (pdoc(n1) or {}).get("status") == "running",
                   "S18 n1 与 g1 并发（异资源）")
        assert enable_doc(g2) is None, "同资源互斥：g1 占着 gpu 不得放行 g2"
        assert pdoc(g2) is None, "未放行不得 spawn"

        wait_until(lambda: (pdoc(g1) or {}).get("final") is True,
                   "S18 g1 终结", timeout=10.0)
        wait_until(lambda: enable_doc(g2) is not None,
                   "S18 g1 释放 gpu 后放行 g2", timeout=6.0)
        wait_until(lambda: (pdoc(g2) or {}).get("status") == "running", "S18 g2 running")
        for t in (g2, n1):
            ctl("control", t, "stop", "--from", "task/tester")
            wait_until(lambda t=t: (pdoc(t) or {}).get("final") is True, "S18 收尾 stop")
    finally:
        stop_scheduler()
        stop_runner()
        start_runner()


# ---------------------------------------------------------------- S19（DAG 新增）

def s19():
    """DAG provides/needs（旧系统 evalNeeds 语义）：
    ① 成功链：B needs cap19，A provides cap19——A running 期间 B 无 enable，
       A 成功终态后 B 放行（enable.note 含 dag 依据）；
    ② 失败不放行 + 自动解锁：provider 失败（exit 1）后 B2 持续无 enable，
       再登记成功的 provider，B2 自动解锁放行。
    成功 provider 均写 report.md（完成判定自证件 收紧后为必要条件；
    exit0 无报告 = 空跑不满足依赖，该面由 S43 专测）。"""
    stop_runner()
    try:
        cmd_short = (FAKE_CMD % (os.path.join(HERE, "fakeagent.py"), ROOT)) \
            .replace("python3", "FAKE_MAX_LOOPS=10 python3", 1) \
            + ' && echo "# report" > "$AGENT_HOME/report.md"'
        a = create(["--name", "s19-provider"], policy="one-shot", command=cmd_short, gate=False)
        set_sched_fields(a, provides=["cap19"])
        b = create(["--name", "s19-needer"], gate=False)
        set_sched_fields(b, needs=["cap19"], resources=[])
        start_runner()
        start_scheduler()

        wait_until(lambda: (pdoc(a) or {}).get("status") == "running", "S19 A running")
        time.sleep(0.8)  # 观察窗：provider 未终态期间 B 必须等待（即使无资源冲突）
        assert enable_doc(b) is None, "能力等待：provider 未成功不得放行 B"
        wait_until(lambda: (pdoc(a) or {}).get("final") is True,
                   "S19 A 成功终结", timeout=10.0)
        assert (pdoc(a) or {}).get("exitcode") == 0
        en = wait_until(lambda: enable_doc(b), "S19 A 成功后放行 B", timeout=6.0)
        assert en["by"] == "agentd-scheduler" and "dag" in en.get("note", ""), en
        wait_until(lambda: (pdoc(b) or {}).get("status") == "running", "S19 B running")
        ctl("control", b, "stop", "--from", "task/tester")
        wait_until(lambda: (pdoc(b) or {}).get("final") is True, "S19 收尾 stop B")

        # ② 失败分支：停机登记再启动，避免字段后写的竞态（同一调度部署内续测）
        stop_scheduler()
        stop_runner()
        af = create(["--name", "s19-provider-fail"], policy="one-shot",
                    command="exit 1", gate=False)
        set_sched_fields(af, provides=["cap19f"])
        b2 = create(["--name", "s19-needer-fail"], gate=False)
        set_sched_fields(b2, needs=["cap19f"], resources=[])
        start_runner()
        start_scheduler()
        wait_until(lambda: (pdoc(af) or {}).get("final") is True, "S19 Af 终结")
        assert (pdoc(af) or {}).get("exitcode") == 1
        time.sleep(1.0)  # 观察窗：provider 全失败 → 不可满足，不放行（旧系统 blocked 语义）
        assert enable_doc(b2) is None, "provider 失败不得放行 B2"
        stop_scheduler()
        stop_runner()
        afix = create(["--name", "s19-provider-fix"], policy="one-shot",
                      command='echo "# report" > "$AGENT_HOME/report.md"; exit 0',
                      gate=False)
        set_sched_fields(afix, provides=["cap19f"])
        start_runner()
        start_scheduler()
        wait_until(lambda: (pdoc(afix) or {}).get("final") is True, "S19 Afix 终结")
        wait_until(lambda: enable_doc(b2) is not None,
                   "S19 成功 provider 出现后自动解锁放行 B2", timeout=6.0)
        wait_until(lambda: (pdoc(b2) or {}).get("status") == "running", "S19 B2 running")
        ctl("control", b2, "stop", "--from", "task/tester")
        wait_until(lambda: (pdoc(b2) or {}).get("final") is True, "S19 收尾 stop B2")
    finally:
        stop_scheduler()
        stop_runner()
        start_runner()


# ---------------------------------------------------------------- S20（DAG 新增）

def s20():
    """全局占位上限 + 越位：调度方 --max-concurrent=2 时至多 2 个并发（t1/t2 在跑，
    t3 等位）；被能力阻塞的 t4（排队在 t5 之前）不阻塞后面条件满足的 t5——
    腾出一个位后放行的是 t3（队首），再腾位后越位放行 t5，t4 持续等待。"""
    stop_runner()
    try:
        t1 = create(["--name", "s20-a"], gate=False)
        set_sched_fields(t1, resources=[])
        t2 = create(["--name", "s20-b"], gate=False)
        set_sched_fields(t2, resources=[])
        t3 = create(["--name", "s20-c"], gate=False)
        set_sched_fields(t3, resources=[])
        t4 = create(["--name", "s20-needs"], gate=False)
        set_sched_fields(t4, needs=["cap20"], resources=[])  # 无 provider → 永久等待（观察越位用）
        t5 = create(["--name", "s20-late"], gate=False)
        set_sched_fields(t5, resources=[])
        start_runner()
        start_scheduler(max_concurrent=2)

        wait_until(lambda: (pdoc(t1) or {}).get("status") == "running", "S20 t1 running")
        wait_until(lambda: (pdoc(t2) or {}).get("status") == "running", "S20 t2 running")
        time.sleep(0.8)  # 观察窗：占位 2/2，其余排队；t4 不可满足等待，不阻塞评估也不放行
        assert enable_doc(t3) is None, "占位上限：2/2 时不得放行 t3"
        assert enable_doc(t4) is None, "能力不可满足：不得放行 t4"
        assert enable_doc(t5) is None, "占位上限：2/2 时不得放行 t5"
        ctl("control", t1, "stop", "--from", "task/tester")
        wait_until(lambda: (pdoc(t1) or {}).get("final") is True, "S20 t1 终结腾位")
        wait_until(lambda: enable_doc(t3) is not None,
                   "S20 腾位后放行候补队首 t3", timeout=6.0)
        assert enable_doc(t4) is None, "t4 仍不可满足，不得放行"
        ctl("control", t2, "stop", "--from", "task/tester")
        wait_until(lambda: (pdoc(t2) or {}).get("final") is True, "S20 t2 终结腾位")
        wait_until(lambda: enable_doc(t5) is not None,
                   "S20 越位：t4 阻塞不挡路，放行后面的 t5", timeout=6.0)
        assert enable_doc(t4) is None, "越位后 t4 仍不得放行"
        wait_until(lambda: (pdoc(t5) or {}).get("status") == "running", "S20 t5 running")
        for t in (t3, t5):
            ctl("control", t, "stop", "--from", "task/tester")
            wait_until(lambda t=t: (pdoc(t) or {}).get("final") is True, "S20 收尾 stop")
    finally:
        stop_scheduler()
        stop_runner()
        start_runner()


# ---------------------------------------------------------------- S21（新增）

def s21():
    """接管孤儿的退出归属（：守护重启后旧守护拉起的
    子进程成孤儿，新 runner 非其父不能 waitpid，正常完成也被报成失败）。
    ① 孤儿正常退出且已写 report.md → 按 §5 完成判定通知 task_done（exitcode 记账
       如实 stale/127，载荷附 note 注明归属依据）；
    ② 孤儿消失且无 report.md → 仍通知 task_failed（不误伤真崩溃）。"""
    # ① 有 report.md：守护崩溃→子进程独立完成工作并退出→新 runner 接手判 stale
    a = create(["--name", "s21-orphan-done"], policy="one-shot")
    doc = wait_until(lambda: (pdoc(a) or {}).get("status") == "running" and pdoc(a),
                     "S21a running")
    child_pid = doc["pid"]
    with open(os.path.join(adir_of(a),  "report.md"), "w") as f:
        f.write("# S21 报告\n")
    kill_runner_hard()                       # 守护崩溃：子进程成孤儿（setsid 不株连）
    assert pid_alive(child_pid), "守护被杀不得殃及子进程"
    ctl("send", a, "--type", "inform", "--body", "CMD:exit", "--from", "task/tester")
    wait_until(lambda: not pid_alive(child_pid), "S21a 孤儿正常退出", timeout=10.0)
    start_runner()                           # 新 runner 接手：只能观测「进程消失」
    d = wait_until(lambda: pdoc(a) if (pdoc(a) or {}).get("status") == "stale"
                   else None, "S21a 接手判死")
    assert d["exitcode"] == 127 and d.get("final") is True, \
        "exitcode 记账如实保持 127：%r" % d
    _e, p = wait_until(lambda: disp_notifs(a)[0] if disp_notifs(a) else None,
                       "S21a 终态通知")
    assert p["event"] == "task_done", \
        "有 report.md 的孤儿正常完成不得报失败：%r" % p
    assert p["status"] == "stale" and p["exitcode"] == 127, p
    assert p.get("report") == os.path.join(adir_of(a),  "report.md"), p
    assert p.get("note"), "应附孤儿归属注明：%r" % p

    # ② 无 report.md：孤儿被杀消失 → 真崩溃语义，仍报失败
    b = create(["--name", "s21-orphan-fail"], policy="one-shot")
    doc2 = wait_until(lambda: (pdoc(b) or {}).get("status") == "running" and pdoc(b),
                      "S21b running")
    kill_runner_hard()
    os.kill(doc2["pid"], signal.SIGKILL)     # 模拟孤儿崩溃/被杀（无产出）
    wait_until(lambda: not pid_alive(doc2["pid"]), "S21b 孤儿消失")
    start_runner()
    d2 = wait_until(lambda: pdoc(b) if (pdoc(b) or {}).get("status") == "stale"
                    else None, "S21b 接手判死")
    assert d2["exitcode"] == 127 and d2.get("final") is True, d2
    _e2, p2 = wait_until(lambda: disp_notifs(b)[0] if disp_notifs(b) else None,
                         "S21b 终态通知")
    assert p2["event"] == "task_failed", \
        "无 report.md 的孤儿消失不得洗白为完成：%r" % p2
    assert "report" not in p2 and not p2.get("note"), p2


# ---------------------------------------------------------------- S22（新增）

def s22():
    """登记写 host（多机阶段 0；B-2 修复，设计 §3.2：
    「spec.host 缺省=登记机」在登记时刻物化落盘，认领侧不再解释缺省；
    host-id 映射文件口径）。
    ① 缺省登记（不传 --host）→ host == createdByHost == 映射查表所得规范名；
    ② 显式 --host 他机 → host ≠ createdByHost（createdByHost 仍是登记机），本机不认领；
    ③ 映射存在但无本机条目 → 回退本机 hostname（登记不失败）；
    ④ host-id 缺失 → 回退本机 hostname（登记不失败，本机正常认领至终态）。"""
    envdir = os.path.join(ROOT, "env")
    os.makedirs(envdir, exist_ok=True)
    hostid = os.path.join(envdir, "host-id")
    with open(hostid, "w") as f:
        f.write("# e2e 映射：注释行与无关机器行均不得影响本机查表\n"
                "other-host other-canonical\n"
                "%s e2e-canonical\n" % socket.gethostname())

    # ① 缺省登记：物化规范名（e2e-canonical 不在本机身份集 → 天然不被认领，无副作用）；
    # 不预放行（不认领者带 enable 会计占调度方占位上限，干扰后续调度场景）
    a = create(["--name", "s22-default"], gate=False)
    s = sdoc(a)
    assert s["host"] == "e2e-canonical", s
    assert s["createdByHost"] == "e2e-canonical", s
    time.sleep(0.8)
    assert pdoc(a) is None, "host=e2e-canonical 不在本机身份集，不得认领"

    # ② 显式他机：host≠createdByHost，本机不认领（与 S8 同口径）
    b = create(["--name", "s22-foreign"], host="nv-other", gate=False)
    s2 = sdoc(b)
    assert s2["host"] == "nv-other", s2
    assert s2["createdByHost"] == "e2e-canonical" != s2["host"], s2
    time.sleep(0.8)
    assert pdoc(b) is None, "它机任务不得被本机认领"

    # ③ 映射存在但无本机条目：回退本机 hostname（登记不失败）
    with open(hostid, "w") as f:
        f.write("other-host other-canonical\n")
    u = create(["--name", "s22-unmapped"], policy="one-shot", command="true")
    su = sdoc(u)
    assert su["host"] == socket.gethostname(), su
    assert su["createdByHost"] == su["host"], su
    wait_until(lambda: (pdoc(u) or {}).get("final") is True, "S22 未命中回退后本机认领至终态")

    # ④ host-id 缺失：回退本机 hostname（登记不失败；本机身份含 hostname → 认领至终态）
    os.remove(hostid)
    c = create(["--name", "s22-fallback"], policy="one-shot", command="true")
    s3 = sdoc(c)
    assert s3["host"] == socket.gethostname(), s3
    assert s3["createdByHost"] == s3["host"], s3
    wait_until(lambda: (pdoc(c) or {}).get("final") is True, "S22 回退后本机认领至终态")


# ---------------------------------------------------------------- S23（新增）

def s23():
    """调度器全局视图 --all-hosts（多机阶段 2，设计 §3.4）：
    ① 缺省（非全局）：只放行归本机的候补，host=他机任务不写 enable；
    ② --all-hosts：为他机任务也写 enable（by=agentd-scheduler，调度方本体；
       认领仍归目标机 runner，本机不拉起——S8 同口径）；
    ③ 非全局下本机任务照常放行（默认行为回归证明）。
    判活门禁：他机放行以探活锁新鲜为前提（§4.4/§4.5）——
    伪锁新鲜才放行；无锁的死主机即使 --all-hosts 也不放行（不占位）。"""
    stop_runner()  # 本场景只验调度侧写 enable，无需 runner（也避免干扰）
    here = socket.gethostname()        # proto.local_hosts 自动含本机 hostname（S22 ③ 同口径）
    a = create(["--name", "s23-local"], host=here, gate=False)
    set_sched_fields(a, resources=[])  # 不占 serial：避开前序场景遗留排队者的串行互斥，只验路由维度
    b = create(["--name", "s23-foreign"], host="other-machine", gate=False)
    set_sched_fields(b, resources=[])
    d = create(["--name", "s23-deadhost"], host="dead-machine", gate=False)
    set_sched_fields(d, resources=[])
    tick_scheduler()                   # 非全局 --once：本机任务放行，他机不放行
    assert enable_doc(a) is not None, "非全局：归本机任务应放行（默认行为不变）"
    assert enable_doc(b) is None, "非全局：他机任务不得放行"
    # 判活门禁布景：他机伪锁新鲜（探活锁格式同 runner 心跳，§4.4）；
    # dead-machine 无锁（判死：即使 --all-hosts 也不得放行，且不占位）。
    sys.path.insert(0, HERE)
    import proto
    foreign_lock = os.path.join(ROOT, "agents", "run", "agentd.other-machine.lock")
    _lock_doc = {"host": "other-machine", "pid": 1, "procStart": None,
                 "startedAt": proto.now_ts(), "updatedAt": proto.now_ts()}
    proto.atomic_write_json(foreign_lock, _lock_doc)
    tick_scheduler("--all-hosts")      # 全局视图 --once：锁新鲜的他机任务放行；死主机阻塞
    en = enable_doc(b)
    assert en is not None, "全局视图：探活锁新鲜的他机任务应被放行"
    assert en["by"] == "agentd-scheduler", en
    assert enable_doc(d) is None, "判活门禁：无锁的死主机任务不得放行（即使 --all-hosts）"
    os.unlink(foreign_lock)            # 布景清理（死主机任务留队列，临时树无害）
    # 交叉证明：本机不认领他机任务（无 enable 时 S8 已验；此处有 enable 亦不拉起）
    start_runner()
    time.sleep(1.0)
    assert pdoc(b) is None, "host=他机：本机 runner 不得认领（即使已放行）"
    # b 不归本机：本机不消费其 control/stop（S8 同口径），无法在此收口——
    # 仅存文件（有 enable 无 pid）无害，随临时树清理；只收尾归本机的 a。
    stop_runner()
    start_runner()


# ---------------------------------------------------------------- S30（新增）

def s30():
    """stale 锁回归（评审 3k8n 建议修 A1）：探活锁
    agentd.<host>.lock 存在但 updatedAt 陈旧（超 HOST_ALIVE_THRESHOLD 60s）→ 判死，
    不放行、不占位（口径）；锁刷新恢复新鲜后
    下一轮放行。S23 已固化「新鲜锁放行/无锁判死」，本场景固化中间态「陈旧锁」。
    runner 保持常驻：本机探活锁持续新鲜，本机任务放行不回归（判活集合含本机身份）。"""
    here = socket.gethostname()
    a = create(["--name", "s30-local"], host=here, gate=False)
    set_sched_fields(a, resources=[])
    b = create(["--name", "s30-stale"], host="stale-machine", gate=False)
    set_sched_fields(b, resources=[])
    sys.path.insert(0, HERE)
    import proto
    stale_lock = os.path.join(ROOT, "agents", "run", "agentd.stale-machine.lock")
    # 布景：陈旧锁（格式同 runner 心跳锁，§4.4），updatedAt 回拨 200s（> 阈值 60s）
    old_ts = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime(time.time() - 200))
    _lock_doc = {"host": "stale-machine", "pid": 1, "procStart": None,
                 "startedAt": proto.now_ts(), "updatedAt": old_ts}
    proto.atomic_write_json(stale_lock, _lock_doc)
    tick_scheduler("--all-hosts", "--max-concurrent", "32")
    assert enable_doc(b) is None, "判活门禁：陈旧锁目标机任务不得放行（即使 --all-hosts）"
    assert enable_doc(a) is not None, "本机任务放行不回归（本机身份集合内锁新鲜）"
    # 恢复：updatedAt 刷新 → 下一轮放行（无状态重扫，§14.5）
    _lock_doc["updatedAt"] = proto.now_ts()
    proto.atomic_write_json(stale_lock, _lock_doc)
    tick_scheduler("--all-hosts", "--max-concurrent", "32")
    en = enable_doc(b)
    assert en is not None, "锁恢复新鲜后应放行"
    assert en["by"] == "agentd-scheduler", en
    os.unlink(stale_lock)                  # 布景清理（b 留队列外：有 enable 无 pid 无害，同 S23）
    ctl("control", a, "stop", "--from", "task/tester")
    wait_until(lambda: (pdoc(a) or {}).get("final") is True, "S30 收尾 stop a")


# ---------------------------------------------------------------- S31（新增）

def s31():
    """agentctl --resources/--provides/--needs CLI 解析与拒绝分支（评审 7fbn/3zyd
    建议修 B2）：合法 JSON 字符串数组写入 spec；不传不落盘
    （调度侧按缺省语义）；非法 JSON / 非数组 / 非字符串元素 / 空串一律拒绝（exit 1，
    parse_str_array 各分支）。只验登记侧落盘与拒绝，不起调度/运行。"""
    a = create(["--name", "s31-sched", "--resources", '["gpu","net"]',
                "--provides", '["cap-a"]', "--needs", '[]'], gate=False)
    spec = sdoc(a)
    assert spec["resources"] == ["gpu", "net"], spec
    assert spec["provides"] == ["cap-a"], spec
    assert spec["needs"] == [], spec
    b = create(["--name", "s31-default"], gate=False)
    sb = sdoc(b)
    for k in ("resources", "provides", "needs"):
        assert k not in sb, (k, sb)
    for name, opt, bad in (("s31-badjson", "--resources", "gpu"),
                           ("s31-notlist", "--provides", '{"a":1}'),
                           ("s31-nonstr", "--needs", '["ok", 3]'),
                           ("s31-empty", "--resources", "")):
        ctl("create", "--name", name, "--command", "true", "--workdir", ROOT,
            "--creator", "tester", opt, bad, expect_rc=1)
        assert sdoc(name) is None, "拒绝分支不得落盘 spec：%s" % name


# ---------------------------------------------------------------- S32（bot 布局）

def s32():
    """bot 布局（设计计划 §1/§2）：
    ① 自动名 create（无 --name）落 task/<rand6>/，runner 拉起并终态，
       终态通知 dir = task/ 路径、taskId = 路径式 id；
    ② id 文法：两段路径式合法；拒三段/穿越/空段/裸名（agentctl 层）；
    ③ agent_dir 直落（存在与否不影响路径产出；未知族拒绝）；
    ④ create-bot 落 bot/<名字>/（只建 inbox，无 spec），撞名拒绝（含跨布局），
       信箱型 bot 不入任务扫描面（list 与 runner 监督面）；
    ⑤ 向 bot inbox 写信封（路径式直落）。"""
    import proto as _proto
    # ① 自动名 → task/<id>（路径式 id 全链路）
    n1 = ctl("create", "--command", FAKE_CMD % (os.path.join(HERE, "fakeagent.py"), ROOT),
             "--workdir", ROOT, "--creator", "tester",
             "--restart-policy", "one-shot").stdout.strip()
    assert re.fullmatch(r"[a-z0-9]{6}", n1), "agentctl create 输出二段 name：%r" % n1
    pid1 = "task/" + n1
    assert os.path.isdir(os.path.join(ROOT, "agents", "task", n1)), \
        "自动名创建应落 task/<id>：%s" % n1
    ctl("enable", pid1, "--by", "e2e")
    wait_until(lambda: (pdoc(pid1) or {}).get("status") == "running", "S32 任务 running")
    ctl("send", pid1, "--type", "inform", "--body", "CMD:exit", "--from", "bot/tester")
    wait_until(lambda: (pdoc(pid1) or {}).get("final") is True, "S32 任务终态")
    _e1, p1 = wait_until(lambda: disp_notifs(pid1)[0] if disp_notifs(pid1) else None,
                         "S32 任务终态通知（taskId 路径式）")
    assert p1["taskId"] == pid1, "终态通知 taskId = 路径式 id：%r" % p1
    assert p1["dir"] == os.path.join(ROOT, "agents", "task", n1), p1
    assert p1["event"] == "task_done", p1
    # ② id 文法拒绝面（agentctl 子命令收两段路径式 id，硬切无过渡别名）
    for bad in ("a/b/c", "bot/../x", "task/", "/x", "./x", "unknown/x"):
        r = ctl("status", bad, expect_rc=1)
        assert "非法" in r.stderr, "非法 id 应拒绝：%r" % bad
    r = ctl("status", n1, expect_rc=1)   # 裸名不再接受（硬切）
    assert "路径式" in r.stderr or "非法" in r.stderr, "裸名拒绝：%r" % r.stderr
    # proto 层单点同口径（与扩展 TS 侧单测向量对齐）
    assert _proto.is_valid_participant_id("bot/a.b-c_1") is True
    assert _proto.is_valid_participant_id("task/x1") is True
    for bad in ("a/b/c", "bot/../x", "task/", "/x", "./x", "bot/.",
                "unknown/x", "dispatcher"):
        assert _proto.is_valid_participant_id(bad) is False, bad
    assert _proto.fs_safe_id("task/65ijbb") == "task.65ijbb", "fs_safe_id 单点"
    # ③ agent_dir 直落：存在与否不影响路径产出；未知族抛错（写侧报错口径）
    assert _proto.agent_dir(ROOT, "task/no-such-1") == \
        os.path.join(ROOT, "agents", "task", "no-such-1"), "直落不做存在性试探"
    assert _proto.agent_dir(ROOT, "bot/no-such-2") == \
        os.path.join(ROOT, "agents", "bot", "no-such-2")
    try:
        _proto.agent_dir(ROOT, "unknown/x")
        raise AssertionError("未知族应拒绝")
    except ValueError:
        pass
    # ④ create-bot：信箱型长驻体（只建 inbox，无 spec）
    pn = ctl("create-bot", "--name", "s32-pal").stdout.strip()
    assert pn == "s32-pal"
    pdir = os.path.join(ROOT, "agents", "bot", pn)
    assert os.path.isdir(os.path.join(pdir, "inbox")), "应建 bot/<名>/inbox"
    assert not os.path.exists(os.path.join(pdir, "spec.json")), "信箱型无 spec"
    r = ctl("create-bot", "--name", "s32-pal", expect_rc=2)
    assert "已存在" in r.stderr, "同名拒绝"
    r = ctl("create", "--name", "s32-pal", "--command", "true", "--workdir", ROOT,
            "--creator", "tester", expect_rc=2)
    assert "已存在" in r.stderr, "跨布局撞名拒绝（task create vs bot）"
    r = ctl("create-bot", "--name", "../evil", expect_rc=1)
    assert "非法" in r.stderr, "穿越名拒绝"
    # 登记侧点名收紧（建议项 1；与运行时侧对称：'.' 开头被
    # agents/ 扫描面当隐藏条目跳过、名内连续点使 fsSafeId 后的 ack 命名空间段含 '..'
    # → core.isSafeAckNs 判不安全 → topic 绑定面不启用）：写入口拒 + 文案指明合法命名
    # 规则 + 拒后零副作用（不落目录）；名内单点不误伤（收紧不过度）。
    for bad in (".hidden", "..evil", "a..b"):
        r = ctl("create-bot", "--name", bad, expect_rc=1)
        assert "非法" in r.stderr and "'.' 开头" in r.stderr, (bad, r.stderr)
        assert not os.path.exists(os.path.join(ROOT, "agents", "bot", bad)), \
            "拒后零副作用（create-bot 不落目录）：%r" % bad
        r = ctl("bot", "register", "--name", bad, "--subscribes", "topic/x",
                "--command", "true", "--workdir", ROOT, "--creator", "t", expect_rc=1)
        assert "非法" in r.stderr and "'.' 开头" in r.stderr, (bad, r.stderr)
        assert not os.path.exists(os.path.join(ROOT, "agents", "bot", bad)), \
            "拒后零副作用（bot register 不落目录/spec）：%r" % bad
    assert _proto.is_valid_name_segment(".hidden") is True, \
        "寻址侧文法不收紧（存量病态名仍可寻址/清理，只收写入口）"
    pn_dot = ctl("create-bot", "--name", "s32.a-b_c").stdout.strip()
    assert pn_dot == "s32.a-b_c" and \
        os.path.isdir(os.path.join(ROOT, "agents", "bot", pn_dot, "inbox")), \
        "名内单点（非开头）仍合法：%r" % pn_dot
    # 扫描面：list_participants = task/* ∪ bot/*（路径式 id）；信箱型 bot 不入任务列表/监督面
    parts = _proto.list_participants(ROOT)
    assert pid1 in parts and "bot/s32-pal" in parts, \
        "扫描面应含 task/* ∪ bot/* 路径式 id：%r" % parts
    out = ctl("list").stdout
    assert pid1 in out, "list 应列 task/ 任务（路径式）"
    for line in out.splitlines():
        if line.strip().startswith("bot/s32-pal"):
            assert "inbox-only" in line, "信箱型 bot 状态 = inbox-only（无 spec 不被监督）"
            break
    else:
        raise AssertionError("list 应列信箱型 bot（inbox-only）")
    assert all(sdoc(x) is None for x in parts if x.startswith("bot/")), \
        "信箱型 bot 无 spec（不被 runner/调度器监督）"
    # ⑤ 向 bot inbox 写信封（路径式直落，文件名含 fs_safe_id 转写）
    mid = ctl("send", "bot/" + pn, "--type", "inform", "--body", "hello",
              "--from", "task/s32-sender").stdout.strip()
    assert os.path.exists(os.path.join(pdir, "inbox", mid + ".msg"))
    assert "/" not in os.path.basename(mid), "from 进文件名经 fs_safe_id 转写：%r" % mid
    with open(os.path.join(pdir, "inbox", mid + ".msg")) as f:
        envx = json.load(f)
    assert envx["from"] == "task/s32-sender", "信封内 from 保真路径式（文件名转写不影响内容）"


# ---------------------------------------------------------------- S24（命令形态约定）

def s24():
    """spec.command 命令形态约定：
    ① 新约定：裸命令（含引号/变量展开）经 runner 显式 bash -c 正确执行；
    ② 存量兼容：旧格式字面值（带 bash -c '...'）在新 runner 下为双重 shell，仍正确执行。"""
    # ① 裸命令：引号保护 + 环境变量展开 + 重定向，全部由 bash 语义承担
    a = create(["--name", "s24-bare"], policy="one-shot",
               command='echo "task=$AGENTD_TASK home=$AGENT_HOME" > "$AGENT_HOME/bare-probe"')
    wait_until(lambda: (pdoc(a) or {}).get("final") is True, "S24 bare final")
    doc = pdoc(a)
    assert doc["status"] == "exited" and doc["exitcode"] == 0, doc
    with open(os.path.join(adir_of(a),  "bare-probe")) as f:
        probe = f.read().strip()
    assert probe == "task=1 home=%s" % adir_of(a), probe

    # ② 旧格式存量：bash -c '...' 字面值 → 双重 shell，语义不变
    b = create(["--name", "s24-legacy"], policy="one-shot",
               command="bash -c 'echo \"legacy=$AGENTD_TASK\" > \"$AGENT_HOME/legacy-probe\"'")
    wait_until(lambda: (pdoc(b) or {}).get("final") is True, "S24 legacy final")
    doc = pdoc(b)
    assert doc["status"] == "exited" and doc["exitcode"] == 0, doc
    with open(os.path.join(adir_of(b),  "legacy-probe")) as f:
        probe = f.read().strip()
    assert probe == "legacy=1", probe


# ---------------------------------------------------------------- S25（路径可移植）

def s25():
    """spec 路径跨 home 可移植（agents 树跨机同步、
    各机 $HOME 不同，@agentd#sync-channel）：
    ① 登记侧归一化：agentctl create 在覆写 HOME（模拟登记机）的环境下运行，
       --workdir 位于该 home 内 → 落盘 spec.workdir 为 `~/...`；非 home 绝对路径原样；
    ② 执行侧展开：模拟「登记机 home ≠ 执行机 home」——spec 为它机登记的 `~/...`
       形式，进程内构造 Runner 并覆写 HOME 为执行机 fake home 后 spawn，
       子进程 cwd 落在执行机 home 内（而非登记机路径）；
    ③ 命令引用 `$AGENT_HOME`/`$AGENT_ROOT`/`$AGENT_SELF` 在执行机正确展开。"""
    sys.path.insert(0, HERE)
    import proto as _proto
    import runner as _runner_mod

    # ① 登记侧：--workdir 在覆写 HOME 内 → `~/...`；非 home 路径保持绝对（存量行为不变）
    reg_home = os.path.join(TMPBASE, "s25-reg-home")
    reg_wd = os.path.join(reg_home, "proj")
    os.makedirs(reg_wd, exist_ok=True)
    env = scrub_env()
    env["HOME"] = reg_home
    r = subprocess.run([sys.executable, os.path.join(HERE, "agentctl.py"),
                        "--root", ROOT, "create", "--name", "s25-reg",
                        "--command", "true", "--workdir", reg_wd,
                        "--creator", "tester"],
                       env=env, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    assert sdoc("task/s25-reg")["workdir"] == "~/proj", sdoc("task/s25-reg")
    r2 = subprocess.run([sys.executable, os.path.join(HERE, "agentctl.py"),
                         "--root", ROOT, "create", "--name", "s25-abs",
                         "--command", "true", "--workdir", ROOT,
                         "--creator", "tester"],
                        env=env, capture_output=True, text=True)
    assert r2.returncode == 0, r2.stderr
    assert sdoc("task/s25-abs")["workdir"] == ROOT, sdoc("task/s25-abs")
    for t in ("task/s25-reg", "task/s25-abs"):
        ctl("control", t, "stop", "--from", "task/tester")   # spawn 前取消收口（未放行未启动）
        wait_until(lambda t=t: (pdoc(t) or {}).get("final") is True, "S25 %s 收口" % t)

    # ② + ③ 执行侧：独立小树（直写 `~/...` spec = 模拟它机登记后同步过来），
    # 进程内 Runner + HOME 覆写为执行机 fake home（spawn 的 expanduser 读本进程环境）
    exec_home = os.path.join(TMPBASE, "s25-exec-home")
    exec_wd = os.path.join(exec_home, "proj")
    os.makedirs(exec_wd, exist_ok=True)
    rootb = os.path.join(TMPBASE, "root-s25")
    name = "s25-exec"
    adir = os.path.join(rootb, "agents", "task", name)
    os.makedirs(os.path.join(adir, "inbox"), exist_ok=True)
    _proto.atomic_write_json(
        os.path.join(adir, "spec.json"),
        {"command": 'pwd > "$AGENT_HOME/cwd-probe" && '
                    'echo "root=$AGENT_ROOT self=$AGENT_SELF" > "$AGENT_HOME/env-probe"',
         "workdir": "~/proj",   # 「登记机」（reg_home）登记的 ~/ 形式，跨机同步而来
         "creator": "tester"})
    old_home = os.environ.get("HOME")
    os.environ["HOME"] = exec_home     # 执行机 HOME ≠ 登记机 HOME（本场景核心）
    try:
        rb = _runner_mod.Runner(rootb, "e2ehost", "", 0.2)
        pid_s25 = "task/" + name   # runner 内部一律路径式 id
        rb.spawn(pid_s25, rb.read_spec(pid_s25), 1)
        rc = rb.popen[pid_s25].wait(timeout=10)
    finally:
        if old_home is None:
            os.environ.pop("HOME", None)
        else:
            os.environ["HOME"] = old_home
    try:
        assert rc == 0, "子进程应正常退出（cwd 存在且命令展开正确）：rc=%s" % rc
        with open(os.path.join(adir, "cwd-probe")) as f:
            cwd = f.read().strip()
        # realpath 归一：macOS 的 /var → /private/var 符号链接（子进程 pwd 输出真实路径）
        assert os.path.realpath(cwd) == os.path.realpath(exec_wd), \
            "workdir `~/proj` 应按执行机 HOME 展开：%r（登记机路径=%r）" % (cwd, reg_wd)
        with open(os.path.join(adir, "env-probe")) as f:
            probe = f.read().strip()
        assert probe == "root=%s self=%s" % (rootb, "task/" + name), \
            "命令内 $AGENT_ROOT/$AGENT_SELF 展开不符（AGENT_SELF=路径式）：%r" % probe
    finally:
        shutil.rmtree(rootb, ignore_errors=True)


# ---------------------------------------------------------------- S26（新增，0828-0018）

def s26():
    """无身份档案的存活孤儿接管：
    pid.json procStart 缺失（非 Linux 正常形态 / 遗留档案）+ 进程存活 → 接管路径必须
    回退裸存活探测并接管（保持 running、心跳刷新），不得误判 stale/127；
    进程真死后仍按约定判死（死因不可得 → stale/127）。"""
    a = create(["--name", "s26-noid-takeover"])
    doc = wait_until(lambda: (pdoc(a) or {}).get("status") == "running" and pdoc(a),
                     "S26 running")
    child_pid = doc["pid"]
    assert pid_alive(child_pid), "子进程应在跑"
    kill_runner_hard()                       # 守护崩溃：句柄丢失，子进程成孤儿（setsid 不株连）
    assert pid_alive(child_pid), "runner 被杀不得殃及子进程"
    # 档案抹去身份记录：模拟非 Linux spawn（procStart 恒缺失）/遗留档案（S11 的无身份变体）
    d = pdoc(a)
    d.pop("procStart", None)
    alive_before = d["lastAliveAt"]
    with open(os.path.join(adir_of(a),  "pid.json"), "w") as f:
        json.dump(d, f)
    start_runner()

    def taken_over():
        x = pdoc(a)
        if not x or x.get("status") != "running":
            return None
        # 接管成功 = 保持 running、pid 不变、进程仍活、心跳（lastAliveAt）前进
        if x.get("lastAliveAt", "") > alive_before and x["pid"] == child_pid \
                and pid_alive(child_pid):
            return x
        return None
    wait_until(taken_over, "S26 无身份档案的存活孤儿被接管（心跳刷新）", timeout=6.0)
    time.sleep(1.0)                          # 多轮观察窗：持续不得误判 stale/127
    d2 = pdoc(a)
    assert d2["status"] == "running" and d2["pid"] == child_pid, \
        "无身份记录不得把存活进程误判为死：%r" % d2
    assert pid_alive(child_pid), "接管路径不得伤害存活进程"
    # 进程真死（CMD:exit）：无句柄探活判死，死因不可得 → 按约定 stale/127（§4.2）
    ctl("send", a, "--type", "inform", "--body", "CMD:exit", "--from", "task/tester")
    d3 = wait_until(lambda: pdoc(a) if (pdoc(a) or {}).get("status") == "stale"
                    else None, "S26 进程真死后判死")
    assert d3["exitcode"] == 127, d3
    assert not pid_alive(child_pid), "子进程应已退出"
    ctl("control", a, "stop", "--from", "task/tester")
    wait_until(lambda: (pdoc(a) or {}).get("final") is True, "S26 stop 收尾")


# ---------------------------------------------------------------- S27（新增，0828-0044）

def s27():
    """缺 host 无人认领：
    旧「缺省→本机认领」兜底已废除——缺 host 任务任何机器都不认领，只留告警。
    ① 双伪节点（node-a / node-b）各起真 runner 扫同一小树：均不写 pid.json，
       各自日志有「无人认领」WARNING；本机主 runner 同样不认领；
    ② 调度器（--all-hosts --once）：缺 host 候补永不写 enable，stderr 有告警；
    ③ 登记侧加固：env/host-id 缺失时回退写 hostname 非空 + stderr 告警含补行提示；
    ④ 回归：带 host（=本机身份）任务正常认领至终态（含显式 --host 路径）。"""
    import proto as _proto

    # ① 双伪节点小树：无 host 的 spec + 已放行（证明 enable 也不能唤回认领）
    rootb = os.path.join(TMPBASE, "root-s27")
    adir = os.path.join(rootb, "agents", "task", "s27-nohost")
    os.makedirs(os.path.join(adir, "inbox"), exist_ok=True)
    _proto.atomic_write_json(os.path.join(adir, "spec.json"),
                             {"command": "true", "workdir": rootb,
                              "creator": "tester", "restartPolicy": "one-shot"})
    _proto.atomic_write_json(os.path.join(adir, "enable.json"),
                             {"ts": _proto.now_ts(), "by": "e2e", "note": "s27"})
    for node in ("node-a", "node-b"):
        logf = os.path.join(TMPBASE, "s27-%s.log" % node)
        p = subprocess.Popen(
            [sys.executable, os.path.join(HERE, "runner.py"), "--root", rootb,
             "--host", node, "--interval", "0.2", "--log-file", logf,
             "--log-level", "DEBUG"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(1.0)
        p.send_signal(signal.SIGTERM)
        p.wait(timeout=3)
        assert not os.path.exists(os.path.join(adir, "pid.json")), \
            "缺 host 任务不得被 %s 认领（即使有 enable）" % node
        with open(logf) as f:
            txt = f.read()
        assert "WARNING" in txt and "s27-nohost" in txt and "无人认领" in txt, \
            "%s 日志应有缺 host 告警：%r" % (node, txt[-400:])
    shutil.rmtree(rootb, ignore_errors=True)

    # ①′ 主树同口径：主 runner（e2ehost）也不认领，且日志有告警；随后 stop 收口
    adir2 = os.path.join(ROOT, "agents", "task", "s27-nohost-root")
    os.makedirs(os.path.join(adir2, "inbox"), exist_ok=True)
    _proto.atomic_write_json(os.path.join(adir2, "spec.json"),
                             {"command": "true", "workdir": ROOT,
                              "creator": "tester", "restartPolicy": "one-shot"})
    time.sleep(0.8)
    assert pdoc("task/s27-nohost-root") is None, "主 runner 不得认领缺 host 任务"
    with open(RUNNER_LOG) as f:
        assert "s27-nohost-root" in f.read(), "主 runner 日志应含缺 host 告警"
    # 无人认领的推论：control 请求也无人消费（不写一个字节）——目录留给临时树清理；
    # 现网处置 = 人工补 spec.host 或重新登记（告警文案已提示）

    # ② 调度器：缺 host 候补永不放行（--all-hosts 全局视图，生产形态）
    adir3 = os.path.join(ROOT, "agents", "task", "s27-nohost-sched")
    os.makedirs(os.path.join(adir3, "inbox"), exist_ok=True)
    _proto.atomic_write_json(os.path.join(adir3, "spec.json"),
                             {"command": "true", "workdir": ROOT,
                              "creator": "tester", "restartPolicy": "one-shot",
                              "resources": []})
    r = subprocess.run(
        [sys.executable, os.path.join(HERE, "scheduler.py"), "--root", ROOT,
         "--host", "e2ehost", "--aliases", "e2e-alias", "--once", "--all-hosts"],
        capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    assert enable_doc("task/s27-nohost-sched") is None, "缺 host 任务永不放行"
    assert "s27-nohost-sched" in r.stderr and "无人认领" in r.stderr, \
        "调度器告警缺失：%r" % r.stderr[-400:]

    # ③ 登记侧加固：映射缺失 → 回退 hostname 非空 + 告警含补行提示
    hostid = os.path.join(ROOT, "env", "host-id")
    if os.path.exists(hostid):
        os.remove(hostid)
    r2 = ctl("create", "--name", "s27-unmapped", "--command", "true",
             "--workdir", ROOT, "--creator", "tester", "--restart-policy", "one-shot")
    s = sdoc("task/s27-unmapped")
    assert s["host"] == socket.gethostname() and s["host"], s
    assert s["createdByHost"] == s["host"], s
    assert "env/host-id" in r2.stderr and "补一行" in r2.stderr, r2.stderr
    ctl("enable", "task/s27-unmapped", "--by", "e2e")  # 手工放行（非调度场景口径）

    # ④ 回归：带 host（=本机 hostname，本机身份集成员）正常认领至终态；
    # 显式 --host 本机规范身份（e2ehost）同样正常（不回归证明）
    wait_until(lambda: (pdoc("task/s27-unmapped") or {}).get("final") is True,
               "S27 回退 hostname 后本机正常认领至终态")
    b = create(["--name", "s27-explicit"], host="e2ehost", policy="one-shot",
               command="true")
    wait_until(lambda: (pdoc(b) or {}).get("final") is True, "S27 显式 host 回归")
    assert pdoc(b)["status"] == "exited" and pdoc(b)["exitcode"] == 0, pdoc(b)


# ---------------------------------------------------------------- S29（needs 127 容忍）

def s29():
    """needs provider 127 容忍（用户拍板方案①）：
    ① provider final ∧ exitcode 127 ∧ report.md 在场 → 依赖者放行；
    ② 127 无 report.md → 依赖者不放行（观察窗）；
    ③ 真实非 0 退出码（exit 1）→ 依赖者不放行（回归，口径同 S19②）；
    ④ 真实 exited/127 + report.md 在场 → 不放行（收紧，
       如 shell command not found 真退 127 不得误当接管孤儿放行）；
    ⑤ 取消 killed/137 + report.md 在场 → 不放行（可忽略项 1）。
    直写 pid.json 构造终态，镜像现网 目录形态
    （final=true/status=stale/exitcode=127/report.md 在场）；provider 无 enable，
    runner 不会拉起它们。"""
    stop_runner()
    try:
        def fake_provider(name, cap, exitcode, status="stale", report=False):
            t = create(["--name", name], gate=False)
            set_sched_fields(t, provides=[cap], resources=[])
            with open(os.path.join(adir_of(t),  "pid.json"), "w") as f:
                json.dump({"gen": 1, "pid": 999999, "status": status,
                           "startedAt": "2026-08-28-23-52-08.706",
                           "lastAliveAt": "2026-08-29-00-16-19.682",
                           "restarts": 0, "final": True,
                           "procStart": 281305321, "exitcode": exitcode,
                           "endedAt": "2026-08-29-00-16-20.193"}, f)
            if report:
                with open(os.path.join(adir_of(t),  "report.md"), "w") as f:
                    f.write("# report\n\u2705 全部验收项通过\n")
            return t

        a = fake_provider("s29-p-127ok", "cap29a", 127, report=True)
        b = create(["--name", "s29-n-127ok"], gate=False)
        set_sched_fields(b, needs=["cap29a"], resources=[])
        c = fake_provider("s29-p-127no", "cap29b", 127, report=False)
        d = create(["--name", "s29-n-127no"], gate=False)
        set_sched_fields(d, needs=["cap29b"], resources=[])
        e = fake_provider("s29-p-exit1", "cap29c", 1, status="exited")
        f = create(["--name", "s29-n-exit1"], gate=False)
        set_sched_fields(f, needs=["cap29c"], resources=[])
        g = fake_provider("s29-p-exit127", "cap29d", 127, status="exited", report=True)
        h = create(["--name", "s29-n-exit127"], gate=False)
        set_sched_fields(h, needs=["cap29d"], resources=[])
        i = fake_provider("s29-p-kill137", "cap29e", 137, status="killed", report=True)
        j = create(["--name", "s29-n-kill137"], gate=False)
        set_sched_fields(j, needs=["cap29e"], resources=[])
        assert a and c and e and g and i  # provider 目录就位（无 enable，runner 不拉起）
        start_runner()
        start_scheduler()

        en = wait_until(lambda: enable_doc(b),
                        "S29 127+report.md provider 放行依赖者", timeout=6.0)
        assert en["by"] == "agentd-scheduler" and "dag" in en.get("note", ""), en
        wait_until(lambda: (pdoc(b) or {}).get("status") == "running", "S29 b running")
        ctl("control", b, "stop", "--from", "task/tester")
        wait_until(lambda: (pdoc(b) or {}).get("final") is True, "S29 收尾 stop b")
        time.sleep(1.0)  # 观察窗：②③ 必须持续不放行（调度器多轮重扫均未写 enable）
        assert enable_doc(d) is None, "127 无 report.md 不得放行依赖者"
        assert enable_doc(f) is None, "真实非 0 退出码不得放行依赖者"
        assert enable_doc(h) is None, "真实 exited/127+报告不得误当接管孤儿放行"
        assert enable_doc(j) is None, "取消 killed/137+报告不得放行依赖者"
    finally:
        stop_scheduler()
        stop_runner()
        start_runner()


# ---------------------------------------------------------------- main

# ---------------------------------------------------------------- S33–S34（rpc 封装形态）
#
# wrap = pi-wrap/pi-rpc-wrap.py（拉起 `pi --mode rpc` 的封装脚本，票 hegipc）；
# e2e 以 fakepi_rpc.py 冒充 pi（AGENTD_WRAP_PI_BIN 注入，经环境洗刷名单外透传）。
# runner 视角始终 = 普通命令 + 退出码。

sys.path.insert(0, HERE)
import proto  # noqa: E402

def _wrap_env(fake_mode):
    fakepi = os.path.join(PI_WRAP_DIR, "fakepi_rpc.py")
    os.chmod(fakepi, 0o755)
    return {"AGENTD_WRAP_PI_BIN": fakepi, "FAKE_MODE": fake_mode,
            "AGENTD_WRAP_INIT_TIMEOUT": "15", "AGENTD_WRAP_EXIT_GRACE": "5"}


# ROOT 传入 argv（wrap 忽略之）——与 FAKE_CMD 同款：命令行含 ROOT 标记，
# 收尾 pkill/pgrep -f ROOT 才能看见/清理 wrap 族残留进程（补缺）。
WRAP_CMD = ('echo "e2e fake prompt" > "$AGENT_HOME/prompt.md" && '
            'exec python3 %s %s' % (WRAP, ROOT))
# session 文件仅首代创建
MK_SESS_ONCE = ('mkdir -p "$AGENT_HOME/session" && '
                '{ [ -f "$AGENT_HOME/session/session.jsonl" ] || '
                'echo init > "$AGENT_HOME/session/session.jsonl"; }')


def s33():
    """wrap 任务全链路（runner 视角 = 普通命令）：spawn 写 pid.json.sock →
    sock 在场且 agents/ 树零 socket → 收敛 exited/0 → final → 通知 task_done →
    sock/.pid 清理；result.md 退役（不落）。"""
    stop_runner()
    try:
        start_runner(env_extra=_wrap_env("ok"))
        a = create(["--name", "s33-wrap"], policy="one-shot", command=WRAP_CMD)
        wait_until(lambda: (pdoc(a) or {}).get("status") == "running", "S33 running")
        # sock 文件名用二段 name（路径式 id 取第二段）
        sock_expect = os.path.join(ROOT, "run", "agentd", a.split("/", 1)[1] + ".sock")
        assert pdoc(a).get("sock") == sock_expect, \
            "pid.json 应记 sock 端点：%r" % pdoc(a).get("sock")
        wait_until(lambda: os.path.exists(sock_expect), "S33 sock 在场")
        # wrap 先 bind/listen 再写 .pid 伴生档（pi-rpc-wrap.py）：sock 在场不等于 .pid 已落盘
        wait_file(sock_expect + ".pid", "S33 .pid 伴生档在场")
        assert os.path.exists(sock_expect + ".pid"), "缺 .pid 伴生档"
        # socket 绝不落 agents/ 同步树（用户拍板约束）
        found = subprocess.run(["find", os.path.join(ROOT, "agents"), "-type", "s"],
                               capture_output=True, text=True).stdout.strip()
        assert found == "", "agents/ 树内出现 socket：%s" % found
        wait_until(lambda: (pdoc(a) or {}).get("final") is True,
                   "S33 收敛终态", timeout=30.0)
        doc = pdoc(a)
        assert doc["status"] == "exited" and doc["exitcode"] == 0, doc
        assert not os.path.exists(sock_expect), "终态后 sock 应清理"
        assert not os.path.exists(sock_expect + ".pid"), "终态后 .pid 应清理"
        adir = adir_of(a)
        assert not os.path.exists(os.path.join(adir, "result.md")), \
            "result.md 已退役，不得落盘"
        assert not os.path.exists(os.path.join(adir, "diagnosis.md")), \
            "正常完成不写诊断"
        notifs = wait_until(lambda: [p for _e, p in disp_notifs(a)] or None,
                            "S33 终态通知到达", timeout=6.0)
        assert len(notifs) == 1 and notifs[0]["event"] == "task_done", notifs
    finally:
        stop_runner()
        start_runner()


def s34():
    """runner 重启不杀任务（wrap 自持管道，不依赖 runner stdin）：杀 runner →
    wrap 存活 → 新守护孤儿接管心跳续刷 → control stop 组杀收敛。"""
    stop_runner()
    env_extra = _wrap_env("hang_settle")   # 长活不收敛，观察窗口内不自行退出
    try:
        start_runner(env_extra=env_extra)
        a = create(["--name", "s34-wrap"], policy="one-shot",
                   command=MK_SESS_ONCE + " && " + WRAP_CMD)
        wait_until(lambda: (pdoc(a) or {}).get("status") == "running", "S34 running")
        wrap_pid = pdoc(a)["pid"]
        kill_runner_hard()
        time.sleep(1.0)
        assert pid_alive(wrap_pid), "runner 重启后 wrap 任务应存活（不依赖 runner stdin）"
        start_runner(env_extra=env_extra)
        time.sleep(1.5)
        doc = pdoc(a)
        assert doc["status"] == "running", "接管后应仍 running：%r" % doc
        alive1 = doc["lastAliveAt"]
        time.sleep(1.2)
        assert pdoc(a)["lastAliveAt"] != alive1, "孤儿接管后心跳应续刷"
        ctl("control", a, "stop", "--from", "task/tester")
        wait_until(lambda: (pdoc(a) or {}).get("final") is True, "S34 stop 终态")
        doc = pdoc(a)
        assert doc["status"] == "killed" and doc["exitcode"] == 137, doc
        assert not pid_alive(wrap_pid), "control stop 组杀应覆盖 wrap"
        notifs = wait_until(lambda: [p for _e, p in disp_notifs(a)] or None,
                            "S34 终态通知到达", timeout=6.0)
        assert any(p.get("event") == "task_canceled" for p in notifs), notifs
    finally:
        stop_runner()
        start_runner()


# ---------------------------------------------------------------- S37（新增）

def _register_resident_bot(name, command):
    """登记进程型 bot（与 channel 转发器同款：手工写 spec.json，§2.1）。
    返回路径式 id。host 物化为本 e2e 节点（与 create() 的登记口径一致）。"""
    botdir = os.path.join(ROOT, "agents", "bot", name)
    os.makedirs(os.path.join(botdir, "inbox"), exist_ok=True)
    proto.atomic_write_json(os.path.join(botdir, "spec.json"), {
        "command": command, "workdir": ROOT, "name": "e2e resident " + name,
        "restartPolicy": "auto", "creator": "tester",
        "host": "e2ehost", "createdByHost": "e2ehost"})
    return "bot/" + name


def s37():
    """常驻能力（设计 期 1）：
    a) resident spawn 不注入 AGENTD_TASK（扩展侧三处连锁天然不触发；扩展零改动），
       AGENT_HOME/AGENT_ROOT/AGENT_SELF 三件套照旧；
    c) 调度豁免：槽满也即时放行 + 不计占位/不持互斥资源，普通任务放行不受挤压。"""
    stop_runner()
    try:
        start_runner()
        # 前置清场：把前序场景遗留的一切非终态本机参与方（运行中/已放行未落地/
        # 排队中）全部收口——否则遗留候补会在本场景 tick 时越位抢先放行，污染占位断言。
        #（S37 是末位场景，清场无副作用；他机/缺 host 任务本机不认领，不动）
        for pid_ in proto.list_participants(ROOT):
            d = pdoc(pid_)
            if (d or {}).get("final") is True:
                continue
            specx = sdoc(pid_)
            if specx is None:
                continue  # 信箱型 bot（无 spec）：非进程型，不参与调度面
            if specx and not proto.host_matches(
                    specx, {"e2ehost", "e2e-alias", socket.gethostname()}):
                continue  # 他机/缺 host 任务：本机不认领、不进本机调度占位面，不动
            ctl("control", pid_, "stop", "--from", "task/tester")
            wait_until(lambda: (pdoc(pid_) or {}).get("final") is True,
                       "S37 前置清场 " + pid_, timeout=6.0)

        # ---- a) spawn 环境：无 AGENTD_TASK ----
        a = _register_resident_bot(
            "s37-env",
            'AGENTD_RESIDENT=1 printenv > "$AGENT_HOME/envprobe" && exec sleep 60')
        ctl("enable", a, "--by", "e2e")
        wait_until(lambda: (pdoc(a) or {}).get("status") == "running", "S37a running")
        probe_f = wait_file(os.path.join(adir_of(a), "envprobe"),
                            "S37a envprobe 落盘", settled=True)
        with open(probe_f) as f:
            probe = f.read()
        envmap = dict(line.split("=", 1) for line in probe.splitlines()
                      if "=" in line)
        assert "AGENTD_TASK" not in envmap, \
            "常驻进程不得带 AGENTD_TASK（扩展三处连锁靠此天然不触发）"
        assert envmap.get("AGENT_SELF") == a, envmap.get("AGENT_SELF")
        assert envmap.get("AGENT_HOME") == adir_of(a), envmap.get("AGENT_HOME")
        assert envmap.get("AGENT_ROOT") == ROOT, envmap.get("AGENT_ROOT")
        assert envmap.get("AGENTD_RESIDENT") == "1", "命令内嵌前缀应在场"

        n1 = create(["--name", "s37-n1"], policy="one-shot", command="sleep 60")
        wait_until(lambda: (pdoc(n1) or {}).get("status") == "running", "S37c n1 running")
        r = _register_resident_bot("s37-res", "AGENTD_RESIDENT=1 exec sleep 60")
        tick_scheduler("--max-concurrent", "1")  # n1 已占满 1/1 → 常驻仍即时放行
        wait_until(lambda: os.path.exists(
            os.path.join(ROOT, "agents", "bot", "s37-res", "enable.json")),
            "S37c 常驻槽满放行")
        with open(os.path.join(ROOT, "agents", "bot", "s37-res", "enable.json")) as f:
            assert json.load(f).get("by") == "agentd-scheduler", "放行者应为调度方"
        wait_until(lambda: (pdoc(r) or {}).get("status") == "running", "S37c resident running")
        # 停掉 n1 → 占位面只剩常驻（应不计入）；新普通任务（缺省 serial）应放行：
        # 若常驻计占位则 1/1 拦，若常驻持有缺省资源则 serial 撞 → 两重豁免同证。
        ctl("control", n1, "stop", "--from", "task/tester")
        wait_until(lambda: (pdoc(n1) or {}).get("final") is True, "S37c n1 final")
        n2 = create(["--name", "s37-n2"], gate=False)
        tick_scheduler("--max-concurrent", "1")
        wait_until(lambda: (pdoc(n2) or {}).get("status") == "running",
                   "S37c 普通任务不受常驻挤压")

        # ---- 收尾 ----
        for x in (a, n2, r):
            if (pdoc(x) or {}).get("final") is not True:
                ctl("control", x, "stop", "--from", "task/tester")
        for x in (a, n2, r):
            wait_until(lambda x=x: (pdoc(x) or {}).get("final") is True,
                       "S37 收尾 " + x)
    finally:
        stop_runner()
        start_runner()


# ---------------------------------------------------------------- S38（新增）

def _ack_doc(pid_, rid):
    p = os.path.join(adir_of(pid_), "control", "ack", rid)
    if not os.path.exists(p):
        return None
    with open(p) as f:
        return json.load(f)


def _session_file(pid_):
    return os.path.join(adir_of(pid_), "session", "session.jsonl")


def _backups_of(pid_):
    pat = os.path.join(ROOT, "run", "backup",
                       proto.fs_safe_id(pid_) + "-*.jsonl")
    return sorted(glob.glob(pat))


def s38():
    """control/clear（弃历史换代）：
    a) running（auto bot）→ clear → ack applied、gen+1、session 空白、备份落位可解、
       新代不写 resumed、新进程可正常消费 inbox（对话注入）；
    b) one-shot running → clear → 立即换新代（边界决策：与 restart 同款）；
    d) final → clear → rejected；
    e) spawn 前 clear → noop + 照常首启。"""
    # ---- a) running → clear → 空白新代 ----
    a = _register_resident_bot(
        "s38-auto",
        'mkdir -p "$AGENT_HOME/session" && '
        '{ [ -f "$AGENT_HOME/session/session.jsonl" ] || '
        'echo GEN1-HISTORY > "$AGENT_HOME/session/session.jsonl"; } && '
        'exec ' + FAKE_CMD % (os.path.join(HERE, "fakeagent.py"), ROOT))
    ctl("enable", a, "--by", "e2e")
    wait_until(lambda: (pdoc(a) or {}).get("status") == "running", "S38a running")
    # running 只证明已 spawn：首代会话由子进程自写，读前等落盘（mac 取证：直读撞 FileNotFoundError）
    wait_file(_session_file(a), "S38a 首代会话落盘", settled=True)
    with open(_session_file(a)) as f:
        assert f.read() == "GEN1-HISTORY\n"
    rid = ctl("control", a, "clear", "--from", "task/tester").stdout.strip()
    ack = wait_until(lambda: _ack_doc(a, rid), "S38a clear ack")
    assert ack["outcome"] == "applied", ack
    wait_until(lambda: (pdoc(a) or {}).get("gen") == 2
               and (pdoc(a) or {}).get("status") == "running", "S38a 空白新代")
    doc = pdoc(a)
    assert "resumed" not in doc, "clear 新代空白起步，不得写 resumed：%r" % doc
    assert doc["restarts"] == 1, doc
    assert os.path.getsize(_session_file(a)) == 0, "clear 后 session 应为空"
    baks = _backups_of(a)
    assert len(baks) == 1, "备份应落位 run/backup/：%r" % baks
    with open(baks[0]) as f:
        assert f.read() == "GEN1-HISTORY\n", "备份内容应可解（=清空前全文）"
    # 新代进程可正常对话注入：inform → fakeagent 追加进 session/memory.txt
    ctl("send", a, "--type", "inform", "--body", "hello-gen2", "--from", "task/tester")
    mem = os.path.join(adir_of(a), "session", "memory.txt")
    wait_until(lambda: os.path.exists(mem)
               and "hello-gen2" in open(mem).read(), "S38a 新代收件注入")
    ctl("control", a, "stop", "--from", "task/tester")
    wait_until(lambda: (pdoc(a) or {}).get("final") is True, "S38a 收尾")

    # ---- b) one-shot → clear → 立即换新代 ----
    b = create(["--name", "s38-oneshot"], policy="one-shot",
               command='mkdir -p "$AGENT_HOME/session" && '
                       '{ [ -f "$AGENT_HOME/session/session.jsonl" ] || '
                       'echo H > "$AGENT_HOME/session/session.jsonl"; } && '
                       'exec sleep 60')
    wait_until(lambda: (pdoc(b) or {}).get("status") == "running", "S38b running")
    # clear 前等首代会话落盘：否则 clear 走 noop（无备份）且子进程后写会破坏「新代为空」断言
    wait_file(_session_file(b), "S38b 首代会话落盘", settled=True)
    rid = ctl("control", b, "clear", "--from", "task/tester").stdout.strip()
    wait_until(lambda: (_ack_doc(b, rid) or {}).get("outcome") == "applied",
               "S38b clear ack")
    wait_until(lambda: (pdoc(b) or {}).get("gen") == 2
               and (pdoc(b) or {}).get("status") == "running",
               "S38b one-shot 立即换新代")
    assert os.path.getsize(_session_file(b)) == 0
    assert len(_backups_of(b)) == 1, "S38b 备份落位"
    ctl("control", b, "stop", "--from", "task/tester")
    wait_until(lambda: (pdoc(b) or {}).get("final") is True, "S38b 收尾")

    # ---- d) final → clear → rejected（用已收口的 b） ----
    rid = ctl("control", b, "clear", "--from", "task/tester").stdout.strip()
    ack = wait_until(lambda: _ack_doc(b, rid), "S38d clear ack")
    assert ack["outcome"] == "rejected" and "final" in ack["detail"], ack

    ctl("control", a, "stop", "--from", "task/tester", expect_rc=None)  # 幂等兼容（已 final 回 noop）

    # ---- e) spawn 前 clear → noop + 照常首启 ----
    stop_runner()
    try:
        e = create(["--name", "s38-prespawn"], policy="one-shot",
                   command="exec sleep 60", gate=False)
        rid = ctl("control", e, "clear", "--from", "task/tester").stdout.strip()
        ctl("enable", e, "--by", "e2e")
        start_runner()
        ack = wait_until(lambda: _ack_doc(e, rid), "S38e clear ack")
        assert ack["outcome"] == "noop" and "no session" in ack["detail"], ack
        wait_until(lambda: (pdoc(e) or {}).get("status") == "running"
                   and (pdoc(e) or {}).get("gen") == 1, "S38e 照常首启")
        ctl("control", e, "stop", "--from", "task/tester")
        wait_until(lambda: (pdoc(e) or {}).get("final") is True, "S38e 收尾")
    finally:
        stop_runner()
        start_runner()


# ---------------------------------------------------------------- S39

def s39():
    """topic 协作容器（设计稿 dispatch/docs/design/topic-design.md §8）：
    寻址四族（文法/直落/扫描面隔离）+ agentctl send 投递闭环（目录自动创建、
    信封字段、文件名格式）+ GC 删除清单通道接受 topic/ 与 bot/（bot 不朽铁律
    2026-09-06 经用户拍板移除，；topic/dispatcher 缺省拒删、--force 审计旁路可删）。"""
    import proto
    # a) 寻址：文法 + 直落（无存在性试探，目录不在场也解析）
    assert proto.is_valid_participant_id("topic/t1")
    assert proto.parse_participant_id("topic/t1") == ("topic", "t1")
    adir = proto.agent_dir(ROOT, "topic/t1")
    assert adir == os.path.join(ROOT, "agents", "topic", "t1"), adir
    assert not os.path.exists(adir), "寻址不得试探/创建目录"
    # 非法形态仍拒：穿越/三段/空段/未知族
    for bad in ("topic/../x", "topic/a/b", "topic/", "chan/x", "topic", "topic/."):
        assert not proto.is_valid_participant_id(bad), bad
    # b) 扫描面隔离：topic 族不入 runner/调度监督面
    os.makedirs(adir, exist_ok=True)
    assert not any(p.startswith("topic/") for p in proto.list_participants(ROOT))
    # b2) queue 族（无状态请求处理站：信箱与处理进程同目录）——寻址同款直落，
    #     但**入扫描面**（进程型 ⇒ runner 靠 list_participants 枚举才会拉起/自愈）。
    assert proto.is_valid_participant_id("queue/q1")
    assert proto.parse_participant_id("queue/q1") == ("queue", "q1")
    qdir = proto.agent_dir(ROOT, "queue/q1")
    assert qdir == os.path.join(ROOT, "agents", "queue", "q1"), qdir
    assert proto.queue_inbox(ROOT, "q1") == os.path.join(qdir, "inbox")
    for bad in ("queue/../x", "queue/a/b", "queue/", "queue", "queue/."):
        assert not proto.is_valid_participant_id(bad), bad
    os.makedirs(qdir, exist_ok=True)
    assert "queue/q1" in proto.list_participants(ROOT), \
        "queue 族必须入进程型扫描面（否则 runner 永不拉起它）"
    # 无 spec 的 queue 目录对报表/调度天然无事可做（与信箱型 bot 同机制）
    assert proto.read_json(os.path.join(qdir, "spec.json")) is None
    # c) 投递闭环：agentctl send 直投 topic/<id>/inbox/，目录不存在自动创建
    t2 = "topic/s39-auto"
    mid = ctl("send", t2, "--type", "inform", "--body", "hello topic",
              "--from", "task/tester").stdout.strip()
    inbox2 = os.path.join(ROOT, "agents", "topic", "s39-auto", "inbox")
    assert os.path.isdir(inbox2), "目录不存在应自动创建"
    mfile = os.path.join(inbox2, mid + ".msg")
    assert os.path.exists(mfile), "信封应落盘：%s" % mfile
    # 文件名格式 <ts>-<fsSafeId(from)>-<rand>.msg（ts 含毫秒；from 转写 task/tester → task.tester）
    assert re.match(
        r"^\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.\d{3}-task\.tester-[a-z0-9]{4}\.msg$",
        os.path.basename(mfile)), os.path.basename(mfile)
    with open(mfile) as f:
        env = json.load(f)
    assert env["from"] == "task/tester" and env["type"] == "inform" \
        and env["body"] == "hello topic" and env.get("id") == mid, env
    # 已存在目录再投（不重建）+ 扫描面仍不含 topic
    mid2 = ctl("send", t2, "--type", "inform", "--body", "second",
               "--from", "bot/tester").stdout.strip()
    assert os.path.exists(os.path.join(inbox2, mid2 + ".msg"))
    assert not any(p.startswith("topic/") for p in proto.list_participants(ROOT))
    # d) GC 通道：topic/ 可入删除清单（对齐 task/ 口径）；bot/ 同样可入
    #    （bot 不朽铁律 2026-09-06 经用户拍板移除）；topic/dispatcher
    #    受 PROTECTED_SYSTEM_PATHS 缺省拒删、--force 审计旁路可删
    gc = os.path.join(HERE, os.pardir, "agents-sync", "gc.py")
    gcenv = dict(scrub_env(), GC_WORKSPACE=ROOT,
                 GC_AGENTS_DIR=os.path.join(ROOT, "agents"))
    r = subprocess.run([sys.executable, gc, "add", "topic/s39-auto/"],
                       capture_output=True, text=True, env=gcenv)
    assert r.returncode == 0, "gc add topic/ 应接受：%s%s" % (r.stdout, r.stderr)
    lists = glob.glob(os.path.join(ROOT, "agents", "gc", "delete-list.*"))
    assert lists, "删除清单应落盘"
    with open(sorted(lists)[-1]) as f:
        assert "topic/s39-auto/" in f.read()
    rb = subprocess.run([sys.executable, gc, "add", "bot/tester/"],
                        capture_output=True, text=True, env=gcenv)
    assert rb.returncode == 0, \
        "bot/ 铁律已移除：应接受：%s%s" % (rb.stdout, rb.stderr)
    lists = glob.glob(os.path.join(ROOT, "agents", "gc", "delete-list.*"))
    with open(sorted(lists)[-1]) as f:
        body = f.read()
    assert "bot/tester/" in body and any(
        ln == "bot/tester/" for ln in body.splitlines()), \
        "bot/ 条目无标记入清单（非旁路，无需审计标记）：" + body
    # PROTECTED_SYSTEM_PATHS 三档：缺省拒 / --force 审计旁路放行 / 形状护栏连 --force 仍拒
    for prot in ("topic/dispatcher/", "queue/dispatcher/"):
        # a) 缺省（不带 --force）：拒
        rn = subprocess.run([sys.executable, gc, "add", prot],
                            capture_output=True, text=True, env=gcenv)
        assert rn.returncode != 0, \
            "%s 缺省应拒（PROTECTED_SYSTEM_PATHS 权限类）：%s%s" % (
                prot, rn.stdout, rn.stderr)
    for prot in ("topic/dispatcher/", "queue/dispatcher/"):
        # b) --force 审计旁路：放行
        rp = subprocess.run([sys.executable, gc, "add", prot,
                             "--force", "--force-by", "s39"],
                            capture_output=True, text=True, env=gcenv)
        assert rp.returncode == 0, \
            "%s --force 审计旁路应放行：%s%s" % (prot, rp.stdout, rp.stderr)
    # 验证清单条目带 #FORCED: 审计标记且审计值含 s39
    lists = glob.glob(os.path.join(ROOT, "agents", "gc", "delete-list.*"))
    assert lists, "删除清单应落盘"
    all_lines = []
    for lp in sorted(lists):
        with open(lp) as f:
            all_lines.extend(f.read().splitlines())
    for prot in ("topic/dispatcher/", "queue/dispatcher/"):
        marked = [ln for ln in all_lines
                  if ln.startswith(prot) and " #FORCED:" in ln]
        assert marked, \
            "%s 条目应带 #FORCED: 审计标记" % prot
        assert any("s39" in ln for ln in marked), \
            "%s 审计值应含 s39：%s" % (prot, marked)
    # c) 形状护栏类（裸族级容器）：连 --force 仍拒
    rs = subprocess.run([sys.executable, gc, "add", "queue/",
                         "--force", "--force-by", "s39"],
                        capture_output=True, text=True, env=gcenv)
    assert rs.returncode != 0, \
        "裸 queue/（形状护栏 BARE_FAMILY_ENTRIES）连 --force 仍拒：%s%s" % (
            rs.stdout, rs.stderr)
    # 清理：临时树内直接 rmtree（非生产 agents/ 网格，e2e 收尾整体删除）
    shutil.rmtree(os.path.join(ROOT, "agents", "topic"), ignore_errors=True)
    shutil.rmtree(os.path.join(ROOT, "agents", "gc"), ignore_errors=True)
    shutil.rmtree(os.path.join(ROOT, "run", "gc"), ignore_errors=True)


def s40():
    """agentctl 脚手架（设计稿 topic-design.md §7/§8 + 协议 §4.1）：
    ① `topic init` 建标准布局（topic.md 骨架：frontmatter when: 占位 + 议题/已决/未决；
       inbox/；watcher/）+ `--watcher <裸名>` 通道 A 订阅登记（存在即订阅）；
    ② 拒绝面：已存在目录/非法段/保留名/跨布局撞名；
    ③ `bot register --subscribes`：通道 B 写入口——新建 spec（去重、只收 topic/ 族）与
       既在场 spec 只改 subscribes（其余字段逐字保留），空串 = 显式清空（删字段）；
    ④ report.py 主题标题跳过 frontmatter（骨架不污染 task tab）；
    ⑤ report.py 主题节「主持人」列（判据 = report.topic_hosts）：宽口径
       通道 B（spec.subscribes）∨ 通道 A（watcher 条目 + bot 目录在场）∧ 会话型 bot；
       链接 host 取 spec.host（跨机前缀）；脚本型/信箱型 bot 不出链；主持人裸名从
       watcher/owner 列剔重（剔空 → `-`）；系统主题（POSITION_TOPIC）恒 `-`。"""
    import proto as _proto
    import report as _report
    tid = "s40-mtg"
    tdir = os.path.join(ROOT, "agents", "topic", tid)
    # ① 标准布局 + watcher 登记（可重复）
    out = ctl("topic", "init", tid, "--title", "S40 演示议题",
              "--watcher", "s40-mod", "--watcher", "s40-pal", "--watcher", "s40-mod").stdout
    assert out.splitlines()[0] == "topic/" + tid, out
    assert os.path.isdir(os.path.join(tdir, "inbox")), "inbox/ 应在场"
    assert os.path.isdir(os.path.join(tdir, "watcher")), "watcher/ 应在场"
    ws = sorted(os.listdir(os.path.join(tdir, "watcher")))
    assert ws == ["s40-mod", "s40-pal"], "订阅条目（去重）：%r" % ws
    with open(os.path.join(tdir, "topic.md"), encoding="utf-8") as f:
        md = f.read()
    assert md.startswith("---\n") and "\nwhen:" in md.split("---")[1], \
        "骨架应带 frontmatter when: 占位"
    for sec in ("## 议题", "## 已决", "## 未决"):
        assert sec in md, "骨架缺小节 %s" % sec
    assert "agents-sync/gc.py" in md and "agents-sync/gc.py" in out, \
        "骨架/回执应注明删除铁律走 gc 通道"
    # ④ 标题提取跳过 frontmatter（首行 `---` 不当标题）
    assert _report.topic_title(tdir) == "S40 演示议题", _report.topic_title(tdir)
    # topic 族仍不入扫描面（布局创建不改变监督面）
    assert not any(p.startswith("topic/") for p in _proto.list_participants(ROOT))
    # ② 拒绝面
    r = ctl("topic", "init", tid, expect_rc=2)
    assert "已存在" in r.stderr, r.stderr
    for bad, rc in (("../evil", 1), ("a*b", 1), ("", 1), ("task", 2), ("bot", 2),
                    (".hidden", 1), ("a..b", 1)):   # 末二 = 登记侧点名收紧
        r = ctl("topic", "init", bad, expect_rc=rc)
        assert "非法" in r.stderr or "保留名" in r.stderr, (bad, r.stderr)
    for bad in (".hidden", "a..b"):
        assert not os.path.exists(os.path.join(ROOT, "agents", "topic", bad)), \
            "topic id 收紧拒后零副作用：%r" % bad
    r = ctl("topic", "init", "s40-x", "--watcher", "../evil", expect_rc=1)
    assert "非法" in r.stderr, "watcher 裸名同走段白名单：%r" % r.stderr
    assert not os.path.exists(os.path.join(ROOT, "agents", "topic", "s40-x")), \
        "watcher 名非法 → 整个 init 不落盘（先校后建）"
    r = ctl("topic", "init", "s40-x", "--watcher", ".hidden", expect_rc=1)
    assert "非法" in r.stderr and "'.' 开头" in r.stderr, \
        "watcher 名同走登记侧收紧（ack 命名空间段对称）：%r" % r.stderr
    assert not os.path.exists(os.path.join(ROOT, "agents", "topic", "s40-x")), \
        "watcher 名被收紧拒绝 → 整个 init 不落盘"
    ctl("create-bot", "--name", "s40-pal")      # 同名 bot 已在场 → topic init 应拒
    r = ctl("topic", "init", "s40-pal", expect_rc=2)
    assert "已存在" in r.stderr, "跨布局撞名（bot 同名已在场）应拒"
    # ③ bot register：新建 spec（通道 B 写入口）
    subs = "topic/%s,topic/%s,topic/s40-other" % (tid, tid)
    out = ctl("bot", "register", "--name", "s40-mod", "--subscribes", subs,
              "--command", "true", "--workdir", ROOT, "--creator", "task/s40",
              "--restart-policy", "auto").stdout
    bspec = os.path.join(ROOT, "agents", "bot", "s40-mod", "spec.json")
    assert os.path.isfile(bspec), out
    with open(bspec, encoding="utf-8") as f:
        doc = json.load(f)
    assert doc["subscribes"] == ["topic/%s" % tid, "topic/s40-other"], \
        "去重保序：%r" % doc["subscribes"]
    assert doc["command"] == "true" and doc["restartPolicy"] == "auto" \
        and doc["creator"] == "task/s40" and doc.get("host"), doc
    assert os.path.isdir(os.path.join(ROOT, "agents", "bot", "s40-mod", "inbox"))
    # 既在场 spec：只改 subscribes，其余字段逐字保留
    before = dict(doc)
    ctl("bot", "register", "--name", "s40-mod", "--subscribes", "topic/s40-only")
    with open(bspec, encoding="utf-8") as f:
        doc2 = json.load(f)
    assert doc2["subscribes"] == ["topic/s40-only"], doc2
    assert all(doc2.get(k) == v for k, v in before.items() if k != "subscribes"), \
        "其余字段不得变：%r vs %r" % (before, doc2)
    # 空串 = 显式清空（删字段 → 回到「字段缺失 = 零声明」现状）
    ctl("bot", "register", "--name", "s40-mod", "--subscribes", "")
    with open(bspec, encoding="utf-8") as f:
        doc3 = json.load(f)
    assert "subscribes" not in doc3, doc3
    assert doc3["command"] == "true", "清空只动 subscribes：%r" % doc3
    # 拒绝面：他族绑定（防抢收）/非法 id/既在场无 --subscribes/新建缺三件/跨布局撞名
    for bad in ("bot/dev-dispatcher", "task/x", "topic/../x", "nope", "topic/"):
        r = ctl("bot", "register", "--name", "s40-mod", "--subscribes", bad,
                expect_rc=1)
        assert ("只有 topic/" in r.stderr) or ("非法订阅项" in r.stderr), (bad, r.stderr)
    with open(bspec, encoding="utf-8") as f:
        assert "subscribes" not in json.load(f), "拒绝后不得落盘"
    r = ctl("bot", "register", "--name", "s40-mod", expect_rc=2)
    assert "只改 subscribes" in r.stderr, r.stderr
    r = ctl("bot", "register", "--name", "s40-new", "--subscribes", "topic/x",
            expect_rc=2)
    assert "--command" in r.stderr, "新建 spec 需三件：%r" % r.stderr
    assert not os.path.exists(os.path.join(ROOT, "agents", "bot", "s40-new", "spec.json"))
    r = ctl("bot", "register", "--name", "../evil", expect_rc=1)
    assert "非法" in r.stderr, r.stderr
    r = ctl("bot", "register", "--name", ".hidden", expect_rc=1)
    assert "非法" in r.stderr and "'.' 开头" in r.stderr, \
        "bot register 既在场分支也走登记侧收紧：%r" % r.stderr
    # 跨布局撞名（同名 task 已在场）应拒，口径同 create-bot（§2.2）
    ctl("create", "--name", "s40-task", "--command", "true", "--workdir", ROOT,
        "--creator", "t")
    r = ctl("bot", "register", "--name", "s40-task", "--subscribes", "topic/x",
            "--command", "true", "--workdir", ROOT, "--creator", "t", expect_rc=2)
    assert "已存在" in r.stderr, "跨布局撞名（task 同名）应拒：%r" % r.stderr
    r = ctl("bot", "register", "--name", "task", expect_rc=2)
    assert "保留名" in r.stderr, r.stderr
    # 信箱型 create-bot 行为零回归（不写 spec）
    assert not os.path.exists(os.path.join(ROOT, "agents", "bot", "s40-pal", "spec.json"))

    # ④ --description / --reaper（§4.1 的两个可选 spec 字段：`name` = 人类可读描述、
    #    `reaper` = 终态通知唯一收件面）
    ctl("bot", "register", "--name", "s40-desc", "--subscribes", "topic/s40-unrelated",
        "--command", "true", "--workdir", ROOT, "--creator", "task/s40",
        "--restart-policy", "auto",
        "--description", "e2e 描述行", "--reaper", "bot/s40-mod")
    dspec = os.path.join(ROOT, "agents", "bot", "s40-desc", "spec.json")
    with open(dspec, encoding="utf-8") as f:
        ddoc = json.load(f)
    assert ddoc["name"] == "e2e 描述行", "--description → spec `name`：%r" % ddoc
    assert ddoc["reaper"] == "bot/s40-mod", "--reaper → spec `reaper`：%r" % ddoc
    assert ddoc["subscribes"] == ["topic/s40-unrelated"] and ddoc["restartPolicy"] == "auto", ddoc
    # 缺省：两键均不在场（新建 spec 与现行为逐字一致，零回归）
    ctl("bot", "register", "--name", "s40-nodesc", "--command", "true",
        "--workdir", ROOT, "--creator", "task/s40")
    with open(os.path.join(ROOT, "agents", "bot", "s40-nodesc", "spec.json"),
              encoding="utf-8") as f:
        ndoc = json.load(f)
    assert "name" not in ndoc and "reaper" not in ndoc, \
        "缺省两 flag → 两键均不在场：%r" % ndoc
    assert ndoc["command"] == "true" and ndoc["creator"] == "task/s40", ndoc
    # 非法 reaper → rc≠0 且零落盘（校验先于 makedirs，口径同 parse_subscribes 的拒后零副作用）
    # 文法判据 = proto.is_valid_participant_id（寻址单点）：裸名/未知族/三段/含 `..`/空 name 均拒
    for badr in ("foo", "nope/x", "topic/../x", "task/a/b", "topic/", "topic/.."):
        r = ctl("bot", "register", "--name", "s40-badreaper", "--command", "true",
                "--workdir", ROOT, "--creator", "task/s40", "--reaper", badr,
                expect_rc=1)
        assert "非法 reaper" in r.stderr, (badr, r.stderr)
    assert not os.path.exists(os.path.join(ROOT, "agents", "bot", "s40-badreaper")), \
        "非法 reaper 拒后不得落目录"
    # 既在场分支：两 flag 不生效（只改 subscribes），其余字段逐字保留
    ctl("bot", "register", "--name", "s40-desc", "--subscribes", "topic/s40-only2",
        "--description", "应被忽略", "--reaper", "task/ignored")
    with open(dspec, encoding="utf-8") as f:
        ddoc2 = json.load(f)
    assert ddoc2["name"] == "e2e 描述行" and ddoc2["reaper"] == "bot/s40-mod", \
        "既在场分支不得改 description/reaper：%r" % ddoc2
    assert ddoc2["subscribes"] == ["topic/s40-only2"], ddoc2

    # ⑤ 主题节「主持人」列（合成 fixture，临时树内；不读现网真实信箱/topic.md 正文）
    sess_cmd = ("DISPATCH_PROFILE=moderator AGENTD_RESIDENT=1 exec python3 "
                "\"$AGENT_ROOT/pi-wrap/pi-rpc-wrap.py\"")
    ctl("bot", "register", "--name", "s40-host", "--subscribes", "topic/" + tid,
        "--command", sess_cmd, "--workdir", ROOT, "--creator", "task/s40",
        "--host", "mac")          # 通道 B + 他机 host（链接前缀跨机正确性）
    ctl("bot", "register", "--name", "s40-wonly", "--command", sess_cmd,
        "--workdir", ROOT, "--creator", "task/s40", "--host", "nv1")
    ctl("bot", "register", "--name", "s40-script", "--subscribes", "topic/" + tid,
        "--command", "exec bash test-fixture-loop.sh", "--workdir", ROOT,
        "--creator", "task/s40", "--host", "dev")   # 脚本型：订阅也不出链
    for nm in ("s40-wonly", "s40-script", "s40-host"):
        open(os.path.join(tdir, "watcher", nm), "w").close()   # 通道 A 条目（含重名者）
    ctl("topic", "init", "s40-solo", "--title", "无主持人议题")   # 零订阅 → `-`
    ctl("topic", "init", "s40-onlyhost", "--title", "仅主持人订阅",
        "--watcher", "s40-wonly")   # watcher 唯一条目即主持人 → 剔重后 `-`
    # 合成形态（机制面仍支持）：系统主题的 watcher 条目绑定一枚会话型 bot ⇒ 报表判出主持人
    sysdir = os.path.join(ROOT, "agents", "topic", _proto.POSITION_TOPIC)
    os.makedirs(os.path.join(sysdir, "watcher"), exist_ok=True)
    open(os.path.join(sysdir, "watcher", "dev-dispatcher"), "w").close()
    ctl("bot", "register", "--name", "dev-dispatcher", "--command", sess_cmd,
        "--workdir", ROOT, "--creator", "task/s40", "--host", "dev")

    rtext, _rtasks = _report.build_report(ROOT, time.time())
    tsec = _report.extract_section(rtext, "topics").splitlines()
    assert tsec[0] == "| topic | 主持人 | 标题 | 消息数 | 最新消息 | watcher/owner |", \
        "主题表列序/表头：%r" % tsec[0]
    assert tsec[1] == "|---|---|---|---|---|---|", tsec[1]
    rows = {}
    for ln in tsec[2:]:
        cells = [c.strip() for c in ln.strip().strip("|").split("|")]
        if len(cells) == 6 and cells[0].startswith("`"):
            rows[cells[0].strip("`")] = cells
    h = rows[tid]
    assert "[s40-host](/mac/agents/bot/s40-host/spec.json?v=chat)" in h[1], \
        "通道 B 主持人应成链，host 取 spec.host（跨机）：%r" % h[1]
    assert "[s40-wonly](/nv1/agents/bot/s40-wonly/spec.json?v=chat)" in h[1], \
        "仅通道 A（watcher 条目 + 会话型 bot）也算主持人：%r" % h[1]
    assert "s40-script" not in h[1] and "s40-script" in h[5], \
        "脚本型 bot（无 pi-rpc-wrap）不出链，留在 watcher 列：%r" % h
    assert "s40-mod" not in h[1] and "s40-pal" not in h[1], \
        "非会话型（command=true）与信箱型（无 spec）bot 不出链：%r" % h[1]
    assert "s40-host" not in h[5] and "s40-wonly" not in h[5], \
        "已单列为主持人的裸名应从 watcher/owner 列剔除：%r" % h[5]
    assert h[5] == "s40-mod, s40-pal, s40-script", h[5]
    assert rows["s40-solo"][1] == "-" and rows["s40-solo"][5] == "-", \
        "零主持人/零 watcher → 纯文本 -：%r" % rows["s40-solo"]
    oh = rows["s40-onlyhost"]
    assert oh[1] == "[s40-wonly](/nv1/agents/bot/s40-wonly/spec.json?v=chat)", oh[1]
    assert oh[5] == "-", "watcher 唯一条目剔重后应为 -：%r" % oh[5]
    sy = rows[_proto.POSITION_TOPIC]
    assert sy[1] == "-", \
        "系统主题无策展 owner：watcher 条目 + 会话型 bot 齐备也不得给链：%r" % sy[1]
    assert "dev-dispatcher" in sy[5], \
        "豁免只作用于主持人列，watcher 登记照常呈现：%r" % sy[5]

    # 清理：临时树内直接 rmtree（e2e 收尾整体删除；生产 agents/ 走 gc 通道）
    shutil.rmtree(os.path.join(ROOT, "agents", "topic"), ignore_errors=True)
    for nm in ("s40-mod", "s40-pal", "s40-host", "s40-wonly", "s40-script",
               "dev-dispatcher", "s40-desc", "s40-nodesc"):
        shutil.rmtree(os.path.join(ROOT, "agents", "bot", nm), ignore_errors=True)
    shutil.rmtree(os.path.join(ROOT, "agents", "task", "s40-task"), ignore_errors=True)


def s41():
    """退役地址护栏：Python 侧与 TS 侧
    `core.RETIRED_MAILBOXES`（core.ts）同源同口径——agentctl **接受路径式地址且会写盘**
    的四个子命令（send/ack/control/enable）命中退役地址一律拒绝 + 回执新址，且拒绝发生在
    任何路径计算/落盘**之前**（不重建无人消费的僵尸信箱）；护栏纯查表、独立于目录是否在
    场（旧目录 gc 删除后仍拦得住）；正常地址（queue/dispatcher、task/<id>、bot/<名>）与
    `require_pid` 的文法校验语义零回归。"""
    import proto as _proto
    bdir = os.path.join(ROOT, "agents", "bot", "dispatcher")
    inbox = os.path.join(bdir, "inbox")
    # ① 退役表同源（键 = 退役的路径式地址，值 = 新地址；单点声明见 proto.py 注释）
    assert _proto.RETIRED_MAILBOXES == {
        _proto.BOT_DIR + "/" + _proto.POSITION_TOPIC: _proto.POSITION_PID,
        _proto.TOPIC_DIR + "/" + _proto.POSITION_TOPIC: _proto.POSITION_PID,
        _proto.BOT_DIR + "/notify-user": _proto.QUEUE_DIR + "/notify-user",
        _proto.BOT_DIR + "/work-lead": _proto.QUEUE_DIR + "/work-lead",
        _proto.BOT_DIR + "/work-lead-watcher": _proto.QUEUE_DIR + "/work-lead",
        _proto.QUEUE_DIR + "/agentfw-lead": _proto.POSITION_PID,
        _proto.BOT_DIR + "/agentfw-lead": _proto.POSITION_PID,
        _proto.BOT_DIR + "/agentfw-lead-watcher": _proto.POSITION_PID}, \
        "退役表须与 core.ts RETIRED_MAILBOXES 同口径（TS 侧字面量钉在扩展单测）：%r" % _proto.RETIRED_MAILBOXES
    # ①b 系统主题豁免名单同源（攒批 5）：report.py 走 proto.PROTECTED_SYSTEM_*，
    #    agents-sync/gc.py 自带 hub 侧副本（刻意 stdlib-only 不 import proto）→ 漏改一侧的后果 =
    #    新系统主题在 task tab 被判出主持人链接、或被 gc 误删，故以断言钉住同值。
    import importlib.util as _ilu
    _gc_spec = _ilu.spec_from_file_location(
        "agents_sync_gc_s41", os.path.join(HERE, "..", "agents-sync", "gc.py"))
    _gc = _ilu.module_from_spec(_gc_spec)
    _gc_spec.loader.exec_module(_gc)
    assert _proto.PROTECTED_SYSTEM_PATHS == _gc.PROTECTED_SYSTEM_PATHS, \
        "系统主题保护名单两侧必须同值（proto.py 单点 ↔ agents-sync/gc.py 副本）：%r vs %r" % (
            _proto.PROTECTED_SYSTEM_PATHS, _gc.PROTECTED_SYSTEM_PATHS)
    assert _proto.PROTECTED_SYSTEM_TOPICS == (_proto.POSITION_TOPIC,), \
        "系统主题名单须由 POSITION_TOPIC 派生（不另立魔数）：%r" % (
            _proto.PROTECTED_SYSTEM_TOPICS,)
    # ①c 裸族容器护栅同源：gc.py 的 BARE_FAMILY_ENTRIES 必须覆盖 proto.LAYOUT_DIRS 全集
    #    （+ 树根同形的 agents 与工作区级的 run）。漏改一侧的后果 = 新族的一行裸容器
    #    条目能删掉整族（列表累计且 append-only ⇒ 同名子树一出现就被删）。
    assert set(_gc.BARE_FAMILY_ENTRIES) == set(_proto.LAYOUT_DIRS) | {"agents", "run"}, \
        "gc.py 裸族容器护栅须覆盖 proto.LAYOUT_DIRS 全集：%r vs %r" % (
            _gc.BARE_FAMILY_ENTRIES, _proto.LAYOUT_DIRS)
    assert _proto.retired_move("bot/dispatcher") == "queue/dispatcher"
    assert _proto.retired_move("bot/notify-user") == "queue/notify-user", \
        "迁族地址必须命中退役表（否则投旧址会静默重建无人消费的僵尸信箱）"
    for old, new in (("bot/work-lead", "queue/work-lead"),
                     ("bot/work-lead-watcher", "queue/work-lead")):
        assert _proto.retired_move(old) == new, \
            "position 信箱与其 watcher 合并后两类旧地址都要进退役表：%s → %r" % (
                old, _proto.retired_move(old))
    # agentfw-lead position 并入 dispatcher ⇒ 它的三个历史地址全改投职位信箱（表单跳、⛔ 递推）
    for old in ("queue/agentfw-lead", "bot/agentfw-lead", "bot/agentfw-lead-watcher"):
        assert _proto.retired_move(old) == _proto.POSITION_PID, \
            "已并入的 position 地址必须直指职位信箱（⛔ 指向同样已退役的中间址）：%s → %r" % (
                old, _proto.retired_move(old))
    assert _proto.retired_move("topic/dispatcher") == "queue/dispatcher", \
        "职位信箱移族后旧址必须进退役表（否则在飞件的旧 reaper 值会落进 tombstone）"
    for live in ("queue/dispatcher", "queue/notify-user",
                 "queue/work-lead",
                 "task/x1", "bot/dev-dispatcher", "bot/dispatcher2"):
        assert _proto.retired_move(live) is None, "在用地址不得命中退役表：%s" % live
    cmds = [("send", "bot/dispatcher", "--type", "inform", "--body", "hi",
             "--from", "task/s41"),
            ("ack", "bot/dispatcher", "s41-mid"),
            ("control", "bot/dispatcher", "stop"),
            ("enable", "bot/dispatcher", "--by", "s41")]
    # ② 旧目录**在场**（含 pid.json，模拟 gc 前现网：文法合法 + 目录在场，能过 require_pid）
    os.makedirs(inbox, exist_ok=True)
    with open(os.path.join(bdir, "pid.json"), "w") as f:
        json.dump({"pid": 1, "gen": 1, "status": "running"}, f)
    for c in cmds:
        r = ctl(*c, expect_rc=2)
        assert "已退役" in r.stderr, r.stderr
        assert "queue/dispatcher" in r.stderr, "回执必须含新地址：%r" % r.stderr
        assert "未写任何文件" in r.stderr, "须明说本次零副作用：%r" % r.stderr
    # ③ 拒后零副作用：无新增信封、无 ack/、无 control/、无 enable.json
    assert os.listdir(inbox) == [], "退役地址不得落信封：%r" % os.listdir(inbox)
    for sub in ("ack", "control"):
        assert not os.path.exists(os.path.join(bdir, sub)), \
            "退役地址不得建 %s/" % sub
    assert not os.path.exists(os.path.join(bdir, "enable.json"))
    # ④ require_pid 语义未破（护栏在文法校验之后，不改变原文案/退出码）
    for bad in ("dispatcher", "bot/a/b", "chan/x", "topic/"):
        r = ctl("send", bad, "--type", "inform", "--body", "hi",
                "--from", "task/s41", expect_rc=1)
        assert "id 非法" in r.stderr, (bad, r.stderr)
    # ⑤ 旧目录**不在场**（= gc 删除之后）：同样被拒，且拒后不 mkdir 重建僵尸信箱
    shutil.rmtree(bdir, ignore_errors=True)
    for c in cmds:
        r = ctl(*c, expect_rc=2)
        assert "已退役" in r.stderr and "queue/dispatcher" in r.stderr, r.stderr
    assert not os.path.exists(bdir), \
        "护栏独立于目录在场性：拒后不得重建 agents/bot/dispatcher/"
    # ⑥ 正常地址零回归：新址（职位信箱）+ task/ + bot/ 三类照旧可投
    mid = ctl("send", "queue/dispatcher", "--type", "inform", "--body", "to new address",
              "--from", "task/s41").stdout.strip()
    pinbox = proto.position_inbox(ROOT)
    assert os.path.exists(os.path.join(pinbox, mid + ".msg")), "新址应正常收信"
    # ack 动词的零回归 **⛔ 拿职位信箱当靶**：共享 ROOT 的职位信箱必须对 S49 的 0828 回归
    # 断言（「职位信箱零 ack 条目」= 子任务抢 ack 事故面）保持洁净 ⇒ 用一枚 bot 族信箱
    # 验同款扁平 ack 落盘（queue 与 bot 同为单消费者信箱，ack 形态一致）。
    os.makedirs(os.path.join(ROOT, "agents", "bot", "s41-box", "inbox"), exist_ok=True)
    midb = ctl("send", "bot/s41-box", "--type", "inform", "--body", "ack target",
               "--from", "task/s41").stdout.strip()
    ctl("ack", "bot/s41-box", midb)
    assert os.path.exists(os.path.join(
        ROOT, "agents", "bot", "s41-box", "inbox", "ack", midb)), "单消费者信箱的扁平 ack 应落盘"
    assert not os.path.exists(os.path.join(pinbox, "ack", mid)), \
        "S41 ⛔ 不得在职位信箱留 ack（S49 的 0828 回归断言要求它零 ack 条目）"
    mid2 = ctl("send", "task/s41-peer", "--type", "inform", "--body", "normal task",
               "--from", "task/s41").stdout.strip()
    assert os.path.exists(os.path.join(
        ROOT, "agents", "task", "s41-peer", "inbox", mid2 + ".msg"))
    mid3 = ctl("send", "bot/tester", "--type", "inform", "--body", "normal bot",
               "--from", "task/s41").stdout.strip()
    assert os.path.exists(os.path.join(
        ROOT, "agents", "bot", "tester", "inbox", mid3 + ".msg"))
    # 只读子命令**不加**护栏（保留对退役目录的考古能力，零副作用）：status 照旧只读谓词
    r = ctl("status", "bot/dispatcher")
    assert "已退役" not in r.stderr and "已退役" not in r.stdout, \
        "status 是只读命令：不按退役表拒（否则退役后无法查旧目录历史）：%r%s" % (r.stderr, r.stdout)
    assert "participant=bot/dispatcher" in r.stdout, r.stdout
    # 清理：临时树内直接 rmtree（e2e 收尾整体删除；生产 agents/ 走 gc 通道）
    shutil.rmtree(os.path.join(ROOT, "agents", "topic"), ignore_errors=True)
    shutil.rmtree(os.path.join(ROOT, "agents", "task", "s41-peer"), ignore_errors=True)


# ---------------------------------------------------------------- S42（新增）

def _write_stop_req(pid_, reason):
    """直写 control/*.req（协议 §5.2 信封形态）——e2e 夹具：模拟「调度员已下取消、
    runner 尚未杀到」的窗口（真链路里 agentctl control stop 与子进程自然退出无同步点）。"""
    cdir = os.path.join(adir_of(pid_), "control")
    os.makedirs(cdir, exist_ok=True)
    mid = "2026-09-06-10-00-00.000-dispatcher-%s" % (pid_.split("/", 1)[1][:8])
    with open(os.path.join(cdir, mid + ".req"), "w") as f:
        json.dump({"id": mid, "from": "queue/dispatcher",
                   "ts": "2026-09-06T10:00:00+08:00",
                   "action": "stop", "reason": reason}, f, ensure_ascii=False)


def _final_fixture(pid_, status, exitcode, ended_at=None):
    """直写终态 pid.json（e2e 夹具）：生产侧 pid.json 单写者 = runner，沙箱里直写是为了
    确定性构造「notify_tick 看到的档案形态」，避开 runner tick 与子进程退出之间的竞态。
    final 是吸收态（§9.2）：runner 不再 spawn/不再改写，只幂等回执 stop 请求。"""
    ended = ended_at or proto.now_ts()   # 缺省 = 当前时刻：本夹具建模的是
    # 「上一实例刚写完 final 就没了」的崩溃窗口档案；终态通知重放护栏据 endedAt 与本次
    # 启动纪元的距离区分「本次运行的新事件」与「历史档案」，写死一个远古时刻会让夹具被
    # 当历史档案抑制 → S42/S43 的通知断言失效（用例跟随口径，断言逐字未变）。
    # 要构造**历史档案**（S44 的重放回归面）显式传 ended_at。
    with open(os.path.join(adir_of(pid_), "pid.json"), "w") as f:
        json.dump({"gen": 1, "pid": 424242, "status": status, "final": True,
                   "exitcode": exitcode, "restarts": 0,
                   "startedAt": "2026-09-06-10-00-00.000",
                   "endedAt": ended,
                   "lastAliveAt": ended}, f)


def _report_verdicts(stdout):
    """report.py --all 的「## 终态任务」表 → {裸 taskId: 结论文本}（markdown 按 | 切列）。"""
    out, sec = {}, ""
    for line in stdout.splitlines():
        if line.startswith("## "):
            sec = line
            continue
        if not sec.startswith("## 终态任务"):
            continue
        m = re.match(r"^\| \[`task/([A-Za-z0-9._-]+)`\]", line)
        if not m:
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) >= 3:
            out[m.group(1)] = cells[2]
    return out


def s42():
    """取消任务不再误报「无报告」（自评 R2）：调度员主动取消（control/ 有
    action=stop 请求，或 exitcode=125）的任务缺 report.md 是取消的预期结果，不是空跑——
    终态通知不得带 warn=no_report（旧行为：心跳每天为约 10 条假告警逐条查 control/）；
    非取消的缺报告 one-shot（exit 0）仍须带 warn（P5 对照，防过度抑制）；呈现面同判据
    （report.py verdict 判「取消」；core.ts 列表标 [已取消] 由扩展侧单测覆盖）。"""
    # ① 真链路取消（agentctl control stop → runner 杀 → killed/137）：不带 warn + 带 stopReason
    a = create(["--name", "s42-cancel-kill"], policy="one-shot")
    wait_until(lambda: (pdoc(a) or {}).get("status") == "running", "S42a running")
    ctl("control", a, "stop", "--reason", "方向错误", "--from", "task/tester")
    wait_until(lambda: (pdoc(a) or {}).get("final") is True, "S42a final")
    _ea, pa = wait_until(lambda: disp_notifs(a)[0] if disp_notifs(a) else None,
                         "S42a 取消终态通知")
    assert pa["event"] == "task_canceled" and pa["status"] == "killed", pa
    assert "warn" not in pa, "取消（killed/137）终态通知不得带 no_report warn：%r" % pa
    assert pa.get("stopReason") == "方向错误", pa
    assert "report" not in pa, pa

    # ②③④ 确定性构造通知时刻的档案形态（runner 停机窗口内直写夹具）：
    #   b = stop 请求 ∧ 子进程自然 exit 0（取消竞态；exited/0 优先 → event 仍 task_done）→ 无 warn
    #   c = 无 stop 请求 ∧ exit 0 缺报告 one-shot（P5 对照）→ 仍带 warn
    #   d = exitcode 125（旧系统 CANCELED 约定值，proto.EXITCODE_CANCELED）→ 无 warn + verdict 取消
    b = create(["--name", "s42-cancel-exit0"], policy="one-shot", gate=False)
    c = create(["--name", "s42-noreport-warn"], policy="one-shot", gate=False)
    d = create(["--name", "s42-cancel-125"], policy="one-shot", gate=False)
    kill_runner_hard()
    _write_stop_req(b, "评审对象消失")
    _final_fixture(b, "exited", 0)
    _final_fixture(c, "exited", 0)
    _final_fixture(d, "killed", 125)
    start_runner()
    _eb, pb = wait_until(lambda: disp_notifs(b)[0] if disp_notifs(b) else None,
                         "S42b 取消竞态终态通知")
    assert pb["event"] == "task_done" and pb["exitcode"] == 0, \
        "exited/0 归类优先（既有语义不变）：%r" % pb
    assert "warn" not in pb, \
        "stop 请求与自然 exit 0 竞态：缺报告是取消的预期结果，不得发 no_report warn：%r" % pb
    assert pb.get("stopReason") == "评审对象消失", \
        "取消上下文（stopReason）仍须带上：%r" % pb
    _ec, pc = wait_until(lambda: disp_notifs(c)[0] if disp_notifs(c) else None,
                         "S42c 对照任务终态通知")
    assert pc.get("warn") == "no_report", \
        "非取消的 exit 0 缺报告 one-shot 仍须带 warn（不得过度抑制）：%r" % pc
    _ed, pd = wait_until(lambda: disp_notifs(d)[0] if disp_notifs(d) else None,
                         "S42d exitcode=125 终态通知")
    assert "warn" not in pd, "exitcode=125（取消约定值）不得带 no_report warn：%r" % pd

    # ⑤ 呈现面同判据：report.py 的 verdict（与 core.ts [已取消] / runner 不发 warn 三处一致）
    r = subprocess.run([sys.executable, os.path.join(HERE, "report.py"),
                        "--root", ROOT, "--all"], capture_output=True, text=True,
                       timeout=120)
    assert r.returncode == 0, "report.py rc=%d: %s" % (r.returncode, r.stderr[-400:])
    verdicts = _report_verdicts(r.stdout)
    for name in ("s42-cancel-kill", "s42-cancel-exit0", "s42-cancel-125"):
        assert "取消" in verdicts.get(name, ""), \
            "%s 应判取消（不得落到「无报告」）：%r" % (name, verdicts.get(name))
        assert "无报告" not in verdicts.get(name, ""), \
            "%s 结论不得含「无报告」：%r" % (name, verdicts.get(name))
    assert "无报告" in verdicts.get("s42-noreport-warn", ""), \
        "非取消缺报告仍判「无报告」（中性标记，不过度扩张取消）：%r" % \
        verdicts.get("s42-noreport-warn")


# ---------------------------------------------------------------- S43（新增）

def _write_report(pid_, text="# report\n\u2705 全部验收项通过\n"):
    """直写 report.md（e2e 夹具）：完成判定的自证件（DISPATCH.md「完成判定」）。"""
    with open(os.path.join(adir_of(pid_), "report.md"), "w") as f:
        f.write(text)


def _dep_line(stdout, pid_):
    """report.py --all 输出中含该 taskId 的表行（依赖列呈现断言用）。"""
    needle = "`%s`" % pid_
    for line in stdout.splitlines():
        if needle in line:
            return line
    return ""


def s43():
    """空跑 provider 不算成功 + 取消豁免 + pending 优先于旧 success（
    缺陷来源 = 🟡1：空跑任务 exit0/零改动/无 report.md 被算成功 provider，
    依赖它的评审任务两次被提前放行空转）：
    ① exit0 ∧ 无 report.md ∧ 非取消（空跑）→ provider 不 success，依赖者不放行，
       调度器打 WARNING，报表依赖列显示「空跑（exit0 无报告）」+ 异常区点名该 cap；
    ② exit0 ∧ 有 report.md → 依赖者照常放行（不回归，口径同 S19①）；
    ③ 取消（control/ 有 action=stop 请求 ∨ exitcode=125）∧ 无 report.md → 既不判失败
       也不判空跑（canceled 态），依赖者不放行，且终态通知不带 warn=no_report
       （豁免不回归）；空跑侧仍带 warn（既有可观测信号未被改坏）；
    ④ 同 cap 下 pending provider 优先于旧 success provider → 依赖者等待不放行（日志附
       ignoring older success provider）；把 pending provider 取消（恢复路径）→ 依赖者按
       旧 success 放行 = 等待不永久锁死；
    ⑤ report.md 在场但零字节/纯空白 → 仍不算交付（存在性口径的廉价逃逸口🟡3）：
       provider 归 idle、依赖者不放行、报表 verdict「无报告」+ 依赖列/异常区点名空跑；
    ⑥ 报表面跟随裁决（🟡2）：④ 形态下依赖列点名**在途** provider 并附「忽略
       旧成功」，异常区出 ⚠️「pending 压住旧 success」条目（含恢复路径）且不落 🚨 unsat。
    夹具手法同 S29/S42：runner 停机窗口直写 pid.json / report.md / control/*.req；
    provider 无 enable → runner 不拉起它们（收尾按 pdoc 为 None 真验，不靠恒真断言）。"""
    import scheduler as _sched          # 日志标记常量同源（不硬编码字面）
    stop_runner()
    try:
        def fixture(name, cap, status, exitcode, report=False, stop_req=None):
            t = create(["--name", name], policy="one-shot", gate=False)
            set_sched_fields(t, provides=[cap], resources=[])
            if stop_req:
                _write_stop_req(t, stop_req)
            _final_fixture(t, status, exitcode)
            if report:
                _write_report(t)
            return t

        def consumer(name, cap):
            t = create(["--name", name], gate=False)
            set_sched_fields(t, needs=[cap], resources=[])
            return t

        idle_p = fixture("s43-p-idle", "cap43a", "exited", 0)               # 空跑
        idle_n = consumer("s43-n-idle", "cap43a")
        ok_p = fixture("s43-p-report", "cap43b", "exited", 0, report=True)   # 真成功
        ok_n = consumer("s43-n-report", "cap43b")
        cs_p = fixture("s43-p-cancel-stop", "cap43c", "exited", 0,
                       stop_req="评审对象消失")                            # 取消判据①
        cs_n = consumer("s43-n-cancel-stop", "cap43c")
        ce_p = fixture("s43-p-cancel-125", "cap43d", "killed",
                       proto.EXITCODE_CANCELED)                             # 取消判据②
        ce_n = consumer("s43-n-cancel-125", "cap43d")
        old_p = fixture("s43-p-old-success", "cap43e", "exited", 0, report=True)
        # ⑤ report.md 在场但无内容（零字节 / 纯空白）：写盘中断或廉价逃逸口的形态
        empty_p = fixture("s43-p-empty-report", "cap43f", "exited", 0)
        _write_report(empty_p, "")
        empty_n = consumer("s43-n-empty-report", "cap43f")
        blank_p = fixture("s43-p-blank-report", "cap43g", "exited", 0)
        _write_report(blank_p, "\n   \t\n  \n")
        blank_n = consumer("s43-n-blank-report", "cap43g")
        start_runner()
        start_scheduler(max_concurrent=32)

        # ③ 告警面（runner 既有信号，本任务未改 runner）：空跑带 warn、取消不带 warn
        _e1, p_idle = wait_until(
            lambda: disp_notifs(idle_p)[0] if disp_notifs(idle_p) else None,
            "S43 空跑 provider 终态通知")
        assert p_idle["event"] == "task_done" and p_idle.get("warn") == "no_report", \
            "空跑（exit0 无报告）应带既有 no_report 信号：%r" % p_idle
        for t in (cs_p, ce_p):
            _e2, p = wait_until(lambda t=t: disp_notifs(t)[0] if disp_notifs(t) else None,
                                "S43 取消 provider 终态通知")
            assert "warn" not in p, \
                "取消缺报告是预期结果，不得发 no_report 告警（ 豁免）：%r" % p

        # ② 有报告 → 放行（不回归）
        en = wait_until(lambda: enable_doc(ok_n),
                        "S43 ② exit0+report.md provider 放行依赖者", timeout=6.0)
        assert en["by"] == "agentd-scheduler" and "dag" in en.get("note", ""), en

        # ④ 同 cap 新 provider 在途（running = 未终态），旧 success 已在场；
        # 依赖者必须在新 provider 在途**之后**才登记，否则首轮重扫即按旧 success 放行
        new_p = create(["--name", "s43-p-pending"], gate=False)
        set_sched_fields(new_p, provides=["cap43e"], resources=[])
        enable_task(new_p)   # 调度方常驻：容忍它抢先放行（观察点 = 新 provider 在途，不是谁写的凭证）
        wait_until(lambda: (pdoc(new_p) or {}).get("status") == "running",
                   "S43 ④ 新 provider running")
        new_n = consumer("s43-n-pending-prio", "cap43e")

        # ①③④ 观察窗：调度器多轮重扫均不得放行
        time.sleep(1.5)
        assert enable_doc(idle_n) is None, "空跑（exit0 无 report.md）不得满足依赖"
        assert enable_doc(cs_n) is None, "取消（control/ stop 请求）不得满足依赖"
        assert enable_doc(ce_n) is None, "取消（exitcode=125）不得满足依赖"
        assert enable_doc(new_n) is None, \
            "同 cap 存在 pending provider 时不得按旧 success 提前放行"
        assert enable_doc(empty_n) is None, \
            "零字节 report.md 不算交付：不得满足依赖（存在性口径的逃逸口）"
        assert enable_doc(blank_n) is None, "纯空白 report.md 不算交付：不得满足依赖"

        log_txt = open(SCHED_LOG, encoding="utf-8", errors="replace").read()
        assert "空跑" in log_txt and idle_p in log_txt, \
            "空跑 provider 应打 WARNING（provider 与 cap 点名）"
        assert any(_sched.PENDING_OVER_SUCCESS_MARK in ln and "WARNING" in ln
                   for ln in log_txt.splitlines()), \
            "pending 压住旧 success 应升 WARNING（不只 INFO 阻塞行；附标记供排障）"
        for t in (empty_p, blank_p):
            assert any("空跑" in ln and t in ln and "WARNING" in ln
                       for ln in log_txt.splitlines()), \
                "report.md 无内容的空跑也应升 WARNING 并点名：%s" % t

        # ①③ 呈现面：verdict 口径不变（空跑=「无报告」中性、取消=「取消」非失败），
        # 依赖列/异常区点名 provider 新态
        r = subprocess.run([sys.executable, os.path.join(HERE, "report.py"),
                            "--root", ROOT, "--all"], capture_output=True, text=True,
                           timeout=120)
        assert r.returncode == 0, "report.py rc=%d: %s" % (r.returncode, r.stderr[-400:])
        verdicts = _report_verdicts(r.stdout)
        assert "无报告" in verdicts.get("s43-p-idle", ""), \
            "空跑 provider 的 verdict 仍为中性「无报告」：%r" % verdicts.get("s43-p-idle")
        for nm in ("s43-p-cancel-stop", "s43-p-cancel-125"):
            assert "取消" in verdicts.get(nm, "") and "失败" not in verdicts.get(nm, ""), \
                "%s 应判取消且不判失败：%r" % (nm, verdicts.get(nm))
        assert "成功" in verdicts.get("s43-p-report", ""), verdicts.get("s43-p-report")
        assert "空跑（exit0 无报告）" in _dep_line(r.stdout, idle_n), \
            "依赖列应显示空跑态：%r" % _dep_line(r.stdout, idle_n)
        assert "已取消" in _dep_line(r.stdout, cs_n), \
            "依赖列应显示已取消（非「已失败」）：%r" % _dep_line(r.stdout, cs_n)
        # ⑤ 呈现面：空报告与无报告同口径（verdict 中性「无报告」、依赖列点名空跑）
        for nm in ("s43-p-empty-report", "s43-p-blank-report"):
            assert "无报告" in verdicts.get(nm, "") and "成功" not in verdicts.get(nm, ""), \
                "%s （report.md 无内容）应判「无报告」而非成功：%r" % (nm, verdicts.get(nm))
        assert "空跑（exit0 无报告）" in _dep_line(r.stdout, empty_n), \
            "零字节报告的依赖列应显示空跑态：%r" % _dep_line(r.stdout, empty_n)
        abn = [ln for ln in r.stdout.splitlines() if "needs 不可满足" in ln]
        assert any("cap43a" in ln and "空跑" in ln for ln in abn), abn
        assert any("cap43c" in ln and "已取消" in ln for ln in abn), abn
        assert any("cap43f" in ln and "空跑" in ln for ln in abn), abn
        # ⑥ 报表面跟随裁决（🟡2）：需:列点名在途 provider + 附被忽略的旧交付，
        # 异常区出 ⚠️ 条目（含恢复路径）；该形态是 wait 不是 unsat → 不得落 🚨 不可满足
        dep = _dep_line(r.stdout, new_n)
        assert "s43-p-pending" in dep and "忽略旧成功" in dep \
            and "s43-p-old-success" in dep, \
            "pending 压旧 success 时需:列应点名在途 provider 并附被忽略的旧交付：%r" % dep
        assert "已成功待消费" not in dep, \
            "需:列不得呈现为「已成功待消费」（看上去像调度器卡死）：%r" % dep
        pos = [ln for ln in r.stdout.splitlines() if "pending 压住旧 success" in ln]
        assert any("cap43e" in ln and "s43-p-pending" in ln and "s43-p-old-success" in ln
                   and "恢复" in ln for ln in pos), pos
        assert not any("cap43e" in ln for ln in abn), \
            "该形态不得落 🚨 needs 不可满足（严重度分级）：%r" % abn

        # ④ 恢复路径：取消 pending provider → canceled 退出 pending 集 → 按旧 success 放行
        ctl("control", new_p, "stop", "--reason", "改换实现路径", "--from", "task/tester")
        wait_until(lambda: (pdoc(new_p) or {}).get("final") is True,
                   "S43 ④ pending provider 收口")
        en2 = wait_until(lambda: enable_doc(new_n),
                         "S43 ④ pending provider 取消后按旧 success 放行", timeout=6.0)
        assert en2["by"] == "agentd-scheduler", en2

        for t in (ok_n, new_n):
            ctl("control", t, "stop", "--from", "task/tester")
            wait_until(lambda t=t: (pdoc(t) or {}).get("final") is True, "S43 收尾 stop")
        # 夹具 provider 未被 runner 拉起（真判据⚪5：旧断言
        # `assert old_p and ok_p and ce_p` 因 create() 返回非空串而恒真）：
        # final 是吸收态（§9.2）→ pid.json 应仍是 _final_fixture 直写的原样（pid/gen 不变，
        # 未起新进程），且调度器不得给它们写 enable.json（已终态 ≠ 候补）。
        for t in (old_p, ok_p, cs_p, ce_p, idle_p, empty_p, blank_p):
            d = pdoc(t) or {}
            assert d.get("pid") == 424242 and d.get("gen") == 1 and d.get("final") is True, \
                "夹具 provider 的 pid.json 不得被改写（= 未被拉起）：%s %r" % (t, d)
            assert enable_doc(t) is None, \
                "已终态的夹具 provider 不是候补，不得被写 enable.json：%s" % t
    finally:
        stop_scheduler()
        stop_runner()
        start_runner()


# ---------------------------------------------------------------- S44（新增）
#
# 隔离树：本场景不用主树 ROOT，另起 S44ROOT——冷启动回归必须「本地职位信箱一个信封都没有」，
# 而主树跑到 S44 时信箱里已積着 S14–S43 的几百条信封（既有判重会把重放全部挡住，根本测不到
# 护栏）。两棵树各自一份 `agents/run/agentd.e2ehost.lock`，互不干扰；主 runner 可同时在跑。

S44ROOT = os.path.join(TMPBASE, "root-s44")
S44_LOG = os.path.join(TMPBASE, "runner-s44.log")


def _s44_ago(seconds):
    """now_ts 格式的历史时刻（夹具用）：距现在 seconds 秒前。"""
    return time.strftime("%Y-%m-%d-%H-%M-%S",
                         time.localtime(time.time() - seconds)) + ".000"


def _s44_dir(name):
    return os.path.join(S44ROOT, "agents", "task", name)


def _s44_create(name, policy="one-shot", enable=True, resident=False):
    """隔离树登记夹具：直写 spec.json（本机认领）+ enable.json（放行）。
    runner 对 enable.json 只判存在性（写入口在调度方/agentctl，§14.2），夹具直写等价。
    `policy` 为假值 ⇒ `restartPolicy` 键**整个不写**（CLI 缺省形态）；`resident=True` ⇒
    命令前缀 `AGENTD_RESIDENT=1`（常驻体：不因代终态被收口，判据单点 = `scheduler.is_resident`）
    ——⑦ 用它构造「代终态 ∧ 生命周期仍开放」的档案（非常驻者会被 runner 每轮幂等收口，
    该形态只余常驻体与 auto 两类载体；auto 会被自愈换代 ⇒ 只有常驻体可用）。"""
    d = _s44_dir(name)
    os.makedirs(os.path.join(d, "inbox"), exist_ok=True)
    cmd = FAKE_CMD % (os.path.join(HERE, "fakeagent.py"), S44ROOT)
    spec = {"command": ("AGENTD_RESIDENT=1 " + cmd) if resident else cmd,
            "workdir": S44ROOT, "creator": "tester",
            "host": "e2ehost", "createdByHost": "e2ehost"}
    if policy:
        spec["restartPolicy"] = policy
    proto.atomic_write_json(os.path.join(d, "spec.json"), spec)
    if enable:
        proto.atomic_write_json(os.path.join(d, "enable.json"),
                                {"ts": proto.now_ts(), "by": "e2e", "note": "S44 夹具"})
    return "task/" + name


_S44_NO_ENDEDAT = object()   # 夹具哨兵：pid.json 整个 endedAt 键不写（畸形档案形态）


def _s44_archive(name, status, exitcode, ended_at, report=False):
    """历史终态档案夹具：spec + pid.json（final=true，指定 endedAt）[+ report.md]。
    无 enable.json → 即使不 final 也不会被拉起（镜像现网旧任务档案形态）。
    endedAt 三种畸形形态（🔴1/🟡1）：`_S44_NO_ENDEDAT` =
    **整个键缺失**（人工补录/异构写入/被裁剪的同步副本被 do_stop「只置 final」收口后的
    形态）、空串与垃圾串 = 不可解析；三者均应被护栏降级为「远古」→ 抑制，绝不崩溃。
    键缺失时 startedAt/lastAliveAt 仍写合法时刻（只让 endedAt 一个字段畸形，不连带
    制造其它字段的解析面）。

    落盘顺序 = pid.json → report.md → **spec.json 最后**：参与方入扫描面的判据是 spec
    （`service()`/`notify_tick()` 见 spec 缺失即跳过），先落 spec 会留出「spec 在场、
    pid.json 未落」的窗口——tick 撞进去就按「尚未启动 = 非终态」记进见证集（护栏条件①），
    随后落地的 final 档案被当本次运行的新事件通知（⑥b/⑥d 共用本夹具，实测约十次命中
    一次的间歇失败）。反序后档案对 runner 是「一次成型」的落地，断言不再靠运气。"""
    d = _s44_dir(name)
    os.makedirs(os.path.join(d, "inbox"), exist_ok=True)
    base = _s44_ago(86400) if ended_at is _S44_NO_ENDEDAT else ended_at
    doc = {"gen": 1, "pid": 424242, "procStart": 1, "status": status, "final": True,
           "exitcode": exitcode, "restarts": 0, "startedAt": base,
           "lastAliveAt": base}
    if ended_at is not _S44_NO_ENDEDAT:
        doc["endedAt"] = ended_at
    proto.atomic_write_json(os.path.join(d, "pid.json"), doc)
    if report:
        with open(os.path.join(d, "report.md"), "w") as f:
            f.write("# report\n\u2705 全部验收项通过\n")
    return _s44_create(name, enable=False)


def _s44_pid(name):
    p = os.path.join(_s44_dir(name), "pid.json")
    if not os.path.exists(p):
        return None
    with open(p) as f:
        return json.load(f)


def _s44_notifs(taskid=None):
    """隔离树职位信箱里 from=agentd 的终态通知（同主树 disp_notifs 口径）。"""
    d = proto.position_inbox(S44ROOT)
    out = []
    for fn in sorted(glob.glob(os.path.join(d, "*.msg"))):
        try:
            with open(fn) as f:
                env = json.load(f)
            payload = json.loads(env["body"])
        except (ValueError, KeyError, OSError):
            continue
        if env.get("from") != "agentd" or env.get("type") != "inform":
            continue
        if taskid and payload.get("taskId") != taskid:
            continue
        out.append((env, payload))
    return out


def _s44_ctl(*args, expect_rc=0):
    r = subprocess.run([sys.executable, os.path.join(HERE, "agentctl.py"),
                        "--root", S44ROOT, *args], capture_output=True, text=True,
                       timeout=60)
    if expect_rc is not None and r.returncode != expect_rc:
        raise AssertionError("agentctl(s44) %s rc=%d (want %d): %s"
                             % (" ".join(args), r.returncode, expect_rc, r.stderr))
    return r


def _s44_start(extra=None):
    """起隔离树 runner（前台子进程，日志落 S44_LOG；与主树 runner 共存不冲突）。"""
    return subprocess.Popen(
        [sys.executable, os.path.join(HERE, "runner.py"), "--root", S44ROOT,
         "--host", "e2ehost", "--aliases", "e2e-alias", "--interval", INTERVAL,
         "--log-file", S44_LOG, "--log-level", "INFO"] + list(extra or []),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _s44_stop(h, hard=False):
    """停隔离树 runner（等待一律有上界，超时升级 SIGKILL）。"""
    if not h:
        return
    if hard:
        h.kill()
    else:
        h.send_signal(signal.SIGTERM)
    try:
        h.wait(timeout=10)
    except subprocess.TimeoutExpired:
        h.kill()
        h.wait(timeout=10)


def _s44_log():
    try:
        with open(S44_LOG, encoding="utf-8", errors="replace") as f:
            return f.read()
    except OSError:
        return ""


def s44():
    """终态通知重放护栏（缺陷实例 = 2026-09-07 nv1/nv2 冷启动重放 45 条
    历史终态通知）：通知面只报「本次运行期间到达终态」的任务。
    隔离树 + 本地职位信箱**一个信封都没有** = 复现旧判重集为空的冷启动现场。六例：
    ① 启动时已终态的历史任务 → 不通知（且逐条留 INFO 痕迹）；
    ② 启动后新到达终态 → 通知一次，载荷字段齐备；③ 同 taskId 多轮 tick → 只一次；
    ④ 孤儿接管判 stale/127 + report.md 在场 → **仍通知**（task_done + note）；
    ⑤ 取消 → task_canceled、不带 warn、stopReason 在（stop 请求与 exitcode=125 两判据）；
    ⑥ 25 个历史终态档案 + 空信箱冷启动 → **零重放**（本次缺陷的直接回归）；
    ⑦ 见证集直证：旧代终态未收口档案（endedAt 3 天前、killed 未 final）本次运行内被 stop
       收口（do_stop 不刷新 endedAt）→ 仍通知 task_canceled（只看 endedAt 会吞掉它）。
    加测：⑥b 迟到档案（runner 在跑时才落地的旧终态目录，纯启动快照方案会漏的那一类）
    同样零重放；⑥c 宽限窗（endedAt 距启动 5s 的终态 → 仍通知；--replay-grace 0 → 不通知，
    且 grace=0 不影响本次运行内新终态的通知与 warn 信号）；⑥d 畸形 endedAt（键缺失/空串/
    垃圾串）→ 降级抑制且不崩，**同树内排序在后的真终态照常通知**（🔴1：AttributeError 逃出护栏会让 notify_tick 每轮整段中断、饿死其后所有参与方）。"""

    sys.path.insert(0, HERE)
    import runner as _runner

    HIST_N = 25
    hist = []          # 历史档案 id 集（任何一条出现在信箱里 = 重放）
    r = None
    try:
        os.makedirs(os.path.join(S44ROOT, "agents", "task"), exist_ok=True)
        inbox_dir = proto.position_inbox(S44ROOT)
        assert not os.path.exists(inbox_dir), "S44 前置：隔离树不得已有职位信箱"

        # ---- ①/⑥ 历史档案（endedAt 分散在 1–7 天前，混合 exited/0、exited/1、
        #      killed/137、stale/127+report）：本地信箱零信封 → 旧代码必全量重放 ----
        for i in range(HIST_N):
            kind = i % 4
            hist.append(_s44_archive(
                "s44-hist-%02d" % i,
                ["exited", "exited", "killed", "stale"][kind],
                [0, 1, 137, proto.EXITCODE_STALE][kind],
                _s44_ago(86400 + i * 21600), report=(kind == 3)))
        # 宽限窗夹具：endedAt 距启动 5s 的终态档案（崩溃窗口形态）
        grace_t = _s44_archive("s44-grace", "exited", 0, _s44_ago(5), report=True)
        assert not os.path.exists(inbox_dir), \
            "S44 前置：冷启动前本地职位信箱不得存在（复现 nv1 12:28:15 现场）"

        # ---- ⑥ 冷启动：零重放 ----
        r = _s44_start()
        wait_until(lambda: os.path.exists(inbox_dir),
                   "S44 隔离树 runner 应建出职位信箱目录", timeout=10.0)
        time.sleep(2.5)                     # ≥ 5 轮 tick 观察窗（interval 0.2s）
        # 零重放断言只针对历史档案：宽限窗夹具（endedAt 距启动 5s）是故意要被通知的
        # 崩溃窗口档案，不在「历史重放」面里（紧接着单独断言它确实被通知）。
        leaked0 = [(p["taskId"], p["event"]) for _e, p in _s44_notifs()
                   if p["taskId"] != grace_t]
        assert not leaked0, "冷启动重放了历史终态通知（本次缺陷的直接回归）：%r" % (leaked0,)
        log_txt = _s44_log()
        n_guard = log_txt.count(_runner.REPLAY_GUARD_LOG)
        assert n_guard >= HIST_N, \
            "每个被抑制的历史档案应逐条留 INFO 痕迹（实得 %d 条，期待 ≥ %d）" % (
                n_guard, HIST_N)
        missing = [t for t in hist if t not in log_txt]
        assert not missing, "抑制日志未点名全部历史档案：%r" % (missing[:5],)
        # 宽限窗对照：endedAt 距启动 5s 的档案属崩溃窗口 → 仍应通知（不被护栏吞）
        _eg, pg = wait_until(
            lambda: _s44_notifs(grace_t)[0] if _s44_notifs(grace_t) else None,
            "S44 宽限窗内的终态档案应照常通知", timeout=6.0)
        assert pg["event"] == "task_done" and pg.get("report"), pg

        # ---- ②/③ 启动后新到达终态 → 通知一次（载荷字段齐备） ----
        live = _s44_create("s44-live")
        wait_until(lambda: (_s44_pid("s44-live") or {}).get("status") == "running",
                   "S44② 新任务 running", timeout=10.0)
        with open(os.path.join(_s44_dir("s44-live"), "report.md"), "w") as f:
            f.write("# report\n\u2705 全部验收项通过\n")
        _s44_ctl("send", live, "--type", "inform", "--body", "CMD:exit",
                 "--from", "task/tester")
        wait_until(lambda: (_s44_pid("s44-live") or {}).get("final") is True,
                   "S44② final", timeout=10.0)
        _e2, p2 = wait_until(
            lambda: _s44_notifs(live)[0] if _s44_notifs(live) else None,
            "S44② 本次运行内新终态应通知", timeout=6.0)
        assert p2["event"] == "task_done" and p2["status"] == "exited" \
            and p2["exitcode"] == 0 and p2["taskId"] == live, p2
        assert p2["dir"] == _s44_dir("s44-live") and p2.get("report"), p2
        assert "warn" not in p2, "有 report.md 不得带 no_report warn：%r" % p2
        time.sleep(1.2)                     # ③ 多轮 tick 观察窗
        assert len(_s44_notifs(live)) == 1, "幂等失效：同一终态多次通知"

        # ---- ⑥b 迟到档案：runner 在跑时才落地的旧终态目录（agents-sync pull 迟到面） ----
        late = [_s44_archive("s44-late-%d" % i, "exited", 0, _s44_ago(3 * 86400 + i),
                             report=True) for i in range(5)]
        before = len(_s44_notifs())
        time.sleep(1.5)
        assert len(_s44_notifs()) == before, \
            "迟到落地的旧终态档案被当新事件重放（纯启动快照方案会漏的那一类）：%r" % (
                [p["taskId"] for _e, p in _s44_notifs()][before:],)
        assert not [t for t in late if _s44_notifs(t)], "迟到档案不得有通知"

        # ---- ⑦ 条件①（见证集）的直证：旧代终态未收口档案（常驻体、endedAt 3 天前、
        #      killed 未 final）在本次运行内被 stop 收口 —— do_stop 的「进程已终态：只置 final」分支
        #      **不刷新 endedAt**，只看 endedAt 的门槛会把这条真取消通知当历史档案吞掉。----
        killed_old = _s44_ago(3 * 86400)
        # 常驻体 + 不写 restartPolicy 键：无 enable 避开 spawn 竞态，且 runner 不因代终态收口
        # 它（非常驻者会被每轮幂等 finalize ⇒ 「未收口的代终态档案」前提不再可达）。
        pt = _s44_create("s44-killed-old", enable=False, policy=None, resident=True)
        proto.atomic_write_json(os.path.join(_s44_dir("s44-killed-old"), "pid.json"), {
            "gen": 1, "pid": 424243, "procStart": 1, "status": "killed",
            "exitcode": proto.EXITCODE_KILLED, "restarts": 0, "startedAt": killed_old,
            "endedAt": killed_old, "lastAliveAt": killed_old, "final": False})
        time.sleep(1.0)                     # 未 final（生命周期开放）→ 不得有任何通知
        assert not _s44_notifs(pt), "未收口的代终态档案不得通知"
        _s44_ctl("control", pt, "stop", "--reason", "S44 旧档案收口", "--from", "task/tester")
        dp = wait_until(
            lambda: _s44_pid("s44-killed-old")
            if (_s44_pid("s44-killed-old") or {}).get("final") is True else None,
            "S44⑦ stop 收口置 final", timeout=10.0)
        assert dp["endedAt"] == killed_old, \
            "夹具前提：do_stop「已终态只置 final」分支不刷新 endedAt（否则本例测不到条件①）"
        _e7, p7 = wait_until(
            lambda: _s44_notifs(pt)[0] if _s44_notifs(pt) else None,
            "S44⑦ 本次运行内收口的旧 endedAt 终态仍须通知（见证集）", timeout=10.0)
        assert p7["event"] == "task_canceled", \
            "旧代终态档案被 stop 收口 = 取消，归类不得被护栏改变：%r" % p7
        assert p7.get("stopReason") == "S44 旧档案收口", "stopReason 不得丢：%r" % p7
        assert "warn" not in p7, "取消不得带 no_report warn：%r" % p7

        # ---- ④ 孤儿接管：本次运行见过的 running 任务 → 接管后判 stale/127 仍通知 ----
        orph = _s44_create("s44-orphan")
        d = wait_until(
            lambda: _s44_pid("s44-orphan")
            if (_s44_pid("s44-orphan") or {}).get("status") == "running" else None,
            "S44④ 孤儿夹具 running", timeout=10.0)
        child_pid = d["pid"]
        with open(os.path.join(_s44_dir("s44-orphan"), "report.md"), "w") as f:
            f.write("# S44 接管报告\n")
        _s44_stop(r, hard=True)             # 守护崩溃：子进程成孤儿（setsid 不株连）
        r = None
        assert pid_alive(child_pid), "守护被杀不得殃及子进程"
        _s44_ctl("send", orph, "--type", "inform", "--body", "CMD:exit",
                 "--from", "task/tester")
        wait_until(lambda: not pid_alive(child_pid), "S44④ 孤儿正常退出", timeout=15.0)
        r = _s44_start()                    # 新实例接管：只能观测「进程消失」
        def _orphan_final():
            # 读侧竞态：runner 对 one-shot 分两次写 pid.json（先标 stale/127、再置 final
            # 重写）⇒ 只等 status==stale 会读到中间态（负载高时 final 仍缺）。
            d = _s44_pid("s44-orphan") or {}
            return d if d.get("status") == "stale" and d.get("final") is True else None
        d2 = wait_until(_orphan_final, "S44④ 接管判死", timeout=15.0)
        assert d2["exitcode"] == proto.EXITCODE_STALE and d2.get("final") is True, d2
        _e4, p4 = wait_until(
            lambda: _s44_notifs(orph)[0] if _s44_notifs(orph) else None,
            "S44④ 接管孤儿终态通知（护栏不得吞）", timeout=10.0)
        assert p4["event"] == "task_done" and p4["status"] == "stale" \
            and p4["exitcode"] == proto.EXITCODE_STALE, \
            "接管后判 stale/127 且 report.md 在场 → 按 §5 记完成并通知：%r" % p4
        assert p4.get("note"), "接管归属 note 不得丢：%r" % p4
        assert p4.get("report") == os.path.join(_s44_dir("s44-orphan"),
                                                "report.md"), p4

        # ---- ⑤ 取消：语义与现状一致 + 两判据纯函数复核 ----
        canc = _s44_create("s44-cancel")
        wait_until(lambda: (_s44_pid("s44-cancel") or {}).get("status") == "running",
                   "S44⑤ 取消夹具 running", timeout=10.0)
        _s44_ctl("control", canc, "stop", "--reason", "S44 取消回归", "--from", "task/tester")
        wait_until(lambda: (_s44_pid("s44-cancel") or {}).get("final") is True,
                   "S44⑤ final", timeout=10.0)
        _e5, p5 = wait_until(
            lambda: _s44_notifs(canc)[0] if _s44_notifs(canc) else None,
            "S44⑤ 取消终态通知", timeout=6.0)
        assert p5["event"] == "task_canceled", "取消归类不得回归：%r" % p5
        assert "warn" not in p5, "取消缺报告是预期结果，不得带 no_report warn：%r" % p5
        assert p5.get("stopReason") == "S44 取消回归", "stopReason 不得丢：%r" % p5
        assert len(_s44_notifs(canc)) == 1, "取消通知幂等失效"
        # 判据单点（proto.EXITCODE_CANCELED）两分支复核：裸 Runner 实例，不起进程
        bare = _runner.Runner(S44ROOT, "e2ehost", "", 1.0)
        assert bare.canceled({"exitcode": proto.EXITCODE_CANCELED}, []) is True, "125 判据"
        assert bare.canceled({"exitcode": 0}, [{"action": "stop"}]) is True, "stop 请求判据"
        assert bare.canceled({"exitcode": 137}, [{"action": "stop"}]) is True, \
            "现网 stop 杀记 137"
        assert bare.canceled({"exitcode": 0}, []) is False, "真空跑不得豁免（仍要报 warn）"
        assert bare.canceled({"exitcode": 1}, []) is False, "真失败不得豁免"

        # ---- ⑥c 宽限窗可调：--replay-grace 0 = 纯启动基线语义 ----
        _s44_stop(r)
        r = None
        pre = len(_s44_notifs())
        r = _s44_start(["--replay-grace", "0"])
        time.sleep(2.0)
        assert len(_s44_notifs()) == pre, \
            "grace=0 重启不得扰动既有通知面（信封总数应不变；已发过的靠信箱判重）"
        g2 = _s44_archive("s44-grace0", "exited", 0, _s44_ago(5), report=True)
        time.sleep(1.5)
        assert not _s44_notifs(g2), \
            "--replay-grace 0 应把启动前已终态者一律当历史档案（endedAt 距启动 5s 也抑制）"
        # grace=0 不得关掉通知面：本次运行内新终态照常发（含 warn 信号）
        live0 = _s44_create("s44-live0")
        wait_until(lambda: (_s44_pid("s44-live0") or {}).get("status") == "running",
                   "S44⑥c grace=0 下新任务 running", timeout=10.0)
        _s44_ctl("send", live0, "--type", "inform", "--body", "CMD:exit",
                 "--from", "task/tester")
        _e6, p6 = wait_until(
            lambda: _s44_notifs(live0)[0] if _s44_notifs(live0) else None,
            "S44⑥c grace=0 不得抑制本次运行内的真终态", timeout=10.0)
        assert p6["event"] == "task_done" and p6.get("warn") == "no_report", \
            "无 report.md 的 exit0 one-shot 仍须带空跑信号（护栏不连带抑制 warn）：%r" % p6

        # ---- ⑥d 畸形 endedAt 档案（🔴1/🟡1）：键缺失 / 空串 /
        #      垃圾串 → 一律降级为「远古」抑制（age=None 分支与 grace 无关，故在 grace=0
        #      的当前实例上测等价），且**绝不得让异常逃出护栏瘫痪整轮 tick**。 ----
        malformed = [
            _s44_archive("s44-mm-nokey", "exited", 0, _S44_NO_ENDEDAT, report=True),
            _s44_archive("s44-mm-empty", "exited", 0, "", report=True),
            _s44_archive("s44-mm-garbage", "stale", proto.EXITCODE_STALE,
                         "not-a-timestamp", report=True),
        ]
        # 夹具前提直证：第一个档案的 pid.json 里 endedAt 键**整个不在场**（不是 null）。
        with open(os.path.join(_s44_dir("s44-mm-nokey"), "pid.json")) as f:
            assert "endedAt" not in f.read(), "夹具前提：s44-mm-nokey 不得写 endedAt 键"
        time.sleep(1.5)                     # ≥ 7 轮 tick 观察窗（interval 0.2s）
        assert not [m for m in malformed if _s44_notifs(m)], \
            "畸形 endedAt 档案应被护栏当『远古』抑制，不得当新事件通知：%r" % (
                [m for m in malformed if _s44_notifs(m)],)
        log_d = _s44_log()
        # ① 不崩（最毒的一面，先钉）：整段 S44 运行里 notify_tick 一次都没因异常中断过
        #    （崩一轮 = 同轮排在其后的所有参与方真通知被饿死，比起重放噪音严重得多）
        assert "notify_tick error" not in log_d, \
            "畸形档案让 notify_tick 整段中断（异常逃出护栏 → 同轮在其后的真通知饿死）：新增 %d 条" % (
                log_d.count("notify_tick error"),)
        # ② 抑制留痕：三个 id 各自出现在一条「重放护栏」INFO 里（不是笼统计数）
        guard_lines = [ln for ln in log_d.splitlines() if _runner.REPLAY_GUARD_LOG in ln]
        for m in malformed:
            assert any(m in ln for ln in guard_lines), \
                "畸形档案 %s 应逐条留「%s」INFO 痕迹" % (m, _runner.REPLAY_GUARD_LOG)
        # ③ 钉住不变式「畸形档案不得瘫痪整轮」：同树内**排序在后**（list_participants
        #    按 id 排序，s44-zz-* > s44-mm-*）的真终态任务照常收到通知。畸形档案每轮都被
        #    重读，所以若护栏会抛，这里必然超时（不是竞态窗口的偶发漏测）。
        real = _s44_create("s44-zz-mm-real")
        assert malformed[0] < real, "夹具前提：畸形档案须排序在真任务之前"
        wait_until(lambda: (_s44_pid("s44-zz-mm-real") or {}).get("status") == "running",
                   "S44⑥d 畸形档案在后的真任务 running", timeout=10.0)
        with open(os.path.join(_s44_dir("s44-zz-mm-real"), "report.md"), "w") as f:
            f.write("# report\n\u2705 全部验收项通过\n")
        _s44_ctl("send", real, "--type", "inform", "--body", "CMD:exit", "--from", "task/tester")
        _ed, pd = wait_until(
            lambda: _s44_notifs(real)[0] if _s44_notifs(real) else None,
            "S44⑥d 排序在畸形档案之后的真终态仍须照常通知（畸形档案不得瘫痪整轮）",
            timeout=10.0)
        assert pd["event"] == "task_done" and pd.get("report"), pd
        assert "notify_tick error" not in _s44_log(), \
            "真通知落地过程中 notify_tick 仍不得因畸形档案中断"

        # ---- 总回归断言：全部信封里没有一条是历史档案 ----
        allp = [p["taskId"] for _e, p in _s44_notifs()]
        leaked = [t for t in hist + late if t in allp]
        assert not leaked, "历史档案泄漏到通知面：%r" % (leaked,)
    finally:
        _s44_stop(r, hard=True)
        # 隔离树可能残留伪 agent（命令行带 S44ROOT）：S9 的 pkill -f ROOT 同样命中
        # （S44ROOT 以 ROOT 为前缀），此处再收一道，不依赖收尾场景
        subprocess.run(["pkill", "-9", "-f", S44ROOT], capture_output=True, timeout=30)


# ---------------------------------------------------------------- S45（新增）

def _synth_bot(name, active=True):
    """合成信箱型 bot 目录（无 spec.json → 不入进程型监督面，runner 对它无事可做）：充当
    「bot 型收件方」的合成收件面，全程在 e2e 临时树内，不触现网 agents/。
    active=True 补一个非隐藏 `watcher/` 条目（信箱型活性 = 有订阅消费者）；active=False 只建
    inbox/ = **目录在场但无消费者**的反例（活性代理判不活 → 通知回落职位信箱带 note）。"""
    d = os.path.join(ROOT, "agents", "bot", name)
    os.makedirs(os.path.join(d, "inbox"), exist_ok=True)
    if active:
        os.makedirs(os.path.join(d, "watcher"), exist_ok=True)
        open(os.path.join(d, "watcher", "sub-" + name), "w").close()
    return d


def _synth_proc_bot(name, reaper=None, creator="tester", policy="auto",
                    command=None, enable=True):
    """合成**进程型** bot（真跑起来 → 供「task 与 bot 两族同规则」的回归面）：手工写 spec.json
    （§2.1 登记口径，host 物化为本 e2e 节点）+ 可选 enable.json 放行。reaper 直写 spec
    （登记侧的路径式 id 校验与拒绝登记面由扩展侧单测覆盖，此处只验运行侧投递面）。
    @returns {str} 路径式 id（`bot/<名>`）"""
    botdir = os.path.join(ROOT, "agents", "bot", name)
    os.makedirs(os.path.join(botdir, "inbox"), exist_ok=True)
    spec = {"command": command or (FAKE_CMD % (os.path.join(HERE, "fakeagent.py"), ROOT)),
            "workdir": ROOT, "name": "e2e bot " + name, "restartPolicy": policy,
            "creator": creator, "host": "e2ehost", "createdByHost": "e2ehost"}
    if reaper is not None:
        spec["reaper"] = reaper
    proto.atomic_write_json(os.path.join(botdir, "spec.json"), spec)
    pid_ = "bot/" + name
    if enable:
        ctl("enable", pid_, "--by", "e2e")
    return pid_


def _drop_spec_field(pid_, field):
    """删掉 spec.json 的一个字段（e2e 夹具：构造 creator 缺失的历史/人工补录形态）。"""
    p = os.path.join(adir_of(pid_), "spec.json")
    with open(p) as f:
        spec = json.load(f)
    spec.pop(field, None)
    proto.atomic_write_json(p, spec)


def _final_fixture_fresh(pid_, status, exitcode):
    """同 _final_fixture，但时间戳取当下（e2e 夹具）：终态时刻新鲜 → 断言不被任何
    「只报本次运行期间到达终态」类时间判据吞掉（本场景只关心收件面，不依赖终态久远程度）。"""
    now = proto.now_ts()
    with open(os.path.join(adir_of(pid_), "pid.json"), "w") as f:
        json.dump({"gen": 1, "pid": 424242, "status": status, "final": True,
                   "exitcode": exitcode, "restarts": 0, "startedAt": now,
                   "endedAt": now, "lastAliveAt": now}, f)


def _legacy_notif(inbox_dir, pid_, bare=False):
    """预写一条存量终态信封（from=agentd、type=inform、event=task_done）到指定信箱且
    不落 notified.json：构造「判重标记机制上线前已通知过」的存量形态。"""
    os.makedirs(inbox_dir, exist_ok=True)
    tid = pid_.split("/", 1)[1] if bare else pid_
    mid = "2026-09-06-00-00-00.000-agentd-" + proto.rand_suffix()
    with open(os.path.join(inbox_dir, mid + ".msg"), "w") as f:
        json.dump({"id": mid, "from": "agentd", "ts": "2026-09-06-00:00:00.000",
                   "type": "inform",
                   "body": json.dumps({"event": "task_done", "taskId": tid,
                                       "dir": adir_of(pid_), "status": "exited",
                                       "exitcode": 0, "name": "s45-legacy"},
                                      ensure_ascii=False)}, f)
    return mid


def _notify_mark(pid_):
    """读参与方自家目录的终态通知判重标记 notified.json（不在场 → None）。"""
    p = os.path.join(adir_of(pid_), "notified.json")
    if not os.path.exists(p):
        return None
    with open(p) as f:
        return json.load(f)


def s45():
    """终态通知收件面（reaper 单收件方）+ 活性代理回落 + 判重标记唯一事实源：
    ① spec.reaper = 活性 bot → 直投自家信箱（载荷不含 role/reaperPid；职位信箱零份）
       + notified.json 判重标记（`to` 集合、`complete` 短路位）；
    ② reaper = 职位信箱本身 → 直落回落面、不带 note；reaper 字段缺失/文法非法 →
       回落职位信箱 + note 点名成因（creator 回落档已裁）；
    ③ reaper 目录不在场 → note「不存在」；目录在场但无消费者（只有 inbox/ ∨ pid.json 已
       final）→ note 含「无消费者」，其自家信箱零信封；
    ④ 判重（粒度 = (taskId, 收件方)）：同 taskId 多轮 tick + 杀重启 runner 均只发一次（标记
       为权威）；信箱里的存量终态信封**不参与判重**——无标记的档案会重发一条（接受的
       崩溃窗口取舍；历史档案的重放由 S44 重放护栏独立兜住）。"""
    _synth_bot("s45-mod")                       # 活性（watcher/ 有条目）
    _synth_bot("s45-dead", active=False)        # 目录在场、无任何消费者痕迹
    _synth_bot("s45-final", active=False)       # 目录在场、pid.json 已 final
    proto.atomic_write_json(os.path.join(ROOT, "agents", "bot", "s45-final", "pid.json"),
                            {"gen": 1, "pid": 424242, "status": "exited", "final": True,
                             "exitcode": 0})
    kill_runner_hard()      # 夹具阶段：确定性构造 notify_tick 看到的档案形态
    # ① 直投形态（reaper = 活性 bot）
    a = create(["--name", "s45-reaper-bot"], policy="one-shot", gate=False)
    set_sched_fields(a, reaper="bot/s45-mod", name="s45-parity")
    # ② 回落/直落形态
    b = create(["--name", "s45-reaper-pos"], policy="one-shot", gate=False)
    set_sched_fields(b, reaper="queue/dispatcher", name="s45-parity")
    c = create(["--name", "s45-no-reaper"], policy="one-shot", gate=False)
    _drop_spec_field(c, "reaper")               # 缺字段（存量/人工档案形态）
    d = create(["--name", "s45-bad-reaper"], policy="one-shot", gate=False)
    set_sched_fields(d, reaper="bot/a/b")       # 文法非法（人工补录形态）
    f = create(["--name", "s45-ghost-reaper"], policy="one-shot", gate=False)
    set_sched_fields(f, reaper="bot/s45-ghost") # 目录不在场
    # ③ 活性代理反例：目录在场但无消费者（两形）
    i_ = create(["--name", "s45-dead-bot"], policy="one-shot", gate=False)
    j = create(["--name", "s45-final-bot"], policy="one-shot", gate=False)
    set_sched_fields(i_, reaper="bot/s45-dead")
    set_sched_fields(j, reaper="bot/s45-final")
    # ④ 信箱有存量终态信封、无标记 → 重发一条（信封不参与判重的直证）
    g = create(["--name", "s45-envelope-only"], policy="one-shot", gate=False)
    set_sched_fields(g, reaper="bot/s45-mod")
    bot_inbox = os.path.join(ROOT, "agents", "bot", "s45-mod", "inbox")
    _legacy_notif(bot_inbox, g)
    for t in (a, b, c, d, f, i_, j, g):
        _write_report(t)
        _final_fixture_fresh(t, "exited", 0)
    start_runner()

    # ① reaper（活性 bot）直投自家信箱
    hit = wait_until(lambda: bot_notifs("s45-mod", a)[0] if bot_notifs("s45-mod", a) else None,
                     "S45① 终态通知直投活性 reaper 自家信箱")
    env_a, pay_a = hit
    assert env_a["from"] == "agentd" and env_a["type"] == "inform", env_a["from"]
    assert pay_a["event"] == "task_done" and pay_a["taskId"] == a, pay_a
    assert pay_a["dir"] == adir_of(a) and pay_a["status"] == "exited" \
        and pay_a["exitcode"] == 0, pay_a
    assert pay_a["name"] == "s45-parity", pay_a
    assert pay_a["report"] == os.path.join(adir_of(a), "report.md"), pay_a
    assert "warn" not in pay_a, "有 report.md 的正常完成不得带 warn：%r" % pay_a
    assert "role" not in pay_a and "reaperPid" not in pay_a, \
        "载荷不含已退役的 role/reaperPid 字段：%r" % pay_a
    assert pay_a.get("note") is None, "reaper 直投命中不带回落 note：%r" % pay_a
    assert not disp_notifs(a), "reaper 直投命中时不得再往职位信箱投一份"
    mark_a = wait_until(lambda: _notify_mark(a), "S45① notified.json 判重标记落位")
    assert mark_a["to"] == ["bot/s45-mod"] and mark_a["event"] == "task_done" \
        and mark_a.get("ts") and mark_a.get("complete") is True, mark_a
    assert "legacy" not in mark_a, "补写档已裁：标记不带 legacy 字段：%r" % mark_a
    assert os.path.exists(os.path.join(bot_inbox, env_a["id"] + ".msg")), \
        "文件名应为 <id>.msg"

    # ② reaper = 职位信箱本身 → 直落、不带 note；载荷与直投版逐字同构（只换按任务字段）
    hit_b = wait_until(lambda: disp_notifs(b)[0] if disp_notifs(b) else None,
                       "S45② reaper=职位信箱 → 直落回落面")
    env_b, pay_b = hit_b
    assert sorted(env_a) == sorted(env_b), (sorted(env_a), sorted(env_b))
    assert pay_b.get("note") is None, "reaper=职位信箱属正常路径，不带 note：%r" % pay_b
    _per_task = ("taskId", "dir", "report")
    strip = lambda p_: {k: v for k, v in p_.items() if k not in _per_task}
    assert strip(pay_b) == strip(pay_a), \
        "两路载荷除任务自身字段外须逐字一致：%r vs %r" % (pay_a, pay_b)
    assert (_notify_mark(b) or {}).get("to") == ["queue/dispatcher"], _notify_mark(b)

    # ②′ reaper 缺失 / 文法非法 → 回落职位信箱 + note 点名成因
    for t, why in ((c, "reaper 字段缺失"), (d, "reaper 文法非法")):
        pay = wait_until(lambda t=t: disp_notifs(t)[0] if disp_notifs(t) else None,
                         "S45② 回落职位信箱（%s）" % why)[1]
        assert pay["event"] == "task_done" and pay["taskId"] == t, (why, pay)
        assert "缺 reaper 字段" in (pay.get("note") or ""), \
            "%s → note 应点名缺字段成因：%r" % (why, pay)
        assert not bot_notifs("s45-mod", t), "%s 不得投 bot 信箱：%r" % (why, pay)
        assert (_notify_mark(t) or {}).get("to") == ["queue/dispatcher"], (why, _notify_mark(t))
    # reaper 目录不在场 → 回落 + note 点名「不存在」
    pay_f = wait_until(lambda: disp_notifs(f)[0] if disp_notifs(f) else None,
                       "S45② 回落职位信箱（reaper 目录不在场）")[1]
    assert pay_f["note"] == "reaper bot/s45-ghost 不存在，回落职位信箱", pay_f
    assert (_notify_mark(f) or {}).get("to") == ["queue/dispatcher"], _notify_mark(f)

    # ③ 活性代理：目录在场但无消费者（两形）→ 回落 + note 含「无消费者」，自家信箱零信封
    for t, botname, why in ((i_, "s45-dead", "只有 inbox/"), (j, "s45-final", "pid.json 已 final")):
        pay = wait_until(lambda t=t: disp_notifs(t)[0] if disp_notifs(t) else None,
                         "S45③ 无消费者 reaper（%s）回落职位信箱" % why)[1]
        assert "无消费者" in (pay.get("note") or ""), \
            "回落 note 应点名「无消费者」成因（%s）：%r" % (why, pay)
        assert pay["note"].startswith("reaper bot/%s " % botname), pay
        assert notifs_in(os.path.join(ROOT, "agents", "bot", botname, "inbox")) == [], \
            "不活的收件方自家信箱不得落信封（%s）" % why
        assert (_notify_mark(t) or {}).get("to") == ["queue/dispatcher"], (why, _notify_mark(t))

    # ④ 信箱存量信封不参与判重：g 无标记 → 重发一条（1 存量 + 1 重发 = 2），随后标记落位
    hits_g = wait_until(lambda: bot_notifs("s45-mod", g) if len(bot_notifs("s45-mod", g)) >= 2
                        else None, "S45④ 无标记档案重发一条（信封不判重）")
    assert len(hits_g) == 2, hits_g
    mk_g = _notify_mark(g)
    assert mk_g is not None and mk_g["to"] == ["bot/s45-mod"] \
        and mk_g.get("complete") is True and "legacy" not in mk_g, mk_g

    # ④′ 判重：标记落位后多轮 tick 只发一次 + 杀重启 runner 不重发（标记为唯一权威）
    time.sleep(1.0)
    assert len(bot_notifs("s45-mod", a)) == 1, "多轮 tick 重发了直投通知"
    assert len(disp_notifs(b)) == 1, "多轮 tick 重发了职位信箱通知"
    kill_runner_hard()
    start_runner()
    time.sleep(1.5)
    assert len(bot_notifs("s45-mod", a)) == 1, "runner 重启后重发了 bot 信箱通知"
    assert len(bot_notifs("s45-mod", g)) == 2, "runner 重启后重发了已标记档案：%r" % \
        bot_notifs("s45-mod", g)
    assert len(disp_notifs(b)) == 1, "runner 重启后重发了职位信箱通知"


# ---------------------------------------------------------------- S49

# 子端收件扩展：子任务自家信箱的推送收件面。e2e 临时树里没有扩展代码，
# 故驱动按**真仓库绝对路径** import 真 receiver-child.ts + 真 core.ts，对 e2e 临时树
# （含真 runner 写的职位信箱终态通知）跑一次子端收件。
CHILD_RECV_REL = EXT_REL + "/receiver-child.ts"

_S49_DRIVER = r"""
// S49 驱动：真 receiver-child.ts 对 e2e 临时树跑一次子端收件，输出 JSON
// 断言材料。有界等待（10s）+ 显式 process.exit（绕对不得吊住：子任务进程靠 pi 收敛退出）。
import fs from "node:fs";
import path from "node:path";
const root = process.env.AGENTD_ROOT;
const self = process.env.AGENT_SELF;
const extDir = process.env.S49_EXT_DIR;
const core = await import(path.join(extDir, "core.ts"));
const { default: factory } = await import(path.join(extDir, "receiver-child.ts"));
const selfInbox = path.join(root, "agents", ...self.split("/"), "inbox");
fs.mkdirSync(selfInbox, { recursive: true });
const ackOf = (id) => path.join(selfInbox, "ack", id);
const posInbox = path.join(root, "agents", "queue", "dispatcher", "inbox");
const entries = [];
const sink = [];
const handlers = {};
const pi = {
  on: (ev, h) => { handlers[ev] = h; },
  sendUserMessage: (m, o) => { sink.push({ text: String(m), deliverAs: o && o.deliverAs }); },
};
factory(pi);
await handlers["session_start"]({ reason: "startup" },
  { mode: "rpc", sessionManager: { getEntries: () => entries } });
// 模拟 pi flush：注入文本落进会话树 → 落盘确认后写终态 ack（真 pi 侧由 message_end 事件 + poll 兜底）
const flush = () => {
  for (const s of sink) {
    if (!entries.some((e) => e.__src === s.text)) {
      entries.push({ __src: s.text, type: "message", id: "e" + entries.length, parentId: null,
        message: { role: "user", content: [{ type: "text", text: s.text }] } });
    }
  }
};
flush();
const ids = process.env.S49_IDS.split(",");
const dl = Date.now() + 10000;
while (Date.now() < dl && !ids.every((id) => fs.existsSync(ackOf(id)))) {
  await new Promise((r) => setTimeout(r, 50));
  flush();
}
await handlers["session_shutdown"]();
const posAckDir = path.join(posInbox, "ack");
const victim = process.env.S49_VICTIM || "@@none@@";
process.stdout.write(JSON.stringify({
  injected: sink.map((s) => ({ deliverAs: s.deliverAs, head: s.text.slice(0, 140) })),
  allPrefixSelf: sink.length > 0 && sink.every((s) => s.text.startsWith("[" + self + " inbox]")),
  acked: ids.map((id) => fs.existsSync(ackOf(id))),
  posMsgCount: fs.existsSync(posInbox)
    ? fs.readdirSync(posInbox).filter((f) => f.endsWith(".msg")).length : 0,
  posAckDirExists: fs.existsSync(posAckDir),
  posAckEntries: fs.existsSync(posAckDir)
    ? fs.readdirSync(posAckDir, { recursive: true }).map(String) : [],
  mentionsVictim: sink.some((s) => s.text.includes(victim)),
  mentionsDispatcherHead: sink.some((s) => s.text.startsWith("[dispatcher 通知]")),
}));
process.exit(0);
"""


def s49():
    """子任务自家信箱推送收件面：真 receiver-child.ts + 真 core.ts 对 e2e
    临时树跑一次子端收件（写入侧 = 真 agentctl send / 真 runner 终态通知）——
    ① 自家 inbox 的 inform 被认领注入 + 扁平 ack 落笔（落盘确认：注入文本落会话树/jsonl 后才写）；
    ② deliver 两形态：steer → deliverAs=steer（立即介入当前轮）、缺省 → followUp（排队）；
    ③ **0828 回归**：职位信箱里由真 runner 写的终态通知全程零触达（无 ack/ 条目、
      原件留存、注入文本零提及、零 [dispatcher 通知] 抬头）——子任务绕不 drain 非自家信箱。
    注：子任务形态 = `pi --mode rpc`（**不是** print）——封装脚本 CHILD_EXTS 只注入子端扩展，
    主端 index.ts（receiver + print 分支所在）根本不进子任务进程；print 分支与
    `if (!printMode) drain("task_status")` 的 0828 防线逐字未动；printMode 分支的消费者
    = 手工 print 会话（旧命令模板的生产者已归零，判据 = 枚举 `agents/task/*/spec.json`
    的 `command` 含 ` -p` 且不含 `pi-rpc-wrap`、`pid.json` 的 `final != true` 者 = 0）。
    rpc 下 steer/followUp 两队列均为一等公民（pi docs/rpc.md §steer / §follow_up）。"""
    node = shutil.which("node")
    assert node, "S49 需 node（驱动真 TS 扩展）；pi 本体即 node 应用，不应缺失"
    ext_dir = EXT_DIR
    assert os.path.exists(os.path.join(ext_dir, "receiver-child.ts")), \
        "子端收件扩展不在场：%s" % ext_dir
    stop_runner()
    try:
        start_runner(env_extra=_wrap_env("ok"))
        # ① 真 runner 写一封终态通知进职位信箱（0828 诱饵 = 当年被抢走的那类信封）
        victim = create(["--name", "s49-victim"], policy="one-shot", command=WRAP_CMD)
        wait_until(lambda: (pdoc(victim) or {}).get("final") is True,
                   "S49 victim 终态", timeout=30.0)
        notifs = wait_until(lambda: [e for e, _p in disp_notifs(victim)] or None,
                            "S49 职位信箱终态通知到达", timeout=8.0)
        assert len(notifs) == 1, notifs
        pos_inbox = proto.position_inbox(ROOT)
        pos_msgs_before = sorted(glob.glob(os.path.join(pos_inbox, "*.msg")))
        assert pos_msgs_before, "职位信箱应有 runner 写的终态通知原件"

        # ② 收件方任务：只建目录不放行（gate=False → runner 不 spawn），子端收件由驱动跑
        recv = create(["--name", "s49-recv"], policy="one-shot",
                      command=WRAP_CMD, gate=False)
        rinbox = os.path.join(adir_of(recv), "inbox")
        os.makedirs(rinbox, exist_ok=True)
        # 写入侧走真链路：缺省投递用 agentctl send（生产 CLI 路径），steer 形态直写信封
        id_plain = ctl("send", recv, "--type", "inform",
                       "--body", "S49 缺省投递：补充信息", "--from", "task/tester").stdout.strip()
        id_steer = proto.now_ts() + "-" + proto.fs_safe_id("bot/dev-dispatcher") + "-" \
            + proto.rand_suffix()
        proto.atomic_write_json(os.path.join(rinbox, id_steer + ".msg"), {
            "id": id_steer, "from": "bot/dev-dispatcher", "ts": proto.now_ts(),
            "type": "inform", "body": "S49 steer 干预：立刻改做 X", "deliver": "steer"})

        # ③ 驱动真扩展（有界：内部 10s 兼底 + 外部 timeout）
        driver = os.path.join(ROOT, "run", "s49-child-driver.mjs")
        os.makedirs(os.path.dirname(driver), exist_ok=True)
        with open(driver, "w", encoding="utf-8") as f:
            f.write(_S49_DRIVER)
        env = scrub_env()
        env.update({"AGENTD_ROOT": ROOT, "AGENT_SELF": recv,
                    "AGENTD_NO_WATCH": "1", "AGENTD_CHILD_POLL_MS": "50",
                    "S49_EXT_DIR": ext_dir, "S49_VICTIM": victim,
                    "S49_IDS": ",".join([id_steer, id_plain])})
        r = subprocess.run([node, driver], capture_output=True, text=True,
                           timeout=90, env=env, cwd=ROOT)
        assert r.returncode == 0, "驱动退出码 %s\nstdout=%s\nstderr=%s" % (
            r.returncode, r.stdout[:500], r.stderr[:1500])
        out = json.loads(r.stdout)

        # ④ 自家信箱：两封均注入 + 扁平 ack 落笔
        assert out["acked"] == [True, True], "自家 inform 应全部提升为终态 ack：%r" % out
        assert len(out["injected"]) == 2, "应注入 2 条：%r" % out["injected"]
        assert out["allPrefixSelf"] is True, \
            "抬头应为自家信箱既有约定 [task/<id> inbox]：%r" % out["injected"]
        assert os.path.exists(os.path.join(rinbox, "ack", id_steer)), "steer 信封扁平 ack 在场"
        assert os.path.exists(os.path.join(rinbox, "ack", id_plain)), "缺省信封扁平 ack 在场"
        # ⑤ deliver 两形态
        # 按注入文本里的**正文片段**配对（不依赖注入顺序：drain 按文件名时序认领）
        steer_as = [i["deliverAs"] for i in out["injected"] if "S49 steer 干预" in i["head"]]
        plain_as = [i["deliverAs"] for i in out["injected"] if "S49 缺省投递" in i["head"]]
        assert steer_as == ["steer"], "deliver:steer → deliverAs=steer（立即介入当前轮）：%r" % out
        assert plain_as == ["followUp"], "缺省（字段不写）→ deliverAs=followUp（排队）：%r" % out
        # ⑥ 0828 回归：职位信箱零触达
        assert out["posAckDirExists"] is False or out["posAckEntries"] == [], \
            "职位信箱不得出现任何 ack 条目（子任务抢 ack = 0828 事故）：%r" % out
        assert not os.path.exists(os.path.join(pos_inbox, "ack", notifs[0]["id"])), \
            "终态通知未被 ack"
        assert sorted(glob.glob(os.path.join(pos_inbox, "*.msg"))) == pos_msgs_before, \
            "终态通知原件留存（不删不移不消费）"
        assert out["mentionsVictim"] is False, "注入文本零提及 victim 任务：%r" % out
        assert out["mentionsDispatcherHead"] is False, \
            "零 [dispatcher 通知] 抬头注入（职位信箱面零触达）：%r" % out
        assert out["posMsgCount"] == len(pos_msgs_before), \
            "职位信箱信封数不变：%r" % out
    finally:
        stop_runner()
        start_runner()


# ------------------------------------------- S50（新增：子端收件面 P0 修复）

# e2e 临时树里没有扩展代码，而 wrap 的 CHILD_EXTS 按 $AGENT_ROOT 相对路径判存在性注入
# （缺失 → WARN 跳过 → child_recv_injected 为假 → 就绪握手不成立）。故把真仓库扩展目录
# **软链**进临时树（不改真仓、不复制大文件；os.path.exists 跟随软链）。
CHILD_EXT_SRC = EXT_DIR


def _ensure_child_ext_in_root():
    dst = os.path.join(ROOT, *EXT_PARTS)
    if os.path.exists(dst):
        return dst
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    os.symlink(CHILD_EXT_SRC, dst)
    return dst


def _race_cmd(mode, extra_env=()):
    """任务命令夹具：写真 prompt.md + 每任务独立 FAKE_FLAG_DIR + 逐任务覆盖 FAKE_MODE/就绪门开关
    （env 前缀在 bash -c 内生效，wrap 再把自己 environ 透给 fake pi；不需重启 runner）。"""
    envs = " ".join(["FAKE_MODE=%s" % mode, 'FAKE_FLAG_DIR="$AGENT_HOME/flags"',
                     "AGENTD_WRAP_INIT_TIMEOUT=10", "AGENTD_WRAP_ARM_TIMEOUT=3"]
                    + list(extra_env))
    return ('echo "e2e S50 race prompt" > "$AGENT_HOME/prompt.md" && '
            'mkdir -p "$AGENT_HOME/flags" && %s exec python3 %s %s'
            % (envs, WRAP, ROOT))


RACE_BODIES = ("e2e S50 合成干预 #1", "e2e S50 合成干预 #2", "e2e S50 合成干预 #3")


def _race_inbox(pid_, bodies, with_reply=True):
    """spawn 前把合成信封写进任务自家 inbox（= 「调度员在任务 spawn 前投 inform」的合法用法，
    修后必须仍可被正常消费）。另带一封 reply 作 🔴2 诱饵（子端不得认领）。"""
    inbox = os.path.join(adir_of(pid_), "inbox")
    os.makedirs(inbox, exist_ok=True)
    ids = []
    for b in bodies:
        mid = proto.file_id()
        proto.atomic_write_json(os.path.join(inbox, mid + ".msg"), {
            "id": mid, "from": "bot/tester", "ts": proto.now_ts(),
            "type": "inform", "body": b})
        ids.append(mid)
    rid = None
    if with_reply:
        rid = proto.file_id()
        proto.atomic_write_json(os.path.join(inbox, rid + ".msg"), {
            "id": rid, "from": proto.POSITION_PID, "ts": proto.now_ts(),
            "type": "reply", "ref": "e2e-s50-ask", "body": "e2e S50 合成答复"})
    return ids, rid


def _race_run(name, mode, extra_env=(), bodies=RACE_BODIES, with_reply=True,
              timeout=60.0):
    """登记（不放行）→ spawn 前写信封 → 放行 → 等终态。返回 (pid_, inform_ids, reply_id)。"""
    a = create(["--name", name], policy="one-shot",
               command=_race_cmd(mode, extra_env), gate=False)
    ids, rid = _race_inbox(a, list(bodies), with_reply=with_reply)
    ctl("enable", a, "--by", "e2e")
    wait_until(lambda: (pdoc(a) or {}).get("final") is True,
               "%s 终态" % name, timeout=timeout)
    return a, ids, rid


def _race_facts(a):
    """一个竞态用例的取证面（判据沉淀：exit 0 不足以证明执行过；session.jsonl 在场也只是必要
    非充分——硬判据 = 会话树含初始 prompt 的 user 事件 🟡3）。"""
    adir = adir_of(a)
    name = a.split("/", 1)[1]
    sess = os.path.join(adir, "session", "session.jsonl")
    flags = os.path.join(adir, "flags")
    fpath = lambda n: os.path.join(flags, n)  # noqa: E731
    fmtime = lambda n: (os.stat(fpath(n)).st_mtime  # noqa: E731
                        if os.path.exists(fpath(n)) else None)
    diag = os.path.join(adir, "diagnosis.md")
    return {
        "doc": pdoc(a), "sess": sess,
        "sess_text": (open(sess, encoding="utf-8").read()
                      if os.path.exists(sess) else ""),
        "diag": (open(diag, encoding="utf-8").read()
                 if os.path.exists(diag) else ""),
        "flags": flags, "fmtime": fmtime,
        "flagset": set(os.listdir(flags)) if os.path.isdir(flags) else set(),
        "ackdir": os.path.join(adir, "inbox", "ack"),
        "marks": [proto.task_ready_path(ROOT, name, k) for k in proto.TASK_READY_KINDS],
    }


def _assert_healthy(a, f, ids, rid, bodies=RACE_BODIES, label=""):
    """健康路径公共断言（A/D/E/F 共用）：exit 0 且 **session.jsonl 在场并含全部注入文本**。"""
    assert f["doc"]["status"] == "exited" and f["doc"]["exitcode"] == 0, \
        "%s 应健康收敛：%r" % (label, f["doc"])
    assert os.path.exists(f["sess"]), (
        "%s 带未读信封 spawn 的子任务必须有 session.jsonl（exit 0 不足以证明执行过；"
        "本断言只是必要面，硬判据 = 会话树含初始 prompt 的 user 事件，见 S53）" % label)
    for b in bodies:
        assert b in f["sess_text"], "%s 注入文本应落会话树：%s" % (label, b)
    for mid in ids:
        assert os.path.exists(os.path.join(f["ackdir"], mid)), \
            "%s spawn 前落地的信封应被消费（ack 在场）：%s" % (label, mid)
    if rid:
        assert not os.path.exists(os.path.join(f["ackdir"], rid)), \
            "%s 🔴2 reply 不得被子端认领（权威消费者 = waitForReply）" % label
        assert not [x for x in os.listdir(f["ackdir"]) if x.endswith(".pending")] \
            if os.path.isdir(f["ackdir"]) else True, \
            "%s 在飞态无盘上形态（记账件已退役：认领只在 receiver 进程内存）" % label
        assert os.path.exists(os.path.join(adir_of(a), "inbox", rid + ".msg")), \
            "%s reply 原件留存（不删不移）" % label
    assert f["diag"] == "", "%s 健康路径不写诊断：%s" % (label, f["diag"][:200])
    assert "gate_open" in f["flagset"] and "gate_bypassed" not in f["flagset"], \
        "%s 子端应经就绪门开门（非抢跑）：%r" % (label, sorted(f["flagset"]))
    assert f["fmtime"]("prompt") is not None and f["fmtime"]("gate_open") is not None \
        and f["fmtime"]("prompt") <= f["fmtime"]("gate_open"), \
        "%s 时序：初始 prompt 被接受早于子端开门" % label
    assert not any(os.path.exists(m) for m in f["marks"]), \
        "%s 就绪标记退出即清：%r" % (label, f["marks"])
    notifs = wait_until(lambda: [p for _e, p in disp_notifs(a)] or None,
                        "%s 终态通知" % label, timeout=8.0)
    assert len(notifs) == 1 and notifs[0]["event"] == "task_done", notifs
    return notifs[0]


def s50():
    """🔴0 P0 spawn 竞态：子端收件面在初始 prompt 被 pi 接受**之前** drain 自家
    inbox 并注入 → pi 侧抢跑起轮，两种现网形态都必须消失：
      A 健康路径（就绪门生效）：exit 0 ∧ session.jsonl 在场含全部注入 ∧ 信封全 ack ∧
        reply 零认领（🔴2）∧ 无诊断 ∧ task_done ∧ 时序（prompt ≤ gate_open）∧ 标记退出即清；
      B 形态①（门被绕过）：exit 1 ∧ diagnosis stage=prompt_rejected ∧ session.jsonl 不在场
        （实证 02:38:15 / 04:53:04）；
      C 形态②（门被绕过）：**exit 0 假成功** ∧ session.jsonl 不在场 ∧ 零执行却报 task_done
        （实证 02:38:12：同秒 3 封未读、inbox 里 2 枚 .pending 停飞）；
      D 变异对照：B/C 两模式各加 FAKE_CHILD_HONOR_GATE=1（唯一差异 = 是否尊重就绪门）→
        均回到健康路径（证明门就是区分因子，断言真咬）；
      E 陈旧标记：预置上一代 init-ok/recv-armed → wrap spawn 前必清（否则骗开本代门）；
      F arm 有界：子端永不回写 recv-armed → wrap 有界等待后照常收敛（不假活）+ stderr WARN。
    注：fake pi 逐字复现两侧真实行为——pi `agent-session.js:833` 的「Agent is already
    processing」拒收串、`sendUserMessage` 空闲即起轮、`session-manager.js::_persist` 的
    no-assistant guard（无 assistant 事件则 session.jsonl 永不落盘）。"""
    stop_runner()
    try:
        _ensure_child_ext_in_root()
        start_runner(env_extra=_wrap_env("ok"))

        # ---- A 健康路径（含 spawn 前落地的 3 封 inform + 1 封 reply 诱饵）----
        a, ids, rid = _race_run("s50-healthy", "child_race")
        fa = _race_facts(a)
        _assert_healthy(a, fa, ids, rid, label="S50A")
        assert "race_landed_%d" % len(ids) in fa["flagset"], \
            "S50A 全部注入应落树：%r" % sorted(fa["flagset"])
        assert "recv_armed_written" in fa["flagset"], \
            "S50A 子端应回写 recv-armed（wrap 有界等它之后才进收敛监督；wrap 侧等待/WARN 断言在 test_wrap T30a/T30f）"
        assert fa["fmtime"]("gate_open") <= fa["fmtime"]("recv_armed_written"), \
            "S50A arm 应在开门补扫之后回写"

        # ---- B 形态①：exit 1 秒死 ----
        b, ids_b, _rid_b = _race_run("s50-reject", "child_race_reject")
        fb = _race_facts(b)
        assert fb["doc"]["exitcode"] == 1, "S50B 形态① 应 exit 1：%r" % fb["doc"]
        assert "prompt_rejected" in fb["diag"], \
            "S50B diagnosis 应记 stage=prompt_rejected：%s" % fb["diag"][:200]
        assert "Agent is already processing" in fb["diag"], \
            "S50B 诊断应带 pi 逐字拒收串（与现网观测同形）"
        assert not os.path.exists(fb["sess"]), \
            "S50B 形态①：session.jsonl 不在场（零执行）"
        assert "gate_bypassed" in fb["flagset"], "S50B 应走绕过门的抢跑路径"
        nb = wait_until(lambda: [p for _e, p in disp_notifs(b)] or None,
                        "S50B 终态通知", timeout=8.0)
        assert nb[0]["event"] == "task_failed", nb

        # ---- C 形态②：exit 0 假成功（比秒死更危险）----
        c, ids_c, _rid_c = _race_run("s50-fake", "child_race_fakesuccess")
        fc = _race_facts(c)
        assert fc["doc"]["status"] == "exited" and fc["doc"]["exitcode"] == 0, \
            "S50C 形态② 应是 exit 0（假成功）：%r" % fc["doc"]
        assert not os.path.exists(fc["sess"]), \
            "S50C 形态②：零执行、session.jsonl 不在场"
        assert fc["diag"] == "", "S50C 形态②：无任何诊断（静默假成功）"
        assert "prompt_accepted_no_turn" in fc["flagset"], \
            "S50C prompt 被接受但零轮次（现网日志同形）：%r" % sorted(fc["flagset"])
        nc = wait_until(lambda: [p for _e, p in disp_notifs(c)] or None,
                        "S50C 终态通知", timeout=8.0)
        assert nc[0]["event"] == "task_done", \
            "S50C 形态②被判成功（这就是假成功的危险面）：%r" % nc[0]
        assert nc[0].get("warn") == "no_report", (
            "S50C 既有的唯一一张网 = warn=no_report（报告面），故判据必须核**会话树含初始"
            " prompt 的 user 事件**（或 report.md 在场），而不是「session.jsonl 在场」：%r" % nc[0])

        # ---- D 变异对照：同一模式只差「是否尊重就绪门」----
        for label, name, mode in (("S50D-①", "s50-reject-honor", "child_race_reject"),
                                  ("S50D-②", "s50-fake-honor", "child_race_fakesuccess")):
            d, ids_d, rid_d = _race_run(name, mode,
                                        extra_env=("FAKE_CHILD_HONOR_GATE=1",))
            _assert_healthy(d, _race_facts(d), ids_d, rid_d, label=label)

        # ---- E 陈旧标记：spawn 前必清（不得骗开本代就绪门）----
        e = create(["--name", "s50-stale-mark"], policy="one-shot",
                   command=_race_cmd("child_race"), gate=False)
        ids_e, rid_e = _race_inbox(e, list(RACE_BODIES))
        for kind, why in (("init-ok", "STALE-上一代残留"), ("recv-armed", "STALE")):
            p = proto.task_ready_path(ROOT, e.split("/", 1)[1], kind)
            os.makedirs(os.path.dirname(p), exist_ok=True)
            proto.atomic_write_json(p, {"why": why})
        ctl("enable", e, "--by", "e2e")
        wait_until(lambda: (pdoc(e) or {}).get("final") is True, "S50E 终态", timeout=60.0)
        fe = _race_facts(e)
        _assert_healthy(e, fe, ids_e, rid_e, label="S50E")
        doc_path = os.path.join(fe["flags"], "init_ok_doc")
        doc_txt = open(doc_path, encoding="utf-8").read() if os.path.exists(doc_path) else ""
        assert "STALE" not in doc_txt and "prompt-accepted" in doc_txt, \
            "S50E 本代 init-ok 应由本次投递写入（陈旧标记已被清）：%r" % doc_txt[:200]

        # ---- F arm 有界等待（子端永不 arm）----
        f_, ids_f, rid_f = _race_run(
            "s50-noarm", "child_race",
            extra_env=("FAKE_CHILD_NO_ARM=1", "AGENTD_WRAP_ARM_TIMEOUT=1"))
        ff = _race_facts(f_)
        _assert_healthy(f_, ff, ids_f, rid_f, label="S50F")
        assert "recv_armed_written" not in ff["flagset"], \
            "S50F 夹具设定子端不回写 arm：%r" % sorted(ff["flagset"])
        # 子端永不 arm 仍照常收敛（wrap 有界等待 → 不假活；WARN 断言在 test_wrap T30f）
    finally:
        stop_runner()
        start_runner()


# --------------------------------------------- S53（真 pi live 测试要求 ③）

_LIVE_REPLY = "STUBLIVE-REPLY-MARKER ok"


def _start_stub_provider():
    """localhost OpenAI 兼容桩供应商（真 pi live 场景用，仅标准库）：SSE 流式回一条固定
    assistant 文本，足以驱动真 pi 的一轮（实测 pi 的 openai-completions 客户端恒 stream=true）。
    @returns (port, shutdown_fn)"""
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class _H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def do_GET(self):
            body = json.dumps({"object": "list", "data": [
                {"id": "stub-model", "object": "model",
                 "owned_by": "stub"}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            if n:
                self.rfile.read(n)
            if not self.path.endswith("/chat/completions"):
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()

            def chunk(obj):
                b = ("data: " + json.dumps(obj, ensure_ascii=False) + "\n\n").encode()
                self.wfile.write(b"%x\r\n" % len(b) + b + b"\r\n")
                self.wfile.flush()

            base = {"id": "chatcmpl-stub", "object": "chat.completion.chunk",
                    "created": int(time.time()), "model": "stub-model"}
            chunk(dict(base, choices=[{"index": 0,
                                       "delta": {"role": "assistant"},
                                       "finish_reason": None}]))
            for w in _LIVE_REPLY.split(" "):
                chunk(dict(base, choices=[{"index": 0,
                                           "delta": {"content": w + " "},
                                           "finish_reason": None}]))
                time.sleep(0.02)
            chunk(dict(base, choices=[{"index": 0, "delta": {},
                                       "finish_reason": "stop"}]))
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()

    srv = ThreadingHTTPServer(("127.0.0.1", 0), _H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv.server_address[1], srv.shutdown


def _live_pi_setup():
    """真 pi live 三件套：桩供应商 + `PI_CODING_AGENT_DIR` 合成配置（不碰真 ~/.pi）+ pi 包装
    脚本（钉死 --model；wrap 的 AGENTD_WRAP_PI_BIN 指向它）。pi 不在 PATH → None（显式 skip）。"""
    pi_bin = shutil.which("pi")
    if not pi_bin:
        return None
    port, shutdown = _start_stub_provider()
    cfg = os.path.join(TMPBASE, "pi-live-config")
    os.makedirs(cfg, exist_ok=True)
    with open(os.path.join(cfg, "models.json"), "w", encoding="utf-8") as f:
        json.dump({"providers": {"stub": {
            "baseUrl": "http://127.0.0.1:%d/v1" % port,
            "api": "openai-completions", "apiKey": "stub-key",
            "compat": {"supportsDeveloperRole": False,
                       "supportsReasoningEffort": False},
            "models": [{"id": "stub-model", "name": "stub model",
                        "input": ["text"],
                        "cost": {"input": 0, "output": 0, "cacheRead": 0,
                                 "cacheWrite": 0},
                        "contextWindow": 32768, "maxTokens": 1024}]}}}, f)
    binp = os.path.join(TMPBASE, "pi-live-stub.sh")
    with open(binp, "w", encoding="utf-8") as f:
        f.write("#!/usr/bin/env bash\nexec %s --model stub/stub-model \"$@\"\n"
                % pi_bin)
    os.chmod(binp, 0o755)
    return {"cfg": cfg, "bin": binp, "shutdown": shutdown, "port": port}


def _live_cmd(live):
    """任务命令夹具：真 pi（经包装脚本）+ 合成配置目录 + 加速落盘确认的 poll 兜底。
    PI_CODING_AGENT_DIR 必须由命令前缀带（runner spawn 会按 envscrub 剥掉 PI_ 前缀族）。"""
    # AGENTD_ROOT 必须显式给：core.rootDir() 优先读它，缺失才回落到「按扩展文件位置推仓库根」
    # ——生产两者同值（<workspace-root> 既是仓库根又是 AGENT_ROOT），但 e2e 临时树的扩展是**软链进真仓快照**的，
    # 回落会把 root 解到快照仓（实测形态：session_start 不装配、日志落错树）。runner 只注入
    # AGENT_ROOT（wrap 侧口径），故由命令前缀补 AGENTD_ROOT（扩展侧口径），两值同源同树。
    return ('echo "e2e S53 live prompt" > "$AGENT_HOME/prompt.md" && '
            'PI_CODING_AGENT_DIR=%s AGENTD_WRAP_PI_BIN=%s AGENTD_ROOT=%s '
            'AGENTD_WRAP_INIT_TIMEOUT=45 AGENTD_WRAP_ARM_TIMEOUT=15 '
            'AGENTD_WRAP_SETTLE_WINDOW=1.5 AGENTD_CHILD_POLL_MS=200 '
            'exec python3 %s %s'
            % (live["cfg"], live["bin"], ROOT,
               WRAP, ROOT))


def s53():
    """真 pi live 覆盖（测试要求 ③； report 遗留 5 自认「未做真实 live 会话
    验证」，其 S49 以 node 直驱扩展且 AGENTD_NO_WATCH=1）：真 `pi --mode rpc`（打桩供应商 +
    PI_CODING_AGENT_DIR 合成配置）+ 真 runner + 真 wrap + 真 receiver-child.ts/core.ts，覆盖
    fake pi 与 node 直驱都碰不到的三件事：
      ① 真 inotify（fs.watch，非 AGENTD_NO_WATCH 降级）路径认领自家 inbox；
      ② 真 steer/followUp 队列：注入文本真进 pi 会话树（session.jsonl 可读回）；
      ③ 真落盘确认：注入文本逐字出现在 session.jsonl → receiver 写终态 ack（在飞态只在
        进程内存，盘上无中间形态；确认触发 = `message_end(role=user)` 事件 + drain 轮首兜底）；
    并交叉验证 🔴0 就绪门在真 pi 下成立（初始 prompt 真被处理：**会话树含初始 prompt 的 user
    事件**——不是「有 user+assistant」，抢跑形态下后者也成立（🟡3 的真 pi 反例），
    即非「exit 0 假成功」形态）与 🔴2（reply 零认领零注入）。pi 不在 PATH → 显式 skip。"""
    live = _live_pi_setup()
    if live is None:
        PLATFORM_SKIPS.append(("S53", "pi 不在 PATH（真 live 场景需 pi 本体）"))
        print("SKIP  S53 真 pi live（pi 不在 PATH）", flush=True)
        return
    stop_runner()
    try:
        _ensure_child_ext_in_root()
        start_runner()
        a = create(["--name", "s53-live"], policy="one-shot",
                   command=_live_cmd(live), gate=False)
        inbox = os.path.join(adir_of(a), "inbox")
        os.makedirs(inbox, exist_ok=True)
        envs = []
        for body, deliver in (("e2e S53 合成干预 steer", "steer"),
                              ("e2e S53 合成干预 followUp", None)):
            mid = proto.file_id()
            doc = {"id": mid, "from": "bot/tester", "ts": proto.now_ts(),
                   "type": "inform", "body": body}
            if deliver:
                doc["deliver"] = deliver
            proto.atomic_write_json(os.path.join(inbox, mid + ".msg"), doc)
            envs.append((mid, body))
        rid = proto.file_id()
        proto.atomic_write_json(os.path.join(inbox, rid + ".msg"), {
            "id": rid, "from": proto.POSITION_PID, "ts": proto.now_ts(),
            "type": "reply", "ref": "e2e-s53-ask",
            "body": "e2e S53 合成答复（子端不得注入）"})
        ctl("enable", a, "--by", "e2e")
        wait_until(lambda: (pdoc(a) or {}).get("final") is True,
                   "S53 真 pi 任务终态", timeout=150.0)
        d = pdoc(a)
        assert d["status"] == "exited" and d["exitcode"] == 0, d
        sess = os.path.join(adir_of(a), "session", "session.jsonl")
        assert os.path.exists(sess), \
            "真 pi 会话必须落 session.jsonl（判据沉淀：exit 0 不足以证明执行过；下一行才是硬判据）"
        txt = open(sess, encoding="utf-8").read()
        assert "e2e S53 live prompt" in txt, "初始 prompt 真被 pi 处理（user 事件在会话树）"
        assert "STUBLIVE-REPLY-MARKER" in txt and "\"assistant\"" in txt, \
            "供应商答复真落会话树（assistant 事件在场 → 非假成功形态）"
        for _mid, body in envs:
            assert body in txt, "注入文本真落会话树（真 steer/followUp 队列）：%s" % body
        assert "e2e S53 合成答复" not in txt, "🔴2 reply 不得被子端注入"
        ackdir = os.path.join(inbox, "ack")
        for mid, _b in envs:
            assert os.path.exists(os.path.join(ackdir, mid)), \
                "真落盘确认：注入文本进 session.jsonl 后写终态 ack：%s" % mid
            doc = proto.read_json(os.path.join(ackdir, mid)) or {}
            assert isinstance(doc.get("claimedTs"), str) and "attempts" not in doc, \
                "终态 ack 带 claimedTs（认领→落盘时延可复算）、不带 attempts（不重投语义）：%r" % doc
        assert not [x for x in os.listdir(ackdir) if x.endswith(".pending")], \
            "在飞态无盘上形态（记账件已退役：认领只在 receiver 进程内存）"
        assert not os.path.exists(os.path.join(ackdir, rid)), \
            "🔴2 reply 零认领（无终态 ack）"
        logp = os.path.join(ROOT, "run", "logs", "agentd-receiver.log")
        assert os.path.exists(logp), "缺子端收件日志（真扩展未装配？）"
        lg = open(logp, encoding="utf-8").read()
        assert "watch=1 目录" in lg, \
            "真 inotify（fs.watch）路径已挂（非 AGENTD_NO_WATCH 降级）：%s" % lg[-300:]
        assert "就绪门开" in lg and "就绪门超时" not in lg, \
            "🔴0 就绪门在真 pi 下按 init-ok 开门（非超时兜底）：%s" % lg[-300:]
        notifs = wait_until(lambda: [p for _e, p in disp_notifs(a)] or None,
                            "S53 终态通知", timeout=10.0)
        assert notifs[0]["event"] == "task_done", notifs
    finally:
        live["shutdown"]()
        stop_runner()
        start_runner()


# ---------------------------------------------------------------- S54（reaper 主模型）

def s54():
    """终态通知收件面 = **{reaper}**（watchers 旁观面已裁）：
    ① reaper=bot/<合成调度员型 bot> → 只投它、职位信箱零份、载荷不含 role/reaperPid；
    ② reaper=bot/<合成主持人型 bot> → 同样直投其信箱（登记方角色三档分类属登记侧，
       由扩展侧单测覆盖；此处只验运行侧投递面）；
    ③ spec 残留 watchers 字段 → **零作用**（不投递、不建目录、标记不含 watcher）；
    ④ (taskId,收件方) 判重 → 多轮 tick + 杀重启后只一份 ∧ 标记 to=[reaper]、complete；
    ⑤ reaper=bot/<不存在> → 回落职位信箱 ∧ note 含「不存在，回落职位信箱」∧ 不建目录。"""
    _synth_bot("s54-disp")
    _synth_bot("s54-mod")
    kill_runner_hard()
    # ① 调度员型登记（reaper = 自家信箱）
    t1 = create(["--name", "s54-disp-task"], policy="one-shot", gate=False)
    set_sched_fields(t1, creator="bot/s54-disp", reaper="bot/s54-disp")
    # ② 主持人型登记
    t2 = create(["--name", "s54-mod-task"], policy="one-shot", gate=False)
    set_sched_fields(t2, creator="bot/s54-mod", reaper="bot/s54-mod")
    # ③ 残留 watchers 字段（已退役：运行侧零消费）
    t3 = create(["--name", "s54-legacy-watchers"], policy="one-shot", gate=False)
    set_sched_fields(t3, creator="bot/s54-disp", reaper="bot/s54-disp",
                     watchers=["bot/s54-w1", "topic/s54-t1"])
    # ⑤ reaper 不存在 → 回落
    t5 = create(["--name", "s54-ghost"], policy="one-shot", gate=False)
    set_sched_fields(t5, creator="bot/s54-disp", reaper="bot/s54-ghost")
    for t in (t1, t2, t3, t5):
        _write_report(t)
        _final_fixture_fresh(t, "exited", 0)
    start_runner()

    # ① 只投 reaper 自家信箱，职位信箱零份
    p1 = wait_until(lambda: bot_notifs("s54-disp", t1)[0] if bot_notifs("s54-disp", t1) else None,
                    "S54① 通知落 reaper 自家信箱")[1]
    assert p1["event"] == "task_done" and p1["taskId"] == t1, p1
    assert "role" not in p1 and "reaperPid" not in p1, \
        "载荷不含已退役的 role/reaperPid 字段：%r" % p1
    assert not disp_notifs(t1), "reaper 解析命中 → 职位信箱不得收终态通知（已退出默认面）"
    assert (_notify_mark(t1) or {}).get("to") == ["bot/s54-disp"], _notify_mark(t1)

    # ② 主持人型 reaper 同样直投自家信箱
    p2 = wait_until(lambda: bot_notifs("s54-mod", t2)[0] if bot_notifs("s54-mod", t2) else None,
                    "S54② 主持人 reaper 直投")[1]
    assert "role" not in p2, p2
    assert not disp_notifs(t2), "主持人 reaper 在场 → 职位信箱零份"

    # ③ 残留 watchers 字段零作用：不投递、不建目录
    p3 = wait_until(lambda: bot_notifs("s54-disp", t3)[0] if bot_notifs("s54-disp", t3) else None,
                    "S54③ 残留 watchers 不影响 reaper 投递")[1]
    assert p3["taskId"] == t3, p3
    assert not os.path.exists(os.path.join(ROOT, "agents", "bot", "s54-w1")), \
        "watchers 字段已退役：不得为其建目录"
    assert not os.path.exists(os.path.join(ROOT, "agents", "topic", "s54-t1")), \
        "watchers 字段已退役：topic 族同样零副作用"
    assert (_notify_mark(t3) or {}).get("to") == ["bot/s54-disp"], _notify_mark(t3)

    # ⑤ reaper 不存在 → 回落职位信箱带 note（不建目录）
    p5 = wait_until(lambda: disp_notifs(t5)[0] if disp_notifs(t5) else None,
                    "S54⑤ reaper 不存在 → 回落职位信箱")[1]
    assert p5["note"] == "reaper bot/s54-ghost 不存在，回落职位信箱", p5
    assert not os.path.exists(os.path.join(ROOT, "agents", "bot", "s54-ghost")), \
        "回落面不得为不存在的 reaper 建目录"
    assert (_notify_mark(t5) or {}).get("to") == ["queue/dispatcher"], _notify_mark(t5)

    # ④ (taskId,收件方) 判重：多轮 tick + 杀重启后只一份
    time.sleep(1.0)
    assert len(bot_notifs("s54-disp", t1)) == 1 and len(bot_notifs("s54-disp", t3)) == 1, \
        "多轮 tick 重发了 reaper 通知"
    kill_runner_hard()
    start_runner()
    time.sleep(1.5)
    for who, reader in (("t1", lambda: bot_notifs("s54-disp", t1)),
                        ("t3", lambda: bot_notifs("s54-disp", t3)),
                        ("t5", lambda: disp_notifs(t5))):
        assert len(reader()) == 1, "runner 重启后重发了 %s 的通知：%r" % (who, reader())
    assert (_notify_mark(t3) or {}).get("complete") is True, _notify_mark(t3)


# ---------------------------------------------------------------- S55（bot 族自身终态）

def s55():
    """bot 族自身终态（文档声明「task 与 bot 两族同规则」的回归面）：合成**进程型** bot
    （spec.reaper=bot/<合成名>、restartPolicy=auto）真跑起来 → `agentctl control stop` →
    终态通知落 **reaper 信箱**（event=task_canceled、载荷不含 role/reaperPid）+ 标记落位
    （to=[reaper]）+ 职位信箱零份；对照 = 无 reaper 字段的进程型 bot → 回落职位信箱 + note。"""
    _synth_bot("s55-reaper")
    _synth_bot("s55-mod")
    kill_runner_hard()
    b1 = _synth_proc_bot("s55-bot", reaper="bot/s55-reaper")
    b2 = _synth_proc_bot("s55-legacy", creator="bot/s55-mod")   # 无 reaper 字段
    start_runner()
    wait_until(lambda: (pdoc(b1) or {}).get("status") == "running", "S55 进程型 bot 运行",
               timeout=15.0)
    wait_until(lambda: (pdoc(b2) or {}).get("status") == "running", "S55 对照 bot 运行",
               timeout=15.0)
    ctl("control", b1, "stop", "--from", "task/tester", "--reason", "S55 散会收口")
    ctl("control", b2, "stop", "--from", "task/tester", "--reason", "S55 对照散会收口")
    wait_until(lambda: (pdoc(b1) or {}).get("final") is True, "S55 bot final", timeout=15.0)
    wait_until(lambda: (pdoc(b2) or {}).get("final") is True, "S55 对照 bot final", timeout=15.0)

    p1 = wait_until(lambda: bot_notifs("s55-reaper", b1)[0]
                    if bot_notifs("s55-reaper", b1) else None,
                    "S55 bot 族终态通知落 reaper 信箱", timeout=10.0)[1]
    assert p1["event"] == "task_canceled" and p1["taskId"] == b1, p1
    assert "role" not in p1 and "reaperPid" not in p1, p1
    assert p1["dir"] == adir_of(b1), p1
    assert not disp_notifs(b1), "bot 族终态：reaper 命中 → 职位信箱零份"
    mk1 = wait_until(lambda: _notify_mark(b1), "S55 标记落位", timeout=6.0)
    assert mk1["to"] == ["bot/s55-reaper"] and mk1["event"] == "task_canceled" \
        and mk1.get("complete") is True, mk1

    p2 = wait_until(lambda: disp_notifs(b2)[0] if disp_notifs(b2) else None,
                    "S55 对照：无 reaper 字段 → 回落职位信箱", timeout=10.0)[1]
    assert p2["event"] == "task_canceled" and p2["taskId"] == b2, p2
    assert "缺 reaper 字段" in (p2.get("note") or ""), \
        "回落应带 note 点名缺字段成因（creator 回落档已裁）：%r" % p2
    assert not bot_notifs("s55-mod", b2), "creator 不再是收件面：其信箱零份"
    assert (_notify_mark(b2) or {}).get("to") == ["queue/dispatcher"], _notify_mark(b2)


# ---------------------------------------------------------------- S56（项 1：ask 写侧收件面）

def s56():
    """S56（项 1）：子端 ask 写侧收件面 = 该 。runner spawn **复用
    resolve_reaper 单点**（不在 .ts 侧另立第二套解析），经 env 注入 AGENTD_ASK_INBOX/AGENTD_ASK_NOTE
    交子端 core.writeAskMessage 取值。真 spawn 取 env（命令只回显两枚 env 到 $AGENT_HOME，不触现网）：
    ① reaper=活性合成 bot → AGENTD_ASK_INBOX 指其自家信箱、AGENTD_ASK_NOTE 空；
    ② reaper=不存在目录 → AGENTD_ASK_INBOX 回落职位信箱、AGENTD_ASK_NOTE 点名成因、不建僵尸目录。"""
    _synth_bot("s56-reaper", active=True)   # 信箱型活性 reaper（watcher 条目 → _pid_active 判活）
    kill_runner_hard()
    probe = ('printf "INBOX=[%s]\\nNOTE=[%s]\\n" "$AGENTD_ASK_INBOX" "$AGENTD_ASK_NOTE" '
             '> "$AGENT_HOME/askenv"')
    t1 = create(["--name", "s56-alive"], policy="one-shot", command=probe, gate=False)
    set_sched_fields(t1, creator="bot/s56-reaper", reaper="bot/s56-reaper")
    t2 = create(["--name", "s56-ghost"], policy="one-shot", command=probe, gate=False)
    set_sched_fields(t2, creator="bot/s56-reaper", reaper="bot/s56-ghost")
    start_runner()
    ctl("enable", t1, "--by", "e2e")
    ctl("enable", t2, "--by", "e2e")
    for t in (t1, t2):
        wait_until(lambda t=t: (pdoc(t) or {}).get("final") is True, "S56 %s final" % t, timeout=30.0)

    def readenv(t):
        d = {}
        with open(os.path.join(adir_of(t), "askenv")) as f:
            for l in f:
                if "=" in l:
                    k, v = l.strip().split("=", 1)
                    d[k] = v[1:-1] if v.startswith("[") and v.endswith("]") else v
        return d.get("INBOX", ""), d.get("NOTE", "")

    pos = proto.position_inbox(ROOT)
    inbox1, note1 = readenv(t1)
    assert inbox1 == os.path.join(ROOT, "agents", "bot", "s56-reaper", "inbox"), inbox1
    assert note1 == "", "reaper 活 → 不带回落 note：%r" % note1
    inbox2, note2 = readenv(t2)
    assert inbox2 == pos, "reaper 不存在 → 回落职位信箱：%r" % inbox2
    assert note2 == "reaper bot/s56-ghost 不存在，回落职位信箱", note2
    assert not os.path.exists(os.path.join(ROOT, "agents", "bot", "s56-ghost")), \
        "spawn 不得为不存在的 reaper 建目录（resolve_reaper 只解析，建目录归发送侧 notify_tick）"


# --------------------------------- S57（控制信封 from 归属单点化）

def s57():
    """`agentctl control` 信封 `from` = 真实控制方（登记方自踩 + 调度员核证）：
    旧行为把 `from` 与文件名前缀硬编码为一个裸名缺省值（违 §2.2 两段路径式文法）⇒
    非调度员会话（领域会话/主持人）的取消一律记成调度员职位 = 审计与权威归属双失真。
    本场景钉住与 TS 侧 `core.resolveCreatorPid` **同源的优先级**：
    ① 显式 `--from`（非法即拒、零副作用）＞ ② 环境 `AGENT_SELF`（过文法白名单，
    非法/缺失静默落下一档，同 TS 侧）＞ ③ 回落职位信箱 `queue/dispatcher`；
    信封 `from` 与文件名前缀（§2.2 fs_safe_id 转写）同源。
    只写控制请求、不起进程（夹具无 enable.json ⇒ 不会被拉起），收尾自清。"""
    import proto as _proto
    pid_ = "task/s57-attrib"
    adir = adir_of(pid_)
    shutil.rmtree(adir, ignore_errors=True)
    os.makedirs(os.path.join(adir, "inbox"), exist_ok=True)
    with open(os.path.join(adir, "spec.json"), "w") as f:
        json.dump({"command": "true", "workdir": ROOT, "restartPolicy": "one-shot",
                   "creator": "task/tester", "host": "e2ehost", "name": "s57-attrib"}, f)
    cdir = os.path.join(adir, "control")

    def ctl_env(args, env_extra=None):
        """真 CLI 子进程 + 受控环境（夹具层：不用共享 ctl() 以便逐档布置 AGENT_SELF）。"""
        env = scrub_env()
        env.pop("AGENT_SELF", None)
        env.update(env_extra or {})
        return subprocess.run(
            [sys.executable, os.path.join(HERE, "agentctl.py"), "--root", ROOT, *args],
            capture_output=True, text=True, env=env, timeout=30)

    def reqs():
        if not os.path.isdir(cdir):
            return []
        # 按 mtime 排序取「最新一封」（同毫秒并列时名字作次键，避开随机后缀扰乱时序）
        return sorted(glob.glob(os.path.join(cdir, "*.req")),
                      key=lambda p: (os.path.getmtime(p), p))

    def latest():
        fs = reqs()
        assert fs, "控制请求应落盘：%s" % cdir
        with open(fs[-1]) as f:
            doc = json.load(f)
        return doc, os.path.basename(fs[-1])

    try:
        # ① 非调度员身份（领域会话/主持人）→ from = 自身路径式 id，文件名前缀同源转写
        r = ctl_env(["control", pid_, "stop", "--reason", "s57①"],
                    {"AGENT_SELF": "bot/s57-lead"})
        assert r.returncode == 0, r.stderr
        doc, fn = latest()
        assert doc["from"] == "bot/s57-lead", doc
        assert fn == doc["id"] + ".req" and "-bot.s57-lead-" in fn, (fn, doc["id"])
        assert doc["action"] == "stop" and doc["reason"] == "s57①", doc

        # ② 无身份（无 AGENT_SELF、无 --from）→ 回落职位信箱（不再回落裸名 "operator"）
        r = ctl_env(["control", pid_, "stop", "--reason", "s57②"])
        assert r.returncode == 0, r.stderr
        doc, fn = latest()
        assert doc["from"] == _proto.POSITION_PID == "queue/dispatcher", doc
        assert "-queue.dispatcher-" in fn, fn   # 回落 from 的 fsSafeId 前缀随职位信箱移族
        assert "operator" not in fn and "operator" not in json.dumps(doc), (fn, doc)

        # ③ 显式 --from 优先于环境（三族均可，文法白名单同一把尺）
        for explicit in ("task/s57-caller", "bot/s57-mod", "topic/s57"):
            r = ctl_env(["control", pid_, "restart", "--from", explicit],
                        {"AGENT_SELF": "bot/s57-lead"})
            assert r.returncode == 0, r.stderr
            doc, fn = latest()
            assert doc["from"] == explicit, (explicit, doc)
            assert "-" + _proto.fs_safe_id(explicit) + "-" in fn, (explicit, fn)

        # ④ 裸名/非法 --from 拒绝（exit≠0 + 零副作用：不得先落盘再报错）
        n_before = len(reqs())
        for bad in ("tester", "operator", "a/b/c", "task/", "bot/../x", "unknown/x"):
            r = ctl_env(["control", pid_, "stop", "--from", bad])
            assert r.returncode != 0, "非法 --from 应拒绝：%r" % bad
            assert "非法" in r.stderr and "路径式" in r.stderr, (bad, r.stderr)
        assert len(reqs()) == n_before, "拒绝分支零落盘副作用"

        # ⑤ 非法 AGENT_SELF（裸名/三段）→ 静默回落职位信箱（同 TS resolveCreatorPid 口径：
        #    env 不是调用方显式意图，报错会把无关会话的合法取消堵住）
        for bad_env in ("tester", "a/b/c", "", "   "):
            r = ctl_env(["control", pid_, "stop"], {"AGENT_SELF": bad_env})
            assert r.returncode == 0, (bad_env, r.stderr)
            doc, _fn = latest()
            assert doc["from"] == "queue/dispatcher", (bad_env, doc)

        # ⑥ 文法单点复用（不新增 proto 逻辑）：解析用的就是 proto.is_valid_participant_id
        src = open(os.path.join(HERE, "agentctl.py")).read()
        assert "proto.is_valid_participant_id" in src, "发送方解析应复用 proto 既有文法校验"
        assert not re.search(r'a\.sender or "operator"', src), "裸名缺省值已退场"
    finally:
        shutil.rmtree(adir, ignore_errors=True)


# ----------------- S58（写侧动词两档：send/answer/cancel/update + 跨语言常量钉桩）

def s58():
    """写侧 CLI 面（收录判据 ②：纯文件读写一律落 agentctl，不做 pi 工具）：
    ① `send` = 协议动词：type 缺省 inform、`--deliver` 显式才落盘、`--body-file -` 走 stdin
       逐字保真（不经 shell 断词）、`type=ask` 自动带 `via`、`from` 三档归属（同 S57 单点）、
       空正文/正文歧义/reply 无 ref 一律拒且零落盘；
    ② `answer` = 用例动词：自动找**最早一条未答 ask**（扫描面 = 职位信箱 ∪ spec.reaper 自家
       信箱，单点 `proto.ask_scan_inboxes`）→ 写 reply（ref 回引 + **继承 ask 的 via**）+ 回执
       点名所答条目；`--deliver` 与 `send` 同形（显式给才落盘该字段、缺省 = **键不在场**
       而非写 followUp、枚举外值 argparse 拒 rc=2，§6.6）；无未答 ask / 已 final / 无 spec
       三种意图落空一律拒且零落盘；
    ③ `cancel` = control stop + 存活前置门；对照 `control`（协议动词）不过门：已 final 仍写；
    ④ `update` = 五道硬校验（至少一字段 / 字符串数组 / 存在 / 非 final / **未放行**）+ 只覆盖传入字段；
    ⑤ **跨语言钉桩**：TS 收件侧（core.ts）的 `ASK_VIA_SEND_MESSAGE` / `DELIVER_MODES` /
       消息型枚举与本仓 proto 常量逐字相等——Python 写的 via/deliver 必须被 TS drain 认得，
       否则答复静默悬空（§4.5：带 via 的 reply 才被收件侧放行消费）；
    ⑥ `--root` 缺省 = 现场发现（本仓父目录）：仓内 / 工作区根 / 根上层三种 cwd 调用均解析到
       同一个根（只读动词，不依赖 cwd/env）；显式传错 root 仍前置拒绝。
    只写信封/请求，不起进程，收尾自清。**夹具期间 runner/scheduler 停机**：本场景是纯落盘面，
    而真 runner 会消费夹具里的 stop 请求（未启动任务被取消 → 写 final/killed 的 pid.json），
    把 ④ 的存活门断言污染成「已终态」；停机后本场景不依赖任何运行侧行为。"""
    import proto as _proto
    stop_runner()
    stop_scheduler()
    t, lead = "task/s58-t", "bot/s58-lead"
    # other = 扫描面外的第三方信箱（非 reaper、非职位信箱）：① 的 ask/via 测试投这里，
    # 不污染 ② 的「最早未答 ask」判定面。
    other = "bot/s58-other"
    tadir, ladir, oadir = adir_of(t), adir_of(lead), adir_of(other)
    pos_inbox = proto.position_inbox(ROOT)
    for d in (tadir, ladir, oadir):
        shutil.rmtree(d, ignore_errors=True)
    for d in (tadir, ladir, oadir):
        os.makedirs(os.path.join(d, "inbox"), exist_ok=True)
    os.makedirs(pos_inbox, exist_ok=True)
    with open(os.path.join(tadir, "spec.json"), "w") as f:
        json.dump({"command": "true", "workdir": ROOT, "restartPolicy": "one-shot",
                   "creator": "task/tester", "host": "e2ehost", "name": "s58-t",
                   "reaper": lead}, f)
    pos_before = set(os.listdir(pos_inbox))

    def run(args, env_extra=None, stdin=None, expect_rc=None):
        """真 CLI 子进程 + 受控环境（逐档布置 AGENT_SELF，不用共享 ctl()）。"""
        env = scrub_env()
        env.pop("AGENT_SELF", None)
        env.update(env_extra or {})
        r = subprocess.run([sys.executable, os.path.join(HERE, "agentctl.py"),
                            "--root", ROOT, *args], capture_output=True, text=True,
                           env=env, input=stdin)
        if expect_rc is not None:
            assert r.returncode == expect_rc, \
                "agentctl %s rc=%d (want %d): %s" % (" ".join(args), r.returncode,
                                                     expect_rc, r.stderr)
        return r

    def msgs(d):
        return sorted(f for f in os.listdir(d) if f.endswith(".msg"))

    def last_env(d):
        fs = msgs(d)
        assert fs, "信箱空：%s" % d
        with open(os.path.join(d, fs[-1])) as f:
            return json.load(f)

    try:
        # ---- ① send（协议动词）----
        linbox = os.path.join(ladir, "inbox")
        run(["send", lead, "--body", "hi"], {"AGENT_SELF": t}, expect_rc=0)
        e = last_env(linbox)
        assert e["type"] == "inform", "type 缺省 = inform：%s" % e
        assert "deliver" not in e, "缺省不落 deliver 字段（§6.6）：%s" % e
        assert "via" not in e and e["from"] == t, e
        run(["send", lead, "--body", "改向", "--deliver", "steer"],
            {"AGENT_SELF": t}, expect_rc=0)
        assert last_env(linbox)["deliver"] == "steer"
        # 长正文/引号/换行/变量字面量走 stdin：逐字保真（不经 shell 断词与展开）。
        # 投扫描面外的 other：本封是 ask（带 via），落在 reaper/职位信箱会被 ② 当成待答项。
        raw = '第一行 "引号" $VAR `cmd`\n第二行\t制表\n'
        run(["send", other, "--type", "ask", "--body-file", "-"],
            {"AGENT_SELF": t}, stdin=raw, expect_rc=0)
        e = last_env(os.path.join(oadir, "inbox"))
        assert e["body"] == raw.strip(), "stdin 正文逐字保真（trim 后）：%r" % e["body"]
        assert e["via"] == _proto.ASK_VIA_SEND_MESSAGE, \
            "type=ask 必带 via 来源标记（否则其 reply 不被收件侧消费）：%s" % e
        # from 三档归属（与 S57 同源单点）
        run(["send", lead, "--body", "x", "--from", "bot/s58-lead"], expect_rc=0)
        assert last_env(linbox)["from"] == "bot/s58-lead"
        run(["send", lead, "--body", "x"], expect_rc=0)
        assert last_env(linbox)["from"] == "queue/dispatcher", "无 AGENT_SELF → 回落职位信箱"
        run(["send", lead, "--body", "x"], {"AGENT_SELF": "裸名"}, expect_rc=0)
        assert last_env(linbox)["from"] == "queue/dispatcher", "非法 AGENT_SELF 静默落下一档"
        # 拒路径：零落盘副作用
        for bad_args, kw in ((["send", lead, "--body", "   "], "正文为空"),
                             (["send", lead, "--body", "a", "--body-file", "-"], "二选一"),
                             (["send", lead, "--type", "reply", "--body", "a"], "--ref"),
                             (["send", lead, "--body", "a", "--from", "operator"], "非法"),
                             (["send", "bot/dispatcher", "--body", "a"], "退役"),
                             (["send", "lead", "--body", "a"], "非法")):
            n = len(msgs(linbox))
            r = run(bad_args, {"AGENT_SELF": t})
            assert r.returncode != 0, "应拒：%s" % bad_args
            assert kw in r.stderr, (bad_args, r.stderr)
            assert len(msgs(linbox)) == n, "拒分支零落盘：%s" % bad_args
        assert run(["send", lead, "--body", "x", "--deliver", "now"]).returncode == 2, \
            "deliver 枚举外的值由 argparse 拒（rc=2）"

        # ---- ② answer（用例动词）----
        # 扫描面并集两路各投一条 ask：职位信箱（reaper 不可达时的回落面）与 reaper 自家信箱。
        # 先投的（职位信箱，**无 via** = 阻塞征询形态）应被先答；隔 10ms 保证 id 字典序即时间序。
        pos_ask_id = _proto.now_ts() + "-" + _proto.fs_safe_id(t) + "-aaaa"
        _proto.atomic_write_json(os.path.join(pos_inbox, pos_ask_id + ".msg"), {
            "id": pos_ask_id, "from": t, "ts": _proto.now_ts(), "type": "ask",
            "body": json.dumps({"question": "阻塞征询：要不要继续？"}, ensure_ascii=False)})
        time.sleep(0.01)
        run(["send", "queue/dispatcher", "--type", "ask",
             "--body", json.dumps({"question": "第二条（也在职位信箱）"},
                                  ensure_ascii=False)], {"AGENT_SELF": t}, expect_rc=0)
        time.sleep(0.01)
        run(["send", lead, "--type", "ask",
             "--body", json.dumps({"question": "第三条（reaper 自家信箱）"},
                                  ensure_ascii=False)], {"AGENT_SELF": t}, expect_rc=0)
        tibox = os.path.join(tadir, "inbox")
        r = run(["answer", t, "--body", "继续，按方案 A"], {"AGENT_SELF": lead}, expect_rc=0)
        rep = last_env(tibox)
        assert rep["type"] == "reply" and rep["ref"] == pos_ask_id, \
            "答最早一条未答 ask（职位信箱里那条）：%s" % rep
        assert "via" not in rep, "阻塞 ask（无 via）的 reply 不带 via（归阻塞面单一消费）：%s" % rep
        assert rep["from"] == lead, rep
        assert r.stdout.split("\n")[0].strip() == rep["id"], "首行 = reply id：%r" % r.stdout
        assert "blocking=yes" in r.stdout and pos_ask_id in r.stdout, \
            "回执当场点名所答条目与阻塞态：%r" % r.stdout
        r = run(["answer", t, "--body", "第二条的答复"], {"AGENT_SELF": lead}, expect_rc=0)
        rep2 = last_env(tibox)
        assert rep2["ref"] != pos_ask_id and rep2["via"] == _proto.ASK_VIA_SEND_MESSAGE, \
            "第二条（send --type ask）的 reply 继承 via → 收件侧 drain 放行：%s" % rep2
        assert "blocking=no" in r.stdout, r.stdout
        run(["answer", t, "--body", "第三条的答复"], {"AGENT_SELF": lead}, expect_rc=0)
        assert len(msgs(tibox)) == 3, "三条 ask 各得一条 reply"

        # ②′ `answer --deliver` 两档（与 send 同形，§6.6）：显式给才落盘该字段、缺省 =
        # **键不在场**（⛔ 不是写了 followUp）。每档先投一条新 ask 再答（不污染上面
        # 「最早未答 ask」的判定面）；信封按 answer 回执首行的 reply id 直取，不依赖
        # 文件名字典序。
        for extra, want in ((["--deliver", "steer"], "steer"), ([], None)):
            tag = want or "缺省"
            run(["send", lead, "--type", "ask",
                 "--body", json.dumps({"question": "deliver 档探针（%s）" % tag},
                                      ensure_ascii=False)],
                {"AGENT_SELF": t}, expect_rc=0)
            r = run(["answer", t, "--body", "deliver 档答复（%s）" % tag, *extra],
                    {"AGENT_SELF": lead}, expect_rc=0)
            mid = r.stdout.split("\n")[0].strip()
            with open(os.path.join(tibox, mid + ".msg")) as f:
                rep = json.load(f)
            assert rep["type"] == "reply" and rep["id"] == mid, rep
            if want is None:
                assert "deliver" not in rep, \
                    "缺省（不带 --deliver）= 信封**不写该键**（⛔ 不是写 followUp，§6.6）：%s" % rep
            else:
                assert rep.get("deliver") == want, \
                    "answer --deliver %s ⇒ 落盘信封的 deliver 字段逐字相等：%s" % (want, rep)
        assert run(["answer", t, "--body", "x", "--deliver", "now"]).returncode == 2, \
            "answer 的 deliver 枚举外的值同由 argparse 拒（rc=2，与 send 同形）"

        n = len(msgs(tibox))
        r = run(["answer", t, "--body", "无主答复"], {"AGENT_SELF": lead})
        assert r.returncode != 0 and "没有未答的 ask" in r.stderr, r.stderr
        assert len(msgs(tibox)) == n, "意图落空零落盘"
        r = run(["answer", "task/s58-nospec", "--body", "x"], {"AGENT_SELF": lead})
        assert r.returncode != 0 and "无 spec.json" in r.stderr, r.stderr
        _proto.atomic_write_json(os.path.join(tadir, "pid.json"),
                                {"pid": 999999, "status": "exited", "final": True})
        r = run(["answer", t, "--body", "x"], {"AGENT_SELF": lead})
        assert r.returncode != 0 and "生命周期终态" in r.stderr, r.stderr

        # ---- ③ cancel 前置门 vs control 不过门 ----
        r = run(["cancel", t, "--reason", "需求作废"], {"AGENT_SELF": lead})
        assert r.returncode != 0 and "生命周期终态" in r.stderr, "已 final → cancel 拒：%s" % r.stderr
        cdir = os.path.join(tadir, "control")
        assert not os.path.isdir(cdir) or not os.listdir(cdir), "cancel 拒分支零落盘"
        run(["control", t, "stop"], {"AGENT_SELF": lead}, expect_rc=0)
        assert len([f for f in os.listdir(cdir) if f.endswith(".req")]) == 1, \
            "协议动词 control 不过存活门（收尾清理也要能写 stop）"
        os.remove(os.path.join(tadir, "pid.json"))          # 回到未启动态
        r = run(["cancel", t, "--reason", "需求作废"], {"AGENT_SELF": lead}, expect_rc=0)
        reqf = [f for f in os.listdir(cdir) if f.endswith(".req")]
        with open(os.path.join(cdir, sorted(reqf)[-1])) as f:
            req = json.load(f)
        assert req["action"] == "stop" and req["reason"] == "需求作废", req
        assert req["from"] == lead and r.stdout.split("\n")[0].strip() == req["id"], (req, r.stdout)

        # ---- ④ update 五道硬校验 ----
        r = run(["update", t])
        assert r.returncode != 0 and "至少传一个" in r.stderr, r.stderr
        r = run(["update", t, "--resources", "gpu"])
        assert r.returncode != 0 and "JSON 字符串数组" in r.stderr, r.stderr
        r = run(["update", t, "--needs", '["ok", 3]'])
        assert r.returncode != 0, "非字符串元素应拒"
        r = run(["update", "task/s58-nospec", "--needs", "[]"])
        assert r.returncode != 0 and "无 spec.json" in r.stderr, r.stderr
        before = json.load(open(os.path.join(tadir, "spec.json")))
        run(["update", t, "--resources", '["gpu","net"]', "--needs", "[]"], expect_rc=0)
        after = json.load(open(os.path.join(tadir, "spec.json")))
        assert after["resources"] == ["gpu", "net"] and after["needs"] == [], after
        assert "provides" not in after, "未传字段不得凭空出现"
        for k in set(before) | set(after):        # 逐键对照（不写死键名清单：夹具与生产 spec 字段集不同）
            if k in ("resources", "provides", "needs"):
                continue
            assert before.get(k) == after.get(k), "只覆盖传入字段：%s 被动了" % k
        _proto.atomic_write_json(os.path.join(tadir, "enable.json"),
                                {"ts": _proto.now_ts(), "by": "s58"})
        r = run(["update", t, "--provides", '["x"]'])
        assert r.returncode != 0 and "已放行" in r.stderr, r.stderr
        assert "provides" not in json.load(open(os.path.join(tadir, "spec.json"))), \
            "已放行拒分支零落盘"
        os.remove(os.path.join(tadir, "enable.json"))
        _proto.atomic_write_json(os.path.join(tadir, "pid.json"),
                                {"pid": 999999, "status": "killed", "final": True})
        r = run(["update", t, "--provides", '["x"]'])
        assert r.returncode != 0 and "生命周期终态" in r.stderr, r.stderr

        # ---- ⑤ 跨语言钉桩（TS 收件侧 ↔ 本仓写侧）----
        core_ts = os.path.join(EXT_DIR, "core.ts")
        if not os.path.exists(core_ts):
            platform_skip("S58⑤ 跨语言常量钉桩",
                          "调用方工作区的 TS 收件侧不在场（本仓单独 checkout）：%s" % core_ts)
        else:
            src = open(core_ts, encoding="utf-8").read()
            m = re.search(r'export const ASK_VIA_SEND_MESSAGE = "([^"]+)"', src)
            assert m, "core.ts 未找到 ASK_VIA_SEND_MESSAGE 声明（钉桩面漂移）"
            assert m.group(1) == _proto.ASK_VIA_SEND_MESSAGE, \
                "via 字面量跨语言不一致：TS=%r proto=%r（不一致 = 答复静默悬空）" % (
                    m.group(1), _proto.ASK_VIA_SEND_MESSAGE)
            m = re.search(r"export const DELIVER_MODES = \[([^\]]*)\]", src)
            assert m, "core.ts 未找到 DELIVER_MODES 声明"
            ts_deliver = re.findall(r'"([^"]+)"', m.group(1))
            assert ts_deliver == list(_proto.DELIVER_MODES), \
                "deliver 枚举跨语言不一致：TS=%r proto=%r" % (ts_deliver, _proto.DELIVER_MODES)
            m = re.search(r"export const PARTICIPANT_MESSAGE_TYPES = \[([^\]]*)\]", src)
            assert m, "core.ts 未找到 PARTICIPANT_MESSAGE_TYPES 声明"
            assert set(re.findall(r'"([^"]+)"', m.group(1))) == set(_proto.MSG_TYPES), \
                "消息型枚举跨语言不一致"
            # 族白名单同源（TS 侧用常量名而非字面量 ⇒ 先取 *_DIR 字面量表再映射）：
            # 漏改一侧的后果 = 一侧拒投/一侧收不到（未知族无回退）。
            ts_dirs = dict(re.findall(r'export const (\w+_DIR) = "([^"]+)"', src))
            for key, pyval in (("TASK_DIR", _proto.TASK_DIR), ("BOT_DIR", _proto.BOT_DIR),
                               ("TOPIC_DIR", _proto.TOPIC_DIR),
                               ("QUEUE_DIR", _proto.QUEUE_DIR)):
                assert ts_dirs.get(key) == pyval, \
                    "族目录名跨语言不一致：%s TS=%r proto=%r" % (key, ts_dirs.get(key), pyval)
            for const in ("LAYOUT_DIRS", "FAMILIES"):
                m = re.search(r"export const %s = \[([^\]]*)\]" % const, src)
                assert m, "core.ts 未找到 %s 声明（钉桩面漂移）" % const
                ts_val = [ts_dirs[i.strip()] for i in m.group(1).split(",") if i.strip()]
                py_val = list(getattr(_proto, const))
                assert ts_val == py_val, \
                    "%s 跨语言不一致：TS=%r proto=%r" % (const, ts_val, py_val)

        # ---- ⑥ --root 缺省 = 现场发现（不依赖 cwd/env）----
        # 读真工作区树（只读动词 list）：本仓单独 checkout 时无树可读 → 显式 skip。
        ws = os.path.dirname(HERE)
        if not os.path.isdir(os.path.join(ws, "agents")):
            platform_skip("S58⑥ --root 缺省现场发现",
                          "本仓不在工作区树内（父目录无 agents/）：%s" % ws)
        else:
            for cwd in (HERE, ws, os.path.dirname(ws)):
                r = subprocess.run([sys.executable, os.path.join(HERE, "agentctl.py"), "list"],
                                   capture_output=True, text=True, cwd=cwd)
                assert r.returncode == 0 and "不是工作区根" not in r.stderr, \
                    "cwd=%s 时 --root 缺省应解析到工作区根：%s" % (cwd, r.stderr)
            # 错 root 样本用本仓目录（其下永无 agents/）：钉的是「`<root>/agents` 不在场」
            # 这条硬前置；`<WS>/agents`（agents 树本身、含残骸在场形态）由 S59 单独钉。
            r = subprocess.run([sys.executable, os.path.join(HERE, "agentctl.py"),
                                "--root", HERE, "list"],
                               capture_output=True, text=True)
            assert r.returncode == 2 and "不是工作区根" in r.stderr, \
                "显式错 root 仍须前置拒绝，不因缺省发现而松动"
    finally:
        for d in (tadir, ladir, oadir):
            shutil.rmtree(d, ignore_errors=True)
        # 职位信箱是共享面：只删本场景写的件，不碰存量
        for fn in set(os.listdir(pos_inbox)) - pos_before:
            os.remove(os.path.join(pos_inbox, fn))


# ------------- S59（--root 前置校验：错 root = agents 树本身，残骸在场也拒）

S59BASE = os.path.join(TMPBASE, "root-s59")   # 本场景自建根（专用临时目录，不用生产树）


def _rm_scenario_root(d):
    """场景根清理（root 身份断言；S59 起，自建根的场景共用）：只删本套件临时基目录下自建的场景根。
    拒两形态 —— 等于生产根（工作区根 ∨ 本仓根）与生产根在其内部；不传
    ignore_errors（吞错 = 把清理失败伪装成成功）。"""
    real = os.path.realpath(d)
    base = os.path.realpath(TMPBASE)
    assert real != base and real.startswith(base + os.sep), \
        "拒绝清理：场景根不在本套件临时基目录内：%s（base=%s）" % (real, base)
    for prod in (os.path.dirname(HERE), HERE):
        rp = os.path.realpath(prod)
        assert real != rp, "拒绝清理：场景根等于生产根 %s" % rp
        assert not rp.startswith(real + os.sep), \
            "拒绝清理：生产根 %s 在场景根内部" % rp
    shutil.rmtree(real)


def s59():
    """`--root` 前置校验的自我击穿回归（缺陷形态 = 硬化被它要防的那棵残骸树击穿）：
    ① 错 root（= agents 树本身）在**嵌套残骸已在场**时仍 die（rc=2）且零新建目录/文件——
      残骸恰好满足「`<root>/agents` 在场」这条硬前置，单留它 ⇒ 同类误用只报一行软 WARN
      就放行、写侧 makedirs 继续往嵌套树里建目录（basename 判据不依赖残骸，故拦得住）；
      写侧动词（send）与只读动词（status）各钉一次；
    ② 零回归：正常工作区根（自建临时根 + 真工作区根）的只读动词照常 rc=0，**残骸在场
      也放行**（判据射程只到「root 是不是工作区根」，不收窄 `<root>/agents/` 下条目的形状）；
    ③ `env/host-id` 软前置未被升硬：缺该文件的临时根只 WARN、rc=0，登记回退 hostname 非空
      （既有裁定：映射缺失不阻塞登记；S22④/S27③ 同族）。
    只跑 agentctl 子进程（不起 runner/scheduler、不读写主树 ROOT），收尾自清。"""
    prod_ws = os.path.dirname(HERE)                       # 真工作区根（本仓单独 checkout 时不是）
    ws = os.path.join(S59BASE, "ws")                      # 自建「工作区根 + 残骸在场」
    os.makedirs(os.path.join(ws, "agents", "task", "s59-plain"), exist_ok=True)
    os.makedirs(os.path.join(ws, "agents", "agents", "bot", "s59-victim", "inbox"),
                exist_ok=True)                            # 嵌套残骸（只有空目录 = 现网形态）
    os.makedirs(os.path.join(ws, "env"), exist_ok=True)
    with open(os.path.join(ws, "env", "host-id"), "w") as f:
        f.write("%s s59-canonical\n" % socket.gethostname())
    nohost = os.path.join(S59BASE, "ws-nohostid")          # ③ 缺 env/host-id 的临时根
    os.makedirs(os.path.join(nohost, "agents", "task"), exist_ok=True)

    def run(root, *args):
        env = scrub_env()
        env.pop("AGENT_SELF", None)                       # from 归属不取宿主会话身份
        return subprocess.run([sys.executable, os.path.join(HERE, "agentctl.py"),
                               "--root", root, *args], capture_output=True, text=True,
                              env=env, timeout=60)

    def ntree(d):
        """(目录数, 文件数)：拒分支「零新建」的计数面。"""
        dirs = files = 0
        for _r, ds, fs in os.walk(d):
            dirs += len(ds)
            files += len(fs)
        return dirs, files

    try:
        # ---- ① 错 root = agents 树本身（残骸在场 ⇒ 硬前置被满足的自我击穿形态）----
        wrong = os.path.join(ws, "agents")
        assert os.path.isdir(os.path.join(wrong, "agents")), \
            "夹具前提：嵌套残骸须在场（那正是本缺陷的成立条件）"
        before = ntree(ws)
        for args in (["send", "bot/s59-victim", "--body", "误投"],   # 写侧动词
                     ["status", "bot/s59-victim"]):                  # 只读动词
            r = run(wrong, *args)
            assert r.returncode == 2, \
                "错 root（agents 树本身）应 die：%s rc=%d %s" % (args, r.returncode, r.stderr)
            assert "不是工作区根" in r.stderr and "agents 树本身" in r.stderr, r.stderr
            assert ntree(ws) == before, "拒后零新建目录/文件：%s" % (args,)
        # 写侧动词也不得往残骸里落任何件（信封/控制请求）
        assert ntree(os.path.join(ws, "agents", "agents"))[1] == 0, \
            "残骸树里不得长出文件"

        # ---- ② 零回归：正常工作区根（含残骸在场的那棵）----
        r = run(ws, "list")
        assert r.returncode == 0 and "不是工作区根" not in r.stderr, \
            "正常工作区根（自带残骸）的只读动词应 rc=0：%s" % r.stderr
        assert "s59-plain" in r.stdout, r.stdout
        if os.path.isdir(os.path.join(prod_ws, "agents")):
            r = run(prod_ws, "list")          # 只读：真工作区根也不得被新判据误伤
            assert r.returncode == 0 and "不是工作区根" not in r.stderr, \
                "真工作区根 %s 的只读动词应 rc=0：%s" % (prod_ws, r.stderr)
        else:
            platform_skip("S59② 真工作区根只读回归",
                          "本仓不在工作区树内（父目录无 agents/）：%s" % prod_ws)

        # ---- ③ env/host-id 软前置（只 WARN、不改 rc）----
        assert not os.path.exists(os.path.join(nohost, "env", "host-id")), "夹具前提"
        r = run(nohost, "list")
        assert r.returncode == 0, "软前置不得升硬（rc 应为 0）：%s" % r.stderr
        assert "WARN env/host-id 不在场" in r.stderr, r.stderr
        r = run(nohost, "create", "--name", "s59-nohostid", "--command", "true",
                "--workdir", nohost, "--creator", "tester", "--restart-policy", "one-shot")
        assert r.returncode == 0, "映射缺失不阻塞登记：%s" % r.stderr
        spec = os.path.join(nohost, "agents", "task", "s59-nohostid", "spec.json")
        assert os.path.exists(spec), spec
        with open(spec) as f:
            s = json.load(f)
        assert s["host"] == socket.gethostname() and s["host"], \
            "回退 hostname 非空（S22④/S27③ 同口径）：%s" % s
    finally:
        _rm_scenario_root(S59BASE)


# ------------- S60（agentctl create --profile：人格装载的登记侧写入口）

S60BASE = os.path.join(TMPBASE, "root-s60")   # 本场景自建根（专用临时目录，不用生产树）


def s60():
    """`agentctl create --profile`（消掉「手写 DISPATCH_PROFILE 前缀、漏写即静默回落」的绕行）四格：

    ① **拼前缀**：`--profile <名>` ⇒ spec.command 逐字 = `DISPATCH_PROFILE=<名> ` + `--command`
      原值（与手写前缀的存量档案不可区分），且 spec **不长 `profile` 键**（落地形态锁在
      command 前缀 ⇒ ⛔ 不动 spec schema、⛔ 不动调度语义）；已声明人格 ⇒ 不打缺省 WARN。
    ② **非法名被拒**：白名单 = 现场枚举 `<root>/bots/profiles/*.json`（⛔ 写死名单）⇒ 枚举外的
      名字 rc=2 + stderr 打印可选名单 + 零落盘；**枚举根按 --root 解析**（真工作区里在场的
      profile 名在本夹具根里不在场 ⇒ 同样拒）；枚举根整棵不在场 ⇒ 拒（⛔ 静默放行）。
    ③ **前缀冲突被拒**：`--command` 已含**异值**前缀 + `--profile` ⇒ rc=2、stderr 同时含两侧值
      （⛔ 静默覆盖）、零落盘；**同值** ⇒ 幂等（不重复拼、command 逐字不动）。
    ④ **缺省 WARN 且照建**：不给 `--profile` 且 command 无前缀 ⇒ stderr 恰一行 profile WARN
      （含可选名单 = 怎么修）、rc=0、spec 照建且 command 逐字 = `--command` 原值（⛔ 硬失败：
      存量调用方与守护 bot 登记面依赖缺省行为）；command 自带前缀而不给 `--profile` ⇒
      不打该行（存量手写形态零回归）。
    另钉 `create --help` 的 `--profile` 文本覆盖三格（白名单来源 / 非法即拒 / 落地 = 前缀）。
    只跑 agentctl 子进程（不起 runner/scheduler、不读写主树 ROOT），收尾自清。"""
    KEY = "DISPATCH_PROFILE"
    WRAP = 'exec python3 "$AGENT_ROOT/pi-wrap/pi-rpc-wrap.py"'   # 现网 handler 登记的命令形态
    AVAIL = ["s60-review", "s60-executor"]                     # 夹具专属名：白名单若被写死
    #                                                            成现网名单，① 就红（反写死名单钉）
    ws = os.path.join(S60BASE, "ws")
    bare = os.path.join(S60BASE, "ws-noprofiles")   # ② 枚举根整棵不在场的对照根
    for root in (ws, bare):
        os.makedirs(os.path.join(root, "agents", "task"), exist_ok=True)
        os.makedirs(os.path.join(root, "env"), exist_ok=True)
        with open(os.path.join(root, "env", "host-id"), "w") as f:
            f.write("%s s60-canonical\n" % socket.gethostname())
    os.makedirs(os.path.join(ws, "bots", "profiles"), exist_ok=True)
    for n in AVAIL:
        with open(os.path.join(ws, "bots", "profiles", n + ".json"), "w") as f:
            f.write("{}")
    assert not os.path.isdir(os.path.join(bare, "bots")), "夹具前提：bare 根无枚举目录"

    def run(root, *args):
        env = scrub_env()
        env.pop("AGENT_SELF", None)
        return subprocess.run([sys.executable, os.path.join(HERE, "agentctl.py"),
                               "--root", root, *args], capture_output=True, text=True,
                              env=env, timeout=60)

    def create(root, name, command, *extra):
        return run(root, "create", "--name", name, "--command", command,
                   "--workdir", root, "--creator", "tester", *extra)

    def spec_of(root, name):
        p = os.path.join(root, "agents", "task", name, "spec.json")
        assert os.path.exists(p), "spec 应在场：%s" % p
        with open(p) as f:
            return json.load(f)

    def no_dir(root, name):
        assert not os.path.exists(os.path.join(root, "agents", "task", name)), \
            "拒后零落盘（不得建 task/%s）" % name

    try:
        # ---- ① 拼前缀（与手写形态逐字同款）+ spec 不长新键 ----
        r = create(ws, "s60-a", WRAP, "--profile", AVAIL[0])
        assert r.returncode == 0, r.stderr
        doc = spec_of(ws, "s60-a")
        assert doc["command"] == "%s=%s %s" % (KEY, AVAIL[0], WRAP), doc["command"]
        assert doc["command"].startswith("%s=%s " % (KEY, AVAIL[0])), doc["command"]
        assert "profile" not in doc, "落地形态 = command 前缀，⛔ 不给 spec 加键：%s" % sorted(doc)
        assert "未指定 --profile" not in r.stderr, "已声明人格 ⇒ 不打缺省 WARN：%s" % r.stderr

        # ---- ② 非法名被拒（枚举外 ∨ 本夹具根不在场的真工作区名）+ 零落盘 ----
        for bad in ("s60-nosuch", "review", "executor"):
            r = create(ws, "s60-b", WRAP, "--profile", bad)
            assert r.returncode == 2, "%r 应拒（rc=2）：rc=%d %s" % (bad, r.returncode, r.stderr)
            assert "非法 --profile" in r.stderr and bad in r.stderr, r.stderr
            for n in AVAIL:
                assert n in r.stderr, "stderr 须打印可选名单（缺 %s）：%s" % (n, r.stderr)
            no_dir(ws, "s60-b")
        r = create(bare, "s60-b2", WRAP, "--profile", AVAIL[0])
        assert r.returncode == 2, "枚举根不在场 ⇒ 拒（⛔ 静默放行）：%s" % r.stderr
        assert "无法校验 --profile" in r.stderr and "bots/profiles" in r.stderr, r.stderr
        no_dir(bare, "s60-b2")

        # ---- ③ 前缀冲突：异值拒（两侧值都在报文里）∧ 同值幂等 ----
        r = create(ws, "s60-c", "%s=%s %s" % (KEY, AVAIL[1], WRAP), "--profile", AVAIL[0])
        assert r.returncode == 2, r.stderr
        assert "冲突" in r.stderr, r.stderr
        assert "%s=%s" % (KEY, AVAIL[1]) in r.stderr and AVAIL[0] in r.stderr, \
            "两侧值都要打印（--profile 侧 %s ∨ command 侧 %s）：%s" % (AVAIL[0], AVAIL[1], r.stderr)
        no_dir(ws, "s60-c")
        r = create(ws, "s60-d", "%s=%s %s" % (KEY, AVAIL[0], WRAP), "--profile", AVAIL[0])
        assert r.returncode == 0, r.stderr
        assert spec_of(ws, "s60-d")["command"] == "%s=%s %s" % (KEY, AVAIL[0], WRAP), \
            "同值 ⇒ 幂等（不重复拼、逐字不动）"

        # ---- ④ 缺省：一行 WARN + 照建（⛔ 硬失败）----
        r = create(ws, "s60-e", WRAP)
        assert r.returncode == 0, "缺省必须照建（⛔ 硬失败）：%s" % r.stderr
        assert spec_of(ws, "s60-e")["command"] == WRAP, "缺省 ⇒ command 逐字原值"
        assert len([x for x in r.stderr.splitlines() if "未指定 --profile" in x]) == 1, \
            "恰一行 profile WARN：%s" % r.stderr
        for n in AVAIL:
            assert n in r.stderr, "WARN 须给可选名单（怎么修）：%s" % r.stderr
        r = create(ws, "s60-f", "%s=%s %s" % (KEY, AVAIL[0], WRAP))
        assert r.returncode == 0, r.stderr
        assert "未指定 --profile" not in r.stderr, \
            "command 自带前缀 = 已声明人格（存量手写形态零回归）：%s" % r.stderr
        assert spec_of(ws, "s60-f")["command"] == "%s=%s %s" % (KEY, AVAIL[0], WRAP)

        # ---- help 面：--profile 在 create --help 里且覆盖三格 ----
        r = run(ws, "create", "--help")
        assert r.returncode == 0, r.stderr
        assert "--profile" in r.stdout, r.stdout
        for tok in ("现场枚举", "非法即拒", "spec.command"):
            assert tok in r.stdout, "help 须写明 %r（白名单来源/非法即拒/落地=前缀）" % tok
    finally:
        _rm_scenario_root(S60BASE)


# ------------- S61（agentctl create --prompt-file：任务书与登记同批落盘）

S61BASE = os.path.join(TMPBASE, "root-s61")   # 本场景自建根（专用临时目录，不用生产树）


def s61():
    """`agentctl create --prompt-file`（消掉「prompt.md 与 create 必须写在同一个 shell 调用里」
    这条竞态绕行规则）七格：

    ① **正常落盘**：可读文件 ⇒ rc=0 ∧ stdout 仍是 taskId 裸串（既有语义）∧ `prompt.md` 在场
      且内容**逐字**相同（多行/UTF-8/反引号/`$VAR`/引号全保留 ⇒ 钉「⛔ 渲染、⛔ 转义」）。
    ② **写点顺序钉**（竞态窗闭合的真判据）：`cmd_create` 里 `prompt.md` 的写点行号 **<**
      `spec.json` 的写点行号（AST 级取，⛔ 文本 grep）。只断言「两者都在场」照不到顺序，而
      写在 spec 之后只是把窗变窄：调度方的可见性锚是 `spec.json`。
    ③ **拒分支 ×5 + 零落盘**：路径不存在 ∨ 是目录 ∨ 零字节 ∨ 纯空白 ∨ 非 UTF-8 ⇒ rc=2 ∧
      stderr 点名 `--prompt-file` ∧ **不建 `task/<名>` 目录**（⛔ 半成品：建了却没 prompt）。
    ④ **缺省行为逐字不变**：不传旗标 ⇒ 无 `prompt.md`、spec 照建、stdout 仍是裸串
      （存量调用方 = watcher 与 heartbeats/register，两者今天都是「create 先、写 prompt 后」）。
    ⑤ **既有拒建语义不变**：已在场目录 + `--prompt-file` ⇒ 仍 `已存在` rc=2 且零落盘
      （不覆写已有任务目录里的 prompt.md）。
    ⑥ **help 面**：`create --help` 写明三格（写点在 spec.json 之前 / 拒分支零落盘 / 缺省不变）。
    ⑦ **与 `--profile` 同用不互斥**：前缀拼接照旧 ∧ prompt.md 照落。
    只跑 agentctl 子进程（不起 runner/scheduler、不读写主树 ROOT），收尾自清。"""
    ws = os.path.join(S61BASE, "ws")
    os.makedirs(os.path.join(ws, "agents", "task"), exist_ok=True)
    os.makedirs(os.path.join(ws, "env"), exist_ok=True)
    with open(os.path.join(ws, "env", "host-id"), "w") as f:
        f.write("%s s61-canonical\n" % socket.gethostname())
    os.makedirs(os.path.join(ws, "bots", "profiles"), exist_ok=True)
    with open(os.path.join(ws, "bots", "profiles", "s61-review.json"), "w") as f:
        f.write("{}")
    # 任务书夹具：多行 + UTF-8 + 会被 shell/转义吃掉的字符族（逐字保真的反例面）
    BODY = ("# 任务：S61 夹具\n\n"
            "分级：M ｜ 中文与 emoji ✅ ｜ `反引号` ｜ $VAR 与 ${VAR} ｜ \"双引号\" 与 '单引号'\n"
            "末行无换行符")
    pf = os.path.join(S61BASE, "prompt-body.md")
    with open(pf, "w", encoding="utf-8") as f:
        f.write(BODY)
    bad_dir = os.path.join(S61BASE, "bad")
    os.makedirs(bad_dir, exist_ok=True)
    zeros = os.path.join(S61BASE, "zero.md")
    open(zeros, "w").close()
    blank = os.path.join(S61BASE, "blank.md")
    with open(blank, "w") as f:
        f.write("  \n\t\n")
    binf = os.path.join(S61BASE, "bin.md")
    with open(binf, "wb") as f:
        f.write(b"\xff\xfe\x00bad")

    def run(*args):
        env = scrub_env()
        env.pop("AGENT_SELF", None)
        return subprocess.run([sys.executable, os.path.join(HERE, "agentctl.py"),
                               "--root", ws, *args], capture_output=True, text=True,
                              env=env, timeout=60)

    def create(name, *extra):
        return run("create", "--name", name, "--command", "true", "--workdir", ws,
                   "--creator", "tester", *extra)

    def adir(name):
        return os.path.join(ws, "agents", "task", name)

    def no_dir(name):
        assert not os.path.exists(adir(name)), "拒后零落盘（不得建 task/%s）" % name

    try:
        # ---- ① 正常落盘：rc=0 + stdout 裸串 + 内容逐字 ----
        r = create("s61-a", "--prompt-file", pf)
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == "s61-a", "stdout 仍是 taskId 裸串（既有语义）：%r" % r.stdout
        got = os.path.join(adir("s61-a"), "prompt.md")
        assert os.path.exists(got), "prompt.md 应在场：%s" % got
        with open(got, encoding="utf-8") as f:
            assert f.read() == BODY, "逐字保真（⛔ 渲染 ∨ 转义 ∨ 补尾换行）"
        assert os.path.exists(os.path.join(adir("s61-a"), "spec.json")), "spec 照建"
        assert os.path.isdir(os.path.join(adir("s61-a"), "inbox")), "inbox/ 照建（布局不变）"

        # ---- ② 写点顺序钉（AST 级，⛔ 文本 grep）----
        with open(os.path.join(HERE, "agentctl.py"), encoding="utf-8") as f:
            tree = ast.parse(f.read())
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "cmd_create")

        def _tag(node):
            """写点的目标文件名标签：直提字串常量 ∨ 名字，也钻一层 `os.path.join(adir, X)`
            取 X（现网两个写点都是 join 形态）⇒ 标签 = `spec.json` ∨ `PROMPT_FILE_NAME`。"""
            if isinstance(node, ast.Call) and node.args:
                node = node.args[-1]
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                return node.value
            if isinstance(node, ast.Name):
                return node.id
            return None

        lin = {}
        for n in ast.walk(fn):
            if not isinstance(n, ast.Call) or not isinstance(n.func, ast.Attribute):
                continue
            if n.func.attr not in ("atomic_write", "atomic_write_json") or not n.args:
                continue
            t = _tag(n.args[0])
            if t:
                lin.setdefault(t, n.lineno)
        assert "spec.json" in lin and "PROMPT_FILE_NAME" in lin, \
            "cmd_create 里应同时有 prompt 与 spec 两个原子写点（按实参识别）：%s" % sorted(lin)
        assert lin["PROMPT_FILE_NAME"] < lin["spec.json"], (
            "prompt.md 的写点必须在 spec.json **之前**（调度方的可见性锚是 spec.json；"
            "写在它之后只是把竞态窗变窄、⛔ 变没）：prompt@%d spec@%d"
            % (lin["PROMPT_FILE_NAME"], lin["spec.json"]))

        # ---- ③ 拒分支 ×5 + 零落盘 ----
        cases = [("不存在", os.path.join(S61BASE, "nosuch.md")), ("是目录", bad_dir),
                 ("零字节", zeros), ("纯空白", blank), ("非 UTF-8", binf)]
        for i, (why, p) in enumerate(cases):
            nm = "s61-b%d" % i
            r = create(nm, "--prompt-file", p)
            assert r.returncode == 2, "%s ⇒ 应拒（rc=2）：rc=%d %s" % (why, r.returncode, r.stderr)
            assert "--prompt-file" in r.stderr, "stderr 须点名该旗标（%s）：%s" % (why, r.stderr)
            assert "本次未创建任何目录/文件" in r.stderr, "须声明零副作用（%s）：%s" % (why, r.stderr)
            no_dir(nm)
        assert os.path.isdir(adir("s61-a")), "对照组 s61-a 不受拒分支影响"

        # ---- ④ 缺省行为逐字不变 ----
        r = create("s61-c")
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == "s61-c", r.stdout
        assert not os.path.exists(os.path.join(adir("s61-c"), "prompt.md")), \
            "不传旗标 ⇒ ⛔ 写 prompt.md（存量调用方行为逐字不变）"
        assert os.path.exists(os.path.join(adir("s61-c"), "spec.json")), "spec 照建"

        # ---- ⑤ 已在场目录 + --prompt-file 仍拒（既有拒建语义不变、⛔ 覆写）----
        before = open(os.path.join(adir("s61-a"), "prompt.md"), encoding="utf-8").read()
        other = os.path.join(S61BASE, "other.md")
        with open(other, "w", encoding="utf-8") as f:
            f.write("# 另一份正文（不得覆写既有任务书）\n")
        r = create("s61-a", "--prompt-file", other)
        assert r.returncode == 2 and "已存在" in r.stderr, (r.returncode, r.stderr)
        assert open(os.path.join(adir("s61-a"), "prompt.md"), encoding="utf-8").read() == before, \
            "拒建 ⇒ 既有 prompt.md 逐字不动"

        # ---- ⑥ help 面 ----
        r = run("create", "--help")
        assert r.returncode == 0, r.stderr
        assert "--prompt-file" in r.stdout, r.stdout
        for tok in ("spec.json` 之前", "零落盘", "行为逐字不变"):
            assert tok in r.stdout, "help 须写明 %r（写点顺序/拒分支零落盘/缺省不变）" % tok

        # ---- ⑦ 与 --profile 同用不互斥 ----
        r = create("s61-d", "--prompt-file", pf, "--profile", "s61-review")
        assert r.returncode == 0, r.stderr
        with open(os.path.join(adir("s61-d"), "spec.json"), encoding="utf-8") as f:
            doc = json.load(f)
        assert doc["command"] == "DISPATCH_PROFILE=s61-review true", doc["command"]
        with open(os.path.join(adir("s61-d"), "prompt.md"), encoding="utf-8") as f:
            assert f.read() == BODY, "两旗标同用 ⇒ prompt 仍逐字"
    finally:
        _rm_scenario_root(S61BASE)


def _scrub_inherited_env():
    """入口自洗继承来的身份族（t-zqm0；就地改 os.environ）。

    e2e 常从 agentd 子任务里直接跑 ⇒ 继承 AGENTD_WRAP_INIT_OK / AGENTD_WRAP_RECV_ARMED
    等身份/信号标记，S49 的嵌套 receiver 会信外层任务的就绪标记而假 FAIL（就绪门身份
    自校正确，属脚手架环境卫生、非产品缺陷）。洗刷判据与名单一律取自 envscrub.scrub_env
    （strip_third_party=False：第三方 key 族是 runner spawn 侧的口径，e2e 不剥——S17 的
    假 key 经 start_runner(env_extra=…) 注入，在洗刷之后叠加，不受影响）。
    """
    clean = scrub_env()
    for k in [k for k in os.environ if k not in clean]:
        del os.environ[k]


def main():
    _scrub_inherited_env()
    os.makedirs(os.path.join(ROOT, "agents"), exist_ok=True)
    # tester：信箱型 bot（只有 inbox，§2.1；bot 布局）
    os.makedirs(os.path.join(ROOT, "agents", "bot", "tester", "inbox"), exist_ok=True)
    start_runner()
    check("S1 创建→运行→正常退出→谓词判定", s1)
    check("S2 崩溃+auto 自愈（只拉崩溃三态）", s2)
    check("S3 pause 已退役：agentctl 拒绝 + runner rejected unknown action 零写入 + restart 健在", s3)
    check("S4 stop→final→restart/已退役动作 rejected", s4)
    check("S5 ask/reply 握手（ack 判重+ref 闭环）", s5)
    check("S6 自定义目录名创建与存在性拒绝", s6)
    check("S7 enable.json 门禁（内置无开关，手工放行才 spawn）", s7)
    check("S8 路由：spec.host 它机不认领", s8)
    check("S10 伪造 pid 复用：探活判消失+kill 被拒不伤及无关进程", s10)
    check("S11 杀 runner 重启接手存活进程不误判 stale（0.b）", s11)
    check("S12 FIFO 串行调度骨架（独立调度方）", s12)
    check("S14 终态通知：done/failed/canceled 三态 + 字段齐备 + 幂等（T1-5）", s14)
    check("S15 runner 重启不重发终态通知（判重基于文件，T1-5）", s15)
    check("S16 探活锁文件：双开拒绝+死锁接管+优雅退出不删+updatedAt 周期刷新（k431）", s16)
    check("S17 子进程环境注入 AGENTD_TASK=1 防递归标记 + 第三方 *_API_KEY 洗刷（T1-6）", s17)
    check("S18 DAG 资源互斥：同资源串行、异资源并发", s18)
    check("S19 DAG provides/needs：成功放行+失败不放行+自动解锁", s19)
    check("S20 占位上限+越位：调度方 --max-concurrent 限流，阻塞者不阻塞后续", s20)
    check("S21 接管孤儿退出归属：有 report.md 报 done / 无 report.md 报 failed", s21)
    check("S22 登记写 host：缺省物化规范名（映射查表）/显式他机≠createdByHost/未命中与缺失回退", s22)
    check("S23 调度器全局视图：--all-hosts 为他机放行/缺省只放本机（行为回归）", s23)
    check("S24 命令形态约定：裸命令执行 + 旧格式（带 bash -c）双壳兼容（klbk）", s24)
    check("S25 spec 路径跨 home 可移植：登记侧 ~/ 归一化 + 执行侧 expanduser + $AGENT_HOME/$AGENT_ROOT 展开（5cyp）", s25)
    check("S26 无身份档案（procStart 缺失）的存活孤儿接管不误判 stale/127（0828-0018）", s26)
    check("S27 缺 host 无人认领：双伪节点不认领+告警/调度器不放行/登记回退非空/带 host 不回归（0828-0044）", s27)
    check("S29 needs provider 127 容忍：stale/127+报告放行 / 非 stale、无报告、非 0 退出码、取消不放行（0829-1052/0829-1150）", s29)
    check("S30 stale 锁回归：陈旧锁判死不放行/锁刷新恢复放行/本机不回归", s30)
    check("S31 agentctl 调度字段 CLI：合法数组落盘/缺省不落盘/非法输入拒绝", s31)
    check("S32 bot 布局：自动名落 task/ 端到端 + id 文法/直落/扫描面 + create-bot + 信箱型 bot 不入任务面", s32)
    check("S33 rpc 封装全链路：pid.json.sock 字段 + agents/ 零 socket + 收敛 exit 0 + 通知 + 清理 + result.md 退役", s33)
    check("S34 runner 重启不杀任务：wrap 自持管道存活 + 孤儿接管心跳续刷 + stop 组杀收敛", s34)
    check("S37 常驻能力：无 AGENTD_TASK 注入 + 调度槽位/资源豁免", s37)
    check("S38 control/clear：杀+备份+截断+空白新代 + one-shot/final/spawn 前边界", s38)
    check("S39 topic 协作容器：寻址四族/扫描面隔离 + send 投递闭环（自动建目录/信封/文件名）+ GC 接受 topic/", s39)
    check("S40 agentctl 脚手架：topic init 布局/骨架/watcher 登记/拒绝面 + bot register --subscribes（通道 B 写入口、只改一字段、清空）+ --description/--reaper（：两键写入/缺省不写/非法 reaper 零落盘/既在场不生效）+ 标题跳 frontmatter+ 主题节主持人列（宽口径判据/跨机链接/剔重/系统主题豁免）", s40)
    check("S41 退役地址护栏：proto.RETIRED_MAILBOXES 与 core.ts 同源 + send/ack/control/enable 拒绝并回执 queue/dispatcher + 拒后零副作用 + 独立于目录在场性 + 正常地址零回归", s41)
    check("S42 取消任务不再误报无报告：取消（stop 请求/exit 0 竞态/125）不发 no_report warn + 非取消缺报告仍发 warn + report.py verdict 同判据", s42)
    check("S43 空跑 provider 不算成功：exit0 无报告不放行/有报告放行 + 取消豁免（两判据）不判失败不发告警 + pending 优先于旧 success（含取消恢复路径）+ 零字节/纯空白 report.md 不算交付（🟡3）+ 报表面跟随裁决（需:列点名在途 provider、异常区 ⚠️ pending 压旧 success 含恢复路径，🟡2）+ 夹具未被拉起真验（⚪5）", s43)
    check("S44 终态通知重放护栏：冷启动零重放（空信箱 + 25 历史档案）+ 迟到档案不重放 + 新终态通知一次且载荷齐备 + 孤儿接管 stale/127+report 仍通知 + 取消不回归（两判据）+ 旧代终态档案本次 stop 收口仍通知 + 宽限窗可调+ 畸形 endedAt（键缺失/空串/垃圾串）降级抑制且不瘫痪整轮、排序在后的真终态照常通知", s44)
    check("S45 终态通知收件面（reaper 单收件方）与回落面：活性 reaper 直投自家信箱（载荷无 role/reaperPid、职位信箱零份）/ reaper=职位信箱直落不带 note / 缺字段/文法非法/目录不在场/在场但无消费者（活性代理）→ 回落 + note 点名成因 / notified.json 标记唯一判重事实源（存量信封不参与判重：无标记档案重发一条后落标记）/ 多轮 tick+杀重启不重发", s45)
    check("S49 子任务自家信箱推送收件面：真 receiver-child.ts 认领注入 + 扁平 ack（落盘确认后写）"
          " + deliver 两形态（steer/缺省 followUp）+ 0828 回归：职位信箱终态通知零 ack/零注入/"
          "原件留存", s49)
    check("S50 子端收件面 P0 spawn 竞态：带未读信封 spawn 必有 session.jsonl（必要非充分：硬判据"
          " = 会话树含初始 prompt 的 user 事件，见 S53）（健康路径：门开晚于"
          " prompt 接受、注入落树、信封全 ack、reply 零认领、标记退出即清）/ 形态① exit 1 秒死"
          "（stage=prompt_rejected ∧ session 不在场）/ 形态② exit 0 假成功（零执行却报 task_done，"
          "只剩 warn=no_report）/ 变异对照（两形态加 honor 门 → 回健康路径）/ 陈旧标记 spawn 前必清"
          " / arm 有界等待不假活（ 🔴0）", s50)
    check("S53 真 pi live：真 fs.watch 认领 + 真 steer/followUp 队列注入落会话树 + 真落盘确认"
          "（注入文本落 jsonl → 写终态 ack）+ 就绪门在真 pi 下成立（会话树含初始 prompt 的 user 事件、非 exit0 假成功）"
          " + reply 零认领零注入（ 测试要求③）", s53)
    check("S54 终态通知 reaper 主模型：reaper 直投自家信箱（职位信箱零份、载荷无 role/reaperPid）/ spec 残留 watchers 字段零作用（零投递零建目录）/ reaper 不存在 → 回落职位信箱带 note / (taskId,收件方) 判重：多轮 tick + 杀重启后只一份且标记 to=[reaper]", s54)
    check("S55 bot 族自身终态（两族同规则）：进程型 bot（spec.reaper、restartPolicy=auto）真跑 → control stop → 通知落 reaper 信箱（event=task_canceled、载荷无 role）+ 标记落位 + 职位信箱零份 / 对照无 reaper 字段 → 回落职位信箱带 note", s55)
    check("S56 子端 ask 写侧收件面=该 （项 1）：runner spawn 复用 resolve_reaper 单点经 env 注入 AGENTD_ASK_INBOX/NOTE——reaper 活→指其自家信箱且无 note / reaper 不存在→回落职位信箱+note 点名成因且不建僵尸目录", s56)
    check("S57 控制信封 from 归属单点化：显式 --from（非法即拒+零副作用）＞环境"
          " AGENT_SELF（过文法白名单，非法/缺失静默落档）＞回落职位信箱 queue/dispatcher；"
          "裸名缺省值（\"operator\"）退场；信封 from 与文件名前缀同源（与 TS 侧"
          " core.resolveCreatorPid 同优先级）", s57)
    check("S58 写侧动词两档：send（协议动词：type 缺省 inform / --deliver 显式才落盘 / "
          "--body-file - 逐字保真 / type=ask 自动带 via / from 三档归属 / 拒分支零落盘）"
          "・answer（用例动词：扫描面并集里找最早未答 ask + reply 继承 via + 回执点名所答条目；"
          "`--deliver` 与 send 同形：steer 逐字落盘 / 缺省 = 信封不写该键（⛔ 非 followUp）/ "
          "枚举外值 argparse 拒 rc=2；无未答 ask/已 final/无 spec 三种意图落空全拒）・cancel（= control stop + 存活前置门；"
          "对照 control 不过门）・update（五道硬校验 + 只覆盖传入字段）"
          "・跨语言钉桩（TS core.ts 的 via/deliver/消息型枚举与 proto 常量逐字相等）"
          "・--root 缺省现场发现（三种 cwd 同根；显式错 root 仍拒）", s58)
    check("S59 --root 前置校验自我击穿回归：错 root（agents 树本身）在嵌套残骸在场时"
          "仍 die（rc=2）且零新建目录/文件（写侧 send + 只读 status 各一次）"
          "・零回归：正常工作区根（自建临时根 + 真工作区根）只读动词 rc=0、残骸在场也放行"
          "（不收窄 <root>/agents/ 下条目形状）・env/host-id 软前置未升硬（只 WARN、rc=0、"
          "登记回退 hostname 非空）", s59)
    check("S60 agentctl create --profile（人格装载的登记侧写入口）：拼 DISPATCH_PROFILE= 前缀"
          "（与手写形态逐字同款、spec 不长新键）・非法名被拒（白名单 = 现场枚举"
          " <root>/bots/profiles/*.json，枚举根按 --root 解析；枚举根不在场也拒）+ 打印可选名单"
          " + 零落盘・前缀冲突被拒（异值 rc=2 且两侧值都在报文里；同值幂等不重复拼）"
          "・缺省一行 WARN 且照建（command 逐字原值；自带前缀不打 = 存量手写形态零回归）"
          "・create --help 覆盖三格", s60)
    check("S61 agentctl create --prompt-file（任务书与登记同批落盘）：可读文件 ⇒ prompt.md 逐字在场"
          "且 stdout 仍是 taskId 裸串・**写点顺序钉**（AST：prompt 写点行号 < spec.json 写点行号"
          " = 竞态窗结构上闭合）・拒分支 ×5（不存在/是目录/零字节/纯空白/非 UTF-8）rc=2 + 零落盘"
          "・缺省不传旗标行为逐字不变（⛔ 写 prompt.md）・已在场目录仍拒且不覆写既有任务书"
          "・create --help 覆盖三格・与 --profile 同用不互斥", s61)
    stop_runner()
    stop_scheduler()

    # 收尾：杀掉任何残留的伪 agent（命令行带 ROOT 标记）
    subprocess.run(["pkill", "-9", "-f", ROOT], capture_output=True)
    time.sleep(0.3)
    left = subprocess.run(["pgrep", "-f", ROOT], capture_output=True, text=True)
    ok_clean = left.returncode != 0
    print(("PASS" if ok_clean else "FAIL") + "  S9 无遗留测试进程", flush=True)
    RESULTS.append(("S9 无遗留测试进程", ok_clean, left.stdout.strip()))

    fails = [r for r in RESULTS if not r[1]]

    # 平台 skip 汇总（不静默：成败两路都报，且点名是哪一项的哪个子断言、为何跳）
    if PLATFORM_SKIPS:
        hit = sorted({lbl.split()[0] for lbl, _r in PLATFORM_SKIPS})
        print("\n平台 skip：%d 项中 %d 项含平台 skip（共 %d 处子断言，平台 %s）："
              % (len(RESULTS), len(hit), len(PLATFORM_SKIPS), sys.platform))
        for lbl, reason in PLATFORM_SKIPS:
            print("  SKIP %s —— %s" % (lbl, reason))
    else:
        print("\n平台 skip：0 处（%d 项全量执行，平台 %s）"
              % (len(RESULTS), sys.platform))

    if fails:
        print("\n%d 个场景失败，保留临时树供排查：%s" % (len(fails), TMPBASE))
        sys.exit(1)
    shutil.rmtree(TMPBASE, ignore_errors=True)
    print("\n全部 %d 项通过（其中平台 skip %d 处子断言）；临时树已清理。"
          % (len(RESULTS), len(PLATFORM_SKIPS)))


if __name__ == "__main__":
    main()
