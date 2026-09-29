---
name: agentd
anchors:
  usage: "用法"
  report: "报表（report.py）"
  run-params: "运行参数（生产化）"
  deploy: "部署形态"
  scheduler: "调度（独立调度方，scheduler.py）"
  scheduler-params: "调度参数（spec.json 可选字段，登记时写入）"
  release-rules: "放行规则（每轮无状态重扫，允许越位）"
  routing: "路由字段"
  sync-channel: "跨机同步通道"
  risks: "已知风险（多机）"
  kill-discipline: "判死纪律与杀纪律（协议 §5.5）"
  conventions: "约定赋值与实现口径"
  intake: "收录判据"
---

# agentd — Agent 文件通信协议生产核心

> 注：正文里的 `` `@<名>#<锚点>` `` 是作者工作区的按名引用记号（解析器在那个工作区内），
> 在本仓单独阅读时按「另一册的节名」理解即可。

协议权威文档：`agent-file-protocol.md`（同仓）。

**全部 python3 标准库实现，零第三方依赖。**

## 收录判据

**① 本仓只住协议级机制**：布局与寻址、信封与 ack、两层谓词、调度与放行、杀纪律、以及这些面的 CLI 写入口。三类内容不得入本仓代码——由调用方注入 ∨ 现场发现：

- **工作区政策文案**：任务书正文（分级门禁措辞、验收条默认项）、人格与岗位资产正文——由调用方渲染并自行落进该任务目录（`create` 只建布局与 `spec.json`，不渲染任务书正文）。
- **实现体路径与会话封装形态**：拉起什么命令、注入哪些扩展/能力/知识面。本仓只写 `spec.command` 字段，值一律由调用方给（包含其中的 env 前缀）。
- **按部署面变化的值**：主机名与机器清单、仓名单、内网端点、凭据面路径。机器身份按 `<root>/env/host-id` 现场查表，`<root>` 由调用方传。

判据（可验证）：**改一处部署 ∨ 改一条政策措辞，不得产生本仓的 diff**。

**② 调用方侧的 pi extension 只住需要 pi 运行时能力的面**：事件钩、消息注入与投递方式（`deliverAs`/steering）、会话树与 jsonl 读写。**纯文件读写一律落本仓 CLI**（`agentctl.py`），不得做成 pi 工具——写侧（登记 / 发消息 / 答复 / 取消 / 改调度字段 / 征询）与只读状态面全部适用。两条理由：

- **门禁位置**：CLI 的门禁在命令入口，对任何调用方（会话、脚本、人）一致生效，且不以「扩展被正确加载」为前提；pi 工具的门禁只能挂 `tool_call` 钩子。
- **冲突面**：工具形态引入「同名工具冲突」这一整类故障（cwd 自动发现件与 `-e` 显式注入件同名 ⇒ pi 中止会话），CLI 形态结构上不可能发生。

## 文件

| 文件 | 角色 |
|---|---|
| `agent-file-protocol.md` | 协议权威文档 |
| `RACE-NOTES.md` | 已知竞态窗留档（收敛竞态窗窗口/成因/收敛措施/残余边界） |
| `proto.py` | 公共库：原子落盘、时间戳/自动名、两层判定谓词、机身份匹配、杀纪律原语（procStart 读取、(pid,procStart) 校验）、**信封读写与 ask 扫描面**（`list_messages`/`write_message`；`ask_scan_inboxes` = 职位信箱 ∪ `spec.reaper` 自家信箱、`find_pending_ask` = 最早一条未答 ask、`ask_summary`；以及三个跨语言常量 `MSG_TYPES`/`DELIVER_MODES`/`ASK_VIA_SEND_MESSAGE`，与 TS 收件侧逐字相等由 e2e S58⑤ 钉桩） |
| `runner.py` | 每机中央守护形态 runner（只含运行半边：spawn/监控/判挂死）；enable.json 门禁内置无开关（调度方 = 独立服务 `scheduler.py`，见下） |
| `scheduler.py` | 调度半边（自 runner.py 拆出，独立常驻）：DAG 调度（resources/provides/needs + 全局占位上限），无参数任务 = FIFO 串行；`--all-hosts` 全局视图（为所有机器放行） |
| `agentctl.py` | CLI（**动词分两档**，判据 = `require_live_participant`：协议动词只负责把布局与信封落盘，对不存在的收件方与已终态的参与方同样合法（目录即队列 §11.4、收尾清理也要能写 stop）；用例动词代表一个**意图**，带前置门（存在 / 非生命周期终态 / 意图可成立），意图落空当场报错而不静默写无人消费的件；写信封与写控制请求的 `from` 归属一律走 `resolve_sender` 单点，不按动词分叉）：create / create-bot / **bot register**（进程型 bot spec + `--subscribes` 通道 B + `--description`（→ spec `name`，人类可读描述）/`--reaper`（→ spec `reaper`，终态通知收件面，文法 = `proto.is_valid_participant_id`，非法即拒且零落盘；两 flag 缺省时不写该键，spec 与既有行为逐字一致；spec 已在场分支只改 subscribes、两 flag 不生效））/ **topic init**（topic 标准布局脚手架）/ send / ack / control / enable / status / list（以上 = 协议动词）+ **answer**（用例动词：自动找最早一条未答 ask → 写 reply、ref 回引、**继承 ask 的 `via`**（丢了该标记 = 收件侧 drain 不放行、答复静默悬空，§4.5）、回执点名所答条目与阻塞态）/ **cancel**（= control stop + 存活前置门）/ **update**（改未放行排队任务的 resources/provides/needs，五道硬校验含「已放行即拒」）。`send` 的 `--type` 缺省 inform、`--deliver` 显式才落盘（缺省 = followUp，§6.6）、`type=ask` 自动带 `via`；长正文走 `--body-file <路径|->`（`-` = stdin，不经 shell 断词与展开）。**只创建不删除**：`agents/` 清理一律走 `agents-sync/gc.py` 通道 |
| `report.py` | 只读 markdown 报表：`agents/` 全链路状态（系统小节/统计头/异常区/活跃表；终态表缺省隐藏，`--finalized`/`--all` 显示），为反复刷新观察设计 |
| `needscheck.py` | 只读判定 CLI（JSON）：「needs 不可满足（dead-ended）」任务清单，供 `task_status` 工具面取信号。判定/文案全部 import 复用（`scheduler._scan` 的 provider 五态 + `scheduler.eval_needs` 三态 + `report._provider_states_zh`），本文件零自实现；判定范围与 `report.py` 异常区「🚨 needs 不可满足」逐条对齐（同源断言钉在扩展单测）。仅呈现：不放行、不取消、不改调度语义 |
| `pi-rpc-wrap.py` | 会话封装（任务/常驻两形态）：拉 `pi --mode rpc` + 观测 socket 透传 + 完成收敛 + 失败诊断；**人格面只做两件事**（装配**不住本文件**：解析层 = `pi-wrap/persona.py`，注入层 = pi 扩展 `pi-core/agent/extensions/profile-loader.ts`；口径 `@dispatch#params`）：① `-e` 注入层扩展——该文件本就在自动发现面（`~/.pi/agent/extensions/`），显式 `-e` 是为**钉装载序**（pi 的 CLI `-e` 先于自动发现，`before_agent_start` 按装载序跑 ⇒ 人格正文落在其它全局扩展〔如 host-info 身份行〕的追加之前；pi 按realpath 去重 ⇒ 同文件两路只装载一次）；② 透传注入层要读的输入 env（`DISPATCH_PROFILE` = profile 名、`AGENTD_RESIDENT` = 形态、`AGENT_ROOT` = 找解析层）。**人格数据一律不走 argv**：正文/知识清单/skill 路径/工具面/模型/压缩策略全部在会话内注入（发射契约由 `test_wrap` T48 钉）。注入层缺失 ⇒ WARN 点名「人格面缺席」+ 会话照起（fail-soft，`test_wrap` T48 钉四面）。profile 级 `contextCompaction`（字段规范 = `@bots#persona-assets`）的 **env 写者与执行体装载同归注入层**⇒ 本文件只**恒洗掉**从宿主继承的 `AGENTD_CONTEXT_COMPACTION`（「无策略 = env 不在场」是硬语义，不靠调用方环境干净）；执行体自建触发 + 对 pi 内建 `threshold` 触发做 cancel 以后移触发点（`overflow`/`manual` 一律放行；只读取证命令 `/compaction-policy`）。**排障面位置随之搬家**：逐能力注入日志（`人格装配（会话内注入）：…`）与解析层告警写在 **pi 的 stderr** ⇒ `run/agentd/<id>.stderr.log`（与诊断引用的 stderr 尾同源），不再在本文件自己的日志里；会话内取证 = `/persona`（生效面全集：caps 与逐能力字符数 / skills / 工具面 / model /压缩策略 / 告警）。**子端扩展注入单点** = `CHILD_EXTS`（只任务形态；resident 不注入，其主端扩展由 workdir 的 `.pi` 自动发现）：反问 / 发参与方消息 / **自家信箱推送收件**（`receiver-child.ts`，收件面锁死自家 `task/<id>/inbox`，口径 `@dispatch#lifecycle`/`@dispatch#notify`）。文件缺失只 WARN 跳过不拖垮会话（→ 收件面会静默失效，test_wrap T29 断言注入路径均在场）。**就绪握手**（🔴0）：初始投递收口后写 `run/agentd/<id>.init-ok`（单点 `proto.task_ready_path`）→ 子端据此开「就绪门」才开始 drain 自家 inbox；子端首次补扫后回写 `<id>.recv-armed`，wrap **有界**（缺省 3s）等它之后才进收敛监督。缺这道握手 = 子端在 `session_start` 抢跑注入，两种形态：pi 拒收初始 prompt（`stage=prompt_rejected`、exit 1 秒死）或注入轮先跑完被当任务收敛（**exit 0 假成功**、`session/session.jsonl` 永不落盘）。两枚标记 spawn 前清陈旧、退出即清；resident 不参与（主端 receiver 行为不变）。**两枚信号 env（`AGENTD_WRAP_INIT_OK`/`AGENTD_WRAP_RECV_ARMED`）是身份/信号类，不得继承**（🔴1）：已收进 `envscrub.ENV_SCRUB_EXACT`（否则被任务内每个孙进程继承 → 经 bash 工具 → `make` → `serviced/serviced.py` 带进被启动的服务），且子端按**自家身份自校**（单点 `core.ownReadyMarks`/`taskReadyPath` 镜像 `proto.task_ready_path`；基名不符 = 视为无标记 → 门保持闭、走有界超时后强制开门 + WARN，绝不用别人的标记开门、也不写别人的 arm）。排障判据（订正，🟡3）：**exit 0 不足以证明任务执行过**，且「`session/session.jsonl` 在场（含 user+assistant 事件）」**也不足以**——抢跑形态下注入轮先起，文件在场且含 user+assistant，但初始 prompt 从未进会话树（真 pi 反例§二）；硬判据 = **会话树含初始 prompt 的 user 事件**（或 `report.md` 在场）。**末轮模型错误档（agentd v92 起）**：会话末条 assistant 的 `stopReason=error` ∧ `report.md` **非空不**在场 ⇒ `diagnosis.md` stage `model_error_stopreason` + **exit 1**（不再假成功）；非空在场 ⇒ stage `model_error_stopreason_delivered` + exit 0（信息性、不参与完成判定；例外已记在 `assistant/docs/task-layout.md` 的 diagnosis 行）。triage 口径 = 按**上游瞬时错误**处置（重派前先核是否已实质交付，见 skill `task-incident-triage` A 节 2b）。**已知缺口（诊断面；提案与方案本体 = agentfw 域 backlog「诊断 `stage` 记收口形态、不记根因族」节）**：`stage` 记的是**收口形态**（进程怎么被收口的），**不记根因族** ⇒ 模型侧错误若走到看门狗收口路径就会被按 `stage` 分族的统计**漏计**（实例 = 任务 ``：树内末 4 条 assistant 全 `stopReason=error`〔`Request timed out.` / `Connection error.` / `terminated`〕，而 `stage` 记的是 `converge_timeout_killed`、`exitcode: 143`；同族的 `` 走 pi 自行退出路径 ⇒ 记 `model_error_stopreason`）。**拟修 = 增并存字段 `rootCauseFamily`**（`stage` 语义不动 ⇒ 纯增量、不破坏既有消费者），判据复用「末轮模型错误档」的**尾窗读法** ⇒ **同款假阴性形态**（尾窗内无 assistant 条目 ⇒ 记 `unknown`，不猜）。**一条 WARN 的语义**：日志里 `WARN 会话尾窗（末 262144B / 文件 …B）内无 assistant 条目` = 该档**拿不到证据、按 fail-soft 未判失败**（假阴性方向、**不是 bug**）；成因 = 末条 assistant 之后又有合计 >256KiB 的条目把它挤出尾窗（现网多数会话文件已超该上界 ⇒「丢弃尾窗首行」是常态路径；而「窗内无 assistant」按 2026-09-15 全量 310 个会话的回放实测 = 0 例） |
| `fakeagent.py` / `fakepi_rpc.py` | 伪 agent / 伪 `pi --mode rpc` 测试负载（仅测试用） |
| `e2e.py` | 端到端测试：49 项 = S1–S58（无 S13/S28/S35/S36/S46/S47/S48/S51/S52；S9 = 收尾「无遗留测试进程」检查）——基础生命周期/判死接手/FIFO/DAG 调度/路由与 host/封装收敛/常驻能力/control 三动作（S3/S4：枚举外动作 agentctl 拒绝 + runner rejected 零写入）/topic 容器与脚手架（S39/S40）/退役地址护栏（S41）/取消不误报无报告（S42）/空跑 provider 不算成功（S43）/终态通知重放护栏（S44）/终态通知收件面与回落面（S45：reaper 单收件方直投 + 缺字段/文法非法/不活回落职位信箱带 note + notified.json 标记唯一判重事实源）/子任务自家信箱推送收件面含 0828 回归（S49；以子进程调 node 驱动真 TS 扩展）/子端收件面 P0 spawn 竞态两形态与就绪门（S50；fake pi 逐字复现 pi 的拒收串与 no-assistant guard）/**真 pi live 覆盖**（S53：localhost 桩供应商 + `PI_CODING_AGENT_DIR` 合成配置驱动真 `pi --mode rpc`，验真 inotify（fs.watch）认领、真 steer/followUp 队列注入落会话树、真落盘确认（注入文本进 jsonl → 写终态 ack）；pi 不在 PATH → 显式 skip），/终态通知 **reaper 主模型**（S54：收件面 = {reaper}、载荷无 role/reaperPid、spec 残留 watchers 字段零作用、reaper 缺失/不存在回落职位信箱带 note、(taskId,收件方) 判重）/bot 族自身终态同规则（S55：进程型 bot `control stop` → 通知落其 reaper 信箱；无 reaper 字段 → 回落职位信箱带 note）/子端 ask 写侧收件面=该任务 reaper（S56：runner spawn 复用 `resolve_reaper` 单点经 env 注入 `AGENTD_ASK_INBOX`/`NOTE`——reaper 活→指其自家信箱且无 note、reaper 不存在→回落职位信箱+note 且不建僵尸目录）/控制信封 `from` 归属单点化（S57：`agentctl control` 的 `from` 与文件名前缀走显式 `--from` ＞ 环境 `AGENT_SELF` ＞ 职位信箱的同源优先级，非法 `--from` 拒绝且零副作用，与扩展侧 `core.resolveCreatorPid` 同口径）/**写侧动词两档**（S58：send 的 type 缺省·`--deliver` 显式才落盘·`--body-file -` 逐字保真·`type=ask` 自动带 via·from 三档归属·六种拒分支零落盘；answer 在扫描面并集里找最早未答 ask + reply 继承 via + 三种意图落空全拒；cancel 带存活门而 control 不过门（对照）；update 五道硬校验 + 只覆盖传入字段；**跨语言钉桩** = TS 收件侧 core.ts 的 via/deliver/消息型枚举与本仓 proto 常量逐字相等；夹具期间停 runner/scheduler，否则真 runner 会消费夹具的 stop 请求把未启动夹具收成 final），逐项清单见文件头 docstring（可带 argv 子串只跑部分场景，如 `e2e.py S44 S45`；**部分场景有前置依赖**：S15 读 S14 的通知产物、S37 会清场前序遗留的非终态参与方，S44/S45/S54/S55/S56/S57/S58 自建夹具可单跑） |
| `test_wrap.py` | 封装单测（编号 `T<n>`，**上界一律取现场值**，不写死 = `grep -oE 'T[0-9]+' pi-wrap/test_wrap.py | sort -uV | tail -1`）：socket 生命周期/透传/收敛/resident/**人格面发射契约**（T48：恰一个 `-e` 指向注入层、**零**人格 flag、输入 env 原样透传、宿主陈旧`AGENTD_CONTEXT_COMPACTION` 被洗掉、注入层缺失 ⇒ WARN + 会话照常 exit 0 无诊断、resident 形态同样注入且不注入`CHILD_EXTS`）——人格面的**内容**判据（caps 展开序 / 任务形态基线前置与回落 / 工具面并集 / knowledge 三档 /model→provider 派生 / `contextCompaction` 归一 / 降级矩阵 / 输出契约键集）不在本套件，归`pi-wrap/test_persona.py`（P 系列，钉解析层）/子端扩展注入面（T29：任务形态按 `CHILD_EXTS` 顺序注入×3、缺失静默跳过不拖垮、resident 零注入）/就绪握手（T30：env 传两枚标记路径、prompt 接受早于开门、陈旧标记 spawn 前必清、失败路径不写标记、resume 幂等路径也写、resident 不参与、子端不 arm 时有界等待 + WARN 不假活、扩展缺失即不传不等）/**就绪门信号 env 的洗刷面**（T31：名单含两枚 + `scrub_env` 真洗掉 + 三调用方同源不复制名单 + `spawn_pi()` 洗刷后显式赋值 ⇒ 握手不受影响）/**`spec.command` env 前缀键的洗刷覆盖钉桩**（T47：扫 `bots/daemon/*/spec.json`、`heartbeats/register.py`、`w/ext/sessiond/proc.py` 的命令前缀键，逐枚断言 `scrub_env` 真洗掉 ⇒ 新增身份标记漏进洗刷名单在**提交前**就红，不靠运行时守卫） |
| `test_runner_notify.py` | runner 终态通知面单测（H9/H13/H14，编号沿用历史命名）：终态通知判重（notified.json 标记唯一事实源，信箱信封不判重）/ 收件面解析（`resolve_reaper` 两档 + 活性代理三判据 + 回落 note 文案）/ 子端 ask 写侧收件面 env（`_ask_inbox_env` 复用 `resolve_reaper` 单点——reaper 活→指其自家信箱无 note、不存在/无消费者→回落职位信箱+note 点名成因、同源自证、值恒为 str） |

运行时树（`agents/`）不入库（见仓库根 `.gitignore`）。

## 用法

```bash
# 起 runner（前台；生产由 loop.sh 监督，日志约定 ~/m/run/logs/agentd.log；
# 机器身份 = 规范名（env/host-id 映射文件按 $(hostname) 查表）+ 本机 hostname 别名，见下「路由字段」；
# 生产由 loop.sh 内联查表，下方命令为手工等价形态）
python3 runner.py --root ~/m --host "$(awk -v h="$(hostname)" '!/^[[:space:]]*#/ && $1==h {print $2; exit}' ~/m/env/host-id)" --aliases "$(hostname)" \
    --interval 0.5 \
    --log-file ~/m/run/logs/agentd.log --log-level INFO

# 起调度方（独立服务；生产由 scheduler-loop.sh 监督，make scheduler.start/stop/status，
# 日志 ~/m/run/logs/scheduler.log；--all-hosts 全局视图 = 为所有机器放行）
python3 scheduler.py --root ~/m --all-hosts --interval 0.5 \
    --log-file ~/m/run/logs/scheduler.log --log-level INFO

# CLI（--root 缺省 = 本仓父目录，现场发现 ⇒ 工作区内任何 cwd 直接调；另一棵树才显式 --root）
A="python3 ~/m/agentd/agentctl.py"
$A list                                    # 全参与方与状态摘要
$A status task/<id>                        # 代终态/生命周期终态判定（§10）
$A create --command 'sleep 10' --workdir /tmp --creator topic/dispatcher  # 自动名；--name 自定义
$A create … --resources '["gpu"]' --provides '["capA"]' --needs '["capB"]'  # DAG 调度字段（可选）
# 消息：协议动词 send/ack（只落盘）、用例动词 answer（带前置门）。
# `from` 缺省取环境 AGENT_SELF，再缺省回落职位信箱（不接受裸名；显式 --from 非法即拒）
$A send task/<id> --body '补充裁定' --deliver steer   # type 缺省 inform；steer = 立即介入当前轮（§6.6）
$A send bot/<名> --type ask --body-file -            # 长正文走 stdin（不经 shell 断词）；ask 自动带 via
$A answer task/<id> --body '按方案 A 继续'           # 自动找最早未答 ask，reply 继承其 via（§4.5）
$A ack task/<id> <msg-id>                            # 收件方传输层确认（§4.6）
# 控制：协议动词 control（收尾清理也可写）、用例动词 cancel/update（带前置门）
$A control task/<id> stop|restart|clear [--inject X] [--reason R]
$A cancel task/<id> --reason '需求作废'              # = control stop + 存活前置门
$A update task/<id> --resources '["gpu"]'            # 只改未放行排队任务；已放行即拒
$A enable task/<id> --by who                        # 手工放行（兜底；正常由调度方写）

# topic 脚手架与 bot 登记（设计稿 `@topic-design#moderator-carrier`/`@topic-design#layout-addressing`）
$A topic init <议题 id> [--title <标题>] [--watcher <会话裸名>]…   # 建 topic.md 骨架 + inbox/ + watcher/ 订阅条目
$A bot register --name <名字> --subscribes topic/<议题 id>[,…] \
     [--command … --workdir … --creator …]   # spec 不在场需后三件；已在场则只改 subscribes（''=清空）
     [--description <一句描述>] [--reaper <family/名>]   # 可选：新建 spec 时写 §4.1 的
     # `name`（人类可读描述）与 `reaper`（终态通知收件面）；缺省不写该键；已在场分支不生效
# 删除铁律：两者都只创建——agents/ 内任何清理走 `python3 agents-sync/gc.py add <路径>` + `reap`

# lore 资产清单 + 全局名字索引（`knowledge` 名 → 清单；规范 `@bots#kb-spec`，
# 机制口径 `@dispatch#params`，lore 内容组织 `@lore#gates`，单测 bots/test_kb_index.py）
python3 bots/kb_index.py index library/agentfw                 # 名（lore 根下相对路径）→ markdown 索引表
python3 bots/kb_index.py index --recursive assistant/docs      # 工作区路径声明（legacy 档）
python3 bots/kb_index.py audit the workspace knowledge base/library/work               # 巡检：未入册（缺 frontmatter when:）
python3 bots/kb_index.py prompt --domains-json '["library/agentfw","desk/agentfw-lead"]'  # 注入用知识清单块
python3 bots/kb_index.py names -v                              # 全局名表（frontmatter name:/anchors:）+ 缓存统计
python3 bots/kb_index.py resolve <名>[#<锚点>]                  # 名 → 路径（未命中 rc=2、撞名 rc=1）
python3 bots/kb_index.py read <名>#<锚点>                       # 输出该节全文（节边界 = 下一个同级或更高级标题）
python3 bots/kb_index.py refs <名> [--scope tasks]              # 反向索引 = 改权威前的知会名单
python3 bots/kb_index.py check                                  # 名引用/锚点契约核对（悬空名/未声明锚点/锚点漂移；有违规 rc=1）
```

```bash
# 端到端测试（临时树在 /tmp，结束后全清）
python3 e2e.py
```

扩展侧 guard 测试 = `assistant/.pi/extensions/agentd/tests/agentd-ext.test.mjs`；guard 行为变更须同步扩断言。
**跑它（以及任何外部命令）一律用 `timeout N` 包一层**（P1 事故 2026-09-15：常驻会话在自己轮里无上界跑本套件
⇒ 主端扩展泄漏的 ref'd 句柄让 node 永不退出、把该会话的 toolCall 楔住 11h45m，用户转呈链路断 11h40m）；
且 rc 不得经管道取（`| tail` 会让 `rc=$?` 变成 tail 的 0）——重定向到文件后取 ∨ `set -o pipefail` ∨ `${PIPESTATUS[0]}`。
套件自身已有界退出（末尾显式 `process.exit(0)`）+ 两组句柄卫生断言：H1 = 子进程「自然排空」模式须在 120s 内
rc=0 退出（三个句柄任一处 `unref` 回退即红）、H0 = 主端装配后零新增 ref'd 活动资源（`getActiveResourcesInfo()`
差分）+ 三处 `unref` 逐处钉住；纪律本体 = `receiver-child.ts` 头注的「生命周期纪律（两端同款）」。
封装单测 = `pi-wrap/test_wrap.py`；人格装配解析层单测 = `pi-wrap/test_persona.py`（注入层是 pi 扩展，其端到端面靠真 pi 取证，见该仓 README「Assembly face」）；lore 资产清单与名字索引工具单测 = `bots/test_kb_index.py`；
能力/profile 资产 lint 单测 = `bots/test_cap_lint.py`（lint 本体 `bots/cap_lint.py`：改人格资产后必跑、
零 ERROR 才 commit，判据与反向索引口径见 `@bots#persona-assets`）。
收件注入面的**真 pi 端到端装置**（opt-in，不进缺省回归面）=
`assistant/.pi/extensions/agentd/tests/inject-gate-live.mjs`，跑法与环境要求见下「约定赋值与实现口径」的
「收件注入闸门」条。

**并发快照隔离纪律（攒批 3）**：`e2e.py`/`agents-sync/gc.py` 正被其它在飞任务修改期间跑 e2e，
必须用快照隔离——`mkdir -p /tmp/<taskId>-snap && git archive HEAD | tar -x -C /tmp/<taskId>-snap`
后在快照里跑（e2e 全程按 `HERE` 相对路径取 `runner.py`/`fakepi_rpc.py`/`agentctl.py` 与
`../assistant/.pi/extensions/agentd`，故整树快照即可）。否则被测文件会在跑到一半时被换掉 →
假失败且临时树里的证据与 HEAD 不一致、无从复现。跑前一律用干净环境：
`env $(python3 -c "import sys;sys.path.insert(0,'agentd');import envscrub;print(' '.join('-u '+k for k in sorted(envscrub.ENV_SCRUB_EXACT)))") <命令>`
（**名单不在此复述枚名**：单一事实源 = `agentd/envscrub.py` 的 `ENV_SCRUB_EXACT`，取现值用上面这段。本节曾长期只列 10 枚、而事实源是 14 枚 ⇒ **复述即滞后，按旧清单跑会得出假失败**）
（子任务/服务进程内继承的调度身份变量会污染 guard 与登记方判据；`AGENTD_WRAP_*` 两枚 = 就绪门信号路径 补入——它们是 `pi-rpc-wrap.py` 显式传给本次会话的**身份/信号类** env，继承来的副本会让子端就绪门
拿到外层任务的标记（🔴1）；`AGENTD_ASK_*` 两枚 = 子端 ask 写侧收件面注入值 补入——
runner spawn 时注入的是**本任务所在生产树的结对路径**，继承来的副本会让测试里的 `writeAskMessage` 把合成信封
写进生产信箱（且既有用例假红并中止整套件🔴3）。名单单一事实源 = `agentd/envscrub.py` 的
`ENV_SCRUB_EXACT`（**本节只记各枚的来历与后果、不复述枚名**，枚名以上面那段命令取现值）；扩展单测另有一组同源断言钉住该清单已含这四枚（防再漏改）。

## 报表（report.py）

`agents/` 任务目录的 markdown 全链路报表：**只读扫描，不写任何任务
目录、不加锁**；缺省打印 stdout，`--out` 走临时文件+rename 原子写。状态判定与 `agentctl
status` 两层判定一致（排队/已放行待拉起、运行中（⚠️心跳停滞=lastAliveAt 超 5 分钟）、
成功/失败/取消/无报告）；其中 final 且 exitcode=127 且有非空 report.md 判为「成功（接管丢退出码）」——
孤儿接管退出码丢失的已知机制、任务实际成功，单独计列不进异常区（口径同心跳对账）；
终态无报告任务以中性标记「· 无报告」显示、不进异常区不计统计列（多为探针/预期无报告任务）；
**报告在场 = 存在 ∧ 去空白后非空**（判据单点 `scheduler.report_nonempty`，与调度器放行判据同源）：
零字节/纯空白 report.md 不算交付 → verdict 落「· 无报告」、不得呈现为「✅ 成功」；
**取消优先于无报告**：终态 ∧ 缺（有效）report.md ∧ 属调度员主动取消（判据见下「约定赋值与实现口径」）
→ verdict 判「🚫 取消」而非「· 无报告」（即使 exitcode=0：stop 请求与子进程自然退出的竞态），
与 core.ts 任务列表的 ` [已取消]` 标记、runner 终态通知不发 `warn=no_report` 三处同判据；
顶部「## 系统」小节（先看调度基础设施是否正常，再看任务）：
生成时间行之后、统计之前，展示① 各 host（nv1/nv2/dev/mac）runner 存活——读 `agents/run/agentd.<host>.lock`
的 updatedAt 新鲜度（复用 `scheduler.HOST_ALIVE_THRESHOLD` 60s 阈值与解析口径，零漂移），
≤60s ✅，否则 ⚠️ stale，锁缺失 ⚠️；② scheduler 存活——dev 本地 `pgrep -f 'agentd/scheduler.py'`，✅/🚨；
③ agents-sync 链路间接口径——远端锁由远端 runner 写、经同步链路到达 dev，内容新鲜即同时证明
「远端 runner 活 + 同步链路通」（✅ 双活）；dev 亦为节点、有本机链路，
表内注明。上述任一异常 → 异常区置顶追加「🚨 基础设施」条目。
活跃表排序：运行中（含心跳停滞）在前、已放行待拉起次之、排队中最后，组内按登记时间。
终态任务表缺省不渲染（报表保持简洁）：
`--finalized` 显示最近 20 条、`--all` 显示全部；统计头/异常区/活跃表/系统小节不受影响。
顶部异常区汇总失败（附 exitcode）、心跳停滞、
「应跑未跑」：未启动任务（无 enable.json ∧ 无 pid.json）
镜像 scheduler.tick 放行门禁计算「应跑」——needs 的 provider 均成功（口径同
scheduler.provider_success）∧ 所需资源与占位者（running/已放行未落地）持有资源无交集 ∧
占位数未饱和（缺省上限 4，同 DEFAULT_MAX_CONCURRENT）∧ 目标主机存活（复用
scheduler.host_runner_alive 读 agentd.<host>.lock 新鲜度）；四者皆满足却仍未放行才报，
提示调度器可能卡住。等待依赖/资源/槽位、主机不存活、缺 host 均属正常调度状态不报；
「needs 不可满足」：未启动任务
（无 enable.json ∧ 无 pid.json）直接复用 `scheduler.eval_needs`（import 复用，口径零漂移）判
三态——unsat（某能力无 provider 或 provider 全部终态非成功）→ 异常区 🚨 needs 不可满足（含缺失
cap 与原因：无 provider / provider 无成功者，逐个点名状态：空跑（exit0 无报告）、已取消、已失败；
调度器永不放行）；wait（仍有未终态 provider，优先于同能力旧 success）属正常等待不报，
但其中「同能力既有在途 provider 又有旧 success」的子形态单报 ⚠️「pending 压住旧 success」
（判据单点 `scheduler.caps_pending_over_success`，与 tick 升 WARNING 同源）：条目点名 cap、
在途 provider、被忽略的旧交付与恢复路径（等其落地终态或取消它，`@dispatch#review-chain`）——
该形态调度器判 wait 不放行，若报表只显示旧 success 会看着像调度器卡死（严重度低于
「needs 不可满足」，故 ⚠️ 不用 🚨）；活跃表「调度依赖」列同序呈现（展示序 = eval_needs
裁决序，pending 优先并附「忽略旧成功 task/<id>」）。
**同一判定的机器可读面 = `agentd/needscheck.py`**：`task_status` 工具（扩展
`assistant/.pi/extensions/agentd/core.ts`）经它取判定，在列表行尾标
`⛔needs不可满足: cap←provider（态）`、在详情单列一节 + 处置三选一（helper 不可用则降级为
不标记 + **输出头部**一行 ℹ️ 提示，工具不报错；提示不放末尾——列表全文已超 50 KB，放尾部会被
工具输出截断吃掉这一元信号）。为何需要这一面：调度员实际扫的是 `task_status` 列表，而它
本来不评估 `needs`（provider 非成功收口且无 report.md 时，下游永久留在 not-started 却看不出
「永不放行」）。TS 侧不重写 provider 状态分类与三态判定（否则与本节口径漂移）；同理不做 `needs`
逐项过滤（候选门只判「数组非空」，空串/纯空白 cap 是否成立归 helper 判），
排队面门 = `pid.json` **存在即出队**（含半截不可解析形态，与 `report.py` 的 has_pid/incomplete
同口径 ⇒ 两面不得分歧）。
**helper 用哪一档解释器 = env 钩子 `AGENTD_PYTHON`**（扩展侧消费，实现在 `core.ts:pythonBin()`；
解析序 `AGENTD_PYTHON` → `~/miniconda3/bin/python3`（工作区 Python 口径）→ PATH 上的 `python3`）：
排查「dead-ended 面为何降级（ℹ️ 提示带的原因）」时先核这一档——三档全不可用即降级。
本 CLI 自身不读该变量（它由调用方选定解释器后直接执行）。
跨机同步半截文件自动降级：JSON 解析失败只跳过该字段并标「⚠️读取不完整」，
绝不整体崩溃（为反复刷新生成物的观察者设计）。仅 python3 标准库（另复用同目录 proto/scheduler 谓词，二者均仅标准库）。

```bash
python3 agentd/report.py                      # 打印到 stdout（终态表缺省隐藏）
watch -n 5 python3 agentd/report.py           # 反复刷新，接近实时观察全链路状态
python3 agentd/report.py --finalized          # 显示终态表（最近 20 条）
python3 agentd/report.py --out /tmp/agents-report.md   # 原子写文件（--all 全量终态表）
python3 agentd/report.py --root ~/m --all     # --root 指定工作区根（扫描 <root>/agents/）
python3 agentd/needscheck.py --root ~/m       # 只读 JSON：dead-ended（needs 不可满足）任务清单
```

仪表板（`~/m/dash.itab` tasks tab）经 8080 实时 API 消费本报表：
`GET /agentd/agentd4web.py?refresh=15`（同目录 `agentd4web.py` type=script rpc
脚本，按需 subprocess 现算本脚本；refresh 由 itab 容器视图统一重载）。

## 运行参数（生产化）

### runner.py

| 参数 | 语义 |
|---|---|
| `--root` | agents/ 树根（布局 `<ROOT>/agents/<family>/<name>/`：族 ∈ {task, bot, topic}，路径式两段 id 直落；topic 族为协作容器、非进程型不入任务面）；spawn 前需 `enable.json` 放行（门禁内置无开关；调度方在独立服务 scheduler.py） |
| `--host` / `--aliases` | 本机身份与别名（§15.3），用于 `spec.host` 认领；生产部署 = 规范名（`env/host-id` 映射查表）+ hostname 别名（见下「路由字段」） |
| `--interval` | 主循环周期（秒），心跳每轮刷新 |
| `--log-file` / `--log-level` | 日志（标准库 logging；缺省 stderr / INFO；级别 DEBUG\|INFO\|WARNING\|ERROR） |
| `--replay-grace` | 终态通知重放护栏宽限窗（秒，缺省 300；0 = 启动前已终态者一律不发）：判据见下「约定赋值与实现口径」的终态通知幂等条 |

### scheduler.py（独立调度方）

| 参数 | 语义 |
|---|---|
| `--root` | agents/ 树根（与 runner 同一棵树） |
| `--host` / `--aliases` | 本机身份（非全局模式下过滤候补用；`--all-hosts` 时不参与过滤） |
| `--max-concurrent` | 全局占位上限（缺省 4） |
| `--all-hosts` | 全局视图：候补不按本机身份过滤，为所有机器放行（缺省关 = 只放本机任务） |
| `--once` | 只跑一轮即退出（调试/人工兜底）；不带即常驻循环（生产由监督脚本拉起） |
| `--interval` / `--log-file` / `--log-level` | 同 runner |

## 部署形态

调度半边与运行半边**彻底拆分**：

| 部署 | 形态 |
|---|---|
| dev | runner 服务（`loop.sh`，enable.json 门禁内置）+ 调度方服务（`scheduler-loop.sh` → `scheduler.py --all-hosts`，`make scheduler.start`）各自独立 |
| 其他机器 | 只有 runner 服务（门禁内置）；放行统一由 dev 调度方经同步树写 enable |

硬约束：**调度方全局任意时刻至多一个实例**（`enable.json` 单写者）。

拓扑规则：登记点/调度方/调度员会话所在机 = dev。调度方是**故意保留的单点**——
不做选主、分布式锁、RPC、消息队列，用「无状态重扫 + 文件幂等」对冲单点风险（`@agent-file-protocol#scheduler-recovery`）。
调度方故障语义：进程挂由监督脚本 2s 自愈重拉、漏放行自动补（重扫幂等）；dev 整机离线 =
全系统停止放行新任务，远端已运行任务不受影响（拉起/监督/判死全在各机本地），登记照常
（文件写入与调度解耦），恢复后重扫即追平；长期离线的人工兜底见下节 `--once`。
单写者纪律跨机不松动：每个文件仍只有一个写入方机器——`spec.json`/`prompt.md` 写于登记机、
`pid.json` 写于 `spec.host` 所指机、`enable.json` 写于调度方所在机、消息信封写于发送方；
双向同步只是「各方向搬运不同文件」，不存在同文件双向写，无 last-write-wins 冲突。

## 调度（独立调度方，scheduler.py）

调度方完全无状态（`@agent-file-protocol#scheduler-recovery` 重启重扫即恢复）；决策可观察：每次放行/
阻塞打日志，放行依据写入 `enable.json` 的 `note`。常驻由 `scheduler-loop.sh`
监督（崩溃 2s 自愈，形态同 loop.sh）；`--once` 供调试与人工兜底
（dev 长期离线时在任一带树镜像的机器上手工放行一批；前提 = 确认无其他调度实例在运行——
双调度器会双放行，靠部署约束排除而非算法防）。

### 调度参数（spec.json 可选字段，登记时写入）

| 字段 | 语义 |
|---|---|
| `resources` | 命名互斥锁数组；同一资源同时只允许一个任务占用。**缺省 = `["serial"]`**（无参数任务互相串行的手段）；显式 `[]` = 不占任何资源 |
| `provides` | 本任务成功后对外提供的能力名数组（缺省 `[]`） |
| `needs` | 依赖的能力名数组（缺省 `[]`）；每个能力需有成功的 provider 才放行。成功口径：生命周期终态 ∧ 任务目录存在**非空** report.md（子进程实际完成的自证；零字节/纯空白视同缺报告，判据单点 `scheduler.report_nonempty`）∧（exited ∧ exitcode 0 或 status=stale ∧ exitcode 127——接管孤儿死因不可得）。**exit 0 但无非空 report.md = 空跑**（idle 态，未提交验收报告，口径同 DISPATCH.md「完成判定」）→ 不算成功 provider；真实非 0 退出码、127 无报告、killed/137 判失败；取消（判据单点 `proto.EXITCODE_CANCELED`）归 canceled 态——不算失败也不算空跑，同样不满足依赖 |
| `--max-concurrent` | 全局占位上限（调度方参数，缺省 4），非 spec 字段 |

### 放行规则（每轮无状态重扫，允许越位）

候补 = 归本机（`--all-hosts` 全局视图时不按本机身份过滤，为所有机器放行）
∧ 有 `spec.json` ∧ 无 `enable.json` ∧ 无 `pid.json`
（排队中，§14.4），按 (spec mtime, 目录名) 升序 = 到达序；候补依序评估，
**被阻塞者不阻塞后面条件满足的候补**（越位）。放行四条件全部满足才写 `enable.json`
（`by=agentd-scheduler`）：

1. **依赖满足**（`eval_needs`，三态语义）：每个 `needs` 能力都有成功 provider
   （成功口径见上「调度参数」表，含 127+报告容忍，且 exit 0 必须有**非空** report.md）
   且无未终态 provider → ok；存在未终态 provider（含排队未落地）→ 等待，**优先于**
   同能力的旧 success（重发场景：新的真实施覆盖旧交付，防旧的假成功提前放行依赖者；
   该形态升 WARNING 并附 `ignoring older success provider <id>`，恢复路径 = 把 pending
   provider 取消或等其落地终态）；无 provider 或全无成功者（failed/canceled/idle 任意组合）→
   不可满足（不放行，后续出现成功 provider 每轮重评自动解锁）；
2. **资源可占**：`resources` 与任何占位者（`status==running` 或 已放行未落地）
   持有的资源无交集；
3. **占位数 < 上限**：`--max-concurrent`（缺省 4）；
4. **目标主机存活**：写 enable 前判 `spec.host` 对应探活锁 `agents/run/agentd.<host>.lock`
   的 `updatedAt` 新鲜度（阈值 60s = `HOST_ALIVE_THRESHOLD`，覆盖数心跳周期 + 一整同步周期
   + 跨机时钟偏差余量）。锁缺失/解析失败/缺字段 = 视为不存活（WARNING，保守方向）；
   不存活不放行、留候补排队（排队不是错误，`@agent-file-protocol#host-offline`），目标机恢复（锁重新保鲜）后
   下一轮自动放行。已放行不回撤（enable 单调，§14.2；门禁只作用于写 enable 之前）。
   **缺 `host` 的目录不进本门禁**（没有目标机可判）：任何机器不认领、调度方永不写 enable，
   周期性告警提示人工补 `spec.host` 或重新登记（见下「路由字段」）。

无参数任务 ⇔ `resources=["serial"]` ∧ `needs=[]`：彼此严格 FIFO 串行（同一时刻至多
一个）；与显式带参任务之间按资源互斥判定（可并发）。
要拦下已放行任务走 `control/` stop（§14.2 单调）。不做能力环检测：成环任务排队等待，
靠日志排查。

## 路由字段

`spec.host` 缺省 = 登记机，**登记侧物化**：「缺省 = 登记机」在登记时刻落盘，不留给认领时解释。

- **规范名映射文件**：`env/host-id`，入库一份全机器共用，
  每行 `<hostname> <空白> <规范名>`，# 注释。登记侧（dispatch 工具 `core.ts` 与 `agentctl
  create`）按本机 `$(hostname)` 查表得登记机规范名；未命中/文件缺失回退本机 hostname 并提示
  （登记不失败）。`spec.host` 一律用规范名，不用裸 hostname。
- **机器名清单**（`host` 合法性校验的唯一来源，`core.ts::knownHostNames`）= `env/ssh-hosts` 的
  `Host <别名>` ∪ `env/host-id` 的规范名列；**两个来源都不在场**才跳过校验（不误杀登记）。
  并集的理由：mac = 本机笔记本、无 ssh 别名（用户拍板的既定例外，经 rsh 反向链路可达）——
  只读 ssh-hosts 会让 `host=mac` 每次登记都带一条恒假告警。
- **登记行为**：两通道均恒写 `spec.host`（缺省 = 登记机规范名；dispatch 可显式传 `host`
  参数指定目标机）与 `spec.createdByHost`（= 登记机）。非法机器名（不在上面那份机器名清单）：
  dispatch 回执告警但放行登记（与 `@agent-file-protocol#host-offline` 排队语义一致）。
- **runner 启动参数**：`--host <规范名> --aliases $(hostname)`（`loop.sh` 启动时
  按 `env/host-id` 映射查表取规范名），使 spec.host 指向本机的任务被本机认领。
- **认领侧语义**（与登记侧物化配套）：`spec.host` 匹配本机身份（规范名或别名）才认领；
  **缺失/空 = 无机器认领**——「缺省→本机认领」式兜底在多机下是双重拉起的危险源，不存在：
  runner 记 WARNING（含目录名与处置提示，内存去重防刷屏），调度方（含 `--all-hosts` 全局视图）
  对缺 host 候补永不写 enable（视为永久排队）并周期性告警（首次 + 每 30 分钟）；
  处置 = 人工补 `spec.host` 或重新登记。登记侧绝不漏写：host 解析链
  （`env/host-id` 映射 → hostname）任何一环产出空值时，最终兜底写 `$(hostname)` 原文。
- **控制与消息跨机自动获得**：`control/`（stop/cancel 等）与 `inbox/`（inform/ask/reply）都写在
  `agents/` 树内，随同步通道到达目标机，runner 按既有规则消费；终态通知回流登记机同理——
  控制面与消息面无新机制，跨机同步通道一并承载。
- **`--root` 前置校验**（`agentctl.py::require_workspace_root`）：`--root` 是**工作区根**（如 `~/m`）、
  不是 `~/m/agents`。硬前置 = `<root>/agents` 不在场即 die（rc=2）且**不静默建目录**：错 root 会让写侧
  `makedirs` 建出一棵无人消费的嵌套树（control 请求写进去、重启不发生、残骸要两条 gc 台账条目才收得掉）。
  软前置 = `<root>/env/host-id` 不在场只出一行指向 root 语义的 WARN、**不改 rc**（映射缺失回退 hostname、
  不阻塞登记是既有裁定，e2e S22④/S27③ 钉住）。
- **e2e**：S22（agentctl 通道登记写 host/显式他机不认领/映射未命中与缺失回退）；dispatch
  通道与告警语义（含机器名清单并集、mac 零告警）在
  `assistant/.pi/extensions/agentd/tests/agentd-ext.test.mjs` 路由字段组。

## 跨机同步通道

终态拓扑（星型，hub = 中立目录
`dev:/data/shared/agents`，零原件：一切落盘打 replica 组）：
watch 进程跑在**四机全部**（dev/nv1/nv2/mac，含 dev）上指向中立目录（`ssh-sync.py watch ~/m/agents
dev:/data/shared/agents`，dev 上 ssh 自连；无 `--delete`），四机对等。
**同步面排除当前为空**（receiver 的在飞态已移进进程内存 ⇒ 盘上不再有「只属写者本机」的短命账本；
排除管道保留，重加一条 = 改 `agents-sync/ssh-sync.py::WATCH_EXCLUDES` 一个常量）**+ gc delete-list 路径拒收**；
除此之外属组闸门后**整棵树都在同步面内**
（无目录级排除）：`session/` 会话记忆体跨机同步（单写者免疫冲突），探活锁在树内双向天然正确。
且 `watch` 子命令拒绝 `--delete` 参数（永不删除是传输契约硬约束）。
单实例锁 = 每机探活锁文件 `agents/run/agentd.<host>.lock`：
在树内、就是要被同步的探活文件（别机靠 `updatedAt` 新鲜度看各机 agentd 存活），不加入 exclude。
ad-hoc 一次性同步仍可用 `ssh-sync.py push/pull/both <显式目录>`。

**传输契约 = 属组闸门**：文件系统 `replica` 组标记「同步落盘的副本」，未标记 = 本机原件。
核心不变式 = **每个文件只有一个可能来源**（单写者纪律的传输层表述）。两条管道**每轮都协商整棵树**
（push 走白名单 = 本地未标记文件悉数上推、pull 走黑名单 = 本地未标记文件受保护其余悉数接收），
跳过判据 = rsync 自己的 `-c` 内容校验和；**检测器只回答「有没有变更」**，不记路径、不做快照、不参与
覆盖裁决 ⇒ 一轮的成本与变更量无关，轮次节奏由速率下限（`--min-cycle`）与强制周期（`--interval`）界定，
**任何变更的重新锚定上界 = `--interval`**（deadline 本身即触发，树完全静默也照跑；与检测器是否失效无关）。
排除面无运行时开关。机制/成本/实测的权威 = `@agents-sync#watch-cost`（本节不复述）。
落盘一律 `--chown=:replica` 打标，组名在接收端本地解析——各机 gid 无需一致，跨机共识只有
`replica` 这一个字符串。rsync 标志 `-rlptDvc`：属组只由 `--chown` 一处决定；`-c`（--checksum）
判跳过——跳过判据 = 内容校验和，**时间戳不参与任何裁决**，字节一致零重写。
**失效不对称**：任何未标记文件绝不被覆盖（最坏 = 陈旧副本滞留，绝不丢数据）；
被跳过/保护的文件记日志（「为什么没同步 → 没标记」可诊断）。
启动闸门三道（任一失败拒绝启动）：本机 `replica` 组可解析、本机 chown 功能自检（用户须为组成员——
rsync 对 --chown 失败静默吞掉）、**端到端 `--chown` 管道探针**（真传一个探针文件到远端、核落地属组）；
「远端组存在 / 远端 chgrp 能力 / 两端 rsync 版本」被第三道语义覆盖，只在它失败后作为诊断跑。
存量树首跑前经 `agents-sync/replica-tag.py` 一次性打标（幂等、干跑留痕、未决保守保持未标记）。
检测层（本机 inotify∨watchdog、远端 ssh 事件流）只是**触发器**：只决定「要不要跑一轮」，不参与覆盖裁决。
防乒乓由闸门本身保证：我 push 上去的副本在对端带标记，回到我这轮 pull 时我本地那份是未标记原件 ⇒ 受保护、
不被覆盖，代价是一轮空转（空转不打日志）。
**无尺寸稳定守卫**：半截文件风险由双层防御覆盖——写侧多为 tmp+rename 原子写，
读侧解析失败下轮重试（`@agent-file-protocol#partial-file-sync`）；残留非原子写的半截副本属应用层可容忍（终态判定不依赖传输中的产物文件）。
文件名**禁用 `*?[]` 字符**：pull 保护清单是模式语义，含通配符的文件名会被拒收+告警并失去保护，
故树内新建文件名一律禁用（登记侧 `agentctl create --name` 与 dispatch 任务名强制校验）。
**树内清理 = 全节点同时处理**：无 `--delete` 契约 = 删除不跨机传播——树内删文件必须全部节点
同时处理（各机本机删 + hub 删，或经 `agents-sync/gc.py` delete-list 由 pull 侧统一执行），
只删单机会被其余节点下一轮同步复活。
探活锁语义：各机 runner 主循环每 5s 重写本机 `agents/run/agentd.<host>.lock` 刷新 `updatedAt`；
别机判存活**基于 `updatedAt` 新鲜度而非存在性**（同步契约不传播删除、崩溃留死锁，优雅退出也
不删文件只停更——停更即自然判死）；本机互斥（防手工双开）用 `(pid, procStart)` 身份校验。
spec 路径可移植：`spec.workdir` 位于 `$HOME` 内者登记侧归一化为 `~/...` 落盘，
`spec.command` 内 `$AGENT_HOME`/`$AGENT_ROOT` 由执行机运行时展开（`@agent-file-protocol#spec-json`）。
延迟量级：正常 1–2s（事件驱动 + debounce + 一轮整树协商 ≈1s），最坏 = `--interval` + 一轮耗时；
调度方无状态重扫天然吸收同步延迟——「已放行未落地」（有 enable 无 pid.json）也计为占位者，
不会双放行；远端终态回流延迟只造成保守占位（延迟放行，不会错误放行），下轮自愈。
机器离线/回归：目标机离线的候补排队（不是错误，§11.9）；其运行中任务保守占位不放
（观察 `lastAliveAt` 停更），机器回归后链路自愈、判死纪律处置存量档案，rsync 幂等追平，
全程无需人工干预；永久退役一台机器 → 人工处置（目标为它的排队任务重新登记到别机 +
对旧目录 `control/ stop`）。

## 已知风险（多机）

- **离线机保守占位**：机器整机挂后其运行中任务保守占位不放（`lastAliveAt` 停更可观察），
  长期占用槽位/资源直到机器恢复或人工对其任务 `control/ stop`——方向保守（不放行不会错放行）。
- **永久退役一台机器**：目标为它的排队任务会永久等待 → 人工处置（重新登记到别机 +
  对旧目录 `control/ stop`）。
- **手工目录漏 `spec.host`**：手工绕过登记工具写的目录漏掉 host = 无人认领、永久排队，
  靠 runner/调度器周期性 WARNING 提示人工补 `spec.host` 或重新登记（登记工具链已加固为绝不留空）。

## 判死纪律与杀纪律（协议 §5.5）

**判死路径只允许两种，禁裸 waitpid**：
1. 本进程 spawn 的句柄 `poll()` 收尾（防线 1，拿得到真死因）；
2. 无句柄（如重启接手）→ `(pid, procStart)` 二元组探测（防线 2）。

**一切基于 pid 的 kill 执行前必校验身份**（`kill_proc` 唯一入口先过 `probe_alive`）：
不匹配 = 目标已消失（pid 被复用）→ 拒绝盲杀，按消失写终态（`stale`/127，
restart 直接换代）并在回执 `detail` 如实说明。校验→kill 的微秒级残余窗口不可完全消除：
触发需原进程恰在窗口内死亡且内核恰好立即复用同一 pid，概率极小、后果有界（杀纪律本身
就是为把爆炸半径压到这个窗口而设）。

**接手路径**：无句柄的 running agent，`(pid, procStart)` 校验通过 → 刷心跳续监督；
校验不通过判 `stale`，绝不触碰。接手场景进程真死时真死因不可得
（非父进程），按 §4.2 约定记 `stale`/exitcode 127。**接管孤儿退出归属**：exitcode 记账如实保持
127（死因不可得是事实），但终态通知分类看
report.md：在场 → 按 `@agent-file-protocol#operations` 完成判定（final + report.md）归为 `task_done`（载荷附 `note` 注明依据），
缺席 → `task_failed`。通知**是否发出**另受重放护栏约束（接管场景属本次运行判定的终态 → 照发），
见下「约定赋值与实现口径」。

## 约定赋值与实现口径

- **终态通知收件面 = `spec.reaper` 单收件方（解析单点 = `Runner.resolve_reaper`，登记侧对偶 =
  `core.resolveReaper`）**：给 reaper 写一份 inform 信封（载荷不带 role/reaperPid——旁观者面
  已裁，收件方恒为唯一收尾方）。对 task 与 bot 两族参与方同规则（bot 自身终态——散会 stop 等
  ——同样回流其 reaper）；建目录只建 `inbox/`（克制口径同 `ensure_dispatcher_inbox`）。
  **reaper 解析两档**：① `spec.reaper` 在场且过文法白名单 → 它（文法非法视作缺字段 + 一行
  WARNING）；② 缺字段/文法非法 → 回落职位信箱 + `note` 点名成因（登记侧恒写 reaper，缺失属
  存量/人工档案形态）。① 的目标过**活性代理**，不活 → 回落职位信箱 + `note`。
  **活性代理**（`Runner._pid_active`）：目录在场 ∧（`pid.json` 可读且 `final != true` ∨ `watcher/`
  有非隐藏条目 ∨ 被任一参与方 `spec.subscribes` 声明）——只判目录存在性会把通知投进无人读的信箱
  （已下线 bot 的目录在 gc 前仍在场、pid.json 已 final）= 静默丢失；第三条判据兜住「主题主持人靠
  登记期订阅声明、其 topic 目录无 `watcher/`」的形态，且只在前两条不成立时才跑一次参与方扫描。
  回落面（职位信箱）自身**不做活性判定**——回落必须无条件可写。
  登记侧：`creator` = **真实登记方**（审计用，投递不由它推导），
  `reaper` 恒写（缺省推导 = creator）；显式值非法（裸名/三段/穿越/
  未知族/非字符串）→ **拒绝登记、零落盘**。缺省推导三档（调度员会话 / topic 主持人 / 其它）结论
  相同 = reaper 自身，分类只用于日志与回执（`registrantRole`），**无任何机器特判**。
  **子端 ask 写侧收件面同源复用 `resolve_reaper`**（项 1）：runner spawn 经
  `_ask_inbox_env` 把解析结果注入 `AGENTD_ASK_INBOX`/`AGENTD_ASK_NOTE` env，子端 `core.writeAskMessage`
  取值（.ts 侧不另立第二套解析；reaper 不可达 → 回落职位信箱带 note，同款语义；**同树守卫**：注入值只在与本次 `root` 同树（`<root>/agents/` 下）时采纳，跨树 = 视作无注入 → 回落职位信箱，防继承来的生产绝对路径把合成信封写进生产信箱；.ts 侧自行回落且无注入 note 时自造一句成因）。
  **读侧扫描面 = `core.askScanInboxes`（唯一定义）**：职位信箱 ∪ `spec.reaper`（= 写侧全部候选落点；Python 侧镜像随会话活性监督面退役）——写侧落点由 spawn 时的活性解析决定、读侧不可知，故读判据扫并集（喂 findPendingAsk / findAskById / findAnsweredTwinAsk / findReusableAsk）。漏扫一档的后果是实证的：只扫职位信箱时，落 reaper 信箱的 ask 对读侧不可见 → `task_status` 不显示 waiting-answer、`task_answer` 找不到可答的 ask。两枚 env 已收进 `envscrub.ENV_SCRUB_EXACT`（身份/信号类，
  不得继承进孙进程：继承会把它带进被启动的服务、并让嵌套 receiver 误用外层任务的 ask 路由）。应用层口径 = `assistant/DISPATCH.md`
  §4 反问协议 / §8；字段登记 = `@agent-file-protocol#spec-json`；细节 = `assistant/docs/ask-protocol.md`。e2e S45/S54/S55/S56 +
  `test_runner_notify.py` H13/H14 + 扩展侧单测 reaper 组与项1组。
- **终态通知幂等 = 自家目录 `notified.json` 标记判重（粒度 = (taskId, 收件方)）∩ 重放护栏**（通知面只报「本次守护运行期间到达终态」的参与方）：
  标记（runner 发完信封后写：`to` = **已投收件方集合**、`ts`/`event`/`complete`；部署层记账，同
  `pid.log` 地位、不入 `@agent-file-protocol#single-writer` 单写者矩阵）是**判重唯一事实源**；
  `complete: true` 让后续轮次走**热路径短路**（零收件方解析、零活性探测）。写序 = 先信封后标记
  （反序会在崩溃窗口丢通知）；崩溃恰在两者之间 → 下一轮**重发一条重复 inform**——接受的取舍：
  重复的代价（收件方多读一条）远小于常驻一套「信箱回落扫描 + 补写标记」机制（历史上该回落面
  还引出过全量扫信箱的常驻热路径成本，实测 868 封 ≈ 51ms/轮 ≈ 10% CPU）。
  **重放护栏**保证冷启动不把历史档案当新事件——标记只覆盖「本部署自己发过的通知」，对标记缺失
  的历史档案（冷启动、新机入网、信箱路径迁移撞上节点离线、agentd 启动早于 agents-sync 首轮收敛）
  判重集为空，单靠标记会把本机认领的全部历史终态任务重放一遍（纯噪音，且淹没同时刻的真实通知）。
  护栏两条件**同时成立**才抑制（保守方向 = 尽量少抑制）：① 本次进程从未见过该参与方处于非终态
  （= 终态判定发生在上一次运行，本次无新事实可报；同时覆盖「启动后同步才把旧终态档案 pull 落地」的
  迟到面）；② `pid.json.endedAt` 早于**本次进程启动纪元**超过 `--replay-grace`（锚在启动纪元而非 now：
  真实事件不会因为通知自身延迟而被自己的阈值吞掉；宽限窗兜住「上一实例已写 final、还没写信封就被杀」）。
  ② 的 `endedAt` **缺失或不可解析**（人工补录/异构写入/被裁剪的同步副本）同样按「远古」降级 → 抑制：
  畸形档案只吞它自己，绝不让异常逃出护栏——`notify_tick` 每轮整段中断会饿死同轮排在其后**所有**参与方的
  真通知（比重放噪音严重得多），故读档面对字段缺失一律降级、不抛（`ts_epoch` 契约：解析失败返回 None）。
  **两类必须通知的场景靠条件①兜住**：孤儿接管（启动时 `status=running` 未 final → 接管后亲手判
  stale/127 + final，`note` 照附，见上「接手路径」）；以及「进程已代终态但生命周期未收口」的旧档案
  在本次运行内被 stop 收口——`do_stop` 的「只置 final」分支**不刷新 `endedAt`**，
  只看时刻门槛会吞掉这条真取消通知。被抑制者逐条留 INFO（日志锚点串 `重放护栏`）；判据**不落本机
  状态文件**（`endedAt` 本就落盘且跨机同步、观测集每进程自持，任意时刻可重算）。e2e S44。
- **收件送达确认 = 在飞态（进程内存）+ 落盘确认（口径权威 = 协议 §4.6；实现单点 = `core.createDeliveryTracker`，
  两端共用 = 主端 `index.ts` / 子端 `receiver-child.ts`，消费点 = `core.drainInboxDir`）**：ack 语义 = 注入文本
  **已逐字落收件会话 jsonl**（更早的两个口径都不算送达：「已交给 pi 内存队列」——steer/followUp 队列纯内存、
  实测悬空 1 min~2.5 h；「已进内存会话树」——pi `_persist` 在无 assistant 消息时不写盘）。认领只记进程内的
  在飞表（键 = 终态 ack 路径，值含注入文本原文与认领时刻；挂 `globalThis` ⇒ 跨 `/reload` 存活、跨进程重启归零，
  与 pi 队列的生命期对齐），**盘上没有中间形态**（无 `.pending` 记账件、无写者纪元、无孤儿接管、无认亏态）。
  确认触发两源 = `message_end(role=user)` 事件（文本逐字匹配；pi 先 await 扩展 handler 后 `appendMessage` ⇒
  确认推到下一 tick）+ 每轮 drain 轮首兜底；确认即写终态 ack（`{id, ts, claimedTs}`，`wx`）并出表。
  **在飞期间不重复注入**；**不判丢**（用户拍板：先观测一段时间再决定是否加判丢）——注入被 pi 静默丢弃
  （activeRun 守卫 throw 被 runtime 吞掉、pi 丢轮）时不检测、不重投，故无重投上界与认亏态；预防面即唯一防线
  = 注入闸门 + 在飞表认领。**跨代重投窗口** = 「已落盘 → 确认写 ack」之间（≤ 一轮扫描）：此窗内进程更替 ⇒
  新代表里没有它、盘上也还没 ack ⇒ 重投一次（§6.4 at-least-once；历史频次上界 = 存量 ack 里带重投记录的
  比例 ≈0.6%）；高频的非重启丢失路径（`/clear`、fork）由 `session_start(reason=new|fork)` 清在飞表 + 下轮
  重投自愈（**`reload` 不清**：队列存活，清了会造双份）。降级两个方向都**不写 ack**（宁留在飞不误报送达）：
  无会话文件（`persist=false`/测试桩）→ 判据退回「已进内存树」+ 一次性 WARN；ctx 失效 → 本轮不确认 +
  一次性 WARN。**并发语义变化（明示）**：认领不再走盘上 `O_EXCL` ⇒ 跨进程互斥消失（两个进程同扫一个信箱会
  各注入一次；终态 ack 仍 `wx` 唯一），正常态每信箱只有一个归属会话（收件面按会话名解析）⇒ 属配置错误面。
  **观测面（决定日后是否加判丢的数据源，只留痕不处置、零新旋钮）**：注入/确认各一行日志（确认行带时延）、
  在飞龄超 `INFLIGHT_STALE_WARN_MS`（10 min）一行 WARN（每件一次，文案点名「可能已被 pi 丢弃」与处置路径）、
  会话卸载时列出仍在飞的件（= 随进程消失的候选丢失清单）。**ack 停在弱档 ≠ 没送达**：送达真值 = receiver 日志的
  「确认落盘 → 写终态 ack」行 ∨ 终态 ack 带 `ts`/`claimedTs`，**二者取或**（日志行不会被拉取侧手写件污染；手写
  ack 与推送确认竞争同一文件名，`wx` 先到先得且先到者永久胜出 ⇒ 手写是**不可逆降档**）。事后可复算指标（双源口径）=
  **分子** = 终态 ack **既无 `ts` 也无 `claimedTs`**（0 字节 ∨ `{}` ∨ 自定义键的手写形态）**且** receiver 日志无对应
  「确认落盘」行的件；**分母** = 全部终态 ack；统计须记时刻与主机（同步面汇各机 ack ⇒ 跨机重复计数让分子虚高）。
  **字段判据必须写成「两字段皆无」**：存量里还有大量只有 `ts` 无 `claimedTs` 的历史形态（`seededFrom` 迁移件、旧认领
  路径直写件），按「任一字段缺」统计会把它们全算进分子 ⇒ 同一份存量两种读法得 8.8%（203/2320）vs 64.4%（1495/2320），
  差 7 倍（口径与实测 = 协议 §4.6）。历史基线 164/2306 的口径 = 0 字节手写件 / 全部终态 ack（单源），双源下会偏低。拉取侧对**同进程**在飞件算已读（`core.inflightHas`，
  避免推送在飞期间自扫重复消费），跨进程观察者恒见未读（= 丢失件的兜底可见性）。旋钮面只剩两个有界读上界
  （`AGENTD_READBACK_FILE_BYTES` / `AGENTD_READBACK_SCAN`）；已退役的四个 = `AGENTD_PENDING_RETRY_MS`、
  `AGENTD_PENDING_MAX_ATTEMPTS`、`AGENTD_EMPTY_QUEUE_GRACE_MS` 与队列判据接线。单测 = 扩展单测「在飞态」
  两组（core 级 14 例 + 工厂级 5 例，含真 jsonl 与事件时序）。
- **收件注入闸门（三态分流；判定与常量单点 = `core.createInjectGate`，消费点 = `core.drainInboxDir` 的注入
  预算）**：注入器 `pi.sendUserMessage(text,{deliverAs})` 在扩展面返回 `void`、异步失败被 pi runtime 收走
  （`emitError` 只通知 mode 侧的 errorListeners ⇒ **扩展侧对注入失败零可见性**），而 pi 用一个可被污染的
  忙碌标志分流「起新轮 vs 入队」⇒ 投递前必须自己判忙闲，三态判据全部取自 `ExtensionContext`：
  `isIdle()===false` = 标志一致忙碌 → 批量入队（轮末 flush）；`isIdle()===true ∧ signal===undefined`
  = 真空闲 → 本轮只投**一个注入单元**（优先级 = steer → 终态 digest → 文件名序），其余**零认领**延后
  （不记在飞表、不注入；拉模式呈现面照常算未读）；
  `isIdle()===true ∧ signal!==undefined` = **假空闲**（`signal` = `agent.activeRun` 的 abort signal：轮还在飞
  而 pi 的忙碌标志已被同轮某次失败注入的 `finally`（无条件 `_emitAgentSettled()`）清空）→ 本轮零注入
  零认领、全部延后，进出各留痕一次（receiver 日志文案「会话处于假空闲态…」/「假空闲态结束…」）——
  该态下任何注入（steer 与 followUp 都一样）都会被 pi 的 activeRun 守卫丢弃且从不入队，故延后是唯一不丢的
  处置；探针不可用（ctx 失效/无 `isIdle`/抛错）→ 批量（降级方向 = 不误延后）。真空闲注入成功后起静默窗
  `INJECT_IDLE_LATCH_MS_DEFAULT`（1500ms，env `AGENTD_INJECT_IDLE_LATCH_MS` 只能向下调）：挡「注入已发出
  而 pi 尚未置起忙碌标志」这段 await 窗里被 inotify 触发的第二次 drain（同 tick 并发注入必造输家）；取值
  < poll 间隔 2s ⇒ 不拖慢 poll 节奏。配套快通道 = 主端/子端各挂一个 `agent_start` handler 触发一次 drain
  （该事件时 pi 已置忙碌标志 ⇒ 真空闲那一轮起后，积压毫秒级批量入队，否则要等 ≤2s 的下一轮 poll）。
  与在飞态/落盘确认**正交**：预算 0 的轮次仍跑轮首确认（`tracker.confirm()`）⇒ 延后不卡确认面。
  **不判丢口径下本闸门是丢弃面的唯一预防手段**（注入被 pi 的 activeRun 守卫丢弃时扩展侧零可见性，而实现
  不重投）⇒ 假空闲态零注入与真空闲态一轮一个单元两条纪律承重等级上升，不得随简化削掉。
  假 ctx 单测 = 扩展单测的注入闸门 G 组（进缺省回归面）。
  **真 pi 端到端装置** = `assistant/.pi/extensions/agentd/tests/inject-gate-live.mjs`（**opt-in**：需
  `AGENTD_LIVE_PROBE=1`，缺省直接跳过并打印跳过原因、不进缺省回归面——它依赖本机 pi 二进制且每场景
  ~10–20s 墙钟；沙箱 = 临时 `AGENTD_ROOT`/cwd/`--session-dir` + 本地假模型 provider + `-ne` 不发现生产
  扩展，零网络、零生产信箱、跑完自清理、有界等待）：
  `AGENTD_LIVE_PROBE=1 node assistant/.pi/extensions/agentd/tests/inject-gate-live.mjs`
  （三场景 = 空闲 burst / 假空闲态下的后续注入 / 标志一致忙碌的 followUp+steer 零回归；`--scenario <名>`
  单跑、`--keep` 保留沙箱、`--ext-dir <另一份扩展目录>` 跑对照面——修前对照的取法写在该文件头注）。
  **实测环境戳**：dev、pi **0.84.1**（`pi --version`）、node v22.23.2；**pi 升级后需重跑**——本装置钉的是
  pi `AgentSession.prompt()`/`_runAgentPrompt()`/`_emitAgentSettled()` 与 `agent.prompt()` 的 activeRun 守卫
  的行为面，升级可能改变分流时序（若 pi 侧修掉了根因，本装置应仍全绿，而修前对照不再复现红）。
  重跑时顺带核 `agent_start`/`agent_end` 是否仍恒配对（`pi-wrap/pi-rpc-wrap.py` 的 `inflight`
  免疫判据以此为前提；配对语义变了 ⇒ 收敛判据要重估）。
- **调度员主动取消判据（单点 = `proto.EXITCODE_CANCELED` 注释）**：`control/` 存在 `action=stop`
  请求 ∨ exitcode=125（旧系统 CANCELED 约定值，现网 stop 杀记 137）。取消不是失败，缺 report.md
  是取消的预期结果而非空跑，三处呈现共用该判据：runner 终态通知**不发** `warn=no_report`
  （其余缺报告情形照发；`stopReason` 仍带）、report.py verdict 判「取消」、core.ts 任务列表标
  ` [已取消]`（详情/通知文案不再引导「排查未写验收报告」）。e2e S42 + 扩展侧单测 task_status 组；
  **已知的口径差（125-legacy，刻意不改 登记）**：`exitcode=125` 且无 stop 请求时，
  `runner.terminal_event` 仍归类 `task_failed`（runner 内 `canceled()` 与 `terminal_event` 的既有口径差），
  而**渲染面**（core.ts `terminalCanceled` 单点：单条渲染抬头与批量 digest 事件名共用）把它归「已取消」/
  `task_canceled`——即同一任务的通知抬头（已取消）与载荷 `event` 字段（task_failed）可能不一致。
  不改 `terminal_event` 的理由：它是放行/报表/依赖判定三面共用的分类语义（改它 = 另案面、需独立评审），
  而渲染面只影响人看到的抬头；扩展侧单测已钉住该档的渲染行为（exitcode=125 无 stopReason → 抬头「已取消」）；
- `killed` exitcode：被 runner 杀记 137；被信号杀记 `128+signum`；
- `stale` exitcode：127（对照 DISPATCH 口径）；接管孤儿（无句柄，死因不可得）亦记 127，
  终态通知分类另看 report.md（在场→`task_done`+note，缺席→`task_failed`，见上「接手加固」）；
  调度器 needs 成功判定同口径容忍：127 ∧ 非空 report.md 在场 计为成功（终态通知分类仍按文件
  在场判，不查内容：通知是信号面、不是放行判据）；
- 登记侧名字收紧（写入口 `agentctl.py check_name_segment` = create-bot / bot register /
  topic init / `--watcher`；core.ts `dispatch` 的 `profile` 名同款）：拒 `.` 开头与名内连续点
  `..`——前者被 agents/ 扫描面当隐藏条目跳过（profile 名同款被 wrap 单值解析拒 → 登记成功
  但运行时告警降级），后者使 `fsSafeId` 转写后的 ack 命名空间段含 `..`（core.isSafeAckNs
  判不安全）→ topic 绑定面整体不启用。**寻址侧文法不收紧**（`proto.is_valid_name_segment` /
  `core.PARTICIPANT_NAME_RE` 仍 §2.1 原样）：存量病态名仍可被寻址/投递/清理；
- 时间戳格式：`YYYY-MM-DD-HH-MM-SS.mmm`；
- inject 投递（实现定义，§5.3）= 换代前向 agent inbox 写一条 `inform` 预置消息；
- 逐代详史：`pid.log` 追加（部署层诊断，§11.7，不参与判定）。


