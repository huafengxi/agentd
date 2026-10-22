---
name: agentd
生产对象客户端入口为 `object_runner.py`，消费调用方固定 serviced 的声明与状态接口；Task/Service 合同见 serviced 仓 RUND.md。运行源码和机器配置由调用方提供，历史实验适配器不属于当前生产执行入口。
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

本仓独立发布文件协议与调度政策；执行由调用方固定的 S 文件接口承接，RPC 封装通过显式 PI_HOST_DIR 使用通用 P。离线零安装入口见 [portable/README.md](portable/README.md)，源码恢复与 provenance 见 [recovery/README.md](recovery/README.md)。

Python 标准库实现。协议字段、寻址、单写者、控制与终态谓词的权威 = [agent-file-protocol.md](agent-file-protocol.md)；本文承载实现入口与运行约定。按名引用 `@名#锚点` 在调用方工作区解析，独立阅读可按对应节名查找。

## 收录判据

本模块只住协议级机制：文件布局与寻址、信封与 ack、终态谓词、调度、执行声明适配及 CLI。任务政策正文、人格资产、启动命令与部署值由调用方注入或现场发现；改部署或政策措辞不得产生本模块 diff。

`agentctl create --prompt-file` 只逐字搬入调用方已渲染的 UTF-8 正文，原子写 prompt 后再写 spec，不渲染模板。`spec.command` 由调用方提供；机器身份从调用方根下 `env/host-id` 查表。

pi extension 只承载事件钩、会话消息注入与会话树读写；纯文件读写与状态查询放 CLI，使门禁对会话、脚本、人一致生效。调用方的政策与人格装配不进入本模块。

任务登记的发布点为 `registration.register_task`，CLI `create` 与 `register-json` 共用该实现。`register-json` 从 stdin 接收 `{name, spec, prompt}`，stdout 返回 `{taskId, agentDir}`；调用方负责渲染正文，协议校验与 prompt 先于 spec 的发布顺序由登记层执行。已存在的任务拒绝覆盖。

共享生命周期与交付判据归 `task_state.py`。`scheduling.py` 提供只读任务快照、调度视图与结构化门禁结果；scheduler、report 与 needscheck 使用同一接口，放行记录只由 scheduler 写入。快照是单轮观察结果，不提供跨文件事务保证。

Web 报表适配器通过宿主提供的 `__file__` 定位相邻 CLI，并使用宿主 Python 解释器；`tasks.md` 的 widget 命令从工作区根执行 `agentd/report.py`。

`task_labels.py` 承载 provider 状态文案；报表与 JSON 检查工具按结构化依赖结果渲染，不从日志文本还原决策。

## 正式执行入口

`python -B rund_runner.py --config /absolute/agent.json [--runtime-source /absolute/pinned-S]` 只连接本机文件 Owner，不持有负载 Popen/wait/signal。配置 apiVersion 为 rund-agent/v1，声明 workspace、host、runtimeRoot、protocolRoot、runtimeSource（可由显式参数提供）、runtimeRevision、runtimeManifest、sources、python、home、path；可选 aliases/env/inheritEnv/interval/legacyCutover。文件客户端来自固定 S 源码的 `runtime/file_owner/protocol.py`，不装载执行 Owner。源码由固定 Git 声明准备；新登记命令用 `$AGENTD_CODE` 定位本次执行的 A。既有登记的根路径仅在编译时映射至同一固定源码。

普通任务和常驻/auto 的分类复用原协议政策；登记、scheduler、resources/needs、报告、ask、通知、控制回执和历史仍由 A 处理。动态发现仅认本 host。S 是唯一执行与停止者。旧 runner 的历史进程须先按实际身份关闭整个组，再提供显式 legacyCutover 证据；保留原观测与归档，A 代次单调映射到 S 的新代次，不伪造 S wait。

保留集成测试使用调用方指定的 A_RUND_RUNTIME_SOURCE、A_RUND_RUNTIME_REVISION 和全新 A_RUND_TEST_OUTPUT。测试运行真实独立 A/S 进程与固定本地 Git 假负载，不调用模型或真实 agents 树。

## 文件

| 入口 | 用途 |
|---|---|
| `agent-file-protocol.md` | 协议契约 |
| `registration.py` / `task_state.py` | 任务登记发布 / 共享生命周期与交付判据 |
| `scheduling.py` / `task_labels.py` | 只读任务快照与门禁结果 / 状态文案 |
| `proto.py` | 原子文件、寻址、信封、ask 与进程身份公共原语 |
| `agentctl.py` | 登记、消息、答复、控制、调度字段更新；参数取 `--help` |
| `rund_runner.py` / `runner.py` | 每机 S 文件客户端、协议代次/控制/历史/终态与通知；runner 的 CLI 转入正式文件入口 |
| `scheduler.py` | 单实例无状态 DAG 放行，作为普通前台 Service 运行 |
| `report.py` / `needscheck.py` | Markdown 报表 / JSON 不可满足依赖清单，均只读 |
| `agentd4web.py` / `tasks.md` | 报表的 web 入口与视图 |
| `wait_state.py` | 封装 socket attach 的内存 dialog 基线（不写等待快照） |
| `envscrub.py` / `safe_delete.py` | 进程环境身份洗刷 / 临时测试根安全删除，契约见模块头 |
| `e2e.py` / `test_*.py` / `fakeagent.py` | 回归与测试负载，覆盖面取各文件头 |

会话封装由独立 pi-wrap 仓承担；任务树由调用方管理，不入本仓 git。`proto.py`、`task_state.py`、`envscrub.py` 归本仓同版本发布，跨仓消费者须显式定位并固定本仓完整 revision；本仓不从其它仓回退导入 helper。

## 用法

脚本的 `--root` 是工作区根（其下已有 `agents/`）。显式 CLI root 优先，其次非空 `PLATFORM_WORKSPACE`（展开 ~ 并绝对化），缺省为真实源码仓的父目录；软链入口不改变源码身份。固定 release 目录的调用方必须注入状态根，不将源码父目录误当生产 workspace。以下从源码仓所在工作区执行（仓名以 agentd 举例）；其它 cwd 使用脚本绝对路径。portable 默认包内 workspace、相对值以包根解析且不展开 ~/$，是独立窄契约。

```bash
python3 agentd/agentctl.py --help
python3 agentd/agentctl.py list
python3 agentd/agentctl.py status task/<id>
python3 agentd/agentctl.py create --command 'sleep 10' --workdir /tmp --creator queue/dispatcher
python3 agentd/agentctl.py create --command '<调用方命令>' --workdir '<绝对目录>' --prompt-file '<正文文件>' --profile '<profile>'
python3 agentd/agentctl.py send task/<id> --body-file '<正文文件>' --deliver steer
python3 agentd/agentctl.py answer task/<id> --body '按方案 A 继续'
python3 agentd/agentctl.py ack task/<id> <msg-id>
python3 agentd/agentctl.py cancel task/<id> --reason '需求作废'
python3 agentd/agentctl.py update task/<id> --resources '["gpu"]'
python3 agentd/agentctl.py control task/<id> stop
```

`send` 的 from 缺省取 `AGENT_SELF`，再回落职位信箱；长正文用 `--body-file`（`-` 为 stdin）。`answer` 找最早未答 ask，reply 继承其 via；`cancel` 校验未 final 后写 stop。`update` 只改未放行任务的 resources/provides/needs；其它 spec 字段不由它修改。`enable` 是人工放行兜底。

`topic init` 创建主题骨架与订阅条目；`bot register` 登记进程或更新 subscribes。自愈须显式 `restartPolicy=auto`，缺省不自愈；reaper 缺省回职位信箱，`none` 为不发终态通知哨兵。两者的缺省告警不阻止登记。

`--profile` 现场枚举调用方 `bots/profiles/*.json`，只核名字、不核 form；非法拒绝且零落盘。有效值写成 command 的 `DISPATCH_PROFILE` 前缀，同值幂等、异值拒绝；缺省 WARN 后照建。

身份、路径与命令等取值旗标显式传空/纯空白拒绝；想用缺省值应省略旗标。可清空的数组与订阅字段按各子命令 --help。create 的正文为空、不可读或非 UTF-8 同样拒绝且零落盘。名字不得以 `.` 开头、含连续点或 `*?[]`；寻址文法仍兼容存量名字，以便读取、投递和清理。

部署服务由调用方启动器管理。直接调试 runner/scheduler 可用 `--help`；人工 scheduler `--once` 必须先确认没有其它调度实例。树内清理走同步层的删除清单，不能只删单机副本。

## 报表（report.py）

```bash
python3 agentd/report.py
python3 agentd/report.py --finalized
python3 agentd/report.py --all --out /tmp/agents-report.md
python3 agentd/needscheck.py
```

报表复用 scheduler 的状态、报告非空与占位判据；终态表缺省隐藏，`-s` 可按小节输出。坏 JSON 降级标读取不完整，不中断整份报表。无报告终态以中性标记呈现；取消优先于无报告。`stale/127` 且报告非空显示接管成功。

基础设施面以 runner 锁新鲜度观察运行与同步链路；有效并发上限取同 root 的 scheduler 实跑 argv，读不到显示未核实，不拿声明值冒充生效值。卡点与异常列的渲染以 `report.py` 为准。

活跃 task 表「当前工具」为只读会话观察：从封装固定的 `session/session.jsonl` 中，沿最后落盘条目的 `parentId` 分支提取本代启动后记录的 assistant `toolCall`，按同分支 `toolCallId` 配对消除 `toolResult`。只展示未配对的工具名与距调用记录的时长，不展示参数/问题正文；多调用并列，不挑一个称作真正执行者。`-` 表示无本代未返回调用或不采集（无 pid、非会话任务）；终态与 bot 不扫描。会话缺坏、半行、格式不支持、分支断链、无本代记录或读取超限显示「不可得」，不改变生命周期结论。读取字节/单行/条目数与展示项数的上限由 `report.py` 定义；使用有界完整快照，不用缺少祖先/结果的尾窗猜测。

这是“已发出、尚未记录结果”的线索，可能排队、异常遗留或同步迟到；时长不是实际执行耗时。JSONL 不记录真实进入阻塞、native dialog 请求/内部 signal 结算，也无法观察尚未落盘的内存分支移动或会话切换；**不是已确认等答或当前实际执行证明**。未结束工具、历史未答 ask 或问题关键词都不足以证明此刻等答，因此不推断、不扫描信箱、不另写等待快照或添加自定义会话协议。工具不可见不代表没有等待，可由 taskId/名称链接打开会话；ask/reply 通信协议不变。旧实例遗留的 wait-state.json 不读取、不删除。观察列测试见 `test_current_tools.py`；实际 widget/HTML、真实工具通信与不猜等待回归见 `test_wait_state.py`。

## 运行参数（生产化）

| 程序 | 参数与用途 |
|---|---|
| runner | `--root` 工作区根；`--host`/`--aliases` 本机认领身份；`--interval` 扫描周期；`--replay-grace` 历史通知宽限窗 |
| scheduler | 同 root/身份/周期；`--all-hosts` 全局视图；`--max-concurrent` 占位上限；`--once` 单轮 |
| 两者 | `--log-file`/`--log-level` 日志；数值缺省取各自 `--help` |

## 部署形态

每机一个 runner，全系统至多一个 scheduler（enable 单写者）。登记、调度与执行通过同一棵同步文件树解耦；scheduler 故障期间暂停新放行，已运行任务由各机续监督，恢复后无状态重扫追平。主机清单、调度方位置与启动命令由调用方声明。

文件写者与路由必须保持协议单写者矩阵；同步不会赋予其它机器改写原件的权利。

## 调度（独立调度方，scheduler.py）

每轮重扫，放行/阻塞写日志，依据写 enable.note。restart 不需要恢复私有队列；人工兜底也必须遵守全局单实例约束。监督壳通过 `AGENTD_MAX_CONCURRENT` 接收调用方并发上限；未设置时使用 CLI 缺省值。

### 调度参数（spec.json 可选字段，登记时写入）

| 字段 | 语义 |
|---|---|
| resources | 互斥资源数组；缺省 `["serial"]`，显式 `[]` 无资源占用 |
| provides | 成功后提供的能力数组，缺省 `[]` |
| needs | 所需能力数组，缺省 `[]` |

成功 provider = 生命周期终态 ∧ report.md 去空白后非空 ∧（exited/0 或 stale/127）。真实非零退出、killed/137、取消、空报告不满足依赖；取消判据 = `proto.EXITCODE_CANCELED`。完成、通知与依赖成功是不同判据，不以通知抬头替代调度判断。

### 放行规则（每轮无状态重扫，允许越位）

候补 = 有 spec、无 enable、无 pid，按 spec mtime 与目录名排序；本机模式还过滤 host。被阻塞候补不挡后面的可放行者。放行须同时满足：

1. needs 各有成功 provider 且无未终态 provider；同能力在途者优先于旧 success。无 provider/全无成功者为不可满足，后续每轮重评。
2. resources 与占位者无交集。
3. 占位数未到上限。
4. 目标机 runner 锁 updatedAt 新鲜；缺锁、坏字段或超阈值保守排队。

running 与已放行未落 pid 均占位；远端终态同步延迟只延后放行。enable 单调不回撤，拦已放行任务用 stop。无参数任务互相 FIFO 串行；能力环不检测，靠日志定位。调度常量与依赖决策归 `scheduling.py`，放行动作归 `scheduler.py`，生命周期与交付判据归 `task_state.py`。

## 路由字段

登记恒写 `host` 与 `createdByHost`：按 `env/host-id` 现场映射，缺映射告警并回落 hostname。runner 只认 host 匹配本机身份/别名的任务；host 缺失或空永不认领、永不放行，告警后由调用方补齐或重新登记。

`--root` 非空，不能是 agents 树本身，且 `<root>/agents` 必须已存在；不满足 rc=2 且零落盘。缺身份映射仅 WARN，不能阻止登记。控制与消息跟随文件同步到目标机。

## 跨机同步通道

协议只要求文件最终同步、原件单写、接收侧容忍半截 JSON 并下轮重试。具体传输、端点、成本与删除清单属于调用方同步层；工作区实现权威 = `@agents-sync#watch-cost` / `@agents-sync#gc`。

home 内 command/workdir 路径存 `~/…`，runner 按执行机 home 展开；非 home 绝对路径保持。路径可移植不改变写者归属。

无删除传播的传输中，单机删除会被其它副本复活；清理须由同步层协调所有节点。runner 探活锁以 updatedAt 新鲜度判活，不以存在性判活；本机互斥用 `run/locks/agentd.<host>.lock` 的 flock，退出由内核释放；同步心跳 JSON 不参与互斥。

## 已知风险（多机）

离线机的运行任务保守占位；永久退役机器的排队件须重新登记并处置旧件。手工漏写 host 的目录无人认领。其它协议边界见协议 §11；会话收敛与临末注入风险见 `pi-wrap/README.md` 的 Exit-code contract。

## 判死纪律与杀纪律（协议 §5.5）

有本进程 spawn 句柄用 poll 收尾；无句柄用 `(pid, procStart)` 校验，禁裸 waitpid。kill 前由 `kill_proc` 经 `probe_alive` 验身份；不匹配不杀，按 stale/127 记账，restart 可换代。校验到 kill 间仍有极窄 pid 复用窗口。

孤儿身份匹配则续监督，真死因不可得记 stale/127。其通知按报告存在性分 done/failed 并附 note；调度成功另要求报告非空。

## 约定赋值与实现口径

- **final**：代终态每轮幂等收口；one-shot 收口，非 auto 且非常驻收口。auto 留生命周期开放以自愈；常驻体经 stop/散会收口。常驻声明为 `spec.resident=true`（CLI：`--resident`），判据单点 `task_state.is_resident`；runner 注入 `AGENTD_RESIDENT=1`，不解析启动命令。已 final 吸收早退，他机档案只读。
- **reaper**：`Runner.resolve_reaper` 是运行侧单点。none 不发终态通知，ask 仍需回落收件面；合法目标必须目录在场且有活 pid、watcher 条目或 subscribes 声明，否则回落职位信箱并附 note。回落面无条件可写。creator 只供审计，登记恒写 reaper。
- **ask 路由**：runner 经 `AGENTD_ASK_INBOX`/`AGENTD_ASK_NOTE` 注入解析结果；子端仅接受同 root 内路径，身份变量须洗刷，不能传进孙进程。读侧扫职位信箱 ∪ reaper；两侧同源单点为 `proto.ask_scan_inboxes` / `core.askScanInboxes`，调用侧规程 = `@ask-playbook`。
- **通知幂等**：先写信封后写 notified.json，按 (参与方, 收件方) 判重，complete 短路。两写之间崩溃可能重复一条，接受 at-least-once。冷启抑制本次从未见非终态、且 endedAt 早于启动纪元宽限窗的旧通知；缺/坏 endedAt 也抑制。孤儿接管或本次 stop 收口是新事件，照发。实现单点 `Runner.notify_tick`。
- **送达**：字段与降级语义取 `@agent-file-protocol#msg-ack`；调用方 receiver 的在飞表、注入闸门与证据取 `@notify-and-delivery`。不判丢是现行取舍，升级前须依据观测重新裁定。
- **取消**：stop 请求在场或 exitcode=125；缺报告为预期，不告警空跑。旧 125 无 stop 的通知 event 与显示抬头可能不同，分类以代码谓词为准。被 runner 杀为 killed/137，信号退出为 128+signum，死因不可得为 stale/127。
- 时间戳 `YYYY-MM-DD-HH-MM-SS.mmm`；inject 在换代前写 inform；pid.log 为诊断详史，不参与判定。

## 验证

运行 `python3 recovery/run-tests.py` 可在自建临时 HOME/TMPDIR 内执行全部根级单测、`e2e.py --core` 和显式 `python3 -I -S -B portable/test_portable.py`，日志及授权根保留。CLI 参数与覆盖场景取 `e2e.py` 文件头；runner 生命周期、通知与报表分别有 `test_runner_final.py`、`test_runner_notify.py`、`test_report_slots.py`；本机互斥回归见 `test_runner_lock.py`。用临时根运行，禁止测试负载落生产树；并发改代码时用完整快照隔离。e2e 缺省为 standalone core，不发现或调用真实模型；跨仓场景须显式 `--integration` 提供测试依赖，S53 还须 `--live-pi`，不属于离线验收。

外部测试有界运行并保留真实退出码。调用方扩展测试及 opt-in 真 pi 注入探针住扩展 tests/；收件闸门的三态判据与探针入口 = `@notify-and-delivery`。封装与人格装配测试住 pi-wrap，资产与名字引用校验住调用方 bots，不在本模块复制步骤。

历史测试基线、环境戳与事故来源可按固定版本 `02ed2173:README.md` 取证；当前行为以上述权威与代码为准。
