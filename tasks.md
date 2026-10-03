<!--
每报表小节一个 cmd widget：单独 run/refresh 互不牵连（替代原整页 15s 刷新）；
页面带 `_v=autorun` 打开自动执行。采集与渲染口径 = `agentd/report.py`（一行未改，仅加小节切片后处理）。
末节「旧 task 清理」是本页唯一含**变更**动作的一节（感叹号前缀的手动 widget，autorun/run-all 跳过）：
候选判据、保留期缺省值与删除动作都住主仓的 `dash/gc-tasks.py`（只读取数）与 `dash/gc-tasks-act.py`（变更），
本页只引用、不写死政策取值（改保留期不产生本仓 diff）。
-->

## 未终态任务

${agentd/report.py -s active}

## 调度基础设施健康度

${agentd/report.py -s system}

## task/bot 两族状态计数

${agentd/report.py -s stats}

## 失败/心跳停滞/应跑未跑/基础设施异常集中区

${agentd/report.py -s abnormal}

## bot 族常驻/一次性进程状态

${agentd/report.py -s bots}

## topic 协作容器

${agentd/report.py -s topics}

## 最近 10 个终态任务

${agentd/report.py -s recent}

## 旧 task 清理

${python3 dash/gc-tasks.py}

${!python3 dash/gc-tasks-act.py --yes}

## 说明

- 调度基础设施健康度：各 host runner / scheduler / agents-sync 链路
- 调度基础设施健康度（口径）：runner 判活 = `agents/run/agentd.<host>.lock` 的 updatedAt 新鲜度，阈值取 `scheduler.HOST_ALIVE_THRESHOLD`（当前 60s，复用调度门禁判活口径）；agents-sync 链路为间接口径——远端锁由远端 runner 写、经同步链路到达 dev，内容新鲜即同时证明「远端 runner 活 + 同步链路通」（✅ 双活）；dev 为 hub 无本地同步链路
- 失败/心跳停滞/应跑未跑/基础设施异常集中区：无异常时一行「（无异常）」
- task/bot 两族状态计数（口径）：bot 族为常驻进程，无成败语义，终态多为被停用/一次跑完；明细见「bot 族常驻/一次性进程状态」节
- 未终态任务：运行中 + 排队/已放行待拉起，含调度依赖与最新进展
- 会话观测链接：任务表的 taskId/名称两格（活跃、最近完成、终态三表）本身即该会话的 `?v=chat` 聊天观测面链接（带 /<host>/ 机器前缀，跨机经反代路由）；点击在新标签页打开（渲染页 `<base target="_blank">`）；运行中=直播+可介入，终态=复活续聊。活跃表原独立「会话」列（`[观测](…)`）已删——与 taskId/名称同链，三处冗余；其余各表本就无独立「会话/观测」列
- bot 族常驻/一次性进程状态：auto/one-shot 语义区分；常驻进程无成败语义——restartPolicy=auto 的 final 通常为被停用/意外死亡（非完成）；pi-rpc-wrap 会话型 bot（`bot/<名>`）的 bot 名/用途名同样可点击弹出 `?v=chat` 观测窗，脚本型 bot 与转发器无会话故保持纯文本（族别名单住工作区的声明面，本仓不复述）；异常见异常集中区（文案带族别）
- topic 协作容器：直扫 `agents/topic/*`（非进程型、不入任务面）；每主题一行 6 列：id/主持人/标题（topic.md 首行）/inbox 消息数/最新消息时间/watcher（owner 登记）；无主题时显示「（无主题）」；机制与列口径见 `@topic-design#tasks-tab`
- topic 协作容器·主持人列（口径）：可点击 `?v=chat` 会话链接（host 前缀取该 bot `spec.host`，跨机经反代路由到会话宿主机；点击在新标签页打开）；判据 = 订阅该 topic 的**会话型** bot——通道 B（`spec.subscribes` 含 `topic/<id>`）∨ 通道 A（`topic/<id>/watcher/<裸名>` 条目 + bot 目录在场，轻场景调度员临时兼主持只开 watcher 条目），且 `is_session_bot` 为真（`spec.command` 含 pi-rpc-wrap.py 或 pid.json 有 sock）；**不以 `DISPATCH_PROFILE=moderator` 作判据**（profile 是人格资产不是身份判据）；脚本型 bot 与信箱型 bot 无会话面 → 订阅了也不出链；系统主题（`dispatcher`）按设计无策展 owner → 主持人格恒纯文本 `-`（绝不造假链接）；零主持人也为 `-`；多主持人全部列出（逗号分隔、各自成链）；watcher 列已剔除主持人裸名避免同名两格冗余（剔空为 `-`）
- 最近 10 个终态任务：完成时间降序
- 旧 task 清理·候选判据：`task/<id>/pid.json` 的 `final=true` ∧ `endedAt` 早于 now − 保留期（缺省 3 天，取值与判据单点 = `dash/gc-tasks.py`）；在飞/未启动、保留窗内、以及 `pid.json` 半截坏或终态却无可解析 `endedAt`（= 不可判定，单独计数）一律不入选；`bot/`、`topic/` 两族不在射程
- 旧 task 清理·删除范围：**整个 `task/<id>/` 目录**（`report.md`/`prompt.md`/`plan.md`/`progress.md` 与 `session/` 转录一并消失，不可逆）⇒ 账本（`lore/desk/`）与域册（`lore/library/`）里指向 `agents/task/<id>/report.md` 的指针会断；裁定与重议触发 = `lore/library/dispatch/facts/deletion-gc.md`
- 旧 task 清理·两枚 widget 的分工：只读表 = 当前候选读数（空态只出表头 = 无可清理项，不是扫描失败）；橙色按钮 = 变更，点一次跑完整轮（分批入 delete-list → 等通道宽限 → reap → 核验），耗时 ≈ 宽限期 5 分钟 + 数十秒，期间每 60s 出一行进度
- 旧 task 清理·执行点恒为 GC hub：widget 命令在**浏览器所连的那台 w 服务**上执行，而删除只允许落在 hub 的权威树 ⇒ 非 hub 上点这个按钮会被拒绝（不写清单、不删）；hub 侧删除后各机副本由 agents-sync 的 pull 侧按清单自行收敛（不在按钮的等待窗内）
