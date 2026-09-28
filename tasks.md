<!--
每报表小节一个 cmd widget：单独 run/refresh 互不牵连（替代原整页 15s 刷新）；
页面带 `_v=autorun` 打开自动执行。采集与渲染口径 = `agentd/report.py`（一行未改，仅加小节切片后处理）。
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

## 说明

- 调度基础设施健康度：各 host runner / scheduler / agents-sync 链路
- 调度基础设施健康度（口径）：runner 判活 = `agents/run/agentd.<host>.lock` 的 updatedAt 新鲜度，阈值取 `scheduler.HOST_ALIVE_THRESHOLD`（当前 60s，复用调度门禁判活口径）；agents-sync 链路为间接口径——远端锁由远端 runner 写、经同步链路到达 dev，内容新鲜即同时证明「远端 runner 活 + 同步链路通」（✅ 双活）；dev 为 hub 无本地同步链路（口径同 svc.status 的 skip）
- 失败/心跳停滞/应跑未跑/基础设施异常集中区：无异常时一行「（无异常）」
- task/bot 两族状态计数（口径）：bot 族为常驻进程，无成败语义，终态多为被停用/一次跑完；明细见「bot 族常驻/一次性进程状态」节
- 未终态任务：运行中 + 排队/已放行待拉起，含调度依赖与最新进展
- 会话观测链接：任务表的 taskId/名称两格（活跃、最近完成、终态三表）本身即该会话的 `?v=chat` 聊天观测面链接（带 /<host>/ 机器前缀，跨机经反代路由）；点击在新标签页打开（渲染页 `<base target="_blank">`）；运行中=直播+可介入，终态=复活续聊。活跃表原独立「会话」列（`[观测](…)`）已删——与 taskId/名称同链，三处冗余；其余各表本就无独立「会话/观测」列
- bot 族常驻/一次性进程状态：auto/one-shot 语义区分；常驻进程无成败语义——restartPolicy=auto 的 final 通常为被停用/意外死亡（非完成）；pi-rpc-wrap 会话型 bot（`bot/<名>`）的 bot 名/用途名同样可点击弹出 `?v=chat` 观测窗，脚本型 bot（notify-user/heartbeat-loop）与转发器（dispatcher）无会话故保持纯文本；异常见异常集中区（文案带族别）
- topic 协作容器：直扫 `agents/topic/*`（非进程型、不入任务面）；每主题一行 6 列：id/主持人/标题（topic.md 首行）/inbox 消息数/最新消息时间/watcher（owner 登记）；无主题时显示「（无主题）」；机制与列口径见 `@topic-design#tasks-tab`
- topic 协作容器·主持人列（口径）：可点击 `?v=chat` 会话链接（host 前缀取该 bot `spec.host`，跨机经反代路由到会话宿主机；点击在新标签页打开）；判据 = 订阅该 topic 的**会话型** bot——通道 B（`spec.subscribes` 含 `topic/<id>`）∨ 通道 A（`topic/<id>/watcher/<裸名>` 条目 + bot 目录在场，轻场景调度员临时兼主持只开 watcher 条目），且 `is_session_bot` 为真（`spec.command` 含 pi-rpc-wrap.py 或 pid.json 有 sock）；**不以 `DISPATCH_PROFILE=moderator` 作判据**（profile 是人格资产不是身份判据）；脚本型 bot（notify-user/heartbeat-loop）与信箱型 bot 无会话面 → 订阅了也不出链；系统主题（`dispatcher`）按设计无策展 owner → 主持人格恒纯文本 `-`（绝不造假链接）；零主持人也为 `-`；多主持人全部列出（逗号分隔、各自成链）；watcher 列已剔除主持人裸名避免同名两格冗余（剔空为 `-`）
- 最近 10 个终态任务：完成时间降序
