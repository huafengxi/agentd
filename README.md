---
name: agentd
anchors:
  intake: "收录判据"
  usage: "用法"
  report: "报表"
  deploy: "部署形态"
  routing: "路由字段"
---

# agentd

Agent 文件通信协议、登记工具、会话封装与通知机制。部署值与政策资产由调用方提供；执行契约由 rund/PROTOCOL.md 定义。

## 收录判据

本仓保存信箱、消息与 ack、会话工具、业务元数据及 rund 客户端适配。执行声明与状态的唯一权威为 rund。跨机资源名逐字比较，由调用方使用 `<host>-<repo>` 表达机器隔离。

## 部署形态

调用方在工作区 `run/execution.json` 提供固定 rund 源码、节点配置和目标节点装配参数。登记必须具备此配置。协议目录与 rund 执行对象通过同一共享树同步。

一次性会话登记为 Task，持续监督对象登记为 Service。`execution.py` 将登记参数发布为 rund 声明，参与方目录的 `execution.json` 只保存对象 id。`spec.json` 只保存业务元数据。登记参数中的 command、cwd、node、resources、文件 needs 和监督策略保存在 rund 声明中。

`watcher.py` 是普通通知 Service，读取 rund 事实并发送终态消息。业务视图按需计算，查询与执行均不依赖通知 Service 在线。通知去重记录属于业务协议。

## 用法

统一业务入口为 `agentctl.py --root <workspace>`，参数以 `--help` 为准。create/register-json 创建参与方与执行对象；send/ack/answer 操作信箱；status/list 读取 rund 状态；cancel 发布取消意图；control stop/restart 操作已有 Service。Task 再次执行必须新建参与方。control clear 先停止 Service，再发布有限 Task 备份并清空所属节点会话，成功后启动 Service。

needs 为目标节点文件路径，resources 为全局互斥资源名。执行字段发布后修改遵循 rund 的 Task 不变性和 Service 版本规则。

| 命令（前缀 `agentctl --root <workspace>`） | 实际行为 |
| --- | --- |
| create --command <命令> --workdir <绝对目录> --creator <登记方> | 创建 task 参与方并发布执行对象；可传 --name、--host、--prompt-file、--needs、--resources、--resident、--restart-policy 等 |
| register-json | 从 stdin 读取 `{name,spec,prompt}`，共用登记与发布流程 |
| submit <参与方> | 无执行引用时发布；已有引用时返回现有声明，引用对应对象未到达则报错 |
| status <参与方> / list | 读取 rund 事实形成业务视图；不维护独立执行状态文件 |
| cancel <参与方> | Task 发布 cancel，Service 发布 offline；返回意图，不等待收口 |
| control <参与方> stop | Service 经目标节点 runctl stop 等待收口；Task 发布 cancel |
| control <参与方> restart | 仅 Service；依次 stop、start，并等待执行结果 |
| control <参与方> clear | 仅 Service；停止后发布一次性 Task 备份并清空会话，成功后启动 |
| create-bot --name <名> | 创建仅有 inbox 的 bot 信箱，不发布执行对象 |
| bot register --name <名> ... | 登记进程型 bot；已有 spec 时只修改订阅 |
| topic init <id> ... | 创建 topic 容器及 watcher 订阅 |
| send <参与方> --body-file <file> / ack <参与方> <消息id> / answer <参与方> --body-file <file> | 投递消息、确认消费、答复未答 ask |

参与方使用 `task/<名>`、`bot/<名>` 等协议路径；业务 taskId、参与方路径与 rund 对象 id 是不同标识，execution.json 负责关联。

登记输入中的 command、workdir、host、resources、needs、resident、restartPolicy 转为执行声明后从 spec.json 移除。默认一次性执行发布 Task，并携带 resources（缺省 `["serial"]`）；`[]` 仍占全局并发名额。resident=true 或 restartPolicy=auto 发布 Service，auto 映射 restart=always，其余映射 manual。resident 模板省略 resources，独立于全局放行；非 resident 的 auto Service 仍携带 resources。

needs 合并目标节点装配依赖与用户路径，用户路径必须以 `/` 或 `${` 起始。命令通过普通 exec_task.py 包装器解释；底层 rund 的 argv 仍为直接执行。`control --inject` 会拒绝，运行中上下文通过 inbox 消息注入。

## 路由字段

登记参数的 host 由调用方身份表解析为执行节点；createdByHost 表示业务登记来源。运行节点、工作目录、进程身份、退出码与执行阶段读取 rund，业务目录只保留执行对象引用。

## 报表

`report.py` 读取业务元数据与 rund 声明、状态，生成 Markdown。`tasks.md` 为视图入口，`agentd4web.py` 为 HTTP 适配器。session_tools.py 只读会话转录中未返回的工具调用，不将转录推断为进程状态。

## 模块

- proto.py：寻址、原子写、信封与 ack。
- registration.py / execution.py：登记校验、执行对象发布和只读业务视图。
- watcher.py / notifications.py：终态通知、收件人路由和去重。
- pi-rpc-wrap.py / wait_state.py：会话与 RPC 适配。
- persona.py / envscrub.py：调用方资产和进程环境处理。
- test_*.py：协议、执行集成与 RPC 测试。
