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

Agent 文件通信协议、登记工具、会话封装与通知机制。部署值与政策资产由调用方提供；执行契约由 serviced/PROTOCOL.md 定义。

## 收录判据

本仓保存信箱、消息与 ack、会话工具、业务元数据及 serviced 客户端适配。执行声明与状态的唯一权威为 serviced。跨机资源名逐字比较，由调用方使用 `<host>-<repo>` 表达机器隔离。

## 部署形态

调用方在工作区 `run/execution.json` 提供固定 serviced 源码、节点配置和目标节点装配参数。登记必须具备此配置。协议目录与 serviced 执行对象通过同一共享树同步。

一次性会话登记为 Task，持续监督对象登记为 Service。`execution.py` 将登记参数发布为 serviced 声明，参与方目录的 `execution.json` 只保存对象 id。`spec.json` 只保存业务元数据。登记参数中的 command、cwd、node、resources、文件 needs 和监督策略保存在 serviced 声明中。

`watcher.py` 是普通通知 Service，读取 serviced 事实并发送终态消息。业务视图按需计算，查询与执行均不依赖通知 Service 在线。通知去重记录属于业务协议。

## 用法

统一业务入口为 `agentctl.py --root <workspace>`，参数以 `--help` 为准。create/register-json 创建参与方与执行对象；send/ack/answer 操作信箱；status/list 读取 serviced 状态；cancel 发布取消意图；control stop/restart 操作已有 Service。Task 再次执行必须新建参与方。control clear 先停止 Service，再发布有限 Task 备份并清空所属节点会话，成功后启动 Service。

needs 为目标节点文件路径，resources 为全局互斥资源名。执行字段发布后修改遵循 serviced 的 Task 不变性和 Service 版本规则。

## 路由字段

登记参数的 host 由调用方身份表解析为执行节点；createdByHost 表示业务登记来源。运行节点、工作目录、进程身份、退出码与执行阶段读取 serviced，业务目录只保留执行对象引用。

## 报表

`report.py` 读取业务元数据与 serviced 声明、状态，生成 Markdown。`tasks.md` 为视图入口，`agentd4web.py` 为 HTTP 适配器。session_tools.py 只读会话转录中未返回的工具调用，不将转录推断为进程状态。

## 模块

- proto.py：寻址、原子写、信封与 ack。
- registration.py / execution.py：登记校验、执行对象发布和只读业务视图。
- watcher.py / notifications.py：终态通知、收件人路由和去重。
- pi-rpc-wrap.py / wait_state.py：会话与 RPC 适配。
- persona.py / envscrub.py：调用方资产和进程环境处理。
- test_*.py：协议、执行集成与 RPC 测试。
