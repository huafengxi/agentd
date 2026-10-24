---
name: agent-file-protocol
anchors:
  layout: "目录与写权"
  spec-json: "目录与写权"
  msg-ack: "消息与通知"
  envelope: "消息与通知"
---

# Agent 文件协议

## 目录与写权

`<workspace>/agents` 指向 rund 共享树中的 `agents/`。参与方地址为 `<family>/<name>`；family 使用 proto.py 的现行文法。信箱地址可以先于参与方登记存在。

```
agents/<family>/<name>/
  spec.json                 # 创建者写业务元数据
  execution.json            # 创建者写 {"id":"rund-object-id"}
  prompt.md                 # 创建者写初始正文
  inbox/<message-id>.msg     # 发送方写不可变信封
  inbox/ack/<message-id>     # 消费者写确认
  session/                  # 会话所属执行者写
  report.md                 # 参与方写交付报告
  progress.md               # 参与方写业务进展
  notified.json             # 所属节点通知 Service 写去重记录
```

spec.json 包含 name、creator、createdAt、createdByHost、reaper、profile、subscribes 等业务元数据。execution.json 只保存 rund 对象 id；执行定义与状态通过该 id 读取 rund。消息文件、初始引用与创建元数据由原写者发布，副本使用 agents-sync 的 replica 标记。

## 执行与生命周期

Task 是一次执行，Service 直接监督常驻进程；两者为平行执行对象。阶段、原生身份、真实 wait 结果和取消状态均以 rund 为准，业务视图只读计算。未知状态保持未知，报告文件不能证明进程正常退出。

登记需要调用方提供 run/execution.json。command、workdir、host、resources、needs、restartPolicy、resident 是登记输入，发布后保存在 rund 声明中。needs 指目标文件，resources 为全局资源名。执行定义修改遵循 rund 的声明契约。

cancel 发布 Task cancel.json 请求，或向 Service 执行节点保存 offline 意图。Service 重启通过 runctl stop/start；Task 重跑新建参与方。Service 会话清理先停止执行，通过所属节点有限 Task 备份并清空会话，成功后启动 Service。

## 消息与通知

信封包含 id、from、ts、type、body；reply 使用 ref 关联 ask。消息类型与字段校验以 proto.py 为准。ack 表示消息消费证据，会话注入确认可包含消费时间；消息投递与进程执行状态相互独立。

reaper 指定终态通知收件方，none 表示不通知。通知 Service 读取 rund 终态，投递后写 notified.json。重启时按通知去重与历史重放窗口处理；先写信封后写去重记录的崩溃窗口允许重复通知。

Pi 会话通过本机 run/agentd/<name>.sock 暴露 RPC，socket 不参与跨机同步。会话观测须验证 rund 当前进程身份及本机通道。
