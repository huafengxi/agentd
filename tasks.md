<!--
每报表小节一个 cmd widget：单独 run/refresh 互不牵连；页面带 `_v=autorun`
打开自动执行。宿主通过 src 注入本页绝对路径，脚本定位相邻 report.py；状态根由
调用方 PLATFORM_WORKSPACE 注入，与源码 checkout 名字、cwd 和 sibling 仓无关。
这里只提供只读报表；任务保留期、删除工具、启动器与 GC hub 由调用方决定，不在本页执行。
-->

## 异常与阻塞

${python3 "$(dirname "$src")/report.py" -s abnormal}

## 未终态任务

${python3 "$(dirname "$src")/report.py" -s active}

## 最近终态

${python3 "$(dirname "$src")/report.py" -s recent}

<details>
<summary>基础设施与状态统计</summary>

## 基础设施健康

${python3 "$(dirname "$src")/report.py" -s system}

## 状态统计

${python3 "$(dirname "$src")/report.py" -s stats}

## bot 状态

${python3 "$(dirname "$src")/report.py" -s bots}

## topic

${python3 "$(dirname "$src")/report.py" -s topics}

</details>

<details>
<summary>说明与操作口径</summary>

- report.py 与 scheduler/needscheck 共享 scheduling.py/task_state.py 的快照与谓词；报表不写 enable。
- runner 锁 updatedAt 的新鲜度是跨机探活及间接同步信号，不把仅有文件视为在线。
- 未终态表涵盖运行、排队与已放行待拉起；终态是否满足依赖另要求报告非空及成功退出谓词。
- 会话观测链接使用任务 spec.host 前缀及 `?v=chat`，真实页面与反代由调用方提供。
- 进程型 bot 以 resident/restartPolicy 声明生命周期；会话型 bot 按 command/pid.sock 判读，不以 profile 名推断身份。
- topic 不在进程任务面；标题跳过 frontmatter，主持人来自 watcher 或 subscribes 的会话型 bot，剔重后显示 watcher。
- 取消、空跑、孤儿接管与在途 provider 优先级均沿用协议谓词；不要从通知抬头或文案反推调度状态。
- 任务树删除必须使用调用方同步层的删除清单；本仓不指定保留期或自动运行清理。

</details>
