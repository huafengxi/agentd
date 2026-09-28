# RACE-NOTES.md — 已知竞态窗留档（攒批④）

rpc 封装形态（`pi-rpc-wrap.py`）收敛路径上已知的竞态窗口：
**结论先行——窗口已收敛到可接受边界，缓解措施在位；此处留档已知残余边界，无需动作。**

## 收敛竞态窗（SETTLE_WINDOW = 0.5s）

**窗口**：完成判定 `agent_settled ∧ 最近 queue_update 队列空` 成立后，到关闭
pi stdin（EOF 优雅退出）前的 0.5s 观察窗。

**成因**：`agent_settled` 只证明「此刻无在途轮次」，不能证明「稍后不会有新注入」——
观测面注入（follow_up/steer/prompt）经 socket 透传异步到达，可能在 settled 判定与
关 stdin 之间的缝隙里入队。若无窗直接关 stdin，临末注入会随 EOF 一并丢失。

**收敛措施**（`pi-rpc-wrap.py` `settle_loop` / `_on_event`）：

1. **窗口观察**：settled 成立后再等 `SETTLE_WINDOW`（0.5s，环境钩
   `AGENTD_WRAP_SETTLE_WINDOW` 仅可向下调），期间每 50ms 查活动位。
2. **活动取消**：窗内出现任一活动即取消本轮收敛，回到等下一次 `agent_settled`：
   - 注入回执受理（`response` success ∧ command ∈ prompt/follow_up/steer）；
   - `queue_update` 报队列非空（steering/followUp）；
   - 新 user 轮次开始（`message_start` role=user）。
3. **队列快照兜底**：消费方（web SocketSupervisor）注入后以 `queue_update`
   快照核对入队成功，未入队的注入可立即重试，不依赖窗口兜住。

**已知残余边界**（不再加码，如实记录）：注入**恰在窗口内到达、且回执/队列事件
晚于窗口结束才被观察到**的极端时序下，该注入可能随 EOF 丢失。实测注入路径全通
（验收 4：注入指令被任务真实执行）；0.5s 窗已足够吸收正常链路的回执时延。
丢失的注入对调度面零影响（完成判定/退出码/终态通知不经观测面）。

## 附：同期收敛的其它竞态（已关闭，仅存目）

- **.pid 伴生档接管竞态**：wrap 崩溃留陈旧 sock/.pid，下次启动按 (pid, procStart)
  双件身份校验——存活拒启（防双宿主）、真死 unlink 接管；裸 pid 的复用误判面已由
  双件口径关闭（`@agent-file-protocol#observability`；web 侧路由判活同口径）。
- **runner 重启与在跑任务**：wrap 自持管道（进程组独立、stdin/stdout 不经
  runner），runner 重启任务不死，靠孤儿接管续监督（e2e S34）。

<!-- 新发现的竞态窗按「窗口/成因/收敛措施/残余边界」四段追加在本文件。 -->
