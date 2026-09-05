# Runtime、Event 与 Checkpoint

## 一、唯一状态路径

```text
Runtime 行为
  -> StateTransition                    # 当前版本的语义请求
  -> StateOperation[]
  -> StateOperationBatch(V -> V + 1)   # 原子状态提交
  -> StateReducer
  -> RuntimeState@V+1
```

`StateReducer` 是 RuntimeState 的唯一写入口。它先在候选 record 上应用严格的
`add | replace | remove` 操作，再恢复并校验完整 typed RuntimeState；任一步失败都不
修改旧 State。Scheduler 状态、NodeOccurrence、EdgeResolution、Loop、OperatorCall、
Wait、Context 和 Child plan 都走这条路径。

`StateTransition` 只用于当前进程规划 Batch，不是公开事件，也不是恢复时必须重新解释的
业务语义。

## 二、RuntimeEvent 捕获

Journal 立即把 Batch 应用到 current RuntimeState，同时把 Batch 与 `RuntimeLog` 放入
pending capture。到达配置的 flush 边界后，一个或多个连续 Batch 形成：

```text
RuntimeEvent(
    sequence,
    from_state_version,
    to_state_version,
    operation_batches=[...],
    logs=[...],
    previous_event_id,
    previous_event_digest,
)
```

因此：

- Batch 是原子状态变化；RuntimeEvent 是可调节粒度的 canonical 信封；
- 一个 RuntimeEvent 可以覆盖多个状态版本；
- sealed Event 的 Batch 必须连续，完整前缀可由 Reducer 重建相同 State；
- Event 不保存完整 RuntimeState；
- pending Batch 已在内存生效，但 flush 前仍处于进程崩溃窗口。

默认 App 在每次 Transition 后尝试导出已形成的 Event。注入
`RuntimeEventSink` 时，App 按 Session 顺序调用异步 `append(event)`，独立 Session 可以
并发。`append` 返回表示已经持久接纳；抛错时 Event 留在 Journal，之后会按同一 Event id
重试。因此 sink 必须幂等，且它的延迟会直接形成 Runtime 背压。数据库事务、Outbox、
重试调度和长期 Event Store 是 Host 的职责；未注入 sink 时 Core 不保存完整执行历史。

## 三、公开观察事件

公开 SDK 不返回 RuntimeEvent 或 StateOperation：

- `TraceEvent`：从即时 StateTransition 做 allowlist 投影，只保留安全身份、状态、错误、
  指标和少量属性；`trace_sequence` 在一个 Root 观察流内单调递增。
- `UserEvent`：来自 Stream Chunk 或 Node 的 UserEvent Mapping，不进入 RuntimeState，
  有独立序列。

普通 Result/Submission 返回自上一个公开边界以来的 Trace/User 事件。`stream/astream`
把每条事件包装为 `InvocationUpdate`，最终再返回 `InvocationResult`。这些观察事件不能
用于恢复；Host 的恢复事实是 canonical RuntimeEvent，SDK 用户的恢复事实是 Checkpoint。

## 四、Checkpoint

```text
RuntimeCheckpointBundle
  root_session_id
  states[root + every reachable child with an opened Invocation]
  captured_at_ns
  id + canonical digest

AppCheckpoint
  roots[RuntimeCheckpointBundle]
```

Bundle 是一个完整父子 Runtime 图，不是单 Session 快照。若 Child 只有 `SessionOpened`
而尚未 `InvocationOpened`，Bundle 会省略该不完整 Child State；父 plan 仍保持
`planned`，恢复时按 plan 重建 Child。构建时 Journal 会 flush 图内全部 pending Batch；
App 在返回前再导出这些 RuntimeEvent。Bundle 校验 Session/Invocation 身份、父子
Revision、终态阶段、无环、无重复父节点和无孤儿 State。

Journal 内部 capture 复用 Reducer 所有的不可变 RuntimeState 引用，只有调用方序列化时
才生成完整 record，避免 Core 同时保留多份深拷贝。公开 `from_states/from_record` 会重新
规范化并隔离外部对象。`to_record/from_record` 使用精确 schema 和摘要；当前不做 schema
migration。

## 五、公开 Checkpoint 时机

所有入口复用同一个底层 capture 方法：

| API | Checkpoint 边界 |
| --- | --- |
| `invoke/ainvoke` | completed、failed、cancelled 或 waiting 返回点 |
| `submit_invoke/asubmit_invoke` | Invocation 已初始化并可靠接纳 |
| `wait/await_result` | 当前可观察返回点 |
| `resume/aresume` | Resume 后的下一终态或 waiting 边界 |
| `submit_resume/asubmit_resume` | Resume 被可靠接纳 |
| `cancel/acancel` | 父子图取消收敛后 |
| `recover/arecover` | 恢复驱动后的下一终态或 waiting 边界 |
| `close/aclose` | 进程内任务停止后，每个独立 Root 一个 Bundle |

### stream/astream

`stream/astream` 是零 backlog、caller-driven 的严格背压流：Runtime 只有收到下一次
迭代请求才创建并提交下一条更新，上一条必须先由调用方确认消费。每个
`InvocationUpdate` 都有 Trace/User Event，但 `checkpoint` 仅在安全恢复边界非空：

- Scheduler 初始化；
- Node started、waiting、completed、failed；
- Wait resumed 与 Invocation recovery；
- Child plan、phase、await suspended/ready；
- Invocation waiting、completed、failed、cancelled。

Node started 是每次 Input Mapping、Capability 选择和 Operator 副作用之前的 write-ahead
边界。OperatorCall started 与 Stream Chunk 不额外生成 Checkpoint；Map 仍是一个原子
NodeOccurrence，恢复时依据该 Node 的 `recovery_mode` 重放整个发生。安全边界的
Transition、RuntimeEvent 导出与 Checkpoint capture 在同一个 Root 锁内完成，因此调用方
中途中断时，可以保存最近一个非空 `InvocationUpdate.checkpoint`，而不必退回到 Invocation
起点。流最后的 `InvocationResult` 始终携带最新 Checkpoint；它一经交付即完成终止握手，
不要求调用方再拉取 END，之后的 detached spawn Child 也不再受该流背压。

attached stream 已到 waiting 但最终 `InvocationResult` 尚未交付时，`resume` 会以
`INVOCATION_RESULT_PENDING` 拒绝，避免新的执行越过旧的公开边界；消费 Result 后即可
Resume。外部 `cancel` 产生的状态更新仍服从该流的调用方背压；取消更新被消费后，流会
继续交付状态为 cancelled 的最终 Result。

## 六、加载与恢复

1. 新 App 注册 Bundle 引用的每个精确 Workflow Revision；
2. `load_checkpoint/aload_checkpoint` 原子安装一个 Bundle 或 AppCheckpoint；
3. completed Session 可开始新 Invocation；waiting Invocation 使用 `resume`；
4. 未完成且物理任务已丢失的 Invocation 使用 `recover`。

Recovery 会把旧 running OperatorCall 标记为 lost。Node 默认
`Recovery(mode="never")`，所以有副作用的运行中调用不会被隐式重复执行；只有
`replay_safe` 且未超过 `max_attempts` 才重新 ready。Recovery 处理父子图时以 Root 为
单位，避免重复创建 Child 或留下不可操作 Handle。await Child 仍由父 Node 等待；spawn
Child 只需可靠重启受管 drive，Root 恢复结果不等待它到终态。若 `recover` 的精确目标
本身位于 spawn 子树，则沿目标祖先路径等待该目标到终态或 waiting 边界，其他 spawn
旁支仍保持 detached。

恢复遇到已经打开 Child Invocation、但父 plan 仍停在 `opened` 的中间边界时，会先补齐
`accepted`，再处理 running、waiting 或 terminal Child，保证后续 Handle 状态能够继续
推进到 `terminal`。

## 七、Session 与历史边界

一个 Session 同时最多一个活动 Invocation。终态后可以提交新的 Invocation，新状态会
替换 Core 内该 Session 的当前 Invocation，并自然淘汰旧 Invocation/Child 的进程内信息。
并行 Child 使用独立 Session，但都属于父 Root 的同一个 Checkpoint 图。

Core 不提供历史查询或公开 replay API，也不长期保存每次 Invocation。Server/Host 从
RuntimeEvent Store 重建所需 RuntimeState，形成 Bundle 后再调用 App 的加载 API；UI、
任意历史状态查询、归档、Fork 和 schema migration 都在 Core 之外。
