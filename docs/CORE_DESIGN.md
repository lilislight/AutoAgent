# AutoAgent V2 Core 结构设计

Core 负责 Workflow 定义、编译和执行，并把当前运行状态、公开观察信息与 Host
持久化信息分成三条边界。Core 不负责数据库、Event Store、HTTP Server、远程执行或 UI。

## 一、核心原则

1. `RuntimeState` 只能由 `StateOperationBatch` 经 `StateReducer` 改变。
2. `StateTransition` 是当前进程提出的语义转换，不是公开事件或持久化事实。
3. `RuntimeEvent` 是可聚合多个 Batch/Log 的 canonical 持久化信封，只交给 Host sink。
4. SDK 只返回安全投影后的 `TraceEvent`、独立的 `UserEvent` 和 Checkpoint。
5. Scheduler 只规划图；WorkflowExecutor 驱动 Invocation；NodeExecutor 执行一次
   NodeOccurrence 内的瞬态调用。
6. 一个 Session 同时最多有一个活动 Invocation；Child Workflow 使用独立 Session。

## 二、模块结构

```text
autoagent/core/
├── app/
│   ├── app.py             # 公共门面、组件装配和生命周期
│   ├── models.py          # InvocationRef/Result/Update/AppCheckpoint
│   ├── ports.py           # Journal、Scheduler、Executor 等窄接口
│   ├── runtime_loop.py    # 同步 API 的私有事件循环线程
│   └── stream.py          # 严格背压的同步/异步流通道
├── compiler/              # Workflow -> WorkflowIR、诊断、定义快照
├── executor/
│   ├── workflow_executor.py
│   ├── node_executor.py
│   └── future.py
├── hosting/
│   └── runtime_events.py  # RuntimeEventSink / UserEventSink
├── operators/             # Operator、契约、Registry、StreamReducer
├── runtime/
│   ├── events.py          # StateTransition、RuntimeLog、RuntimeEvent
│   ├── operations.py      # StateOperation 与原子 Batch
│   ├── reducer.py         # 唯一 StateReducer
│   ├── state.py           # 完整 RuntimeState
│   ├── journal.py         # 默认进程内 State/捕获边界
│   ├── checkpoint.py      # Root + Child State Bundle
│   ├── trace.py           # 公开 Trace 投影
│   ├── user_events.py     # 非权威用户事件
│   └── task_runtime.py    # 进程内 asyncio Task 所有权
├── scheduler/             # DAG 与 Loop 规划
└── workflow/              # Authoring 模型与不可变 IR
```

依赖方向：

```text
AutoAgentApp
  -> Compiler
  -> WorkflowExecutor -> Scheduler / NodeExecutor / TaskRuntime / Runtime ports
  -> OperatorRegistry

StateTransition -> Journal -> StateReducer -> RuntimeState
                         └──> RuntimeEvent -> Host sink

Stream/User mapping -> UserEvent -> SDK channel
                               └──> optional Host observation sink
```

Runtime、Scheduler 和 Workflow 不反向依赖 App；NodeExecutor 不读取 Journal 或
RuntimeState；Host sink 不参与 Runtime 状态判定。

## 三、执行职责

### WorkflowExecutor

- 从 Scheduler 获取 ready NodeOccurrence 并并行推进；
- 编排 Input Mapping、Condition、Output Binding 和失败路由；
- 提交 Node、Wait、Child 与 Invocation 的语义转换；
- 处理 `fail_fast` 或 `continue_active_branches`；
- 创建和等待 Child Invocation。

### NodeExecutor

- 执行同步/异步 Operator；
- 在一个 NodeOccurrence 内完成 Map、有序结果与可选聚合；
- 消费同步/异步迭代器，以 StreamReducer 得到一个 Node Output；
- 通知 OperatorCall 生命周期和 Stream Chunk。

当前没有 Operator Retry、Fallback 或 Timeout 策略。同步 Operator 在线程 Worker 中
运行；Python 不能强制终止已经进入用户函数的线程，因此逻辑取消后，它仍占用并发名额，
直到物理调用退出。只有真实的 Task cancellation 才是 Runtime 控制取消；用户
Operator/Hook 主动抛出的 `asyncio.CancelledError` 会按普通 Node 失败收敛，不能留下
running Occurrence。Worker、Scheduler、RuntimeLoop 和等待桥接均不做周期轮询。

## 四、App 公共边界

主要入口：

| 类别 | 同步 | 异步 | 返回 |
| --- | --- | --- | --- |
| 执行并等待边界 | `invoke` | `ainvoke` | `InvocationResult` |
| 可靠接纳后台执行 | `submit_invoke` | `asubmit_invoke` | `InvocationSubmission` |
| 读取当前状态 | `status` | `astatus` | `InvocationResult` |
| 等待后台结果 | `join` | `ajoin` | `InvocationResult` |
| 恢复 Wait | `resume`/`submit_resume` | `aresume`/`asubmit_resume` | Result/Submission |
| 流式恢复 Wait | `stream_resume` | `astream_resume` | Updates，最后为 Result |
| 取消 | `cancel` | `acancel` | `InvocationResult` |
| 崩溃恢复 | `recover` | `arecover` | `InvocationResult` |
| 观察执行 | `stream` | `astream` | `InvocationUpdate`，最后为 Result |
| 加载状态 | `load_checkpoint` | `aload_checkpoint` | `CheckpointLoadResult` |
| 关闭 | `close` | `aclose` | `AppCheckpoint` |

`join/ajoin` 只等待已经接纳的 Invocation，不创建新 Invocation。超时会抛出内置
`TimeoutError`，但不会取消后台执行，调用方之后可以再次 join 或显式 cancel。

控制操作必须使用精确的 `InvocationRef(session_id, invocation_id)` 或完整
`ChildInvocationHandle`，不能只凭 Session 误操作后来的 Invocation。已有活动 Invocation 的 Session 拒绝再次执行；终态
Invocation 可被同一 Session 的新 Invocation 替换，Core 不保留其完整历史。

同步与异步入口都进入 App 私有 RuntimeLoop。取消异步调用会传递到 RuntimeLoop 内的
Invocation Task。`max_operator_concurrency` 是同一个 App 内、跨 Workflow、跨
Invocation 的同步/异步 Operator 物理全局上限；`Map.max_parallelism` 只能进一步收紧。
并发 `close/aclose` 共享一次关闭操作和同一个 AppCheckpoint。调用超时只让该调用方
脱离，不会取消已经开始的关闭；后续调用可以重新等待同一操作。即使 RuntimeLoop 从未
启动，关闭也会捕获注入 Journal 中已有的 Root/Child 状态。

## 五、三类输出

### TraceEvent

由每个即时 `StateTransition` 投影，包含 allowlist 后的身份、状态、错误和指标，不包含
StateOperation 或完整业务值。它用于 SDK 观察，不用于恢复。

### UserEvent

来自 Stream Chunk 或 Node 的 `UserEventMapping`，有独立顺序，不修改 RuntimeState，
映射失败也不会回滚已经完成的业务状态。可选 `UserEventSink` 在事件交给 SDK 前按
Invocation 顺序接纳观察记录；它与 canonical `RuntimeEventSink` 是两条独立通道。
某个 Invocation 的观察写入首次失败后，App 记录该失败并停止继续写入该 Invocation，
避免后续记录掩盖序列缺口；其他 Invocation 不受影响，Workflow 状态也不回滚。

### RuntimeEvent

Journal 把一个或多个连续 `StateOperationBatch` 与对应 `RuntimeLog` 封装为 canonical
`RuntimeEvent`。App 只通过可选 `RuntimeEventSink.append()` 按 Session 顺序导出；
`InvocationResult` 不暴露 RuntimeEvent、StateOperation 或事件游标。Host 负责可靠
保存、去重、构建 Checkpoint 和 UI Trace。sink 返回表示持久接纳；抛错后 Core 保留
未确认 Event 并可能用相同 Event id 重试，所以 sink 必须幂等。独立 Session 可并发导出，
而 sink 延迟会对对应 Runtime 形成背压。

## 六、Checkpoint 与恢复

`RuntimeCheckpointBundle` 包含一个 Root Session，以及从它可达且已经打开
Invocation 的 Child `RuntimeState`。若 Child 只有 `SessionOpened` 而尚未
`InvocationOpened`，Bundle 省略该不完整 State；父 plan 保持 `planned`，恢复时据此
重建 Child。Bundle 校验 Root 唯一、父子身份一致、无环、无孤儿，并带 canonical 摘要。
`AppCheckpoint` 是多个互不重叠 Root Bundle 的集合。

所有公开返回点和流式中间点都复用同一套 Journal capture 方法：先 flush Root/Child
图的 pending Batch，导出形成的 RuntimeEvent，再构造 Checkpoint。

- `invoke/ainvoke`、`join/ajoin`、`resume/aresume`、`cancel/acancel`、`recover/arecover` 在返回边界携带 Checkpoint；
- submit 在可靠接纳后携带 Checkpoint；
- `close/aclose` 在停止进程内任务后返回所有 Root 的 AppCheckpoint；
- `stream/astream` 和 `stream_resume/astream_resume` 仅在可恢复转换后给 `InvocationUpdate.checkpoint` 赋值，最终
  `InvocationResult` 总有最新 Checkpoint。

最终 `InvocationResult` 的交付本身就是流的正常终止握手；调用方无需再拉取 END。此后
spawn Child 已脱离父流背压，即使调用方立即 break/close，也不会取消 Child 的后续提交。
waiting Result 尚未从 attached stream 交付前，Resume 会被拒绝；交付后才能开始下一段
执行，防止旧 Result 与新 running State 交叉。外部 Cancel 后仍会交付 cancelled Result。

流式安全边界包括 Scheduler 初始化、Node started/waiting/终态、Wait resume、Recovery、
Child 计划/阶段/await 边界和 Invocation waiting/终态。Node started 是 Input Mapping、
Capability 选择和 Operator 副作用之前的 write-ahead 边界；OperatorCall started 和
Stream Chunk 不额外携带 Checkpoint，Map 仍作为一个 NodeOccurrence 恢复。严格
caller-driven 背压保证调用方未请求下一条更新时 Runtime 不越过下一次握手，因此收到的
安全 Checkpoint 确实对应当时已提交并已导出的状态。

新 App 必须先注册 Checkpoint 引用的精确 Workflow Revision，再调用
`load_checkpoint`。未完成 Invocation 随后调用 `recover`；waiting Invocation 继续使用
`resume`。运行中 Node 默认 `Recovery(mode="never")`，只有显式 `replay_safe` 且未超出
`max_attempts` 才能重放整次 Node（包括 Hook）；同一次正常 live NodeOccurrence 的
Binding/Condition 与终态提交串行完成，不因并行 Context 竞争而隐式重试 Hook。

## 七、Child Workflow

Workflow 作为 Node executable 时创建独立 Child Invocation 和独立 Session Context：

| Map | `execution_mode` | Node 完成条件 | Node Output |
| --- | --- | --- | --- |
| 无 | `await` | Child 到终态 | Child Output |
| 无 | `spawn` | Child 被可靠接纳 | `ChildInvocationHandle` |
| 有 | `await` | 全部 Child 收敛 | 有序 Output 列表或聚合结果 |
| 有 | `spawn` | 全部 Child 被可靠接纳 | 有序 Handle 列表或聚合结果 |

Handle 包含 Child Session、Invocation、Workflow 和精确 Revision。父状态保存稳定的
Child plan/creation id；Checkpoint 始终以整个父子图为单位。await Map 的任一 Child
失败时先取消并收敛兄弟，再让父 Node 失败。App 只提供根据 Parent 查询 Handle 的
`child_handles/achild_handles`；Handle 可直接传给通用的 status、join、resume、cancel、
recover 和 stream_resume API。父子消息与远程执行不在当前 Core。恢复整个图时，await Child 继续服从父 Node 的等待
边界；spawn Child 只需可靠重启其受管 drive，Root Result 不等待它到终态，之后仍可通过
Handle 观察或 join。若精确恢复目标就是 spawn 子树中的 Child，则沿目标祖先路径等待该
Child 的终态或 waiting 边界，其他 spawn 旁支仍保持 detached。
恢复会把已经存在完整 Child Invocation 的 `opened` unit 先推进到 `accepted`，包括
Child 已经 waiting 的情况；Child 终态随后仍能可靠推进父 plan 到 `terminal`。

## 八、Revision、Context 与失败

Revision/`WorkflowDefinitionSnapshot` 来自同一份规范化定义。用户 Hook 只记录短名称、
声明 contract 和 `@workflow_hook(version=...)`，不记录 module 或文件路径；Snapshot
是 JSON 兼容定义，不含 live callable。

Workflow IR closure、Snapshot 与 Capability contract 先完整预检，再原子注册；失败不会
残留新 Revision、改变 latest 或部分绑定 Capability。App 的注册/查询 facade 与 close
线性化。`operator_registry` property 仅是高级注入/检查面；调用方若绕过 facade 直接修改
该 Port，需要自行负责与 App 生命周期的并发协调。

`ContextPatch` 是唯一 Context 写入形式。Node 成功 Batch 原子提交 Output、Patch 和
Scheduler 后继；并行写用起始 state version 与路径版本检测重叠冲突。Condition 读取
已提交 Context 加当前 Node 待提交 Patch 的候选视图。

Workflow 失败模式：

- `fail_fast`：未被 error edge 处理的失败立即取消活动兄弟；
- `continue_active_branches`：已激活兄弟先收敛，再确定 Invocation 终态。

App 的 `max_node_executions_per_invocation` 是防止错误 Loop 无限执行的全局保护，不是
Node authoring 字段。

## 九、Core 边界验收

- Core 产生且 Host 已接纳的任意 sealed RuntimeEvent 前缀，都可由同一个 Reducer
  快速重建状态；不可信 Event 的逐边界验收使用 `apply` / `apply_batch`；
- SDK 结果不泄漏 canonical StateOperation；
- RuntimeEvent sink 按 Session 顺序接收，并能按 Event id 幂等处理失败重试；
- Stream 中间 Checkpoint 可在新 App 恢复剩余工作，不重放已完成 Node；
- Child Root/Session 图能整体序列化、加载和恢复；
- 同一个 Session 不并行运行两个 Invocation；
- 全局 Operator 并发同时覆盖同步、异步、Map 和 Child Map；
- 完整 unittest、compileall、benchmark import 和 whitespace 检查通过。
