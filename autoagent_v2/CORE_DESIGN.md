# AutoAgent V2 Core 结构设计

本文定义 V2 Core 的目标结构。Core 只负责 Workflow 的定义、编译与执行，
以及产生可重放的 Runtime Event 和非状态性的 User Event；不负责数据库、
消息发送、HTTP Server 或 UI。

## 一、设计原则

1. Runtime State 的每次变化只能由原子 `StateOperationBatch` 经
   `StateReducer` 产生；Runtime Event 只是可调节粒度的持久化信封。
2. Scheduler 只做确定性的图规划，不执行 Operator，也不写 Runtime State。
3. WorkflowExecutor 只驱动一次 Invocation；NodeExecutor 只执行一次
   NodeOccurrence。
4. AutoAgentApp 是公共门面，不包含节点执行、路由或父子任务算法。
5. TaskRuntime 管理进程内 Task，并通过 Runtime Event 保存父子 Invocation
   关系；可持久化实现以后可以替换它。
6. 依赖始终从上层指向下层，不允许 Scheduler、Runtime 或 Workflow
   反向依赖 App。

## 二、模块结构

```text
autoagent/core/
├── app/
│   ├── app.py                 # 公共 API、组件装配、同步/异步入口
│   ├── ports.py               # Core 外部依赖的最小 Protocol
│   └── runtime_loop.py        # 稳定事件循环线程与宿主唤醒适配
├── compiler/
│   ├── compiler.py            # Workflow -> WorkflowIR，一次编译管线
│   ├── diagnostic.py
│   └── snapshot.py            # Portable WorkflowDefinitionSnapshot
├── executor/
│   ├── node_executor.py       # 单个 NodeOccurrence 的瞬态执行
│   ├── workflow_executor.py   # 单个 Invocation 的控制循环
│   ├── future.py              # concurrent Future 的取消桥接
│   └── result.py
├── operators/
│   ├── operator.py
│   ├── contract.py
│   └── registry.py            # Capability 实现的唯一来源
├── runtime/
│   ├── events.py              # Runtime Event schema 与 codec
│   ├── operations.py          # StateOperation 与原子 Batch
│   ├── journal.py             # 默认内存 Journal
│   ├── reducer.py             # 唯一 Runtime State Reducer
│   ├── state.py               # 完整可恢复状态
│   ├── task_runtime.py        # Task、父子 Invocation、唤醒与取消
│   ├── user_events.py
│   └── values.py
├── scheduler/
│   ├── scheduler.py           # DAG 调度
│   └── loop_scheduler.py      # 受限 Loop Scope 调度
└── workflow/
    └── models.py              # Workflow 静态定义与 IR

core/context.py                # Workflow 与 Runtime 共享的 Context 原语
```

## 三、依赖关系

```text
AutoAgentApp
  ├── WorkflowCompiler
  ├── WorkflowExecutor
  │     ├── Scheduler
  │     ├── NodeExecutor
  │     ├── TaskRuntime
  │     ├── RuntimeJournalPort
  │     └── UserEventJournalPort
  ├── OperatorRegistry
  └── TaskRuntime

WorkflowExecutor -> Runtime / WorkflowIR
NodeExecutor     -> Operator / WorkflowIR 中的节点执行定义
Scheduler        -> Runtime State / WorkflowIR
StateReducer     -> StateOperationBatch / Runtime State
```

禁止的依赖：

```text
Runtime  -> App
Scheduler -> App 或 Executor
NodeExecutor -> Journal、StateReducer 或 AutoAgentApp
TaskRuntime -> WorkflowExecutor 的具体类
```

## 四、两个 Executor

### WorkflowExecutor

WorkflowExecutor 是一次 Invocation 的唯一控制循环，负责：

- 从 Scheduler 取得 ready NodeOccurrence；
- 提交并跟踪多个并行 NodeOccurrence；
- 将开始、完成、失败、等待和路由结果提交为 Runtime Event；
- 执行 Input Mapping、Output Binding 和 Condition 的编排；
- 根据 Workflow 的失败模式决定 fail-fast 或等待活动分支；
- 判断 Invocation 的 completed、waiting、failed 状态；
- 调用 TaskRuntime 执行 await/spawn 子 Workflow。

它不负责：编译、线程池实现、数据库、公开同步 API。

### NodeExecutor

NodeExecutor 负责一个 NodeOccurrence 内部的瞬态工作：

- 调用同步或异步 Operator；
- Map 并行、顺序保持与聚合；
- Retry、Fallback、Timeout；
- Stream 消费和 StreamReducer；
- 调用用户 Hook；
- 产生 OperatorCall 生命周期通知。

NodeExecutor 不读取或修改 Runtime State。同步函数通过延迟创建的有界 Worker
执行；Worker 排空当前工作队列后退出，不等待或轮询下一批任务。Future 到
asyncio 的桥接使用每个 Event Loop 共享的非阻塞 pipe：Worker 完成时写入通知，
Event Loop 收到可读事件后完成对应 Future。等待过程不使用定时轮询，并发等待也不
会为每个 Future 创建独立通知通道。

同一个 App 的 `max_operator_concurrency` 是跨 Workflow、跨 Invocation 的物理
Operator 全局上限，同时统计同步、异步和 Stream Operator。Map 使用固定数量 Worker
按索引领取输入，`Map.max_parallelism` 只能收紧该上限。同步调用被逻辑取消后，如果
用户函数仍在线程中运行，它会继续占用全局名额，直到物理调用退出。

Python 无法强行终止已经进入用户同步函数的线程，因此取消只保证：取消等待、
停止尚未开始的工作、Runtime 状态收敛，并明确记录可能仍在退出的同步调用。

## 五、App 与 Runtime Loop

AutoAgentApp 只保留：

- Workflow 注册、Revision 查找；
- Operator/Capability 注册；
- `invoke/ainvoke/submit/wait/resume/cancel/recover` 公共 API；
- 严格调用方背压的 `stream/astream`；
- Runtime 组件装配和统一关闭。

同步 API 使用一个 App 私有事件循环线程。同步和异步入口都通过 RuntimeLoop 的
显式提交队列进入该线程，异步 API 使用统一的可取消 Future Bridge 等待结果；
调用者取消时必须把取消传递给 Runtime Loop 内的 Invocation Task。正常宿主不需要
轮询；RuntimeLoop 使用非阻塞 pipe 唤醒 selector，避免依赖嵌入宿主可能限制的
跨线程 socket send 和 `call_soon_threadsafe`。Scheduler、WorkflowExecutor、Worker
与 RuntimeLoop 均不执行周期轮询。

## 六、父子 Invocation

```text
父 NodeOccurrence
  └── ChildInvocationLinked Runtime Event
        └── ChildInvocationHandle
              ├── session_id
              ├── invocation_id
              ├── workflow_id
              └── workflow_revision_id
```

- `await`：创建子 Invocation 后等待终态；子任务 waiting 时父节点保持运行，
  收到 resume/cancel 后通过 TaskRuntime 的 Event 唤醒。
- `spawn`：创建子 Invocation 并立即完成父 NodeOccurrence，输出 Handle。
- Handle 的合法性由 Journal 中的子 Session/Invocation 状态验证，不依赖 App
  内存字典。
- 父 Invocation 保存 Child Link，因此重放父事件仍能恢复父子关系。
- TaskRuntime 只保存当前进程中的 Task 和 Event；Task 完成后立即移除，避免泄漏。

Child Workflow 与 Node Map 采用同一个 NodeOccurrence 边界：

| Map | execution_mode | Node 完成条件 | Node Output |
| --- | --- | --- | --- |
| 无 | `await` | Child 完成 | Child Output |
| 无 | `spawn` | Child 初始状态完整建立 | ChildInvocationHandle |
| 有 | `await` | 所有 Child 终态 | 有序 Output 列表或 Aggregation |
| 有 | `spawn` | 所有 Child 初始状态完整建立 | 有序 Handle 列表或 Aggregation |

`Map.max_parallelism` 同时限制活跃 Child 执行；排队 Child 已具有可操作 Handle 和完整初始 Runtime State。任一 awaited Child 失败或父任务取消时，兄弟 Child 必须全部收敛后父 Node 才进入终态。

## 七、Workflow Revision 与定义快照

Revision 不记录 Python module、文件路径或 import 位置。用户方法只记录短方法名、声明 contract 和 `@workflow_hook(version=...)`；实现语义变化由用户提升 hook version。Operator 的部署版本与 Workflow Revision 分离。

编译器从同一份规范化语义定义同时计算 `definition_hash` 并生成 `WorkflowDefinitionSnapshot`，避免 Revision 与展示/恢复快照漂移。Snapshot 只含 JSON 兼容的图、契约、Hook 身份与策略，不含 live callable；App 按 Workflow id 或 Revision id 保留并提供快照。

## 八、Context 更新

ContextPatch 是 Runtime 原语，不属于 Workflow 定义层。Reducer 对每条路径执行
不可变 path-copy：只复制从根到被修改值的映射节点，未修改分支保持对象身份。

Condition 读取的是：

```text
已提交 Context + 当前 Node 的待提交 ContextPatch
```

候选视图也通过 path-copy 生成，不复制和冻结完整 Context；它只用于 Condition。
Node 完成 Batch 提交时，Reducer 对规范 Runtime State 应用正式 Patch。

## 九、Capability 与 Operator

Capability 只定义能力标识和名义输入输出契约，不保存实现列表。
OperatorRegistry 是运行时实现的唯一来源：

```text
Capability.id -> OperatorRegistration[]
```

默认实现、优先级、启用状态和动态加载均在同一个候选集合中处理。Retry/Fallback
作用于选定实现；Capability Resolver 只能从 Registry 当前候选中选择。

## 十、失败与资源限制

Workflow 提供两种失败模式：

| 模式 | 行为 |
| --- | --- |
| `fail_fast` | 未被 error edge 处理的节点失败后，立即终止 Invocation 并取消兄弟工作 |
| `continue_active_branches` | 已经激活的兄弟分支继续，全部静止后再决定 Invocation 终态 |

运行预算分为：

- 单次调用 Timeout：强制等待上限；
- Invocation 累计 Operator Call 数；
- Invocation 累计 Operator Runtime：每次调用前计算剩余额度，并将剩余时间作为
  当前 NodeOccurrence 执行的强制 Timeout。

## 十一、State Operation、Event 与返回游标

一次运行操作先产生不可变 candidate State，再生成最小结构差异：

```text
Runtime 操作
  -> StateOperation[]
  -> StateOperationBatch(from_version, to_version)
  -> StateReducer
  -> RuntimeState@to_version
```

一个 Batch 是原子提交边界。多个 Batch 可以先进入 pending buffer，再由一次
`flush()` 封装进一个 Runtime Event：

```text
RuntimeEvent(
    from_state_version=10,
    to_state_version=14,
    operation_batches=[10->11, 11->12, 12->13, 13->14],
    logs=[...],
)
```

因此 Event 数量和状态变更数量不必相等。Event 负责 UI、Trace、持久化和恢复；
StateOperationBatch 才是 Reducer 的输入。`RuntimeLog.state_version` 把语义观察点
绑定到对应状态版本。User Event 是非权威观察输出，不参与状态重建。
两者都使用显式游标：

```python
InvocationResult(
    ...,
    events=runtime_events_after_cursor,
    user_events=user_events_after_cursor,
    next_event_cursor=...,
    next_user_event_cursor=...,
)
```

`RuntimeEvent.causation_id` 记录导致当前信封的上一个 Runtime Event，信封内的
Runtime Log 保留各自身份、因果和状态版本，
便于追踪因果链。Core Journal 是立即返回的进程内状态日志，不允许在 Runtime Loop
内执行阻塞数据库操作；外部持久化模块消费 App 返回的增量 Event，Core 不包含
数据库或消息发送实现。

Runtime Event 游标属于 Session 全局序列，而不是单个 Invocation 的局部序列。首次
调用返回的 Event 必须包含 `SessionOpened`；后续 Invocation 从上一次游标继续，因此
外部模块合并这些批次即可得到完整且连续的可重放日志。

## 十二、Compiler 与 Codec

- Compiler 的成功路径只编译一次；失败后才进入独立 diagnostics 收集路径，存在
  error 时不产生 IR。
- Runtime Event codec 使用严格字段检查，不静默执行 `str(value)`、`bool(value)`
  等宽松转换。
- RuntimeErrorInfo 包含稳定 code、phase、retryable 和 cause；展示文本不作为机器
  判断依据。

## 十三、验收标准

1. 一个空闲 App 只创建 Runtime Loop 线程，不预创建 Operator Worker，也不注册周期
   heartbeat。
2. `ainvoke()` 被取消后，Invocation 最终为 cancelled，异步 Operator 收到取消。
3. 任意 Runtime Event 前缀都能重建完全一致的 Runtime State；一个 Event 内还可
   按 state version 重建每个 Batch 之后的状态。
4. 小 ContextPatch 不复制未修改的大分支。
5. 新 App 使用同一 Journal 时能校验和操作已产生的 Child Handle。
6. `fail_fast` 和 `continue_active_branches` 都有并行分支测试。
7. Capability 的所有候选只来自 Registry，动态候选能够参与统一选择。
8. Compiler 对一个 Node 只执行一次编译。
9. 已有 Session 再传 `session_context` 明确报错，不静默忽略。
10. Runtime Event 和 User Event 都能从调用者指定游标增量读取。
11. 完整 V2 单元测试、编译检查和性能基准通过。

测试分层、覆盖矩阵及方法说明规则见 `TESTING.md`。
