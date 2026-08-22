# AutoAgent V2 待办

本文依据当前 V2 Core 源码、测试、`workflow.md`、`runtime.md`、最初的重构设计和后续脑暴结论整理。优先级表示进入持久化、Server 和 Harness 前的处理顺序，不代表立即实现。

当前基线：Core 使用静态 Workflow IR、受控 Loop、单次 NodeOccurrence、Node 内 Map、Child Invocation、StateOperationBatch、Full Runtime Event 和唯一 StateReducer；通用多发生、动态图和当前 Invocation 内的 Stream Edge 已明确不采用。

## P0：Core 正确性与基础模型

### 已完成：State Change 与 Runtime Event 的两层模型

当前已实现两层：

```text
StateOperationBatch
    细粒度、有序、原子地改变 RuntimeState，是 StateReducer 的输入。

RuntimeEvent
    UI、Trace、持久化和 Recovery 使用的外层记录；一个 Event 按配置的边界，携带上一个 Event 之后累计的多个 StateOperationBatch。
```

RuntimeEvent 不是一个原子状态转换；StateOperationBatch 才是最小原子提交。Event 需要记录 `from_state_version`、`to_state_version` 和有序 batches，使一个数据库记录内部仍可按 state version 重放中间状态。Full/Standard/Minimal 或其他 Capture 配置只决定 Event 边界与观察 Payload，不改变 StateReducer 和 RuntimeState 的正确性。

已经支持 Event 内按 state version 重放、pending/flush 分离、失败原子 Batch、
Runtime Log 到 state version 的绑定，以及 Event 前缀原子导入。Full 模式仍默认每次
转换立即 flush；Standard/Minimal 的具体 Capture Policy 和外部 Event Store 接纳
确认属于后续模块。尚未 flush 的 Batch 明确定义为进程崩溃恢复窗口。

已验收：调整 Event 记录间隔不改变最终 State；持久化重放不解释语义 Payload；
单个 Event 内可按 Batch/state version 重放；无效 Batch 不改变旧 State；恢复前缀
晚期冲突不会产生部分导入。

### 已完成：Child Workflow 与 Map 的组合

WorkflowExecutor 已在一个 NodeOccurrence 内统一处理 Child Map，并验证四种组合：

| Map | Child 模式 | Node 完成条件 | Node 输出 |
| --- | --- | --- | --- |
| 无 | `await` | Child 完成 | Child Output |
| 无 | `spawn` | Child 可靠创建 | ChildInvocationHandle |
| 有 | `await` | 全部 Child 完成 | 有序 Child Output 列表或 Aggregation 结果 |
| 有 | `spawn` | 全部 Child 可靠创建 | 有序 Handle 列表或 Aggregation 结果 |

当前已覆盖空 Map、并行上限、乱序完成后的稳定排序、await/spawn Aggregation、失败收敛和父取消。每个 Child 在返回 Handle 前已建立完整初始 Runtime State；跨 Session 崩溃原子性、幂等创建和恢复协调继续由下一项处理。

### 已完成：稳定 Revision 与 Portable WorkflowDefinitionSnapshot

Revision 与定义快照现在来自同一份规范化语义定义。用户方法记录短方法名、声明 contract 和可选 `@workflow_hook(version=...)`，不记录 module、文件路径或 import 位置；Operator 部署版本不改变 Workflow Revision。编译结果和 App Registry 均提供 JSON 兼容的 `WorkflowDefinitionSnapshot`，其中不保存 live callable。

### 3. 定义 Checkpoint Bundle 与 Child 的跨 Session 恢复边界

当前父侧先提交 `ChildInvocationLinked`，随后同步建立 Child Session/Invocation 初始状态，再提交进程内执行 Task；正常返回的 Handle 已可操作。但父 Link、Child 初始状态与执行接纳仍分属不同 Session Event 序列，进程崩溃可能截断在任一边界。

Core 不提供 `create_checkpoint()`，而是在 `invoke`、`wait`、`resume`、`stream`
到达固定边界时，把可序列化 Checkpoint 随结果返回。一个父 Invocation 可能关联多个
独立 Child RuntimeState，因此 Checkpoint 不能再只是单个 Session State，必须是一组具有
唯一 Root 的 State Bundle。

还需要确定一个可恢复的创建协议：

- 父 Link 与 Child 初始事件具有稳定 command/creation id；
- 重试创建必须幂等；
- 孤儿 Link、孤儿 Child 和部分 Map 创建都有确定恢复规则；
- `spawn` 只在达到“已被 Runtime 可靠接纳”的边界后返回 Handle；
- App 提供 `load_checkpoint`，先验证 Bundle 内全部 Workflow Revision 已注册，再原子加载；
- 未完成的父子 Invocation 图必须作为一组恢复，不能让父节点重跑并重复创建 Child；
- 已完成 Session 加载后可以作为同一 Session 下一次 Invocation 的起点；
- Checkpoint 是恢复加速边界，Runtime Event/StateOperation 仍是运行时变化的唯一事实来源。

验收测试：父、await Child、spawn Child 和 Child Map 在创建/等待/终态各边界序列化并
加载；不能产生不可操作 Handle、重复 Child、无归属 Child 或错误 Workflow Revision。

### 已确定：第一版不做 schema migration

当前 Event 与 RuntimeState 只接受精确 schema version；未知或旧版本稳定拒绝。迁移由
未来存储/Server 层在进入 Core 之前完成，不在当前 Core 引入 upcast 注册表。

### 已确定：一个 Session 最多一个活动 Invocation

RuntimeState 保存该 Session 当前或最近一次 Invocation；新 Invocation 只能替换已到终态的
旧 Invocation。长期并行父子任务使用独立 Child Session，不在同一 Session 内并行写
Context。历史 Invocation、Event 持久化和 Session 归档属于 Server/Event Store，不由
Core 常驻保存。

## P1：Core 接口与运行质量

### OperatorPolicy 暂停扩展并重新设计

当前 `operator_policy` 已包含 Retry、Fallback、Timeout、最大并发、调用次数和累计运行时间，但这些内容尚未经过当前最小 Core 设计确认。暂不继续增加行为；后续分别判断哪些属于 Operator Call 语义、Runtime/Executor 全局限制或外部 Harness 策略，再决定是否保留一个组合 Policy。

### 已完成：App 级 Operator 全局并发与 Map 有界调度

`max_operator_concurrency` 限制同一个 App 内、跨 Workflow、跨 Invocation、同步与异步
Operator 的物理并发总量。同步调用被逻辑取消后，只要线程仍在执行，就继续占用名额。
Operator Map 使用固定数量 Worker，不为每个 item 预先创建 Task；局部
`Map.max_parallelism` 只能进一步收紧，不能突破 App 上限。Child Workflow Map 也遵循
App 上限；spawn Map 为全部输入建立 Handle，但只允许受限数量的 Child 实际执行。

### 6. 建立 Runtime Command 基础契约

当前 resume/cancel 是 App 方法调用，还没有可持久化、可去重的 Command 身份。Server、Mailbox、父子通信和权限控制接入前需要最小 Command Envelope：

```text
command_id
target_session_id
target_invocation_id
command_type
payload
expected_sequence/version（可选）
```

同一个 command id 重试必须返回同一结果或稳定冲突；Command 的接纳、应用和拒绝需要可观察记录，但不能引入第二份权威 Runtime State。

验收测试：重复 Resume/Cancel、并发相反命令、过期 expected sequence、未知目标和进程恢复后重复投递。

### 7. 补齐 Mailbox、Signal 与父子通信

在 Command 基础上实现：

- Invocation Mailbox/Event Log 与单调 cursor；
- `SendSignal`、`AwaitSignal`，并保证“检查已有 Signal + 注册 Wait”原子化；
- Parent/Child message、question/reply、acknowledgement 和 cancel；
- Guidance、Permission、Tool/Capability Binding 的 Invocation 级更新；
- 长时间 Operator 只在安全边界协作读取消息，不能假装修改已发出的不可中断调用。

外部消息继续通过短 Handler Invocation 发送 Command，不直接激活当前 Invocation 的任意中间 Node。

验收测试：Signal 先到/后到、重复消息、cursor、父问子答/子问父答、权限拒绝、取消竞争和恢复后继续消费。

### 8. 明确后台执行的 Event 导出边界

同步 invoke 可以从 `InvocationResult` 取得增量 Event；submit 返回后继续产生的 Event 目前只能由调用方再次 wait/events 拉取。进入 Server 前需明确一种不丢后台 Event 的装配方式：

- Core 提供非阻塞 Event Observer/Publisher Port；或
- Server 注入只做内存接纳的 Journal/Queue，再异步写 Event Store。

无论选择哪种方式，都不能在 Runtime Loop 中执行阻塞数据库 I/O，也不能同时保存多份完整 RuntimeState。Event 是恢复真相，Snapshot 只做加速。

验收测试：submit 后调用方断开、慢消费者、队列满、关闭 flush、Event 顺序和 Reducer 重放等价。

### 10. 完善 App 与 Child Workflow 公共边界

- 决定 submit 返回 `InvocationResult` 还是轻量 InvocationRef；控制 API 应精确携带 invocation id。
- 用 Checkpoint Bundle 加载 API 替换当前只接受单 Session Event 前缀的临时
  `recover_events`；补齐同步/异步加载与恢复矩阵。
- Child Workflow 当前要求单入口、单出口；需要决定是否增加显式 child entry 选择，以及多出口结果是否返回 `exit_node_id + output`。若继续限制，应把它记录为有意边界。
- Attached Stream 留在 Server/SDK 还是 Core 调用层需要一次明确决定；它不能改变 StreamReducer 和 Canonical Event 语义。

验收测试：错误目标不会误操作最新 Invocation、多入口选择、多出口结果、同步/异步取消传播和 API 关闭行为一致。

### 11. 优化 RuntimeLoop 与 Executor 宿主适配

- App 构造目前立即创建线程；恢复最初“导入和空 App 不启动线程，首次执行再惰性创建”的目标。
- 保留当前线程 Executor 的协作取消语义；为需要硬终止、资源隔离或远程运行的 Operator 增加 Process/Remote Executor Port。
- close 必须区分 Core 自有资源与注入资源的所有权。

验收测试：空 App 零线程、首次执行单次初始化、无丢失唤醒、无常驻轮询、取消/关闭有界、同步函数逃逸可观测、注入资源不被误关。

### 12. 收紧 Port、类型与静态质量门禁

- App 已定义 Port，但 WorkflowExecutor 仍依赖部分具体类型并存在 `type: ignore`；将共享 Protocol 放到中立依赖层，避免 App 反向成为 Executor 的接口定义来源。
- 为 RuntimeJournal、NodeExecutor、Scheduler、OperatorRegistry 和 Clock 建立静态类型验收。
- 增加 Ruff 与 Pyright/Mypy 配置，并把测试 docstring、公开 import 和禁止依赖检查加入自动门禁。

验收：无非必要 `type: ignore`，Core 类型检查、lint、compileall、完整 unittest 和 import 边界测试全部通过。

## P2：已规划的后续模块

### 13. Operator 与 Task Runtime 扩展

- 外部副作用稳定幂等键；
- 更丰富的 Capability 选择与 Invocation 级 Tool Registry；
- 可插拔配额服务；
- 远程 Child Workflow 和远程 Executor；
- Task status/await/cancel/message 的统一协议。

### 14. Event Store 与捕获模式

- Append-only Runtime Event Store、UserEvent Store、事务 Outbox；
- 可选 Snapshot、压缩、归档，以及进入 Core 前的 schema migration；
- Full/Standard/Minimal 只改变观测保留或投影，不删除恢复所需的 Canonical Transition；
- 三种模式必须通过最终 RuntimeState、Resume 和 Recovery 等价性测试。

### 15. Server 与执行 API

- Invoke/Submit/Wait/Resume/Cancel/Recover；
- Event/SSE 流、Session admission、背压、健康状态和优雅关闭；
- Handler Workflow 路由、Command 鉴权、幂等和限流。

### 16. Agent SDK 与 Coding Agent Skill

- LLM Operator、Tool Dispatch、动态工具描述与权限；
- 静态 Agent Loop 模板和结构化 Command；
- Workflow 生成、编译诊断和修复 Skill；
- Harness 通过 Task Runtime、Mailbox、Capability 和 Command 组合，不热修改运行中的 Workflow IR。

### 17. Trace、Debug、Replay/Fork 与 UI

- Timeline、任意 sequence State、Event 因果链；
- Fork seed、差异比较、Evaluation；
- Trace Projection 和 UI 不反向写 Core 权威状态。

## 明确不是待补功能

以下内容已被后续设计替代，不应按“最初文档有、当前没有”直接恢复：

- 通用 Node 多发生、ActivationGroup、跨 Trigger Join 和动态修改运行中 Workflow；
- Map item 沿普通 Edge 扩散；Map 仍是一次 NodeOccurrence；
- 当前 Invocation 内由 Stream Chunk 反复激活普通 Node；Chunk 目前只形成 UserEvent；
- 独立完整 RecoveryCheckpoint；Canonical Runtime Event + StateReducer 是恢复真相；
- RuntimeEvent 内保存完整 State；RuntimeEvent 只保存增量 StateOperationBatch；
- dataclass、任意对象和结构兼容的数据契约；当前只接受名义 TypedDict、受限 Pydantic Model 或 None；
- 为兼容 V1 恢复旧 Policy、Sink、Checkpoint 或 API 数据模型。

## 每次实施后的统一验收

```bash
cd autoagent_v2
../.venv/bin/python -m unittest discover -s tests -v
../.venv/bin/python -m compileall -q autoagent tests
git diff --check
```

每个新增 `test_*` 方法第一行必须有一句简短 docstring，说明它验证的行为或失败边界。
