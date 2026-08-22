# Workflow 静态模型

V2 Workflow 是静态有向图。Runtime 只执行编译后的 `WorkflowIR`，不读取可变的 Authoring 对象。

```python
Workflow(
    id: str,
    nodes: list[Node],
    edges: list[Edge],
    sub_workflows: list[SubWorkflow],  # 编译时展开
    version: str | int = "1",
    failure_mode: "fail_fast" | "continue_active_branches" = "fail_fast",
)

Node(
    id: str,
    executable: callable | Operator | Capability | Wait | Workflow,
    input_mapping: InputMapping | None,
    output_binding: OutputBinding | None,
    execution_mode: "await" | "spawn" = "await",  # 仅 Workflow 生效
    map: Map | None,
    stream: Stream | None,
    user_events: tuple[UserEventMapping, ...],
    operator_policy: OperatorPolicy | None,
    recovery_mode: Recovery = Recovery(mode="never"),
    max_occurrences_per_invocation: int | None,
)

Edge(
    source: str,
    target: str,
    condition: Condition | None,
    on: "complete" | "error" = "complete",
)
```

## 数据契约

Operator 只接受零个或一个业务输入。输入、输出、Wait 请求/响应只允许模块级 `TypedDict`、受限 Pydantic Model 或 `None`。契约采用名义匹配，并在进入 Runtime Event 前转换成可持久化记录。

Node 默认接收唯一选中 Activation 的输出。多入边必须提供 `InputMapping`；`incoming` 以 Edge id 为键，只包含实际选中的 Activation。`OutputBinding` 只能返回 `ContextPatch`，不能直接修改权威 Context。

## Map 与 Stream

Map 是一次 NodeOccurrence。`InputMapping` 返回 `list[ExecutableInput]`，Runtime 有界并行执行物理 OperatorCall 或 Child Invocation，结果保持输入顺序；Operator Map 使用固定数量 Worker 按索引领取 item，不为每个 item 预建 Task。可选 Aggregation 生成一个 Node Output。`Map.max_parallelism` 是局部上限，不能突破 App 的 `max_operator_concurrency`。Child Workflow 支持 `map + await` 和 `map + spawn`：前者聚合有序 Child Output，后者为全部输入建立有序 ChildInvocationHandle，但只允许受限数量 Child 实际执行；空 Map 不创建 Child。

Stream Operator 返回同步或异步迭代器。StreamReducer 将 Chunk 归约成一个 Node Output；每个 Chunk 同时产生独立 UserEvent，不进入 RuntimeState。

## Operator 执行边界

`OperatorPolicy` 只作用于实际 Operator Call，包含 Retry、Backoff、Timeout、Fallback、最大并发、Invocation 内最大 Call 数和累计运行时间。Node 的 `max_occurrences_per_invocation` 独立限制 Loop 中的逻辑发生次数。Map 的每个单元及每次 Retry/Fallback 都是独立物理 Call；一个单元失败时，其余 Map Task 必须取消并收敛后，NodeOccurrence 才能进入终态。

Capability 的静态定义只给出能力 id 和名义契约，不保存实现。Operator Registry 是实现的唯一来源，可以注册、禁用实现并设置默认实现或优先级，但所有实现必须保持相同名义契约；Registry 变化不修改 Workflow Revision。选择顺序为显式 default、唯一候选、唯一最高优先级；仍有歧义时必须提供 CapabilityResolver。

## 分支、Join 与 Loop

- 出边采用 all-match：状态匹配且 Condition 为真的 Edge 全部选中。
- Node 等待同一 ExecutionScope 的全部预期入边决议；至少一条选中则执行，全部未选中则跳过。
- 自然 Loop 在编译期形成 `LoopRegionIR`。每轮使用独立 `ExecutionScope` 和 NodeOccurrence。
- Back/Exit 决议先进入 Loop 边界状态；当本轮所有并行工作终态后再原子提交。
- 同一轮不能同时选择 Continue 与 Exit。支持 Self Loop、嵌套 Loop、同 Header 嵌套和互斥 sibling Loop。

## 子 Workflow

- `SubWorkflow` 只做编译时结构展开，共享父 Invocation 和 Context。
- Node 的 executable 为 Workflow 时创建独立子 Invocation。
- `await` 等待子 Invocation 终态并返回结果。
- `spawn` 立即返回可持久化 ChildInvocationHandle，Handle 包含 child session、Invocation 和精确 Workflow Revision。
- App 已提供 Child status、await 和 cancel；父子消息、远程执行仍属于后续 Task Runtime。

## Revision 与可移植定义快照

用户自定义方法通过方法名、声明的参数/返回 contract 和可选的 `@workflow_hook(version="...")` 参与 Workflow Revision；Python module、文件路径和 import 位置不参与。方法实现发生语义变化时必须提升 hook version。Operator 的部署版本不产生新的 Workflow Revision。

编译成功同时生成 `WorkflowDefinitionSnapshot`。它是纯 JSON 兼容的静态图和执行语义记录，不包含可执行 callable；可用于历史图展示、Revision 兼容检查和后续恢复装配，但执行仍要求应用注册匹配代码。App 可按 Workflow id 或精确 Revision id 读取已注册快照。

## Crash Recovery

Recovery 与活进程内的 Retry 不同，它会重放整个 NodeOccurrence。Node 默认 `Recovery(mode="never")`，避免带副作用的 Operator 被隐式重复调用；只有显式标记 `replay_safe` 且未超过 `max_attempts` 的运行中 occurrence 才能在进程恢复后重新 Ready。
