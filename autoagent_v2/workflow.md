# Workflow 静态模型

V2 Workflow 是静态有向图。Runtime 只执行 Compiler 生成的不可变 `WorkflowIR`，不读取
可变 Authoring 对象。

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
    input_mapping: InputMapping | None = None,
    output_binding: OutputBinding | None = None,
    execution_mode: "await" | "spawn" = "await",  # 仅 Child Workflow 生效
    map: Map | None = None,
    stream: Stream | None = None,
    user_events: tuple[UserEventMapping, ...] = (),
    recovery_mode: Recovery = Recovery(mode="never", max_attempts=1),
)

Edge(
    source: str,             # Source Node id
    target: str,             # Target Node id
    condition: Condition | None = None,
    on: "complete" | "error" = "complete",
    id: str | None = None,
)

Map(
    aggregate: Aggregation | None = None,
    max_parallelism: int | None = None,
)
```

## 契约与 Hook

Operator 只接受零个或一个业务输入。Operator 输入/输出、Wait 请求/响应和需要业务值
契约的 Hook 输出只接受模块级 `TypedDict`、受限 Pydantic Model 或 `None`；契约采用
名义匹配，不按字段结构猜测兼容。Condition 必须返回 `bool`，Output Binding 必须返回
`ContextPatch | None`，其他 Hook 也由 Compiler 检查精确签名。

用户调用 Operator/Resume 时传入严格的 Python domain value。值进入 RuntimeState 前只在
一个边界序列化成 canonical JSON record，例如 Enum 变为其值、tuple 变为 list；隐式
Edge、Wait 继续执行和 Child 边界会按契约恢复 domain value 后再调用 executable。这样
运行状态可持久化，同时不会用宽松类型强转掩盖用户输入错误。

所有 Hook 可同步或异步，并接收只读 Context：

- `InputMappingContext`：durable Invocation 输入、按 Edge id 索引的已选中 incoming、两层 Context；
- `OutputBindingContext`：只读 durable Node Output 和两层 Context；
- `ConditionContext`：Source Node、只读 durable Output/Error 和两层 Context；
- `AggregationContext`：有序 Map inputs/outputs 和两层 Context；
- `StreamContext`：当前 Node input 和两层 Context。

`OutputBinding` 只能返回 `ContextPatch | None`，不能直接修改 Runtime Context。多入边、
Error Edge 的目标以及类型无法直连的节点必须提供 Input Mapping。`incoming` 只包含实际
选中的 Activation；未选中边由 Scheduler 决议，不伪造业务值。

## Edge、分支与 Join

- 同一 Source/Target 之间最多一条 Edge；Complete 与 Error 路由不能重复指向同一 Target。
- Source 终态后，状态匹配且 Condition 为真的出边全部选中，即 all-match。
- Target 等待当前 ExecutionScope 内全部预期入边决议；至少一条选中才执行，全部未选中
  则跳过并继续传播不可达状态。
- 多入口/多出口保留在 IR；调用多入口 Workflow 时必须提供 `entry_node_id`。

## Map

Map 始终是一次 NodeOccurrence：

1. Input Mapping 返回 `list[ExecutableInput]`；
2. Runtime 有界并行执行每个 item；
3. 结果按输入索引稳定排序；
4. 没有 Aggregation 时 Node Output 是结果列表，有 Aggregation 时为其返回值；
5. 任一单元失败时，先取消并收敛同一 Map 已开始的工作，再结束 NodeOccurrence。

`Map.max_parallelism` 是局部上限，不能超过 App 的 `max_operator_concurrency`。Map item
不会沿普通 Edge 独立扩散，因此连续 Map 不引入通用 Node 多发生。

## Stream

设置 `Stream(reducer=...)` 后，Operator 必须返回同步或异步迭代器。每个 Chunk 产生
非 canonical 的 `UserEvent(kind="stream.chunk")`；StreamReducer 将全部 Chunk 归约成
一个 Node Output，Node 仍只完成一次。当前没有 Stream Edge，Chunk 不激活下游 Node，
也不是 Checkpoint 边界。

## Loop

Compiler 从自然 Loop 生成 `LoopRegionIR`，每轮使用独立 `ExecutionScope` 和
NodeOccurrence。Back/Exit 决议会等待当轮活动分支收敛；同一轮不能同时 Continue 与
Exit。当前支持 Self Loop、嵌套 Loop、共享 Header 的合法结构，并拒绝不可归约图、
多 Back Edge 和无出口 Loop。

## SubWorkflow 与 Child Workflow

- `SubWorkflow` 只做编译期带命名空间展开，共享父 Invocation 与 Context。
- Node 的 executable 为 Workflow 时，创建独立 Child Session/Invocation。
- `execution_mode="await"` 等待 Child 终态并返回 Child Output。
- `execution_mode="spawn"` 在 Child 被可靠接纳后返回可持久化
  `ChildInvocationHandle`。
- Child Workflow 可与 Map 组合，输出为有序 Child Output 或 Handle 列表，也可再聚合。

当前 Child Workflow 要求边界无歧义；父子 Context 不共享。App 可查询、等待或取消
Handle，父子消息和远程 Child 不在当前静态模型。

## Capability 与 Revision

Capability 只声明 id 和名义 contract。实现来自 App 的 Operator Registry；选择顺序为
显式 default、唯一候选、唯一最高优先级，仍有歧义时交给 CapabilityResolver。Resolver
不能返回 Registry 之外的 Operator，动态实现也必须保持 Capability 的名义 contract。

Revision 不记录 Python module、文件路径或 import 位置。用户方法的短名称、声明
contract 和 `@workflow_hook(version="...")` 参与 Revision；语义变化时由用户提升 Hook
version。Compiler 同时生成 JSON 兼容且不含 callable 的
`WorkflowDefinitionSnapshot`。

## Crash Recovery

`Recovery` 只控制进程崩溃后是否允许重放整个运行中 NodeOccurrence，与当前不存在的
调用级重试无关。默认 `never`；只有显式 `replay_safe` 且恢复次数不超过
`max_attempts` 才重新进入 ready。整次重放包含 Input Mapping、Operator、Output Binding
和 Condition；正常 live 执行不会因并行 Context 提交而隐式重试 Binding 或 Condition。
