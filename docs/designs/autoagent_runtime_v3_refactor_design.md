# AutoAgent Core Runtime V3 重构设计稿

> 面向 `lilislight/AutoAgent` 的 `refactor` 分支，对 `autoagent/core` 的 Runtime / Scheduler / Executor 进行 V2 → V3 重构。
>
> **设计基线**：`refactor` branch, commit `6140534cbc478d05ea7fced83f70be6a4ff19850`
>
> **目标读者**：后续直接负责修改代码的 Coding Agent。
>
> **范围**：优先修改 `autoagent/core`。其他模块只在编译或公开 API 必须适配时做最小兼容修改。

---

## 1. 重构目标

V2 已经解决了 V1 最严重的一部分问题：`RuntimeState` 已经是不可变状态，Scheduler 也已经基本变成纯 planner。但 V2 仍然存在一个核心结构性问题：同一件 Runtime 状态变化被重复表达。

当前大致链路：

```text
semantic payload
    ↓
StateReducer._transition()
    ↓
candidate RuntimeState
    ↓
diff_runtime_states(old, candidate)
    ↓
StateOperation[]
    ↓
StateOperationBatch
    ↓
RuntimeEvent
      ├── payload
      ├── RuntimeLog[]
      └── operation_batches[]
```

这会带来：

- RuntimeEvent 不再对应一个清晰的执行边界；
- state version、event sequence、runtime log 三套顺序同时存在；
- replay / checkpoint / fork 的边界不清晰；
- `diff_runtime_states()` 进入 hot path；
- Journal 需要 stage / flush / event group / live state / captured state 多套状态；
- Trace、Runtime correctness、Debug/Fork 被混在同一个 Event 模型中；
- 用户自定义 hook（input mapping / aggregate / output binding / condition）没有成为可寻址的 durable execution boundary。

V3 目标模型：

```text
RuntimeEvent
    = 一个有实际执行意义的 durable boundary
    = semantic payload
    + 0..1 atomic StateDelta

StateDelta
    = 该 boundary 对 RuntimeState 的原子影响
    = N 个有确定应用顺序的 StateOperation
    = Delta 内部没有 fork/checkpoint boundary

RuntimeState
    = RuntimeEvent 历史的 materialized projection

Checkpoint
    = 某个 RuntimeEvent sequence 上 RuntimeState 的稀疏 snapshot

Recovery
    = Checkpoint + RuntimeEvent replay + continuation

Fork
    = 从某个 RuntimeEvent boundary 派生新的 execution history

Trace
    = RuntimeEvent + StateDelta + WorkflowIR 的 UI projection
```

---

# 2. 必须锁定的核心不变量

## 2.1 RuntimeEvent 是唯一的 durable execution history

一个 Session 内：

```text
E1 → E2 → E3 → ... → En
```

`sequence` 是唯一 canonical execution ordering：

```python
event.sequence == previous.sequence + 1
```

不要再维护另一套和 Event 平行的：

```text
state_version
RuntimeLog.state_version
operation batch version
```

`RuntimeState` 只需要：

```python
sequence: int
```

含义：这个 RuntimeState 已经应用该 Session 的 RuntimeEvent history 到哪个 sequence。

## 2.2 一个 RuntimeEvent = 一个 durable semantic boundary

RuntimeEvent boundary 必须满足：

> Core 在该 Event 完成之后可以安全停止；之后可以 replay、checkpoint、recovery 或 fork，而不需要重新执行该 Event 之前已经成功完成且应当复用的用户计算。

有意义的例子：

```text
NodeStarted
InputMapped
OperatorCallStarted
OperatorCallCompleted
Aggregated
OutputBound
RoutingResolved
NodeCompleted
```

反例：

```text
B became ready
C became ready
D became ready
```

这些通常只是 Scheduler 对 `NodeCompleted(A)` 的一个 `StateDelta`，不值得变成独立 RuntimeEvent。

## 2.3 StateOperation 不是 fork boundary

`StateOperation` 是最小 state mutation representation，例如：

```text
replace occurrence[A].status = completed
replace occurrence[A].output = X
add occurrence[B] = ready
add occurrence[C] = ready
```

一个 `StateDelta` 可以包含多个 operation。

这些 operation：

- 为 replay 提供确定的应用顺序；
- 可以顺序执行以实现 deterministic replay；
- 但 operation 之间不形成 checkpoint / recovery / fork point；
- 整个 StateDelta 必须原子生效。

如果两个 mutation 之间真的值得暂停、恢复或 fork，那么它们应该属于两个 RuntimeEvent。

## 2.4 Scheduler 的 derived consequences 默认不是 RuntimeEvent

下面这些默认不形成独立 RuntimeEvent：

```text
NodeReady
NodeSkipped
EdgeSelected
EdgeRejected
JoinResolved
LoopAdvanced
LoopBoundaryClosed
OccurrenceRevived
```

它们属于 Scheduler 的 deterministic graph transition，可以进入上层 RuntimeEvent 的 `StateDelta`。

例如：

```text
NodeCompleted(A)

StateDelta:
    A.status = completed
    edge e1 = selected
    edge e2 = rejected
    B = ready
    C = skipped
    D = ready
```

Trace UI 可以把这一个 RuntimeEvent 展开成多条视觉变化。

## 2.5 RuntimeEvent 数量与数据库事务数量无关

B/C/D 同时 ready，Executor 几乎同时开始三个 Node：

```text
E52 NodeStarted(B)
E53 NodeStarted(C)
E54 NodeStarted(D)
```

逻辑上必须是三条 RuntimeEvent，因为可能出现：

```text
B started
C started
CRASH
D still ready
```

但物理存储可以：

```sql
BEGIN;
INSERT E52;
INSERT E53;
INSERT E54;
COMMIT;
```

原则：

> **Batch write events，不要 batch semantic events。**

性能优化发生在 EventStore 的 group commit / transaction batching 层，而不是通过合并 RuntimeEvent 改变语义粒度。

---

# 3. V3 核心对象模型

## 3.1 RuntimeEvent

建议把当前 `RuntimeEvent` 简化为：

```python
@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    id: str

    session_id: str
    invocation_id: str | None

    sequence: int

    payload: RuntimeEventPayload
    delta: StateDelta | None

    causation_id: str | None

    occurred_at_ns: int

    schema_version: int = RUNTIME_EVENT_SCHEMA_VERSION
```

第一阶段不必增加更多关联字段；如果未来 Trace 需要，可以再加 `correlation_id`。

### 从当前 V2 删除

```text
from_state_version
to_state_version
operation_batches
logs
RuntimeLog
previous_event_digest
semantic digest
```

`previous_event_id` 也不是 correctness 必需。如果以后确实需要 tamper-evident audit chain，应在 EventStore 层单独设计，不再和 Runtime correctness 耦合。

## 3.2 StateOperation

当前 `add / replace / remove + path` 可以继续作为第一版实现：

```python
@dataclass(frozen=True, slots=True)
class StateOperation:
    op: Literal["add", "replace", "remove"]
    path: tuple[str | int, ...]
    value: DurableValue = None
```

但是必须删除：

```python
diff_runtime_states()
```

StateOperation 不再是 candidate state 生成后反向 diff 出来的副产品，而是 Transition Planner 明确生成的正式结果。

## 3.3 StateDelta

将当前：

```python
StateOperationBatch
```

重构为：

```python
@dataclass(frozen=True, slots=True)
class StateDelta:
    operations: tuple[StateOperation, ...]
```

删除：

```text
from_state_version
to_state_version
occurred_at_ns
id
```

因为 version/order/time/identity 都属于 RuntimeEvent。

一个 RuntimeEvent 最多有一个 StateDelta：

```python
delta: StateDelta | None
```

不要继续允许：

```python
operation_batches: tuple[StateOperationBatch, ...]
```

否则一个 RuntimeEvent 又可能包多个可独立 state transition。

## 3.4 TransitionPlanner

建议新增：

```python
class TransitionPlanner:
    def plan(
        self,
        state: RuntimeState,
        payload: RuntimeEventPayload,
        *,
        occurred_at_ns: int,
    ) -> StateDelta | None:
        ...
```

职责只有：

> 根据当前 RuntimeState + semantic payload 显式生成 StateDelta。

例如：

```python
def plan_node_started(state, payload, occurred_at_ns) -> StateDelta:
    return StateDelta(
        operations=(
            replace_status(payload.occurrence_id, "running"),
            replace_started_at(payload.occurrence_id, occurred_at_ns),
        )
    )
```

不要：

```text
先构建完整 candidate RuntimeState
→ 再 diff
```

## 3.5 StateReducer

Reducer 只负责应用已确定的 StateDelta：

```python
class StateReducer:
    def apply(
        self,
        state: RuntimeState,
        event: RuntimeEvent,
    ) -> RuntimeState:
        self._validate_sequence(state, event)

        next_state = (
            apply_state_delta(state, event.delta)
            if event.delta is not None
            else state
        )

        return replace(
            next_state,
            sequence=event.sequence,
        )
```

Replay 绝对不重新执行：

```text
input_mapping
aggregate
output_binding
condition
Scheduler.plan
CapabilityResolver
Operator
LLM
random UUID
clock
```

---

# 4. RuntimeState V3

当前 Session / Invocation / Scheduler / Occurrence 的不可变 dataclass 基础结构可以继续保留。

## 4.1 RuntimeState

建议：

```python
@dataclass(frozen=True, slots=True)
class RuntimeState:
    session: SessionState | None = None
    invocation: InvocationState | None = None

    sequence: int = 0

    schema_version: int = RUNTIME_STATE_SCHEMA_VERSION
```

删除：

```text
state_version
last_event_digest
last_event_semantic_digest
```

`last_event_id` 可选保留用于 causation/debug，但不是 replay correctness 必需。

## 4.2 NodeOccurrenceState 增加 ready_at_ns

建议：

```python
ready_at_ns: int | None = None
started_at_ns: int | None = None
completed_at_ns: int | None = None

started_sequence: int | None = None
```

Scheduler 产生 ready occurrence 时：

```text
ready_at_ns = 当前 RuntimeEvent.occurred_at_ns
```

`NodeStarted` 时：

```text
started_at_ns = NodeStarted.occurred_at_ns
```

这样 Trace 可以展示 ready → start 的调度等待时间。

## 4.3 新增 NodeExecutionState

这是 V3 支持 fine-grained recovery / fork 的关键。

```python
@dataclass(frozen=True, slots=True)
class NodeExecutionState:
    phase: Literal[
        "none",
        "started",
        "input_mapped",
        "capability_resolved",
        "executing",
        "aggregated",
        "output_bound",
        "routing_resolved",
        "faulted",
    ] = "none"

    mapped_input: DurableValue = None

    resolved_capability_id: str | None = None
    resolved_operator_id: str | None = None

    aggregate_output: DurableValue = None

    pending_context_patch: ContextPatch = ContextPatch()

    routing: tuple["EdgeConditionResult", ...] = ()

    fault: RuntimeErrorInfo | None = None
```

`NodeOccurrenceState` 增加：

```python
execution: NodeExecutionState
```

### 为什么 intermediate result 必须进入 Runtime execution state

如果 `InputMapped` 后允许：

```text
checkpoint
crash
load checkpoint
continue from Operator
```

恢复时必须知道 mapped input。

如果 mapped input 只存在旧 RuntimeEvent payload，而 `SessionCheckpoint` 仍然是一个 standalone `RuntimeState` snapshot，那么单独加载 checkpoint 后没有足够信息继续。

因此必须二选一：

1. intermediate result/materialized value 进入 RuntimeState；
2. RuntimeState 持有 checkpoint 可以解析的 durable external reference。

V3 第一版建议直接采用 1。

这里 mapped input 进入的是 **Runtime execution state**，不是 Invocation/Session 的业务 Context。

Node terminal 后可以清空 `execution` workspace 以减小当前 State。Fork 到更早 sequence 时，由 Event replay 自然重建当时 workspace。

未来如果大值重复造成存储压力，再引入：

```text
DurableValueRef / ArtifactRef / PayloadRef
```

不要在第一阶段同时引入复杂引用系统。

---

# 5. 时间模型

Runtime 必须同时记录：

1. 事件什么时候发生；
2. 用户 hook 执行多久；
3. Operator 等待调度多久；
4. Operator 实际执行多久；
5. Condition 各自执行多久；
6. Node / Wait / Invocation 生命周期时间。

不能只用一种时钟。

## 5.1 Wall clock

继续使用注入的：

```python
Clock = Callable[[], int]
```

生成：

```python
occurred_at_ns
```

语义：这个 semantic boundary 在真实时间线上什么时候发生。

用于：

```text
UI timeline
Node lifecycle timestamps
Wait timestamps
Session/Invocation timestamps
跨进程观察
```

### sequence 才是 ordering authority

不要依赖 wall clock 判断 Event 顺序。

```text
sequence
```

才是 authoritative order。

NTP、VM clock adjustment 等可能导致 wall clock 轻微回退，因此不建议继续让：

```text
occurred_at_ns < previous.occurred_at_ns
```

成为 Runtime fatal error。

## 5.2 Monotonic duration clock

所有精确 duration 必须使用：

```python
time.perf_counter_ns()
```

包括：

```text
InputMapping duration
CapabilityResolver duration
Operator queue duration
Operator execution duration
Aggregation duration
OutputBinding duration
Routing batch duration
Condition duration
```

不要用两个 wall-clock 时间相减作为精确执行时间。

## 5.3 RuntimeEvent envelope time

所有 Event 都有：

```python
occurred_at_ns: int
```

Start Event（`NodeStarted` / `OperatorCallStarted` / `WaitRequested`）表示进入该生命周期状态的真实时间。

Completion Event（`InputMapped` / `OperatorCallCompleted` / `Aggregated` / `OutputBound` / `RoutingResolved` / `NodeCompleted`）表示该结果已经形成 durable boundary 的时间。

## 5.4 EventStore persisted time

如果以后要统计：

```text
event produced → durable DB commit
```

不要污染 RuntimeEvent semantic schema。

EventStore 可以保存 side metadata：

```python
@dataclass(frozen=True)
class EventStoreMetadata:
    persisted_at_ns: int
```

`persisted_at_ns` 是 infrastructure observation，不是 Workflow semantic fact。

---

# 6. Canonical RuntimeEvent List

## 6.1 SessionOpened

```python
@dataclass(frozen=True, slots=True)
class SessionOpened:
    context: DurableValue
```

StateDelta：

```text
create SessionState
created_at_ns = event.occurred_at_ns
updated_at_ns = event.occurred_at_ns
```

Session 与 Invocation 分开，因为 Session Context 可以跨 Invocation 存活。

## 6.2 InvocationStarted

合并 V2：

```text
InvocationOpened
InvocationStarted
SchedulerInitialized
```

```python
@dataclass(frozen=True, slots=True)
class InvocationStarted:
    workflow_id: str
    workflow_revision_id: str
    entry_node_id: str
    input: DurableValue
```

StateDelta 原子完成：

```text
create Invocation
status = running
started_at_ns = occurred_at_ns
scheduler.initialized = true
initial entry occurrence = ready
other scheduler initialization result applied
```

不要保留：

```text
Invocation created but not started
Invocation running but scheduler not initialized
```

这种没有用户调试意义的中间状态。

## 6.3 NodeStarted

```python
@dataclass(frozen=True, slots=True)
class NodeStarted:
    occurrence_id: str
```

每个 NodeOccurrence 单独一条 Event。

即使 Executor 一次启动 B/C/D，也仍然是：

```text
NodeStarted(B)
NodeStarted(C)
NodeStarted(D)
```

StateDelta：

```text
occurrence.status: ready → running
occurrence.started_at_ns = occurred_at_ns
occurrence.started_sequence = event.sequence
occurrence.execution.phase = started
```

## 6.4 InputMapped

只在存在用户自定义 `input_mapping` 时生成。默认 framework mapping 不需要 Event。

```python
@dataclass(frozen=True, slots=True)
class InputMapped:
    occurrence_id: str
    mapped_input: DurableValue
    duration_ns: int
```

StateDelta：

```text
execution.phase = input_mapped
execution.mapped_input = payload.mapped_input
```

Fork before：重新执行 input_mapping。

Fork after：复用 mapped input，从 dispatch/operator 开始。

## 6.5 CapabilityResolved

只针对动态 `Capability`；直接绑定 Operator 的 Node 不生成。

```python
@dataclass(frozen=True, slots=True)
class CapabilityResolved:
    occurrence_id: str
    capability_id: str
    operator_id: str
    duration_ns: int
```

StateDelta：

```text
execution.phase = capability_resolved
execution.resolved_capability_id = ...
execution.resolved_operator_id = ...
```

第一版建议 Capability 一律记录，保证 dynamic dispatch 可 replay。

## 6.6 OperatorCallStarted

每一个 Map unit 都是独立 call/event。

```python
@dataclass(frozen=True, slots=True)
class OperatorCallStarted:
    call_id: str
    occurrence_id: str
    operator_id: str
    unit_index: int
    input: DurableValue
    queue_duration_ns: int
```

当前代码是：

```text
await semaphore.acquire()
→ OperatorCallStarted
→ physical operator
```

V3 在 acquire 前启动 monotonic timer：

```python
queue_started = perf_counter_ns()
await self._operator_capacity.acquire()
queue_duration_ns = perf_counter_ns() - queue_started
```

再 emit `OperatorCallStarted`。

StateDelta 创建：

```text
OperatorCallState:
    status = running
    input = ...
    started_at_ns = occurred_at_ns
    queue_duration_ns = ...
```

**OperatorCallStarted 必须 durable 后才能执行 physical operator。**

## 6.7 OperatorCallCompleted

```python
@dataclass(frozen=True, slots=True)
class OperatorCallCompleted:
    call_id: str
    output: DurableValue
    execution_duration_ns: int
```

StateDelta：

```text
call.status = completed
call.output = output
call.completed_at_ns = occurred_at_ns
call.execution_duration_ns = ...
```

Fork after 可以复用昂贵 LLM/API/operator result。

## 6.8 OperatorCallFailed

```python
@dataclass(frozen=True, slots=True)
class OperatorCallFailed:
    call_id: str
    error: RuntimeErrorInfo
    execution_duration_ns: int
```

StateDelta：

```text
call.status = failed
call.error = error
call.completed_at_ns = occurred_at_ns
call.execution_duration_ns = ...
```

如果 crash 发生在 Started 后但没有 terminal Call Event，则 recovery 将 call 标记为 `lost / outcome_unknown`，由 `RecoveryApplied` 完成。

## 6.9 Aggregated

只在：

```python
node.map.aggregate is not None
```

时生成。

```python
@dataclass(frozen=True, slots=True)
class Aggregated:
    occurrence_id: str
    output: DurableValue
    duration_ns: int
```

StateDelta：

```text
execution.phase = aggregated
execution.aggregate_output = output
```

默认 Map outputs → list 是 framework deterministic behavior，不需要额外 Event。

## 6.10 OutputBound

只在存在用户 `output_binding` 时生成。

```python
@dataclass(frozen=True, slots=True)
class OutputBound:
    occurrence_id: str
    patch: ContextPatch
    duration_ns: int
```

StateDelta：

```text
execution.phase = output_bound
execution.pending_context_patch = patch
```

此时不要立刻修改全局 Session/Invocation Context。

真正 Context commit 在 `NodeCompleted` 中完成。

## 6.11 RoutingResolved

如果当前 source status 下有用户自定义 condition，则产生一条 batch Event。默认不要每条 edge 单独一条 RuntimeEvent。

```python
@dataclass(frozen=True, slots=True)
class EdgeConditionResult:
    edge_id: str
    selected: bool
    duration_ns: int


@dataclass(frozen=True, slots=True)
class RoutingResolved:
    occurrence_id: str
    source_status: Literal["complete", "error"]
    conditions: tuple[EdgeConditionResult, ...]
    duration_ns: int
```

当前 V2 `select_edges()` 是逐条顺序 await；V3 建议允许并发 condition evaluation：

```python
await asyncio.gather(...)
```

前提：Edge Condition 是 pure / replay-safe hook，不允许依赖相互执行顺序产生 side effect。

结果最终按 WorkflowIR edge order 或稳定 edge_id order 持久化。

Unconditional edge：

```python
condition is None
```

不进入 `RoutingResolved.conditions`。

它在 `NodeCompleted` 的 SchedulerDelta / StateDelta 中自然 resolve，Trace UI 仍然可以显示它被选中。

StateDelta：

```text
execution.phase = routing_resolved
execution.routing = condition results
```

## 6.12 NodeFaulted

表示 Node pipeline 某个阶段失败，但 graph failure routing 尚未 commit。

```python
NodeFaultPhase = Literal[
    "input_mapping",
    "capability_resolution",
    "operator",
    "aggregation",
    "output_binding",
    "condition",
    "validation",
]

@dataclass(frozen=True, slots=True)
class NodeFaulted:
    occurrence_id: str
    phase: NodeFaultPhase
    error: RuntimeErrorInfo
    duration_ns: int | None = None
```

StateDelta：

```text
execution.phase = faulted
execution.fault = error
```

之后可以：

```text
RoutingResolved(source_status="error")
→ NodeFailed
```

`OperatorCallFailed` 与 `NodeFaulted(stage="operator")` 属于不同层次：

```text
OperatorCallFailed = 某一个 physical call 的结果
NodeFaulted        = 整个 Node pipeline 进入 error routing
```

## 6.13 WaitRequested

替代当前 `NodeOccurrenceWaiting` 命名。

```python
@dataclass(frozen=True, slots=True)
class WaitRequested:
    occurrence_id: str
    wait_id: str
    request: DurableValue
```

StateDelta 原子完成：

```text
occurrence.status = waiting
create WaitState(status=waiting, request=..., created_at_ns=occurred_at_ns)
```

如果 Invocation 此时已经没有其他 runnable/running work，可以在同一 Delta 中：

```text
invocation.status = waiting
```

不再需要独立 `InvocationWaiting` RuntimeEvent。

## 6.14 WaitResumed

```python
@dataclass(frozen=True, slots=True)
class WaitResumed:
    wait_id: str
    response: DurableValue
```

Wait 可能跨进程/重启，因此精确 duration 不应依赖一个进程内 monotonic clock。

State 保存：

```text
created_at_ns
resumed_at_ns
```

UI 可以展示 wall elapsed wait time。

StateDelta：

```text
wait.status = resumed
wait.response = response
wait.resumed_at_ns = occurred_at_ns
occurrence.status = running
invocation.status = running
```

一个 occurrence 不应该因为 Wait resume 再次产生第二个 `NodeStarted`。

## 6.15 NodeCompleted

这是 Node transaction 的 graph commit boundary。

```python
@dataclass(frozen=True, slots=True)
class NodeCompleted:
    occurrence_id: str
    output: DurableValue
    metrics: NodeMetrics | None = None
```

建议 `NodeMetrics` 只保留聚合信息：

```python
@dataclass(frozen=True, slots=True)
class NodeMetrics:
    call_count: int
    peak_parallelism: int
```

精确阶段 duration 已经记录在对应 RuntimeEvent。

StateDelta 一次原子应用：

```text
occurrence.status = completed
occurrence.output = final output
occurrence.completed_at_ns = occurred_at_ns
apply pending ContextPatch
apply SchedulerDelta:
    edge resolutions
    loop/join changes
    downstream ready
    skipped
    revived
    boundary resolutions
set new ready occurrence.ready_at_ns = occurred_at_ns
clear occurrence.execution transient workspace
```

所有：

```text
NodeReady
NodeSkipped
EdgeSelected
LoopAdvanced
JoinResolved
```

默认都属于这个 StateDelta，不生成单独 RuntimeEvent。

## 6.16 NodeFailed

与 `NodeCompleted` 对称：

```python
@dataclass(frozen=True, slots=True)
class NodeFailed:
    occurrence_id: str
    error: RuntimeErrorInfo
```

StateDelta：

```text
occurrence.status = failed
occurrence.error = error
occurrence.completed_at_ns = occurred_at_ns
apply error SchedulerDelta
ready / skipped downstream error branches
clear execution workspace
```

## 6.17 InvocationCompleted

```python
@dataclass(frozen=True, slots=True)
class InvocationCompleted:
    output: DurableValue
```

StateDelta：

```text
invocation.status = completed
invocation.output = output
invocation.completed_at_ns = occurred_at_ns
```

不要和最后一个 `NodeCompleted` 合并，因为：

```text
all nodes terminal
invocation still running
```

是一个合法、可恢复的短暂中间状态。

## 6.18 InvocationFailed

```python
@dataclass(frozen=True, slots=True)
class InvocationFailed:
    error: RuntimeErrorInfo
```

终止整个 Invocation。

## 6.19 InvocationCancelled

```python
@dataclass(frozen=True, slots=True)
class InvocationCancelled:
    reason: str | None
```

一次完成逻辑 cancel。物理 asyncio Task 是否立刻停止是 transient execution concern。

## 6.20 RecoveryApplied

替代当前：

```text
InvocationRecoveryRequested
```

建议：

```python
@dataclass(frozen=True, slots=True)
class RecoveryApplied:
    recovered_occurrence_ids: tuple[str, ...]
    lost_call_ids: tuple[str, ...]
```

StateDelta 例如：

```text
running operator calls → lost
recoverable running occurrences → ready
recovery_attempts += 1
ready_at_ns = occurred_at_ns
```

这是 crash recovery 本身的 durable history。

---

# 7. 默认不应该成为 RuntimeEvent 的东西

默认删除/不新增：

```text
SchedulerInitialized
NodeReady
NodeSkipped
EdgeSelected
EdgeRejected
JoinResolved
LoopAdvanced
LoopBoundaryClosed
OccurrenceRevived
InvocationWaiting
ChildInvocationPhaseChanged
ChildAwaitSuspended
ChildAwaitReady
```

它们应属于：

```text
StateDelta
Trace projection
framework derived state
```

---

# 8. Child Workflow

Parent 不应该再维护复杂 Child lifecycle Event。

对于 Parent：

```text
Child Workflow ≈ 一种 Operator implementation
```

Parent 看到：

```text
OperatorCallStarted
OperatorCallCompleted
OperatorCallFailed
```

如果需要 stable child identity，可以作为 `OperatorCallStarted` payload 扩展：

```python
child_ref: InvocationRef | None
```

Child 自己拥有：

```text
SessionOpened
InvocationStarted
NodeStarted
...
InvocationCompleted
```

自己的完整 RuntimeEvent stream。

V3 应逐步删除 Parent-specific：

```text
ChildInvocationPlanned
ChildInvocationPhaseChanged
ChildAwaitSuspended
ChildAwaitReady
```

如果 migration 第一阶段必须暂时保留 child admission 数据，允许内部兼容，但不要继续作为目标 architecture。

---

# 9. Scheduler V3

Scheduler 当前 pure planner 方向正确，应继续保留。

建议接口从“返回 Runtime payload”进一步改成“只返回 graph decision”：

```python
class Scheduler:
    def initialize(
        self,
        workflow: WorkflowIR,
        state: RuntimeState,
        *,
        occurred_at_ns: int,
    ) -> SchedulerDelta:
        ...

    def complete(
        self,
        workflow: WorkflowIR,
        state: RuntimeState,
        occurrence_id: str,
        *,
        selected_edge_ids: frozenset[str],
        occurred_at_ns: int,
    ) -> SchedulerDelta:
        ...

    def fail(...) -> SchedulerDelta:
        ...
```

删除：

```python
scheduler.start()
```

Node 是否真正开始不是 scheduler decision，而是 Executor / Dispatcher lifecycle。

Scheduler 不再构造 RuntimeEvent payload，只产生 SchedulerDelta。

---

# 10. Executor V3

## 10.1 Hook 执行必须返回 timing

建议统一结果类型：

```python
@dataclass(frozen=True)
class TimedValue:
    value: object
    duration_ns: int
```

或者每个 API 显式返回 tuple。

例如：

```python
mapped, duration_ns = await node_executor.map_input(...)
patch, duration_ns = await node_executor.bind_output(...)
result, duration_ns = await node_executor.aggregate(...)
routing = await node_executor.evaluate_conditions(...)
```

Timing measurement 尽量集中在 NodeExecutor，不要散落在 WorkflowExecutor。

## 10.2 Input Mapping

当前：

```text
map_input()
→ 直接进入 operator
```

修改为：

```text
map_input()
→ emit InputMapped
→ reread RuntimeState / continue
```

Recovery 如果 state 已经是 `phase = input_mapped`，则不要再次调用 user mapping。

## 10.3 Capability Resolution

当前 `_resolve_executable()` 结果没有 durable boundary。

修改为：

```text
resolve capability
→ emit CapabilityResolved
→ execute resolved operator
```

Recovery 后使用 persisted `operator_id`。

## 10.4 Operator 调度等待时间

当前 semaphore acquire 在 `OperatorCallStarted` 前。

加入：

```python
queue_started = perf_counter_ns()
await self._operator_capacity.acquire()
queue_duration_ns = perf_counter_ns() - queue_started
```

然后：

```text
OperatorCallStarted(queue_duration_ns)
```

`OperatorCallStarted` durable 成功后才能进入 physical handler。

## 10.5 Operator execution time

在 durable `OperatorCallStarted` 完成之后：

```python
execution_started = perf_counter_ns()
```

handler settle 后：

```python
execution_duration_ns = perf_counter_ns() - execution_started
```

写入：

```text
OperatorCallCompleted
OperatorCallFailed
```

## 10.6 Aggregation

当前 aggregate 在 `NodeExecutor.execute()` 内部完成，外层只能拿最终 output。

需要拆出：

```text
operator map execution
→ collect outputs
→ custom aggregate
→ Aggregated Event
```

没有 custom aggregate 时，framework list assembly 不生成 Event。

## 10.7 Output Binding

当前 WorkflowExecutor：

```text
bind_output
preview_context_patch
select_edges
scheduler.complete
emit NodeOccurrenceCompleted
```

全部压在 completion lock 里。

V3 改为：

```text
bind_output
→ OutputBound

preview pending patch
→ evaluate conditions
→ RoutingResolved

scheduler.complete
→ NodeCompleted
```

其中：

- `OutputBound` 只写 pending patch；
- `RoutingResolved` 只写 condition results；
- `NodeCompleted` 才 commit global Context + SchedulerDelta。

## 10.8 Condition evaluation

当前代码逐条 await。

V3 可以并发：

```text
condition e1 ─┐
condition e2 ─┼─ gather → RoutingResolved
condition e3 ─┘
```

每条 conditional edge 记录：

```text
selected
duration_ns
```

总 batch 记录：

```text
duration_ns
```

Unconditional edge 不执行 hook，也不进入 `RoutingResolved` payload。

---

# 11. Trace 模型

Trace 不是 canonical Runtime State，也不是 RuntimeEvent 本身。

推荐：

```text
RuntimeEvent
    +
StateDelta
    +
WorkflowIR
       ↓
TraceProjector
       ↓
TraceEvent / TraceModel
       ↓
WebSocket
       ↓
Trace UI
```

例如一个：

```text
NodeCompleted(A)
```

可以投影成：

```text
node.completed(A)
edge.selected(e1)
edge.rejected(e2)
edge.selected(e3)    # 即使 e3 无 condition
node.ready(B)
node.ready(C)
node.skipped(D)
context.changed(...)
loop.iteration(...)
```

所以：

> “没有 unconditional edge RuntimeEvent” 不等于 Trace UI 看不到该 edge。

TraceProjector 应读 `StateDelta / SchedulerDelta`。

## 11.1 RuntimeEvent 可以直接用于基础 Timeline

简单 UI 可以直接显示：

```text
InputMapped
OperatorCallStarted
OperatorCallCompleted
OutputBound
RoutingResolved
NodeCompleted
```

但 architecture contract 不要定义为：

```text
TraceEvent == RuntimeEvent
```

否则未来 `hook.started`、stream chunk、progress、token usage、cost、scheduler detail、animation 会反向污染 RuntimeEvent schema。

## 11.2 Ephemeral Trace

下面内容可以只是 ephemeral trace：

```text
input_mapping.started
condition.started
operator waiting for capacity
stream.chunk
scheduler planning started
```

它们不改变 canonical execution。

如果 UI 实时在线，可以直接 WebSocket 推送；丢失不影响 recovery/replay/fork/checkpoint。

---

# 12. Checkpoint

当前 `SessionCheckpoint` immutable snapshot 思路可以保留。

目标：

```python
@dataclass(frozen=True)
class SessionCheckpoint:
    session_id: str
    sequence: int
    state: RuntimeState
    captured_at_ns: int
    ...
```

约束：

```text
checkpoint.sequence == checkpoint.state.sequence
```

Checkpoint 不是 RuntimeEvent。

## 12.1 logical checkpoint point vs physical snapshot

逻辑上，每一个 committed RuntimeEvent boundary 都可以重建 point-in-time RuntimeState。

但物理 snapshot 不需要每个 Event 都保存：

```text
E1 ... E1000

checkpoint@100
checkpoint@500
checkpoint@900
```

State@723：

```text
checkpoint@500
+
events 501..723
```

---

# 13. Replay

定义：

```text
S0 = genesis
Sn = Apply(Sn-1, En)
```

从 checkpoint：

```text
S723 = Checkpoint@500 + E501...E723
```

Replay：

- 只读 persisted RuntimeEvent；
- 只 apply Event.delta；
- 不重新执行 payload 对应 computation；
- 不重新执行 scheduler planner；
- 不重新执行 hooks/operator。

---

# 14. Recovery

Crash recovery：

```text
load latest checkpoint
    ↓
replay event suffix
    ↓
RuntimeState
    ↓
inspect in-flight state
    ↓
RecoveryApplied
    ↓
continue
```

典型情况：

### Node ready

直接重新 dispatch。

### Node started，但还没有 external effect

根据 recovery policy 重试。

### InputMapped 已完成

复用 mapped input。

### OperatorCallStarted，没有 terminal Call Event

```text
call outcome = unknown
```

通过 `RecoveryApplied` 标记 lost。是否重试取决于 Operator effect policy。

### OperatorCallCompleted

复用 output，不重新触发 physical effect。

### OutputBound

复用 patch。

### RoutingResolved

复用 condition result，直接重新 scheduler commit。

---

# 15. Fork

Fork 不修改 parent history。

定义独立 metadata：

```python
@dataclass(frozen=True)
class ForkOrigin:
    parent_session_id: str
    parent_invocation_id: str
    parent_sequence: int
```

Fork：

```text
Parent:
E1 E2 E3 E4 E5 E6

          \
           Branch
           base = State@E4
```

Parent 不写 `ForkCreated` RuntimeEvent，因为 Debug 操作不是 parent Workflow execution semantic。

## 15.1 Fork safety

逻辑上可以 fork 到任何 committed RuntimeEvent boundary，但重新执行安全性不同。

### Pure / safe

```text
InputMapped
Aggregated
OutputBound
RoutingResolved
```

非常适合 fork-before / fork-after。

### External effect boundary

`OperatorCallStarted` 可以作为 recovery/fork point，但 Debug UI 必须标记：

```text
effect outcome may be unknown
rerun may duplicate side effect
```

`OperatorCallCompleted` 是非常有价值的 fork point：可以保留 expensive external result，重新执行后面的 aggregate/binding/routing。

---

# 16. 基于该历史模型可以扩展的能力

V3 不只是提供 Checkpoint / Recovery / Fork，还能自然扩展：

```text
Time Travel
Step Debugger
Breakpoints
Selective Recompute
Retry From Stage
Run Comparison
Branch Diff
What-if Execution
Workflow Hot Patch
Manual Value Override
Counterfactual Execution
Audit / Provenance
Worker Migration
Result Reuse
Speculative Branching
Compensation
Execution Diff
Trace Reconstruction
```

其中很值得优先考虑的是 Workflow Hot Patch：

```text
OperatorCallCompleted
    ↓
OutputBinding 有 bug
    ↓
Workflow 失败

修改 workflow revision
    ↓
fork from OperatorCallCompleted
    ↓
复用原来的 expensive LLM/API result
    ↓
重新执行新的 OutputBinding / Conditions
```

---

# 17. StateOperation / RuntimeEvent / Trace 最终关系

```text
                    Runtime Execution
                           │
                           ▼
                 Transition / Executor
                           │
                           ▼
                 ┌──────────────────┐
                 │   RuntimeEvent   │
                 │                  │
                 │ semantic payload │
                 │       +          │
                 │   StateDelta?    │
                 └────────┬─────────┘
                          │
              ┌───────────┴────────────┐
              ▼                        ▼
        StateReducer              TraceProjector
              │                        │
              ▼                        ▼
        RuntimeState                Trace UI
              │
       ┌──────┴──────┐
       ▼             ▼
   Checkpoint      Recovery
       │
       ▼
      Fork
```

一句话定义：

> **RuntimeEvent 是 AutoAgent 唯一有顺序的 durable execution history；StateDelta 描述该 Event 对 RuntimeState 的原子影响；RuntimeState 是 Event history 的 materialized projection；Checkpoint 是其稀疏 snapshot；Fork 从 Event boundary 派生新 history；Trace 是 RuntimeEvent + StateDelta + WorkflowIR 的可视化 projection。**

---

# 18. 对当前 V2 文件的修改建议

## `autoagent/core/runtime/events.py`

保留：

```text
RuntimeErrorInfo
typed payload classes
freeze/thaw durable payload discipline
```

删除：

```text
RuntimeLog
StateTransition
operation_batches
from_state_version
to_state_version
RuntimeLog serialization
event multi-log envelope
```

新增/重构 payload：

```text
InvocationStarted       # 合并 Opened/Started/SchedulerInitialized
NodeStarted
InputMapped
CapabilityResolved
OperatorCallStarted
OperatorCallCompleted
OperatorCallFailed
Aggregated
OutputBound
RoutingResolved
NodeFaulted
WaitRequested
WaitResumed
NodeCompleted
NodeFailed
InvocationCompleted
InvocationFailed
InvocationCancelled
RecoveryApplied
```

## `autoagent/core/runtime/operations.py`

删除：

```text
diff_runtime_states
_diff_runtime_value
_diff_runtime_sequence
```

保留并简化：

```text
StateOperation
apply operation
path copy-on-write
```

重命名：

```text
StateOperationBatch → StateDelta
```

`StateDelta` 不拥有 version/time/id。

## `autoagent/core/runtime/state.py`

删除：

```text
state_version
last_event_digest
last_event_semantic_digest
started_state_version
```

新增/重命名：

```text
started_state_version → started_sequence
NodeOccurrenceState.ready_at_ns
NodeOccurrenceState.execution: NodeExecutionState
OperatorCallState.queue_duration_ns
OperatorCallState.execution_duration_ns
```

必要时继续保留 WaitState created/resumed timestamps。

## `autoagent/core/runtime/reducer.py`

删除：

```text
diff
semantic→candidate→diff planning
validate_sealed
semantic/operation digest cross-check
StateOperationBatch version machinery
```

目标职责：

```text
validate Event sequence
apply StateDelta
advance RuntimeState.sequence
```

完整 `validate_runtime_state()` 只用于：

```text
checkpoint creation
checkpoint restore
external import
tests
debug mode
```

不要默认每个 Event 都完整遍历大 RuntimeState。

## 新增 `autoagent/core/runtime/transitions.py`

放置 explicit Delta planning：

```text
TransitionPlanner
plan_session_opened
plan_invocation_started
plan_node_started
plan_input_mapped
...
plan_node_completed
```

这是 live execution planner；Replay 不调用它。

## `autoagent/core/runtime/journal.py`

当前：

```text
stage
flush
max_batches_per_event
event_group
live state
persisted state
RuntimeLog merge
```

都服务于多个 semantic transition 合并一个 RuntimeEvent 的旧设计。

V3 用：

```text
RuntimeRepository
+
RuntimeEventStore
+
StateCache
```

替代。

## 新增 `autoagent/core/runtime/event_store.py`

```python
class RuntimeEventStore(Protocol):
    async def append(
        self,
        event: RuntimeEvent,
        *,
        expected_sequence: int,
    ) -> None:
        ...

    async def read(
        self,
        session_id: str,
        *,
        after_sequence: int = 0,
        through_sequence: int | None = None,
    ) -> tuple[RuntimeEvent, ...]:
        ...
```

第一版实现 `InMemoryRuntimeEventStore`；以后 DB adapter 可做 group commit。

## 新增 `autoagent/core/runtime/repository.py`

```python
class RuntimeRepository:
    async def commit(
        self,
        *,
        session_id: str,
        invocation_id: str | None,
        payload: RuntimeEventPayload,
        causation_id: str | None = None,
    ) -> RuntimeEvent:
        ...

    def state(self, session_id: str) -> RuntimeState:
        ...

    async def state_at(
        self,
        session_id: str,
        sequence: int,
    ) -> RuntimeState:
        ...
```

Commit 流程：

```text
current state@N
    ↓
TransitionPlanner.plan()
    ↓
StateDelta
    ↓
RuntimeEvent #N+1
    ↓
Reducer.apply() -> candidate state
    ↓
EventStore.append(expected_sequence=N)
    ↓ durable
publish StateCache = candidate
    ↓
TraceProjector
```

对于真正 durable EventStore，visible State 不应该领先于 durable Event prefix。

## `autoagent/core/runtime/checkpoint.py`

保留总体结构，改为只以 `RuntimeState.sequence` 作为历史锚点，删除 state_version 依赖。

确保 active `NodeExecutionState` 可以完整序列化。

## 新增 `autoagent/core/runtime/fork.py`

第一阶段实现：

```text
ForkOrigin
state_at(sequence)
new branch metadata
```

先不要求一次完成完整 UI debugger。

## 新增 `autoagent/core/runtime/trace.py`

第一版：

```python
class TraceProjector:
    def project(
        self,
        before: RuntimeState,
        event: RuntimeEvent,
        after: RuntimeState,
        workflow: WorkflowIR,
    ) -> tuple[TraceEvent, ...]:
        ...
```

至少支持：

```text
node.started
node.completed
node.failed
node.ready
node.skipped
edge.selected
edge.rejected
operator.started
operator.completed
operator.failed
hook.input_mapped
hook.aggregated
hook.output_bound
routing.resolved
wait.requested
wait.resumed
invocation.completed
```

## `autoagent/core/scheduler/scheduler.py`
## `autoagent/core/scheduler/loop_scheduler.py`

保留 pure planner 思路。

修改为返回 `SchedulerDelta`，不再负责 RuntimeEvent 构造。

删除 `scheduler.start()`。

Scheduler ready/skipped/resolutions 都进入上层 Event 的 StateDelta。

## `autoagent/core/executor/node_executor.py`

重点修改：

```text
hook timing
operator queue timing
operator execution timing
condition batch evaluation
aggregate 从 execute() 中拆出或暴露中间边界
```

当前 Map worker 并发机制可以保留。

当前 streaming 第一阶段不要扩大 durable 粒度。Stream chunk 保持 UserEvent / Trace。以后如果要 chunk-level durable replay，再单独设计。

## `autoagent/core/executor/workflow_executor.py`

目标 pipeline：

```text
NodeStarted
    ↓
InputMapped?                 RuntimeEvent
    ↓
CapabilityResolved?          RuntimeEvent
    ↓
OperatorCallStarted          RuntimeEvent
    ↓
Operator
    ↓
OperatorCallCompleted        RuntimeEvent
    ↓
Aggregated?                  RuntimeEvent
    ↓
OutputBound?                 RuntimeEvent
    ↓
RoutingResolved?             RuntimeEvent
    ↓
Scheduler.plan()
    ↓
NodeCompleted                RuntimeEvent
```

Failure：

```text
stage error
    ↓
NodeFaulted
    ↓
RoutingResolved(error)?
    ↓
Scheduler.fail()
    ↓
NodeFailed
```

Wait：

```text
NodeStarted
→ InputMapped?
→ WaitRequested

external response
→ WaitResumed
→ post-processing
→ NodeCompleted
```

## `autoagent/core/app/app.py`

当前 root admission：

```text
SessionOpened
InvocationOpened
InvocationStarted
SchedulerInitialized
event group commit
```

改为：

```text
SessionOpened?       # 仅新 Session
InvocationStarted
```

删除 admission event-group complexity。

当前 `_emit()`：

```text
StateTransition
→ journal.apply_transition
→ export_runtime_events
```

改为：

```text
repository.commit(payload)
```

App 不再知道 RuntimeLog / StateOperationBatch / flush / event group。

Child lifecycle 逐步移出 Parent Runtime event stream。

## `autoagent/core/app/ports.py`

重写 `RuntimeJournalPort` 为更简单的：

```text
RuntimeRepositoryPort
RuntimeEventStorePort
```

SchedulerPort 返回 `SchedulerDelta`。

NodeExecutorPort 暴露 timing-aware hook / routing results。

---

# 19. 并发规则

## Scheduler 一次让多个 Node ready

```text
一个上层 RuntimeEvent
+
一个 StateDelta
+
多个 ready StateOperation
```

不存在单独 NodeReady RuntimeEvent。

## 多个 Node 真正 started

```text
每个 NodeStarted 一条 RuntimeEvent
```

因为它们具有独立 physical lifecycle boundary。

## Map Operator Calls

每个 unit：

```text
OperatorCallStarted(unit N)
OperatorCallCompleted(unit N)
```

独立 Event。

完成顺序按真实 completion order 进入 Event history，不要为了保持 unit_index 顺序而篡改真实历史。

## Edge Conditions

默认：

```text
一个 occurrence / source_status
→ 一批 conditional edge evaluation
→ 一条 RoutingResolved
```

Condition 内部可以并行，Payload 使用稳定 edge order。

如果未来需要 forensic per-edge fork，可以做 DebugCapture.FULL 扩展，但不要进入第一版 core correctness。

---

# 20. Hook Contract

下面这些必须明确约定为 pure / replay-safe user hook：

```text
input_mapping
output_binding
aggregate
edge condition
```

它们不应该直接产生不可恢复 external side effect。

External side effect 必须通过 Operator。

框架无法完全阻止用户在 Python callback 中做 I/O，但文档和 API contract 必须明确这一点，因为：

- hook 可以因为 crash 在 Event commit 前被重新执行；
- condition 未来可以并行执行；
- fork-before-hook 会重新执行 hook；
- deterministic debugger 依赖这个 contract。

---

# 21. 性能原则

V3 第一阶段不要为了减少 Event 数量破坏 semantic boundary。

优先采用：

```text
logical events fine-grained
physical writes batched
physical checkpoints sparse
Trace projection on demand
```

而不是：

```text
merge multiple transitions into one RuntimeEvent
```

后续性能优化顺序：

1. EventStore group commit；
2. StateDelta path copy-on-write；
3. incremental State validation；
4. checkpoint policy；
5. large value ArtifactRef；
6. Trace retention/sampling；
7. event compaction（如果确有需要）。

---

# 22. 推荐实现阶段

## Phase 1 — 数据模型

先修改类型和测试：

```text
RuntimeEvent
StateDelta
StateOperation
NodeExecutionState
timing fields
new payload classes
```

验收：

```text
serialize/deserialize 正确
sequence invariant 明确
StateDelta 可 replay
```

## Phase 2 — Reducer / Transition Planner

实现：

```text
TransitionPlanner
StateReducer
```

删除：

```text
diff_runtime_states
RuntimeLog
validate_sealed
operation batch version
```

建立测试：

```text
live apply == replay apply
```

## Phase 3 — Executor / Scheduler

依次接入：

```text
NodeStarted
InputMapped
CapabilityResolved
Operator timing
Aggregated
OutputBound
RoutingResolved
NodeCompleted
```

Scheduler 改成只输出 `SchedulerDelta`。

## Phase 4 — Repository / EventStore

替换旧 Journal：

```text
RuntimeRepository
InMemoryRuntimeEventStore
StateCache
```

删除：

```text
stage
flush
event grouping
pending RuntimeLog merge
captured/live state duality
```

## Phase 5 — Checkpoint / Recovery

迁移：

```text
Checkpoint.sequence
state_at(sequence)
RecoveryApplied
in-flight NodeExecutionState continuation
```

重点验证：

```text
InputMapped 后 crash
OperatorCompleted 后 crash
OutputBound 后 crash
RoutingResolved 后 crash
```

都不会错误重执行前置用户逻辑。

## Phase 6 — Fork

先实现 Core API：

```text
state_at
ForkOrigin
new branch from sequence
```

再实现：

```text
fork-before
fork-after
override intermediate value
workflow revision change
```

## Phase 7 — TraceProjector

Runtime correctness 稳定后再实现 Trace UI projection。

不要让 Trace 需求反向改变 RuntimeEvent correctness model。

---

# 23. 必须添加的核心测试

### Replay

```text
从空 State replay 全 Event history
==
live RuntimeState
```

### Checkpoint replay

```text
checkpoint@N + events N+1..M
==
full replay 1..M
```

### Parallel ready

A completion 让 B/C/D ready：

```text
只有 NodeCompleted(A) 一个 RuntimeEvent
StateDelta 同时创建/更新 B/C/D ready
```

### Parallel starts

B/C/D 被执行：

```text
存在三个独立 NodeStarted Event
```

### Input mapping recovery

```text
InputMapped durable
crash
recover
```

断言 input_mapping 不再次调用。

### Operator write-ahead

断言 `OperatorCallStarted` durable 发生在 physical operator handler 之前。

### Operator queue timing

Semaphore 等待后 `queue_duration_ns > 0`，且来自 monotonic clock。

### Operator result recovery

```text
OperatorCallCompleted
crash
recover
```

断言 physical operator 不再次执行。

### Output binding recovery

```text
OutputBound
crash
recover
```

断言 output_binding 不再次执行。

### Routing recovery

```text
RoutingResolved
crash
recover
```

断言 condition 不再次执行。

### Unconditional edge trace

没有 Edge RuntimeEvent，但：

```text
NodeCompleted StateDelta
→ TraceProjector
```

必须得到 `edge.selected`。

### Fork after OperatorCallCompleted

Fork 后修改 OutputBinding：

```text
operator 不重跑
binding / routing 重跑
```

### Fork before OperatorCallStarted

Debugger 必须根据 effect policy 提示 side-effect 风险。

### Wall clock regression

Runtime ordering 仍由 sequence 决定，不因 wall clock 轻微回退破坏 replay。

---

# 24. Coding Agent 的明确约束

1. **不要再使用 `diff_runtime_states()` 作为 Event StateOperation 的来源。**
2. **不要把多个 semantic RuntimeEvent 为了性能合并成一个 Event。**
3. **不要为 NodeReady / EdgeSelected / LoopAdvanced 等 Scheduler bookkeeping 新增 RuntimeEvent。**
4. **每个真正开始执行的 NodeOccurrence 必须拥有独立 `NodeStarted` Event。**
5. **每个 physical Operator call 必须拥有独立 Started + terminal Event。**
6. **OperatorCallStarted 必须 durable 后才能进入 external handler。**
7. **InputMapping / Aggregate / OutputBinding / Condition 的成功结果必须形成可恢复 execution boundary。**
8. **用户 hook duration 用 monotonic clock 测量。**
9. **Event occurred time使用 wall clock；Event ordering 使用 sequence。**
10. **Trace 不得成为 Runtime correctness source of truth。**
11. **Checkpoint 不得成为 RuntimeEvent。**
12. **Fork metadata 不得写入 parent Runtime execution history。**
13. **Child Workflow 的内部 lifecycle 由 Child 自己的 RuntimeEvent stream 维护。**
14. **第一阶段不要同时引入复杂 artifact store、event compaction、per-edge forensic capture。**
15. **保留 V2 已经正确的不可变 RuntimeState、pure Scheduler、Operator write-ahead 等设计。**

---

# 25. 最终目标执行示例

一个包含 Mapping、Capability、Map、Aggregate、Binding、Condition 的 Node：

```text
#100 NodeStarted(A)

#101 InputMapped(A)
     duration = 0.3ms

#102 CapabilityResolved(A)
     operator = search-v2
     duration = 0.1ms

#103 OperatorCallStarted(A/unit0)
     queue = 4ms

#104 OperatorCallStarted(A/unit1)
     queue = 5ms

#105 OperatorCallCompleted(unit1)
     execution = 120ms

#106 OperatorCallCompleted(unit0)
     execution = 180ms

#107 Aggregated(A)
     duration = 0.4ms

#108 OutputBound(A)
     duration = 0.2ms

#109 RoutingResolved(A)
     total = 1.2ms
     e1 = true,  0.8ms
     e2 = false, 1.0ms

#110 NodeCompleted(A)

StateDelta of #110:
    A completed
    apply ContextPatch
    unconditional edge e0 selected
    conditional e1 selected
    conditional e2 rejected
    B ready
    C ready
    D skipped

TraceProjector:
    node.completed(A)
    edge.selected(e0)
    edge.selected(e1)
    edge.rejected(e2)
    node.ready(B)
    node.ready(C)
    node.skipped(D)
```

这里：

```text
RuntimeEvent 数量
```

表达真实有意义的 durable execution history。

```text
TraceEvent 数量
```

可以更细。

```text
physical DB commit 数量
```

可以比 RuntimeEvent 数量更少。

这三个粒度彻底解耦。

---

# 26. 最终设计结论

V3 不应该继续把 Runtime 看成：

```text
State changes
→ diff
→ store patches
```

而应该看成：

```text
Execution
→ meaningful durable boundary
→ RuntimeEvent
→ explicit StateDelta
→ RuntimeState projection
```

其中：

```text
RuntimeEvent
    决定“历史发生了什么”

StateDelta
    决定“RuntimeState 如何变化”

RuntimeState
    决定“现在是什么状态”

Checkpoint
    决定“从哪里快速恢复状态”

Recovery
    决定“crash 后如何继续”

Fork
    决定“从哪个历史边界产生新未来”

Trace
    决定“如何把历史解释给人看”
```

这些概念相关，但不再互相混用。

这应作为 AutoAgent Core 下一版 Runtime 的基础。
