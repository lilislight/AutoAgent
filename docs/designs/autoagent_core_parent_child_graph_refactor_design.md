# AutoAgent Core Parent/Child Runtime Graph Refactor Design

**Status:** Draft  
**Target:** `refactor` branch current Core  
**Scope:** Parent/Child ownership、ChildHandle、Invocation lifecycle、App API、Unload/Load/Checkpoint/Recovery、RuntimeEvent sequence  
**Out of Scope:** Runtime System Command（Signal / AwaitChild / SendMessage 等）的具体设计与实现

---

## 1. 目标

本次修改重新定义 Runtime Child 的角色：

> **Child Invocation 是 Root Invocation Runtime Graph 内部的结构化子任务，而不是一个对外独立可控制的 Job。**

核心目标：

1. `InvocationRef` 只表示对外可控制的 Root Invocation。
2. Child 不再暴露 `InvocationRef`，Workflow 内部使用 `ChildHandle`。
3. Parent 与所有 descendants 强绑定，组成一个 Runtime Graph。
4. Parent 自身正常执行完成后，只要仍有 Child 未 terminal，Parent 就不能进入 `completed`。
5. Parent failure / cancellation 必须向所有 descendants 传播 cancellation。
6. Child failure 不自动改变 Parent Invocation 状态；是否处理 Child failure 属于 Parent Workflow 自己的逻辑。
7. Unload / Load / Checkpoint / Recovery 以完整 Root Runtime Graph 为最小生命周期单位。
8. Parent 与每个 Child 仍然拥有独立的 Session、RuntimeState、RuntimeEvent sequence；**不引入 Graph 全局 sequence**。
9. 第一阶段暂不支持 Child 中的 external `Wait`，避免引入 Graph 层复杂的 partial-wait 语义。
10. 本次不设计 Runtime System Command，只为后续 ChildHandle 交互预留正确的生命周期模型。

---

# 2. 当前实现的问题

当前 Core 已经具有 Parent/Child Runtime 基础能力，但 External Control Plane 和 Runtime Child Plane 混在一起。

当前 Child 可以被构造成：

```python
InvocationRef(
    session_id=child_session_id,
    invocation_id=child_invocation_id,
    workflow_id=...,
    workflow_revision_id=...,
)
```

然后通过：

```python
app.status(child_ref)
app.join(child_ref)
app.resume(child_ref, ...)
app.cancel(child_ref)
app.recover(child_ref)
app.unload_session(child_ref)
```

直接操作。

当前还公开：

```python
app.child_invocations(parent_ref)
```

这使 Child 同时承担两种角色：

```text
Runtime structured subtask
+
Externally controllable Invocation
```

本次修改将这两个角色彻底分开。

---

# 3. Target Runtime Model

最终模型：

```text
                         Host
                          │
                    InvocationRef
                          │
                          ▼
                   Root Invocation
                          │
                    Runtime Graph
                          │
            ┌─────────────┼─────────────┐
            │             │             │
            ▼             ▼             ▼
         Child A       Child B       Child C
            │
            ▼
       GrandChild D
```

外部 Host 只认识：

```text
InvocationRef
```

Workflow Runtime 内部认识：

```text
ChildHandle
```

两者不能混用。

---

# 4. InvocationRef

`InvocationRef` 的语义修改为：

> **一个 externally controllable Root Invocation 的稳定引用。**

所有 `AutoAgentApp` lifecycle API 都只接受 `InvocationRef`。

例如：

```python
app.status(root_ref)
app.join(root_ref)
app.resume(root_ref, wait_id, response)
app.cancel(root_ref)
app.recover(root_ref)
app.unload_session(root_ref)
```

不再存在由正常 Public API 返回的 Child `InvocationRef`。

不需要增加 `_root_control_ref()` 这种新概念。

现有 `_state_for_ref()` / public admission validation 可以保证：

- ref 对应 current Root Invocation；
- ref 不能指向内部 Child；
- Child identity 通过 `ChildHandle` 表达。

从类型和 API 语义上：

```text
InvocationRef == Root
ChildHandle   == Child
```

---

# 5. ChildHandle

ChildHandle 不应该是一个需要 Runtime 再维护额外映射的 opaque id。

它应直接携带定位 Child 所需的 identity：

```python
@dataclass(frozen=True, slots=True)
class ChildHandle:
    child_session_id: str
    child_invocation_id: str
    workflow_revision_id: str
```

这里不额外增加：

```text
handle_id -> child runtime
```

这样的 canonical mapping。

当前 RuntimeState 本身已经存在：

```python
ChildInvocationPlan.workflow_revision_id

ChildUnitState.session_id
ChildUnitState.invocation_id
```

因此 `ChildHandle` 可以直接从已有 durable state 构造：

```text
ChildInvocationPlan
      +
ChildUnitState
      ↓
ChildHandle
```

不会产生新的 durable Child-specific mapping。

现有 `_child_owners` 如果继续存在，只作为可重建的 transient reverse index / performance optimization，不承担 ChildHandle identity 的持久化职责。

---

# 6. Spawn Output

当前 Spawn Child Node 输出：

```text
InvocationRef
```

修改为：

```text
ChildHandle
```

单个 Spawn：

```python
ChildHandle
```

Map + Spawn：

```python
list[ChildHandle]
```

顺序继续与 Map input unit order 一致。

Compiler / ValueContract / durable value codec 需要像当前支持 `InvocationRef` 一样支持 `ChildHandle` 的 durable round-trip。

---

# 7. Parent / Child Ownership Invariants

必须满足：

```text
I1. Every Child belongs to exactly one Parent.

I2. Every Child belongs to exactly one Root Runtime Graph.

I3. Child cannot outlive its Root Runtime Graph.

I4. Root is the only externally controllable Invocation.

I5. ChildHandle is a Workflow Runtime value, not an App control reference.

I6. Detached independent work is a new Root Invocation, not a Child.
```

因此不存在：

```text
Root 已 unload
Child 仍 resident
```

也不存在：

```text
Root graph 已彻底结束
Child 仍在后台运行
```

---

# 8. Spawn 的重新定义

`spawn` 表示：

> **Parent 不在 Child 创建点立即等待 Child，但 Child 仍属于 Parent Runtime Graph。**

例如：

```text
Parent
  │
  ├── spawn Child A
  │
  ├── execute B
  │
  ├── execute C
  │
  └── reach workflow exit
```

如果 Child A 仍未 terminal：

```text
Parent 不能 completed
```

因此：

```text
spawn != detached background job
```

而是：

```text
spawn == asynchronous structured child
```

如果未来需要真正 detached 的后台任务，应由上层 Host/System Service 启动新的 Root Invocation。

---

# 9. Invocation Status 修改

当前：

```python
InvocationStatus = Literal[
    "created",
    "running",
    "waiting",
    "completed",
    "failed",
    "cancelled",
]
```

增加：

```text
joining_children
```

最终：

```python
InvocationStatus = Literal[
    "created",
    "running",
    "waiting",
    "joining_children",
    "completed",
    "failed",
    "cancelled",
]
```

`joining_children` 含义：

> 当前 Invocation 自己的 Workflow execution 已成功完成，但仍存在 owned Child 未 terminal，因此 Invocation 尚不能进入 `completed`。

这里不增加：

```text
InvocationTerminalIntent
pending terminal object
```

也不增加额外的 terminal-intent durable structure。

继续沿用现有 `InvocationState`。

---

# 10. Parent 正常完成

当前可能近似：

```text
Scheduler reaches workflow completion
        ↓
InvocationCompleted
```

修改为：

```text
Scheduler reaches workflow completion
        │
        ▼
any non-terminal child?
        │
   ┌────┴────┐
   │         │
  No        Yes
   │         │
   ▼         ▼
Completed   joining_children
              │
              ▼
       wait all children terminal
              │
              ▼
          Completed
```

当进入：

```text
joining_children
```

时，可以直接利用现有：

```python
InvocationState.output
```

保存 Parent 自己已经计算完成的最终 output。

不需要增加额外 pending-output 结构。

建议增加一个语义 RuntimeEvent，例如：

```text
InvocationChildrenJoinStarted
```

或者：

```text
InvocationJoiningChildren
```

作用：

```text
status = joining_children
output = final parent output
```

当最后一个 Child terminal 后，再使用现有：

```text
InvocationCompleted(output)
```

完成 Parent。

---

# 11. Child Terminal 如何唤醒 Parent

Child 自己完成：

```text
Child Runtime Session
    │
    └── InvocationCompleted / Failed / Cancelled
```

之后 Parent Session 中仍然记录自己的 ownership transition：

```text
ChildInvocationPhaseChanged(
    creation_id,
    unit_index,
    "terminal",
)
```

注意：

> Child RuntimeEvent 和 Parent `ChildInvocationPhaseChanged` 不属于同一个 RuntimeEvent sequence。

它们分别属于两个 Session。

Parent 收到 Child phase terminal 后：

```text
if parent.status == "joining_children"
and all parent owned children are terminal:
    emit InvocationCompleted(parent.output)
```

如果 Parent 还有 Parent，则继续向上传播 Child terminal phase。

最终形成：

```text
GrandChild terminal
        ↓
Child child-plan terminal
        ↓
Child completed
        ↓
Parent child-plan terminal
        ↓
Parent completed
        ↓
Root completed
```

---

# 12. Child Failure 语义

Child failure **不能自动改变 Parent Invocation status**。

即：

```text
Child failed
```

不能直接得到：

```text
Parent failed
```

Runtime ownership 只关心：

```text
Child 是否 terminal
```

而不自动决定 Parent 的业务结果。

因此对于 Spawn Child：

```text
Child failed
        ↓
Child becomes terminal
        ↓
Parent continues / joins remaining children
```

如果 Parent 自己已经处于：

```text
joining_children
```

那么 failed Child 仍然只是一个 terminal Child。

当所有 Child terminal 后，Parent 可以正常：

```text
completed
```

即使某个 Spawn Child 曾经 failed。

如果 Parent 业务需要根据 Child failure 做决策，应由 Parent Workflow 自己显式处理。

这部分未来由 Runtime System Command / AwaitChild 等机制提供能力，本设计暂不展开。

---

# 13. execution_mode="await" 的 Child Failure

`await` Child 和 `spawn` Child 要区分。

对于：

```python
Node(
    executable=child_workflow,
    execution_mode="await",
)
```

Parent occurrence 本身明确依赖 Child result。

因此 Child failure 可以解析为：

```text
owning Parent NodeOccurrence execution error
```

之后仍走 Parent 自己已有的普通 Workflow error semantics：

```text
NodeFailed
   ↓
on="error" Edge / Parent workflow handling
```

这仍然属于：

> Parent 自己的 Workflow 逻辑处理 Child failure。

而不是：

> Runtime 看到任意 Child failed，就直接把 Parent Invocation 改成 failed。

对于 `spawn`，则完全不存在这种自动传播。

---

# 14. Parent Failure

Parent 自己因为自己的 Workflow 逻辑失败：

```text
Parent failed
```

则必须取消所有仍 active 的 descendants。

语义：

```text
Parent failure decision
        │
        ▼
cancel active descendants
        │
        ▼
wait descendant cancellation convergence
```

当前 Core 已经存在：

```python
_cancel_descendants(...)
```

以及 Recovery 中：

```text
failed/cancelled ancestor
→ cancel active descendants
```

的基础逻辑。

本次继续保留。

这里不需要增加 `InvocationTerminalIntent`。

允许 Parent 的 failure decision 已经写入自己的 RuntimeState，然后 Runtime 收敛 descendants。

但：

```text
Graph considered settled / unloadable / replaceable
```

必须等 descendants 全部 terminal。

Public failure result 的返回也应在 descendant cancellation convergence 后完成。

---

# 15. Parent Cancellation

同样：

```python
app.cancel(root_ref)
```

只接受 Root `InvocationRef`。

语义：

```text
Root cancelled
     │
     ▼
cancel every active descendant
     │
     ▼
wait all descendants settle
     │
     ▼
return cancelled boundary
```

Host 不直接 cancel Child。

未来 Parent Workflow 自己如果需要 cancel 某个 Child：

```text
ChildHandle
```

将通过 Runtime System Command 完成，不属于本设计范围。

---

# 16. completed 的强约束

本次最重要的 lifecycle invariant：

```text
Invocation.status == "completed"
    ⇒
all owned direct children terminal
```

递归后：

```text
Root.status == "completed"
    ⇒
all descendants terminal
```

因此不再允许：

```text
Root completed
Child running
```

这种状态。

对于 `failed/cancelled`：

- failure/cancel decision 可以先发生；
- Runtime 必须立即收敛 descendants；
- Graph 只有在所有 descendants terminal 后才允许 unload / replacement；
- Public control operation 应等待 cancellation convergence 后再返回稳定 terminal result；
- crash recovery 发现 failed/cancelled ancestor + active descendants 时，继续 cancel descendants。

---

# 17. 同一 Session 的下一次 Invocation

当前代码需要额外判断：

```text
old Root terminal
but Child still active?
```

因为现在允许：

```text
Root completed
Child running
```

新模型下正常 completion 不再允许这种状态。

因此：

```text
Root completed
```

本身已经意味着：

```text
all descendants terminal
```

于是同一个 Root Session 可以安全进入下一次 Invocation。

对于 failed/cancelled：

在 admission 新 Invocation 前仍然要求：

```text
old Runtime Graph fully settled
```

即 descendants 已完成 cancellation convergence。

---

# 18. Child Wait：第一阶段禁止

如果允许 Child 使用 external `Wait`：

```text
Root running
   │
   ├── Child A waiting external input
   └── Child B still running
```

会出现复杂的 Graph boundary：

- Root 自己可能仍 running；
- 一个 Child waiting；
- 另一个 Child running；
- Host 只有 Root InvocationRef；
- `join(root)` 是否应该返回？
- `status(root)` 是否应该暴露 partial waits？
- `resume(root, ...)` 如何定位 exact Child？
- Graph 是否算 externally stable？

这些语义可以设计，但本轮 Parent/Child 重构没有必要一起引入。

因此第一阶段采用最简单规则：

> **Runtime Child Workflow 不允许包含 external `Wait`。**

---

# 19. Child Wait 的编译期限制

Root Workflow：

```text
允许 Wait
```

当一个 Workflow 被作为 Runtime Child：

```python
Node(
    executable=child_workflow,
)
```

Compiler 应递归检查 Child Workflow closure。

如果 Child closure 中出现：

```text
Wait
```

编译失败，例如：

```text
CHILD_EXTERNAL_WAIT_UNSUPPORTED
```

检查范围包括：

- Child workflow 自身节点；
- Child 内 flatten 的 SubWorkflow；
- Child 的 nested runtime Child closure。

同一个 Workflow 如果独立注册并作为 Root 使用，可以包含 Wait。

限制只作用于：

```text
Workflow used as Runtime Child
```

---

# 20. Future Child Wait 方向（本次不实现）

未来如果确实需要 Child Wait，推荐仍然保持：

```text
Host only uses Root InvocationRef
```

而不是重新暴露 Child control API。

可以让：

```python
app.status(root_ref)
```

返回整个 Runtime Graph 的 externally resumable waits。

例如：

```python
InvocationWait(
    id=...,
    request=...,
)
```

`wait_id` 在一个 Runtime Graph 内唯一。

然后：

```python
app.resume(root_ref, wait_id, response)
```

Runtime 根据 graph wait index 自动路由到 exact Child Session。

不需要 Host 传 ChildHandle。

这样仍然满足：

```text
App API only accepts InvocationRef
```

但这不是本次实现范围。

---

# 21. Root Wait + Child Running

Root 自己仍然可以使用现有 `Wait`。

例如：

```text
Root Parent waiting for external approval
        │
        └── spawned Child still running
```

这可以存在。

此时：

```text
Root status = waiting
```

表示：

> Root Workflow execution 当前需要 external resume。

它不等价于：

```text
整个 Runtime Graph 已 quiescent
```

因此：

- `resume(root_ref, ...)` 合法；
- Child 可以继续运行；
- `unload_session(root_ref)` 如果仍有 live Child，必须拒绝。

也就是说：

```text
waiting != graph fully idle
```

这个区别要在 API 文档中明确。

---

# 22. 当前 unload_session() 的真实逻辑

当前 `refactor` 实现不是 Graph unload。

当前流程：

```text
unload_session(ref)
        │
        ▼
find root
        │
        ▼
find all resident related sessions
        │
        ▼
validate ALL related sessions:
    status in waiting/completed/failed/cancelled
    no live task
        │
        ▼
BUT:
capture checkpoint only for ref.session_id
        │
        ▼
discard only ref.session_id
```

也就是说：

> 当前代码使用整个 graph 判断“能不能卸载”，但真正只卸载请求的单个 Session。

因此当前允许：

```text
Parent checkpoint 单独卸载
Child checkpoint 单独卸载
```

只要整个 related component 当时都 quiescent。

测试也专门覆盖了：

```text
parent unload checkpoint contains only parent session
child unload checkpoint uses generic ref
```

这与新的强绑定模型不一致。

---

# 23. 当前 load_checkpoint() 的真实逻辑

当前：

```python
load_checkpoint(
    SessionCheckpoint | AppCheckpoint
)
```

`SessionCheckpoint` 只包含一个 Session。

`AppCheckpoint`：

```python
sessions: tuple[SessionCheckpoint, ...]
```

是 flat session collection。

当前 load：

1. 收集所有传入 Session state；
2. 检查与现有 resident Session 是否 conflict；
3. 如果 Parent 和 Child 同时出现，验证 identity；
4. **不要求 Parent 引用的所有 Child 必须同时存在**；
5. 安装传入的所有 RuntimeState；
6. `_rebuild_child_owners()`；
7. 返回每一个加载 Session 的 `InvocationRef`，包括 Child。

所以当前支持 partial graph load：

```text
只 load Parent
只 load Child
Parent + Child 一起 load
```

新模型不再允许这种行为。

---

# 24. Target unload_session(): Graph Unload

Public API 可以继续叫：

```python
app.unload_session(root_ref, ...)
```

不必增加 `unload_graph()`。

但是语义修改成：

> **卸载 Root Invocation 所拥有的完整 Runtime Graph。**

例如：

```text
Root A
 ├ Child B
 │   └ Child D
 └ Child C
```

调用：

```python
app.unload_session(root_ref)
```

必须统一处理：

```text
A
B
C
D
```

不能只卸载 A。

---

# 25. Graph Unload Preconditions

Unload 是 clean residency transfer，不是 cancel。

因此保持当前核心限制：

```text
不能 unload 正在物理执行的 Runtime Graph
```

在 root graph exclusive gate 内：

1. 找到完整 descendants；
2. settle 所有 Session pending RuntimeEvent append；
3. 确保没有 live Task；
4. 确保没有 attached stream / active result reader；
5. Root/Child 当前状态必须处于允许的 quiescent 状态；
6. 然后才能 checkpoint/discard。

典型允许：

```text
Root waiting
all children terminal
```

```text
Root completed
all children terminal
```

```text
Root failed/cancelled
all descendants cancellation settled
```

不允许：

```text
Root waiting
Child running
```

```text
Root joining_children
Child running
```

---

# 26. RuntimeGraphCheckpoint

因为一个 Root Runtime Graph 可以包含多个 Session：

```text
SessionCheckpoint
```

不再适合作为 public unload result。

保留 `SessionCheckpoint` 作为每个 Session 的基础 snapshot 单元。

新增：

```python
@dataclass(frozen=True, slots=True)
class RuntimeGraphCheckpoint:
    root_session_id: str
    sessions: tuple[SessionCheckpoint, ...]
```

要求：

```text
sessions
```

必须构成以：

```text
root_session_id
```

为 Root 的完整 ownership closure。

不能：

```text
missing child
extra orphan child
multiple root
ownership conflict
```

---

# 27. Graph Checkpoint 不需要 Global Sequence

这一点非常重要。

Root 和 Child 仍然拥有完全独立的 RuntimeEvent stream：

```text
Root Session A:
sequence 1,2,3,...N

Child Session B:
sequence 1,2,3,...M

Child Session C:
sequence 1,2,3,...K
```

因此：

```python
RuntimeGraphCheckpoint
```

不应该增加：

```text
graph_sequence
```

每个：

```python
SessionCheckpoint
```

继续保存自己的：

```text
sequence
last_event_id
RuntimeState
```

GraphCheckpoint 只是：

> 一致性 ownership bundle。

不是新的 Event Stream。

---

# 28. Graph Checkpoint 一致性

Graph capture 时：

```text
exclusive root GraphGate
        │
        ▼
block graph transitions
        │
        ▼
settle pending event append for every session
        │
        ▼
capture each SessionCheckpoint
        │
        ▼
release graph gate
```

这样得到的是：

> 一个一致的 Root + descendants snapshot。

不需要给 Parent/Child RuntimeEvent 建立全局 total order。

---

# 29. Target unload implementation

建议：

```text
_unload_session(root_ref)
        │
        ▼
graph_session_ids =
    root + descendants
        │
        ▼
exclusive GraphGate(root)
        │
        ▼
settle all graph sessions
        │
        ▼
validate unloadability
        │
        ├── capture_checkpoint=True
        │       ↓
        │   RuntimeGraphCheckpoint
        │
        ▼
repository.discard_states(graph_session_ids)
        │
        ▼
cleanup transient runtime resources for all sessions
```

cleanup 包括：

```text
TaskRuntime wake events
graph/session transition locks
UserEvent journal
UserEvent sink errors
child capacities
child owners transient indexes
attached stream metadata
```

必须按整个 graph 清理。

---

# 30. Target load_checkpoint()

Public load 接受：

```python
RuntimeGraphCheckpoint
```

或者：

```python
AppCheckpoint
```

一个 RuntimeGraphCheckpoint 的 load：

```text
validate entire graph checkpoint
        │
        ▼
ensure no session conflict/live owner
        │
        ▼
install all Session RuntimeStates together
        │
        ▼
rebuild transient indexes
        │
        ▼
return Root InvocationRef
```

不能再：

```text
load only one Child Session
```

也不能：

```text
load Parent while one required Child checkpoint is missing
```

---

# 31. CheckpointLoadResult

当前：

```python
CheckpointLoadResult(
    invocations=tuple[InvocationRef, ...]
)
```

会返回每一个 loaded Session 的 ref。

修改后只返回 Root refs。

例如加载：

```text
Graph A:
  Root A + B + C

Graph D:
  Root D + E
```

结果：

```python
CheckpointLoadResult(
    invocations=(root_A_ref, root_D_ref)
)
```

Child 不返回 InvocationRef。

---

# 32. AppCheckpoint

当前：

```python
AppCheckpoint:
    sessions: tuple[SessionCheckpoint, ...]
```

建议调整为：

```python
AppCheckpoint:
    graphs: tuple[RuntimeGraphCheckpoint, ...]
```

例如：

```text
AutoAgentApp
 ├ Root Graph A
 │   ├ A
 │   ├ B
 │   └ C
 │
 └ Root Graph D
     ├ D
     └ E
```

得到：

```text
AppCheckpoint(
    GraphCheckpoint(A,B,C),
    GraphCheckpoint(D,E),
)
```

这样 External Host 的 persistence unit 与 Runtime ownership unit 一致。

---

# 33. close(capture_checkpoint=True)

当前 close：

1. detach streams；
2. cancel physical Runtime tasks；
3. settle RuntimeEvent；
4. capture 每一个 resident Session；
5. 返回 flat AppCheckpoint。

目标：

1. quiesce Runtime tasks；
2. settle every Session；
3. 找到所有 resident Root；
4. 按 Root Runtime Graph 分组；
5. capture 每个完整 graph；
6. 返回 `AppCheckpoint(graphs=...)`。

注意：

`close(capture_checkpoint=True)` 与 clean `unload_session()` 不完全相同。

Close 可以为了 process restart 保存：

```text
running
joining_children
```

等可恢复状态。

因此：

```text
AppCheckpoint
```

可以包含需要 `recover(root_ref)` 的 graph。

---

# 34. Recovery

External API：

```python
app.recover(root_ref)
```

只接受 Root `InvocationRef`。

Recovery 内部递归恢复：

```text
Root
 ├ Child
 │   └ GrandChild
 └ Child
```

Child 不需要 external recover API。

现有：

```text
_recover_session()
_accept_recovered_child_invocations()
_descendant_sessions()
_parent_plan()
```

可以继续复用和简化。

---

# 35. joining_children Recovery

如果 crash 前：

```text
Root status = joining_children
Child still running
```

恢复：

```text
load complete graph
        │
        ▼
recover(root_ref)
        │
        ▼
recover unfinished Child
        │
        ▼
Child terminal
        │
        ▼
Parent Child phase terminal
        │
        ▼
all children terminal
        │
        ▼
InvocationCompleted(parent.output)
```

不需要重新运行已经完成的 Parent Workflow body。

这也是必须把 Parent output 存入现有：

```python
InvocationState.output
```

的原因。

---

# 36. RuntimeEvent Sequence 设计

Parent 和 Child RuntimeEvent 继续独立记录。

例如：

```text
Root Session R

R:1 SessionOpened
R:2 InvocationStarted
R:3 NodeStarted
R:4 ChildInvocationPlanned
R:5 ChildInvocationPhaseChanged(opened)
R:6 ChildInvocationPhaseChanged(accepted)
...
R:15 InvocationJoiningChildren
R:16 ChildInvocationPhaseChanged(terminal)
R:17 InvocationCompleted
```

Child Session C：

```text
C:1 SessionOpened
C:2 InvocationStarted
C:3 NodeStarted
C:4 OperatorCallStarted
C:5 OperatorCallCompleted
C:6 NodeCompleted
C:7 InvocationCompleted
```

这里：

```text
R:16
```

和：

```text
C:7
```

之间不存在一个共同 graph sequence number。

只有 causal relation：

```text
Child C terminal Event durable
        ↓
Parent R Child phase -> terminal
```

这是正确的。

---

# 37. RuntimeEventSink Contract

RuntimeEventSink 继续按照：

```text
session_id + sequence
```

提供 per-session ordered append。

不要求：

```text
cross-session total ordering
```

也不需要 RuntimeEvent schema 增加：

```text
graph_sequence
```

Graph consistency 由：

```text
ownership state
GraphGate
durable child phase markers
```

保证。

---

# 38. Child lifecycle metadata 仍属于 Parent State

Parent 当前已经保存：

```python
ChildInvocationPlan(
    creation_id,
    parent_occurrence_id,
    mode,
    workflow_id,
    workflow_revision_id,
    units,
)
```

和：

```python
ChildUnitState(
    unit_index,
    session_id,
    invocation_id,
    input,
    phase,
)
```

这些结构继续保留。

不为了 `ChildHandle` 新增另一套 child mapping。

`ChildHandle` 只是这些已有 identity 的 runtime/public workflow value projection。

---

# 39. resident_invocations()

当前：

```python
app.resident_invocations()
```

返回 repository 中所有 current Invocation，包括 Child。

修改后：

> 只返回 resident Root Runtime Graph 的 `InvocationRef`。

例如 repository 中：

```text
Root A
 ├ B
 └ C

Root D
 └ E
```

结果：

```python
(A_ref, D_ref)
```

不是：

```python
(A_ref, B_ref, C_ref, D_ref, E_ref)
```

这样：

```text
one InvocationRef == one externally managed Runtime Graph
```

---

# 40. 删除 child_invocations()

Public：

```python
app.child_invocations(...)
```

删除。

Child hierarchy 是 Runtime internal structure。

需要 Trace / Debug 查看时，应通过：

```text
RuntimeEvent projection
Runtime inspector / trace layer
```

而不是把 Child 重新暴露成可控制的 App Invocation。

---

# 41. App API Target

目标生命周期 API：

```python
# Definition

register_workflow(...)
workflow_definition_snapshot(...)

register_capability(...)
register_operator(...)
set_operator_enabled(...)


# Root Invocation

invoke(...)
ainvoke(...)

submit_invoke(...)
asubmit_invoke(...)

stream(...)
astream(...)


# Root Control

status(root_ref)
astatus(root_ref)

join(root_ref)
ajoin(root_ref)

resume(root_ref, wait_id, response)
aresume(...)

stream_resume(...)
astream_resume(...)

cancel(root_ref)
acancel(root_ref)

recover(root_ref)
arecover(root_ref)


# Residency

resident_invocations()      # Root only

unload_session(root_ref, ...)
aunload_session(...)

load_checkpoint(...)
aload_checkpoint(...)


# App lifecycle

close(...)
aclose(...)
```

删除：

```text
child_invocations
achild_invocations
```

---

# 42. status() / join() 语义

本次不修改：

```python
status() -> InvocationResult
```

的 public type。

也不新增 `inspect()`。

但是补充 `joining_children`：

### status(root_ref)

可以即时看到：

```text
running
waiting
joining_children
completed
failed
cancelled
```

### join(root_ref)

仍然表示：

> 等待 externally meaningful stable boundary。

`joining_children` 不应该让 `join()` 返回。

因为：

```text
joining_children
```

表示 Root Graph 仍然有运行中的 structured work。

`join()` 应继续等待：

```text
waiting
or terminal
```

---

# 43. InvocationSubmission.status

当前：

```python
InvocationSubmission.status == "running"
```

并不严格准确。

Submission 真正保证的是：

```text
Invocation admission succeeded
```

Invocation 在调用者拿到 Submission 时可能已经迅速：

```text
waiting
completed
failed
```

因此建议作为低优先级 API cleanup：

方案 A：

```python
InvocationSubmission(ref=...)
```

删除 `.status`。

方案 B：

```text
status = "accepted"
```

这个修改与 Parent/Child 重构无强耦合，可单独实施。

---

# 44. 暂不增加 checkpoint(ref)

不增加：

```python
app.checkpoint(ref)
```

继续让 checkpoint 主要代表：

```text
residency handoff / process lifecycle boundary
```

即：

```python
unload_session(..., capture_checkpoint=True)

close(capture_checkpoint=True)
```

产生 checkpoint。

避免现在同时引入 hot snapshot + continued execution + historical replay 语义。

---

# 45. 暂不增加 inspect()

不增加：

```python
app.inspect(ref)
```

Node / Operator / Condition / Routing / Child hierarchy / timing 等 Debug 信息继续优先由：

```text
RuntimeEventSink
```

构建 Projection。

Core App API 保持 lifecycle-oriented。

---

# 46. RuntimeState 修改

主要修改：

```python
InvocationStatus = Literal[
    "created",
    "running",
    "waiting",
    "joining_children",
    "completed",
    "failed",
    "cancelled",
]
```

不增加：

```text
InvocationTerminalIntent
```

`InvocationState.output` 在：

```text
joining_children
```

状态下允许已经保存最终 Parent output。

Lifecycle validation 需要允许：

```text
status == joining_children
completed_at_us is None
output may be present
```

`completed` 后：

```text
completed_at_us != None
```

---

# 47. RuntimeEvent 修改

建议新增一个语义事件：

```python
InvocationJoiningChildren(output)
```

或者等价命名。

用途：

```text
Parent own workflow successful execution completed
but child graph not yet settled
```

Transition：

```text
status = joining_children
output = parent output
```

Child terminal 仍使用现有：

```text
ChildInvocationPhaseChanged(..., "terminal")
```

最后：

```text
InvocationCompleted(output)
```

完成 Parent。

不增加 graph event sequence。

---

# 48. WorkflowExecutor 修改

WorkflowExecutor 在准备：

```text
InvocationCompleted
```

前增加检查：

```text
all owned children terminal?
```

如果 Yes：

```text
InvocationCompleted
```

如果 No：

```text
InvocationJoiningChildren
```

然后当前 Parent drive 可以暂停。

最后一个 Child settle 后：

```text
wake / finalize parent
```

但不能重新执行 Parent 已完成的业务 Node。

---

# 49. _settle_child 修改

当前 `_settle_child()` 主要处理：

```text
Child phase terminal
+
await mode Parent wake
```

目标增加：

```text
if parent.status == joining_children
and all parent Child units terminal:
    complete parent
```

递归 settlement：

```text
child -> parent -> grandparent -> root
```

需要保证：

- 每一层 Parent completion event 写入自己的 Session stream；
- 每个 Session sequence 独立递增；
- 不跨 Session 共享 sequence。

---

# 50. Scheduler / Child Failure

Scheduler 不应该因为：

```text
spawn child failed
```

自动 fail Parent。

对于 await Child：

```text
failure -> owning occurrence error
```

走现有 Parent Workflow error routing。

对于 spawn Child：

```text
failure -> terminal child unit
```

不产生 Parent failure transition。

---

# 51. Compiler 修改

增加：

### ChildHandle contract

Spawn Child output：

```text
ChildHandle
```

Map spawn：

```text
list[ChildHandle]
```

### Child Wait validation

当 Workflow 作为 runtime child 使用：

```text
recursively reject Wait
```

错误：

```text
CHILD_EXTERNAL_WAIT_UNSUPPORTED
```

这属于 compile-time structural constraint。

---

# 52. Checkpoint 修改

保留：

```text
SessionCheckpoint
```

作为低层单 Session snapshot。

新增：

```text
RuntimeGraphCheckpoint
```

Public unload/load 使用 GraphCheckpoint。

`AppCheckpoint` 调整为 GraphCheckpoint collection。

所有 checkpoint serialization 都必须继续：

- exact schema；
- canonical record；
- digest validation；
- immutable isolated RuntimeState；
- per-session sequence preserved。

GraphCheckpoint 自己可以拥有：

```text
schema_version
id
root_session_id
captured_at_us
sessions
digest
```

但不拥有：

```text
graph_sequence
```

---

# 53. Repository 修改

`RuntimeRepository` 仍然以：

```text
session_id -> RuntimeState
```

存储。

不要改成：

```text
graph_id -> graph state
```

Parent/Child 依旧是多个独立 RuntimeState。

Graph 操作在 App / lifecycle coordination 层组合多个 Session。

这可以保持当前 RuntimeRepository 的：

```text
per-session commit
per-session sequence
per-session Event sink
```

模型不变。

需要的只是能够：

```text
capture N session checkpoints
install N states
discard N states
```

在 GraphGate 保证下作为 graph lifecycle operation 使用。

---

# 54. 不引入额外 Child Runtime Storage

本次明确不增加：

```text
ChildRuntimeState
ChildHandleMap
GraphState
GraphSequence
```

等新的 canonical storage。

继续使用：

```text
RuntimeState per Session

Parent.InvocationState.child_plans
```

表达 ownership。

`ChildHandle` 自带：

```text
child_session_id
child_invocation_id
workflow_revision_id
```

避免 handle lookup storage。

---

# 55. Migration Plan

## Phase 1 — Lifecycle

先修改：

```text
joining_children
Parent completion barrier
Child failure non-propagation for spawn
Parent fail/cancel descendant cancellation
```

保证核心 runtime invariant。

---

## Phase 2 — Identity/API

加入：

```text
ChildHandle
```

修改 spawn output。

删除：

```text
child_invocations()
Child InvocationRef external control
```

`resident_invocations()` 改成 root-only。

---

## Phase 3 — Graph Residency

加入：

```text
RuntimeGraphCheckpoint
Graph unload
Graph load
Root-only CheckpointLoadResult
AppCheckpoint graph grouping
```

---

## Phase 4 — Child Wait Restriction

Compiler 增加：

```text
Runtime Child cannot contain external Wait
```

保留 Root Wait。

---

## Phase 5 — Tests / Cleanup

删除旧的：

```text
direct child status/cancel/recover/unload
partial parent/child checkpoint load
```

语义测试。

新增 Graph lifecycle invariant tests。

---

# 56. Required Tests

## Parent successful completion barrier

```text
Root body completed
Child running
```

必须：

```text
Root.status == joining_children
```

不能：

```text
completed
```

Child terminal 后：

```text
Root -> completed
```

---

## Nested child completion

```text
Root
  Child
    GrandChild
```

GrandChild running：

```text
Child cannot completed
Root cannot completed
```

GrandChild terminal 后逐层 settle。

---

## Spawn child failure

```text
Root spawn Child
Child failed
```

如果 Root 没有显式处理：

```text
Child terminal
Root status unchanged by failure itself
```

Root own body完成后，等所有 Child terminal，然后可以正常 completed。

---

## Await child failure

```text
execution_mode="await"
Child failed
```

Owning Parent occurrence 得到 error。

Parent 是否最终 failed 由正常 Parent workflow error routing 决定。

---

## Root cancellation

```python
app.cancel(root_ref)
```

所有 descendants 收到 cancel。

Public cancel result 返回前：

```text
all descendants terminal
```

---

## Root failure

Root 自身失败：

```text
active descendants cancelled
```

Graph settlement 后返回 Root failure。

---

## ChildHandle

Spawn output：

```python
ChildHandle(
    child_session_id=...,
    child_invocation_id=...,
    workflow_revision_id=...,
)
```

不依赖 handle map。

Map spawn 顺序稳定。

---

## Root-only API

正常 Public API 不返回 Child InvocationRef。

构造 ChildHandle 后不能传给：

```text
status
join
resume
cancel
recover
unload
```

类型层直接不匹配。

---

## resident_invocations

两个 Root，各自多个 Child：

```text
resident_invocations()
```

只返回两个 Root refs。

---

## Graph unload

```text
Root + Child + GrandChild
```

unload root：

```text
all RuntimeStates removed
all transient graph resources removed
```

不能 partial unload。

---

## Graph checkpoint

Graph checkpoint 必须包含完整 ownership closure。

缺少 Child：

```text
load rejected
```

存在 orphan Child：

```text
load rejected
```

---

## Graph load

load GraphCheckpoint：

```text
all RuntimeStates installed together
ownership indexes rebuilt
CheckpointLoadResult only exposes root ref
```

---

## Per-session sequence

Graph：

```text
Root + Child
```

验证：

```text
Root RuntimeEvent sequence independent
Child RuntimeEvent sequence independent
```

不能引入 graph-wide sequence dependency。

---

## joining_children recovery

Checkpoint：

```text
Root joining_children
Child running
```

load + recover root：

```text
Child recovery
→ Child terminal
→ Root completed
```

Parent own completed Node 不重复执行。

---

## Child Wait rejection

Workflow 单独作为 Root：

```text
Wait allowed
```

同一个 Workflow 被作为 Child：

```text
compile rejected with CHILD_EXTERNAL_WAIT_UNSUPPORTED
```

---

# 57. Final Core Invariants

重构后必须始终成立：

```text
1. InvocationRef only represents Root Invocation.

2. ChildHandle only represents Runtime Child.

3. ChildHandle contains:
   child_session_id
   child_invocation_id
   workflow_revision_id

4. No extra durable ChildHandle mapping exists.

5. Every Child has exactly one Parent.

6. Every Child belongs to exactly one Root Runtime Graph.

7. Spawn means asynchronous structured child, not detached job.

8. Root completed implies every descendant terminal.

9. Spawn Child failure does not automatically fail Parent.

10. Await Child failure is handled through Parent occurrence/error logic.

11. Parent failure/cancel cascades cancellation to descendants.

12. Unload operates on Root + all descendants.

13. Load restores Root + all descendants as one graph bundle.

14. Partial Child unload/load is not supported.

15. RuntimeRepository remains per Session.

16. RuntimeEvent remains per Session.

17. Parent and Child do not share RuntimeEvent sequence.

18. RuntimeGraphCheckpoint has no graph sequence.

19. Child external Wait is not supported in the first implementation.

20. System Command design is explicitly deferred to the next design phase.
```

---

# 58. Summary

本次修改后的核心抽象可以浓缩为：

```text
InvocationRef
    =
Root Runtime Graph external handle

ChildHandle
    =
Parent Workflow internal child handle

Runtime Graph
    =
Root Invocation
+ recursively owned Child Invocations
```

生命周期：

```text
Parent body complete
        │
        ├── no child active
        │       ↓
        │   completed
        │
        └── child active
                ↓
        joining_children
                ↓
        all children terminal
                ↓
            completed
```

Failure / cancel：

```text
Parent fail/cancel
        ↓
cancel descendants
        ↓
settle graph
```

Persistence：

```text
unload(root)
     ↓
Root + every Child
     ↓
RuntimeGraphCheckpoint
```

Recovery：

```text
load GraphCheckpoint
        ↓
recover(root_ref)
        ↓
recursive graph recovery
```

Events：

```text
Root Session:  its own sequence
Child Session: its own sequence
```

不存在：

```text
global graph event sequence
```

这一模型保持了当前 Core 最重要的 per-session RuntimeState / RuntimeEvent 架构，同时把 Parent/Child 收敛成真正的 structured runtime hierarchy，并为下一阶段的 `ChildHandle + Runtime System Command` 留出了清晰边界。
