# AutoAgent System Command Design

## 1. 目标

在现有 AutoAgent Runtime 之上增加一组 **System Command**，用于 Workflow 在运行过程中操作 Runtime 本身。

System Command 与普通 Operator 的职责必须分离：

- **Operator**：操作外部世界，例如 LLM、HTTP、数据库、MCP、文件系统等。
- **System Command**：操作 AutoAgent Runtime，例如创建 Workflow Runtime、等待、查询状态、取消、恢复 Wait、发送 Signal 等。

这样可以让 Agent Harness 把 System Command 直接暴露为 Tool，同时保持 Runtime 状态变更仍然经过受控的 RuntimeEvent / State transition，而不是允许普通 Operator 直接修改 Runtime。

## 2. 基本原则

### 2.1 System Command 是 Runtime Primitive

System Command 由 Core 定义，不允许用户任意实现 Runtime mutation。

普通 Workflow Node 可以执行 System Command，使其与 Operator 在 Workflow authoring 层保持一致，但 Runtime 内部使用独立执行逻辑。

### 2.2 External Control 与 Runtime Control 分离

外部控制继续使用：

```python
InvocationRef
```

Workflow 内部的 System Command 使用：

```python
RuntimeHandle
```

建议：

```python
class RuntimeHandle:
    session_id: str
    invocation_id: str
    workflow_id: str
    workflow_revision_id: str
```

两者字段可以相近，但语义不同：

- `InvocationRef`：App / Host 控制 Root Invocation。
- `RuntimeHandle`：Workflow 内部通过 System Command 操作 Runtime。

System Command 不绑定 Parent / Child 概念，而只操作 `RuntimeHandle`。Parent / Child 只是 Handle 的一种来源和 ownership 关系。

## 3. Workflow 调用模型

删除当前：

```python
Node(executable=workflow, execution_mode="await" | "spawn")
```

不再把 Workflow 本身作为特殊 executable，也不再需要 `execution_mode`。

改为显式 System Command：

### Spawn

```text
Spawn(workflow)
    → RuntimeHandle
```

创建一个新的 Runtime Invocation，并立即返回 Handle。

适合后台 Agent、并行 Sub-Agent、动态任务等。

### AwaitWorkflow

```text
AwaitWorkflow(workflow)
    → RuntimeBoundary
```

创建一个 Workflow Runtime，并等待其到达稳定边界。

用于替代原来的：

```text
Workflow + execution_mode="await"
```

### Await

```text
Await(handle)
    → RuntimeBoundary
```

等待已经存在的 RuntimeHandle 到达稳定边界。

### Status

```text
Status(handle)
    → RuntimeBoundary
```

立即返回当前状态，不阻塞调用方。

## 4. Runtime Boundary

`Status`、`Await`、`AwaitWorkflow` 等内部控制操作应尽量返回统一的 Runtime 状态描述，例如：

```text
RuntimeBoundary
    handle
    status
    waits
    output
    error
```

稳定边界主要包括：

- waiting
- completed
- failed
- cancelled

`Status` 可以额外返回 running 等即时状态。

Target 进入 Wait 时，调用者可通过 `Status` 或 `Await` 发现 Wait，然后使用 `Resume` 回答。

## 5. Wait / Resume

继续复用现有 `Wait`，不区分 `WaitExternal`、`WaitParent` 等不同类型。

Wait 的本质始终是：

> 当前 Runtime 执行挂起，等待一个 response。

区别只在于谁拥有 Resume 权限：

- Root Wait：通常由 Host 通过 `app.resume(...)` 恢复。
- RuntimeHandle 指向的 Invocation Wait：由 Workflow 内部通过 `Resume(handle, wait_id, response)` 恢复。

因此不增加 Parent-specific Wait。

## 6. 其他核心 System Command

第一阶段建议包含：

```text
Spawn(workflow)
AwaitWorkflow(workflow)
Await(handle)
Status(handle)

Wait(request)
Resume(handle, wait_id, response)

Cancel(handle)

SendSignal(handle, endpoint, payload)
```

另外建议支持两个并发 / 时间相关 primitive：

```text
AwaitAny(handles)
Sleep / Timer
```

### AwaitAny

等待多个 Handle 中任意一个到达稳定边界。

主要用于 Supervisor / Manager Agent 同时管理多个后台 Agent。

### Sleep / Timer

提供可持久化的时间等待能力。

不能简单依赖 `asyncio.sleep()`，因为 Runtime crash / recovery 后必须保持原 deadline，而不是重新计时。

## 7. Signal

Signal 与 Wait 是不同机制。

- **Wait**：Receiver 主动停止执行，等待 response。
- **Signal**：Sender 在任意时间向正在运行的 Invocation 注入异步消息，Receiver 不需要提前等待。

`SendSignal` 可以出现在 Workflow 的任意位置。

Signal 不应该：

- 自动 Resume Wait；
- 自动改变 Invocation status；
- 自动启动新的普通 Workflow branch；
- 强行中断正在运行的 Operator。

Signal 只是将信息可靠地送达目标 Runtime。

## 8. Signal Endpoint

Workflow 显式声明可接受的 Signal：

```text
Workflow
    nodes
    edges
    signal_endpoints
```

例如：

```text
signal_endpoints:
    user_message
    requirement_update
    feedback
```

`signal_endpoint` 不是普通 Scheduler Node：

- 没有 Edge；
- 不产生普通 NodeOccurrence；
- 不参与 Workflow completion；
- 同一个 Invocation 生命周期中可以触发多次；
- UI 可以将其特殊渲染成类似 Entry Node 的元素。

外部 Host：

```text
app.signal(ref, endpoint, payload)
```

Runtime 内部：

```text
SendSignal(handle, endpoint, payload)
```

两者最终进入同一套 Signal delivery 逻辑。

## 9. Mailbox

Signal 被 Target Runtime 接受后写入该 Invocation 的 Mailbox。

Mailbox 放在 `InvocationState` 中，与 Invocation Context 并列，例如：

```text
InvocationState
    context
    mailbox
    scheduler
    ...
```

Mailbox 属于 Runtime State，但业务如何消费消息由用户决定。

典型流程：

```text
SignalReceived
    ↓
append mailbox
    ↓
Operator / ContextBuilder 读取 mailbox
    ↓
业务处理
    ↓
ContextPatch 删除已处理消息
```

Runtime 不判断“用户是否已经处理消息”。

建议 Mailbox 使用基于 Signal ID 的结构，而不是简单 List，以避免并发 append 与用户删除产生覆盖冲突。

Runtime 仍需保留必要的 Signal delivery / dedup 信息，避免 crash recovery 后重复投递已接收的 Signal。

## 10. Runtime Handle 与关系模型

System Command 不使用：

```text
AwaitChild
CancelChild
SendSignalToParent
SendSignalToChild
```

而统一使用：

```text
Await(handle)
Cancel(handle)
SendSignal(handle)
```

Parent / Child 关系负责：

- RuntimeHandle 的产生；
- ownership；
- Runtime Graph 生命周期。

Command 本身只关心目标 Handle。

Runtime execution context 可以提供只读信息，例如：

```text
self_handle
owner_handle
```

使 Child 可以向 Owner 发 Signal，但 Command 本身不需要理解 Parent / Child。

## 11. 与 Agent Harness 的关系

这套 Runtime Primitive 可以直接支撑常见 Agent Harness 能力：

- Agent as Tool：`AwaitWorkflow`
- Background Sub-Agent：`Spawn`
- Parallel Sub-Agent：多个 `Spawn`
- Supervisor polling：`Status`
- Supervisor blocking：`Await` / `AwaitAny`
- Human-in-the-loop：`Wait` / `Resume`
- Agent-to-Agent communication：`SendSignal`
- 用户运行过程中追加消息：`app.signal`
- Cancellation：`Cancel`
- Nested Agent：Runtime 内继续 `Spawn`
- Durable long-running Agent：RuntimeEvent + Checkpoint + Timer
- Crash recovery：沿用现有 Runtime Graph / Event replay 体系

Handoff、Group Chat、Memory、Guardrail、Tool Approval 等更高层 Agent 能力不作为新的 Runtime Primitive，优先使用上述基础能力在 Agent / Harness 层组合实现。

## 12. 实现方向

System Command 在 Workflow authoring 中仍可作为 Node executable 使用，但内部不经过普通 OperatorRegistry。

建议区分两类执行：

```text
Immediate Command
    Spawn
    Status
    Resume
    Cancel
    SendSignal

Suspending Command
    AwaitWorkflow
    Await
    AwaitAny
    Wait
    Sleep / Timer
```

Suspending Command 应将等待条件持久化到 RuntimeState，并由对应 RuntimeEvent 唤醒。

所有具有 Runtime side effect 的 Command 都必须支持 crash recovery / idempotency，避免恢复后重复 Spawn、重复 Signal、重复 Resume 等。

Parent、Child 各自继续保持独立 RuntimeEvent sequence；System Command 不引入 Graph-global sequence。
