# Runtime、State Operation 与 Event

## 两层模型

Runtime State 的权威变更和持久化记录是两个不同层次：

```text
Runtime 行为
  -> StateOperation
  -> StateOperationBatch              # 原子状态提交
  -> StateReducer
  -> RuntimeState
  -> pending batches + Runtime Logs
  -> RuntimeEvent                     # 可调节的记录信封
```

`StateReducer.apply_batch()` 只解释 `StateOperationBatch`，不根据 Event 名称重新运行
旧版调度、Policy 或执行逻辑。语义转换对象只用于当前版本规划 State Operation；
持久化后的重放以 Event 中的 Operation Batch 为准。

## State Operation Batch

`StateOperation` 使用严格的 `add | replace | remove`、规范 State 路径和可持久化值。
`StateOperationBatch` 包含：

```text
id
from_state_version
to_state_version
occurred_at_ns
operations[]
```

一个 Batch 必须把版本精确推进一位。Reducer 先在候选记录上应用全部 Operation，
再恢复并校验完整 typed RuntimeState；任一 Operation 或 State schema 校验失败时，
旧 State 保持不变。

## Runtime Event

Runtime Event 是 UI、Trace、持久化和恢复使用的信封，包含：

```text
session event sequence
from_state_version / to_state_version
有序 StateOperationBatch[]
有序 RuntimeLog[]
```

Runtime Log 记录语义名称、身份、因果、时间和对应 `state_version`。一个 Event 可以
携带一个或多个 Batch，也可以只记录没有改变 State 的观察点。

Full 模式当前使用 `journal.append()`，即每次转换立即 `stage + flush`。Journal 同时
提供独立 `stage()` 和 `flush()`：后续 Standard/Minimal 或宿主 Capture Policy 可以
调整 Event 边界，而不改变状态正确性。pending Batch 已在进程内 State 生效，但在
形成并被外部可靠保存的 Event 前不具备进程崩溃恢复能力，这一段就是明确的恢复窗口。

## Replay 与 Recovery

- `replay(through_sequence=N)` 恢复到第 N 个 Runtime Event。
- `replay(through_state_version=V)` 可以停在某个 Event 内第 V 个 Batch 之后。
- `append_many()` 先在隔离 State 上验证完整 Event 前缀，再一次性导入 Journal；
  中间冲突不会留下半个恢复 Session。
- UI Replay 可以查看任意已持久化 State version；执行 Recovery 仍需遵守 Node 的
  `recovery_mode`，不能把所有可重放状态都当作安全续跑点。
- Wait 保持 waiting；进程恢复时 running OperatorCall 标记为 lost，只有允许安全
  重放的 NodeOccurrence 才重新 Ready。

## 原子业务边界

Node 成功终态的同一 Batch 同时包含：

```text
Node Output + ContextPatch + Edge/Scheduler Delta
```

并行 ContextPatch 使用 Node 的 `started_state_version` 与路径版本检测相同、祖先和
子路径写冲突。Event sequence 只标识持久化信封，不再承担 Context 冲突版本。

## Core 边界

Core 只维护进程内 State、pending buffer，并产生 Runtime Event/User Event。
数据库、Event Store、Outbox、消息发送、Server、远程 Executor 和 UI 不属于 Core。
外部持久化层必须在可靠接纳 Event 后才推进自己的导出游标；Snapshot 只能作为
重放加速，不能成为第二份权威状态日志。
