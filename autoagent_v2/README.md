# AutoAgent V2 Core

V2 是独立的 Workflow Core，当前包含：

- Workflow 静态定义、名义值契约、Compiler、Revision 与不可变 `WorkflowIR`；
- DAG、多入口/多出口、并行 Fan-out/Fan-in、受控自然 Loop；
- 同步/异步 Operator、Map、StreamReducer、Wait、Capability 与 Child Workflow；
- `StateTransition -> StateOperationBatch -> StateReducer -> RuntimeState` 的唯一状态路径；
- 面向 SDK 的 `TraceEvent`/`UserEvent`，以及面向 Host 的 canonical `RuntimeEvent` sink；
- `RuntimeCheckpointBundle`、`AppCheckpoint`、加载与崩溃恢复；
- 同步/异步 invoke、submit、wait、resume、cancel、recover、stream API；
- App 级 Operator 全局并发限制和 `Map.max_parallelism` 局部限制。

Core 只执行 Workflow 并维护每个 Session 当前的 Runtime State。数据库、Event Store、
Server、远程执行、Mailbox 和 UI 不属于 Core。

## 公开结果边界

- `InvocationResult`：终态或 waiting 边界、Trace/User 事件和最新 Checkpoint。
- `InvocationSubmission`：可靠接纳后的 `InvocationRef` 和 Checkpoint。
- `InvocationUpdate`：`stream/astream` 的一条 Trace/User 事件；仅安全恢复边界携带
  Checkpoint；Node started 会在用户 Hook/Operator 副作用前给出 write-ahead Checkpoint，
  流最后返回 `InvocationResult`。
- `close/aclose`：返回包含所有独立 Root 及其 Child 图的 `AppCheckpoint`。

`RuntimeEvent` 不放进公开调用结果。需要持久化和重建状态的 Host 通过
`runtime_event_sink` 接收它；单独使用 Core 的 SDK 用户直接保存结果中的 Checkpoint。

## 验证

在 `autoagent_v2/` 下运行：

```bash
../.venv/bin/python -m unittest discover -s tests -v
../.venv/bin/python -m compileall -q autoagent tests
../.venv/bin/python -m tests.benchmarks.benchmark_full_core
```

详细结构见 `CORE_DESIGN.md`，静态模型见 `workflow.md`，运行模型见 `runtime.md`，
测试矩阵见 `TESTING.md`，后续边界见 `TODO.md`。
