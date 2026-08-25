# AutoAgent V2

V2 的 distribution 名称仍为 `autoagent`，它是 V1 的直接替代版本，不支持与 V1 在同一
Python 环境中共存；安装 V2 wheel 会按正常升级语义替换旧版本。

V2 是独立的 Workflow Runtime，当前包含 Core、Host、本地持久化和只读 Tracing：

- Workflow 静态定义、名义值契约、Compiler、Revision 与不可变 `WorkflowIR`；
- DAG、多入口/多出口、并行 Fan-out/Fan-in、受控自然 Loop；
- 同步/异步 Operator、Map、StreamReducer、Wait、Capability 与 Child Workflow；
- `StateTransition -> StateOperationBatch -> StateReducer -> RuntimeState` 的唯一状态路径；
- 面向 SDK 的 `TraceEvent`/`UserEvent`，以及面向 Host 的 canonical `RuntimeEvent` sink；
- `RuntimeCheckpointBundle`、`AppCheckpoint`、加载与崩溃恢复；
- 同步/异步 invoke、submit、wait、resume、cancel、recover、stream API；
- App 级 Operator 全局并发限制和 `Map.max_parallelism` 局部限制。
- 标准 `autoagent.toml`、环境配置和 `AutoAgentHost` 生命周期；
- SQLite canonical RuntimeEvent Store、HTTP RuntimeEvent Sink 和父子 Checkpoint 重建；
- 本地只读 Tracing API、可恢复 SSE、Workflow 图、Timeline 与 State Inspector；
- `compile`、`invoke`、`trace` 基础 CLI。

Core 只执行 Workflow 并维护每个 Session 当前的 Runtime State。数据库、Event Store、
Server 和 UI 位于独立的 Host/Tracing 层，不反向进入 Core。

## 项目入口

```toml
# autoagent.toml
schema_version = 1

[project]
name = "example"
version = "0.1.0"

[[workflows]]
entrypoint = "workflows.research:workflow"
```

```bash
autoagent compile
autoagent invoke research --input '{"topic":"agents"}'
autoagent trace
```

Python Host 会按 `.env` 和进程环境创建 App/Sink；进程环境优先：

```python
from autoagent.host import AutoAgentHost

with AutoAgentHost.from_project(".") as host:
    result = host.invoke("research", {"topic": "agents"})

# 异步代码使用非阻塞工厂。
async with await AutoAgentHost.afrom_project(".") as host:
    result = await host.ainvoke("research", {"topic": "agents"})
```

默认 RuntimeEvent Store 为 `.autoagent/runtime.db`，Tracing Server 只监听
`127.0.0.1:8765`。完整配置和边界见 `HOSTING.md`，示例环境见 `.env.example`。

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

详细结构见 `CORE_DESIGN.md`，静态模型见 `workflow.md`，运行模型见 `runtime.md`，Host 与
Tracing 见 `HOSTING.md`，测试矩阵见 `TESTING.md`，后续边界见 `TODO.md`。
