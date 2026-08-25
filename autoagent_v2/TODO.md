# AutoAgent V2 后续事项

本文只记录当前实现之外仍有价值的工作。当前 Core 已完成静态 Workflow、受控 Loop、
Map、StreamReducer、Wait/Resume、Child await/spawn + Map、唯一 StateReducer、Host
RuntimeEvent sink 协议、公开 Trace/User Event、Checkpoint Bundle 和同步/异步 App API。
以下项目是 Host/Harness、可选捕获模式或工程工具，不是当前 Full Core 的未闭合语义。

## P1：中心化 Host 与 Harness 原语

### RuntimeEvent Store 的生产部署扩展

本地 SQLite Store 已实现 Event 幂等、Session sequence/hash chain、事务内索引/Trace 投影和
父子 Checkpoint 重建；HTTP Sink 已使用 Event id 作为幂等键。生产部署仍需要：

- 远程接收端的 commit 后丢失响应测试与请求查询；
- 多 Host Session lease、Outbox、队列容量和运维告警；
- 正式数据库 schema migration 和 PostgreSQL backend；
- 外部持久化确认不能反向成为 Runtime State 的第二份权威状态。

### Command、Mailbox 与 Signal

在 Server/Host 层定义可持久化、可去重的 Command envelope，再接入：

- Resume/Cancel 的 command id 与 expected state version；
- `SendSignal`/`AwaitSignal` 的原子“先检查、再等待”；
- Parent/Child message、question/reply、guidance 和 permission；
- 长时间 Operator 只在协作安全点读取消息，不能伪装成强制修改已发出的同步调用。

外部消息使用 Command 驱动当前 Invocation，不直接从普通 Edge 激活任意中间 Node。

### 中心化 Server

- 本地只读 Tracing Server/UI 已实现，不持有 App，也不提供执行命令；
- 远程 RuntimeEvent 接收、独立 UserEvent Store、Artifact Store 与 Outbox；
- Invoke/Submit/Wait/Resume/Cancel/Recover/Stream 服务接口；
- Session admission/lease、鉴权、多租户、幂等、限流和优雅关闭；
- 历史 Trace、任意 state version 查询、Replay、Fork 与 UI。

Schema migration 在 Event 进入当前 Core codec 前由 Host 完成，Core 继续只接受精确当前
schema。

### 远程与隔离执行

- Process/Remote Operator Executor；
- 远程 Child Workflow 的 admission、status、await 和 cancel；
- 外部副作用的稳定幂等键与配额服务；
- Invocation 级 Tool/Capability Registry。

## P2：Agent SDK

- LLM Operator、Tool Dispatch 和动态工具描述；
- 静态 Agent Loop 模板；
- Coding Agent Workflow 生成、Compiler 诊断与修复 Skill；
- Harness 通过 Child、Command、Mailbox、Capability 和 Wait 组合，不热修改运行中的
  `WorkflowIR`。

## P3：可选捕获模式与工程工具

当前 StateOperation/Reducer 语义已经与 RuntimeEvent 信封分离。后续可实现
Full/Standard/Minimal capture profile，但三种模式只能调整 Event 聚合和 Trace 投影，
不能删除恢复所需的 StateOperationBatch；以最终 RuntimeState、Checkpoint、Resume 和
Recovery 等价性验收。

工程层可补充 Ruff 与 Pyright/Mypy 配置、Port 静态类型验收和持续 import graph 检查；
这些不改变 Core 运行契约。

### Live transition 的长期扩展性

长串行图仍会超线性：每个 transition 都完整校验持续增长的 NodeOccurrence 和
OperatorCall 历史。DAG Scheduler 与 Scheduler delta 的 map 复制不是当前瓶颈，契约
也已经复用编译后的 Pydantic adapter。若真实 workload 需要数百个以上 Node，应先明确
终态 Occurrence/Call 是否必须常驻当前 Checkpoint；只有收窄权威 RuntimeState 的历史
范围，才可能在保留逐 transition 完整校验的同时改善渐进复杂度。不要为此增加第二套
State codec、增量镜像或绕过权威 Validator。

## 明确不在当前模型中

- 通用 Node 多发生、ActivationGroup 和跨 Trigger Join；
- 运行中动态修改 Workflow 图；
- Map item 沿普通 Edge 独立扩散；
- Stream Chunk 反复激活普通 Node 或 Stream Edge；
- 同一 Session 内同时运行多个 Invocation；
- 公开结果暴露 RuntimeEvent、StateOperation、历史游标或 replay API；
- Operator Retry、Fallback、Timeout 的既定组合策略；
- Core 内数据库、历史归档、schema migration 或 UI。

## 统一验收

在 `autoagent_v2/` 下运行：

```bash
../.venv/bin/python -m unittest discover -s tests -v
../.venv/bin/python -m compileall -q autoagent tests
../.venv/bin/python -m tests.benchmarks.benchmark_full_core
git diff --check
```

每个新增 `test_*` 方法第一行必须用简短 docstring 说明行为或失败边界。
