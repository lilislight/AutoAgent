# AutoAgent V2：Host / Sink / Server / CLI 模块设计

> 基于 `refactor` 分支当前 Core（检查时 HEAD: `f2a532443f992a7a0ccc58395fddb8ac61112cc8`）。
>
> 目标：Core 功能基本冻结后，明确外围模块边界、依赖关系、部署方式和实现顺序。本文只描述模块级设计，不展开具体 API contract。

---

## 1. 当前 Core 的边界

Core 负责 **Workflow 的正确执行语义**，包括：

- Workflow / Compiler / IR
- Scheduler / Executor / Operator
- Runtime State / Runtime Event / State Delta
- Session / Invocation / Parent-Child Graph
- Wait / Signal / System Command
- Checkpoint / Load / Recover
- Invoke / Resume / Cancel / Status / Join
- `RuntimeEventSink` / `UserEventSink` 外部输出端口

Core 不负责：

- 长期持久化
- 数据库 / 文件 / 远程传输
- Trace 查询
- HTTP API
- UI
- CLI
- 多 Host 路由与服务发现

原则：**外围模块适配 Core，不为了数据库、HTTP、UI 在 Core 中增加特殊逻辑。**

---

## 2. 当前 unload / checkpoint 的真实行为

### 2.1 Invocation 完成不会自动 unload

Terminal Invocation 会继续保留在 Core 中。

同一个 Session 再次 `invoke()` 时：

1. Core 检查旧 Invocation 已 terminal；
2. 确保所有 Child 已完成并且 Sink ACK 已 settle；
3. 提交新的 `InvocationStarted`；
4. 新 Invocation 替换旧 Invocation workspace；
5. Root Session 继续存在，Session Context 保留；
6. 旧 Child Session 被回收。

因此正常的同 Session 连续调用 **不需要 load checkpoint**。

### 2.2 `unload_session()` 是显式的 Root Graph 卸载

当前卸载只能针对 Root Invocation，并要求整个 Graph quiescent：

- 没有 live runtime task；
- 没有 control operation；
- 没有 active result reader / attached stream / recovery；
- pending Runtime Event 已 settle；
- Invocation 至少处于 `waiting / settling / completed / failed / cancelled`；
- terminal graph 不允许还有未 settle 的 Child ownership。

卸载对象不是单独一个 Session，而是：

```text
Root Session
  + all descendant Child Sessions
```

成功后整个 Graph 从 Core resident memory 中移除。

### 2.3 Checkpoint 是 opt-in

```text
unload_session(..., capture_checkpoint=False)
```

默认直接卸载，不构造 checkpoint。

```text
unload_session(..., capture_checkpoint=True)
```

才返回完整 `RuntimeGraphCheckpoint`。

Checkpoint 包含 Root + 所有仍属于该 Graph 的 Session State，可以恢复 waiting / settling / terminal graph。

### 2.4 Capture 与网络大小是两个问题

Core 内部 checkpoint capture 使用 immutable RuntimeState 的可信引用，capture 本身是浅操作。

真正可能昂贵的是：

```text
checkpoint.to_record()
  -> 完整序列化
  -> 压缩
  -> 数据库 / 文件 / 网络
```

因此第一版不应该因为担心 checkpoint 大，就修改 Core 的 unload 语义。

---

# 3. 总体模块划分

```text
                        UI
                         │
                         ▼
                       Server
                    ┌────┴────┐
                    │         │
                 Runtime    Trace
                 Control    Query
                    │         │
                    ▼         ▼
                   Host    TraceStore
                    │
                    ▼
                   Core
                    │
                    ▼
             exactly one Sink
```

模块定义：

| 模块 | 核心职责 |
|---|---|
| Core | Workflow 执行语义 |
| Host | 持有 Core、生命周期、Session residency、唯一 Sink |
| Sink | Runtime/User Event 的唯一外部出口 |
| TraceStore | 历史 Trace 的读模型 |
| Server | HTTP / Streaming / Ingest / Control |
| CLI | 面向人和 Coding Agent 的操作接口 |
| UI | Runtime / Trace 可视化 |

---

# 4. Host

## 4.1 定位

`AutoAgentHost` 是一个长期运行的 Runtime 容器：

```text
AutoAgentHost
├── AutoAgentApp
├── one Sink
├── Workflow registration
├── lifecycle
├── Session residency
├── checkpoint lifecycle
└── optional local TraceService
```

Server 不应该直接访问 `host.app._repository` 等内部状态。

Runtime 操作统一经过 Host facade：

```text
Host.invoke
Host.status
Host.resume
Host.cancel
Host.signal
Host.unload
Host.load
Host.recover
Host.close
```

Host 内部再调用 Core。

## 4.2 Session Residency

第一版采用非常保守的策略：

**Invocation terminal 后 Session 保持 resident。**

```text
Invocation A completed
        ↓
Session remains resident
        ↓
Invocation B
        ↓
直接复用 Core 中的 SessionState
```

第一版不做自动 terminal unload。

后续再增加：

- idle TTL
- LRU eviction
- memory high watermark eviction
- manual eviction
- Pod draining eviction

Eviction 流程：

```text
quiescent graph
   ↓
capture checkpoint
   ↓
persist checkpoint
   ↓
Core.unload_session()
```

只有真正被 eviction 的 Session，下一次调用才需要 restore。

---

# 5. Sink

## 5.1 一个 Host 只允许配置一个 Sink

不支持：

```text
Core -> DB Sink
     -> File Sink
     -> Remote Sink
```

避免产生 ALL / ANY / PRIMARY / QUORUM 等复杂 ACK policy。

Core 永远只有：

```text
Runtime Event
    ↓
one Sink
    ↓
ACK
    ↓
publish Runtime State
```

Host 层可以把同一个对象同时传给 Core 的 `RuntimeEventSink` 与 `UserEventSink` 端口，但用户配置上仍然只有 **一个 Sink 实例**。

## 5.2 Sink 类型

### DatabaseSink

```text
append
 ↓
DB commit
 ↓
ACK Core
```

适合本地 / 单机可靠模式。

### FileSink

```text
append
 ↓
file/WAL durable write
 ↓
ACK Core
```

适合简单运行、离线调试、测试。

### RemoteSink

```text
Core
 ↓
RemoteSink
 ↓
Central Ingest API
 ↓
durable acceptance
 ↓
ACK Core
```

用于 GKE / 分布式 Host。

### BufferedSink

BufferedSink 本身仍然是 **唯一 Sink**，只是内部带 writer：

```text
Core
 ↓
BufferedSink
 ↓
accepted buffer / local WAL
 ↓
ACK Core

background writer
 ↓
DB / File / Remote
```

如果只是 memory queue，则进程崩溃可能丢失已 ACK Event；如果需要强可靠，应使用 local WAL / durable outbox。

---

# 6. Trace 数据模型

## 6.1 Historical Trace

历史数据以持久化 Store 为准：

```text
TraceStore
├── workflows / revisions
├── sessions
├── invocations
├── runtime events
├── user events
├── node executions
├── operator calls
├── waits / signals
└── projections
```

TraceService 不直接依赖 Core 内存来回答历史查询。

## 6.2 Running Trace：Store + Live Tail

BufferedSink 场景不能每产生一条 Event 都：

```text
write DB -> query DB -> UI
```

而应该：

```text
DB durable prefix      Live Buffer tail
      │                      │
      └────────┬─────────────┘
               ▼
         TraceService merge
               ▼
              UI
```

例如：

```text
Core sequence       = 1050
Sink accepted       = 1050
DB durable sequence = 1000
```

UI 看到：

```text
DB:     1..1000
Buffer: 1001..1050
```

通过 `(session_id, sequence)` / `event_id` merge + deduplicate。

DB writer durable 到 1020 后，Buffer 即可释放 `<=1020` 的 tail。

UI 打开后可进一步使用 SSE / WebSocket 直接订阅新的 live event。

---

# 7. Checkpoint 与 RemoteSink

RemoteSink **不意味着每个 Invocation 都从远程重新 load checkpoint**。

正常路径：

```text
same Session
   ↓
same Host owner
   ↓
Core resident memory
   ↓
next Invocation
```

只有以下情况才需要 checkpoint restore：

- Host 主动 eviction；
- Host restart；
- Pod migration / crash；
- manual unload；
- deployment drain。

## 7.1 CheckpointStore 与 Event Sink 是不同概念

Checkpoint persistence 不参与每条 Runtime Event 的 ACK。

因此即使 Host 只有一个 Event Sink，也可以存在独立的 checkpoint storage capability。

本地：

```text
DatabaseSink + same DB checkpoint table
```

远程：

```text
RemoteSink -> Central Event Store
Central -> materialized checkpoint / snapshot store
```

Central Server 可以利用 canonical Runtime Event + StateDelta 增量维护最新 RuntimeState，并周期性生成 checkpoint，避免每次恢复从 event 1 replay。

恢复使用：

```text
latest checkpoint @ sequence N
          +
      tail events
          ↓
latest state
```

第一版甚至可以更简单：只在显式 unload / shutdown 时保存完整 checkpoint。

## 7.2 当前完整 terminal checkpoint 可能偏大

当前 Root terminal Invocation 没有类似 ChildResult 的 root-level compact representation。

因此 terminal graph 的完整 checkpoint 可能包含较大的旧 Invocation workspace。

第一版处理：

1. 默认不自动 unload terminal Session；
2. checkpoint 只在低频 eviction / shutdown 时传输；
3. checkpoint 使用压缩；
4. 用实际 benchmark 决定是否需要优化。

后续若确认是瓶颈，再考虑 Core 增加 root terminal compaction / Session continuation 格式，而不是现在提前修改 Core。

---

# 8. Server

Server 应该是可组合组件，而不是假设永远与 Host 同进程。

```text
server/
├── runtime/     # 操作 live Host
├── trace/       # 查询 TraceStore
├── ingest/      # RemoteSink event ingestion
├── control/     # Host routing / proxy
├── streaming/   # SSE / WebSocket
└── app.py       # composition
```

## 8.1 Runtime API

只操作 live Runtime：

- invoke
- status
- join / wait
- resume
- cancel
- signal
- recover
- unload / load（管理接口）

依赖：

```text
Runtime Router -> Host facade
```

禁止直接操作 Core private state。

## 8.2 Trace API

- list workflows / revisions
- list sessions
- list invocations
- invocation detail
- runtime events
- user events
- state / projection
- node execution
- operator call
- edge / condition evaluation
- parent-child graph

依赖：

```text
Trace Router -> TraceService -> TraceStore (+ LiveTail)
```

不依赖 live Host 才能查询历史。

## 8.3 Ingest API

给 `RemoteSink` 使用：

- idempotent Event acceptance
- Session sequence validation
- Runtime/User Event ingestion
- durable ACK
- materialized state / checkpoint update

## 8.4 Control Router

中心化部署时负责：

```text
session_id -> host_id
```

以及：

```text
UI/CLI
  ↓
Central Server
  ↓
Host Runtime API
  ↓
Core
```

UI 不直接访问 GKE Pod。

---

# 9. 两种部署模式

## 9.1 Standalone

```text
┌─────────────────────────────┐
│ process                     │
│                             │
│ Host                        │
│ ├── Core                    │
│ └── Database/File Sink      │
│                             │
│ Server                      │
│ ├── Runtime API             │
│ └── Trace API               │
└─────────────────────────────┘
              ↑
              UI / CLI
```

Trace：

```text
TraceStore + optional LiveTail
```

Runtime 操作：

```text
Server -> local Host
```

## 9.2 Distributed / Centralized

```text
GKE Host A ── RemoteSink ──┐
GKE Host B ── RemoteSink ──┼──> Central Ingest
GKE Host C ── RemoteSink ──┘        │
                                    ▼
                              Central Store
                                    ▲
                                    │
                               Trace Service
                                    ▲
                                    │
                                  UI/CLI
```

Runtime control：

```text
UI / CLI
   ↓
Central Control
   ↓
Host Registry
   ↓
owner Host
   ↓
Core
```

Host 与 Central Trace 完全解耦；即使 Host 已销毁，历史 Trace 仍然存在。

---

# 10. CLI

CLI 主要面向 **Coding Agent + 开发者**，本身不包含 Runtime 逻辑。

```text
CLI -> AutoAgentClient -> Server API
```

主要命令：

```text
workflow list/show/check
invoke
status
wait
resume
cancel
signal
trace
state
events
inspect node
inspect call
inspect edge
compare
```

从第一版开始支持：

- `--json`
- `--jsonl`
- stable exit code
- machine-readable error code

Coding Agent 不应该解析 Rich/Table 文本。

---

# 11. 模块依赖规则

推荐依赖方向：

```text
Core
 ↑
Host
 ↑
Runtime Server

Core event schema
 ↑
Trace / Ingest Server

Server
 ↑
CLI / UI
```

更完整地：

```text
CLI ──────────────> Server Client
UI ───────────────> Server API

Runtime Server ───> Host ───> Core
Trace Server ─────> TraceService ───> TraceStore
Ingest Server ────> TraceStore / Materializer
Central Control ──> HostRegistry ───> Remote Host Client

Host Sink adapters ────────────────> Core Sink Ports
```

禁止：

```text
Core -> Host
Core -> Server
Core -> Database
Core -> HTTP
Host -> CLI
TraceService -> Core private repository
UI -> Core
UI -> GKE Pod directly
```

---

# 12. 推荐实现顺序

## Phase 1 — Host Foundation

先只实现最薄的 Host：

- `AutoAgentHost`
- 持有一个 `AutoAgentApp`
- 配置唯一 Sink
- workflow/operator registration
- invoke/status/resume/cancel/signal facade
- resident session 管理
- explicit checkpoint/unload/load/recover
- close lifecycle

第一版 **不自动 eviction**。

目标：不用 Server，Host 自己即可完整运行 Core。

---

## Phase 2 — Local Sink + TraceStore

先做一个数据库实现，建议 SQLite 起步：

- Runtime Event append
- User Event append
- idempotency
- Session sequence
- workflow revision snapshot
- checkpoint persistence
- basic TraceStore query

之后再做 FileSink。

目标：解决完整的 standalone persistence + historical trace。

---

## Phase 3 — BufferedSink + Live Tail

实现：

- single top-level BufferedSink
- queue / WAL
- background writer
- accepted sequence / durable sequence
- backpressure
- retry
- live tail subscription
- TraceStore + LiveTail merge

目标：Core execution 与慢数据库写入解耦，同时保证 Trace UI 可以看到最新运行数据。

---

## Phase 4 — TraceService

把 v1 `TraceService / DebugQuery` 的能力重新整理为独立 query service：

- event pagination
- invocation projection
- runtime state projection
- node/operator/edge inspection
- child graph
- comparison/debug 基础能力

TraceService 只依赖 TraceStore/LiveTail，不依赖 HTTP。

---

## Phase 5 — Standalone Server

实现可组合 Server：

- Runtime Router
- Trace Router
- SSE/WebSocket
- health/readiness
- authentication 留简单扩展点

Standalone composition：

```text
Server(Runtime + Trace)
       ↓
Local Host + Local Store
```

目标：先恢复 v1 最核心的 Server + Trace UI 后端能力。

---

## Phase 6 — RemoteSink + Central Server

新增：

### Host 侧

- RemoteSink
- Runtime-only Server
- host identity
- session ownership
- heartbeat / registration

### Central 侧

- Ingest API
- Central TraceStore
- RuntimeState materializer
- checkpoint/snapshot materialization
- HostRegistry
- Control proxy/router
- Trace API

目标：支持多个 GKE Host 接入同一个 Trace/Control 平台。

---

## Phase 7 — CLI

基于 Server API 实现：

- Client abstraction
- runtime commands
- trace/debug commands
- JSON / JSONL 输出
- Coding Agent-friendly errors

CLI 不直接读数据库，也不直接使用 Core private API。

---

## Phase 8 — UI / Evaluation / AI Integrations

最后逐步恢复 v1 外围能力：

- Trace UI
- Evaluation
- AI providers/operators
- Coding Agent debugging workflow
- invocation comparison / rerun analysis

这些模块都建立在前面的稳定 Host/Server/Trace contract 上。

---

# 13. 第一阶段建议暂时不做的事情

为了避免再次把外围复杂性推回 Core，第一阶段不建议：

- terminal Invocation 自动 unload；
- 每个 Invocation 都生成并上传 checkpoint；
- Core 支持多个 Sink；
- Server 直接读取 Core repository 做历史 Trace；
- CLI 直接访问 DB；
- 一开始就设计复杂的分布式一致性协议；
- 提前增加 SessionContinuation / root compact state。

先通过实际 benchmark 测量：

- terminal checkpoint serialized size
- checkpoint encode/decode 时间
- RemoteSink RTT
- resident Session 内存占用
- Event/sec 与 DB writer throughput

只有数据证明 checkpoint / resident memory 是瓶颈，再扩展 Core 的 terminal compaction。

---

# 14. 最终核心原则

```text
Core    = 正确执行
Host    = 运行与生命周期
Sink    = Event 唯一出口
Store   = 历史事实
Server  = 远程访问与路由
CLI     = Agent/Developer 操作界面
UI      = 人类观察界面
```

其中最重要的两个解耦是：

```text
Execution != Persistence
Runtime Residency != Historical Trace
```

这两条成立后，Standalone 与 GKE + Central Trace 两种模式可以建立在同一套 Core 和 Host contract 上，而不需要两套 Runtime 架构。
