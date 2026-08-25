# AutoAgent V2 Host 与本地 Tracing 设计

Core 已经只负责 Workflow 编译、执行和当前 Runtime State。本层负责把 Core 装配成一个
可运行项目，并保存、查询 Core 产生的 canonical `RuntimeEvent`。

## 一、模块边界

```text
autoagent.toml + environment
             │
             ▼
      ProjectLoader
             │ Workflow definitions
             ▼
      AutoAgentHost ────────────────┐
             │ owns                │ saves definitions
             ▼                     ▼
      AutoAgentApp ──RuntimeEvent──> RuntimeEventSink
                                      │
                         ┌────────────┴────────────┐
                         ▼                         ▼
                SQLiteRuntimeStore       HttpRuntimeEventSink
                         │
                         ▼
                 Tracing Server
                         │ read-only HTTP/SSE
                         ▼
                    Tracing UI
```

- `autoagent.host`：项目加载、环境配置、App/Sink 生命周期和恢复入口。
- `autoagent.hosting`：SQLite Store、HTTP Sink 与持久化查询接口。
- `autoagent.tracing`：只读本地 FastAPI Server，不执行 Workflow。
- `autoagent.cli`：`compile`、`invoke` 和 `trace` 三个基础命令。
- `ui/`：独立前端源码；构建结果作为 `autoagent.tracing` 的包资源。

Tracing Server 不拥有 `AutoAgentApp`，也不提供 invoke/resume/cancel 接口。未来中心化平台
可以复用 Runtime Event 接收契约和查询模型，而不改变 Core 或 Host。

## 二、项目定义与环境配置

项目文件固定为 `autoagent.toml`，只保存可版本化的代码入口：

```toml
schema_version = 1

[project]
name = "example"
version = "0.1.0"
description = "示例项目"

[[workflows]]
entrypoint = "workflows.research:workflow"
```

`entrypoint` 必须是 `<module>:<object>`，目标必须是 `Workflow`。Workflow id 在项目内唯一；
加载器只在导入期间临时加入项目根目录，不永久改变 `sys.path`。

运行和部署参数只从进程环境或项目 `.env` 读取，进程环境优先：

| 环境变量 | 默认值 | 作用 |
| --- | --- | --- |
| `AUTOAGENT_MAX_OPERATOR_CONCURRENCY` | `32` | 一个 Host 内全局 Operator 并发上限 |
| `AUTOAGENT_MAX_NODE_EXECUTIONS_PER_INVOCATION` | `1000` | 单 Invocation NodeOccurrence 保护上限 |
| `AUTOAGENT_RUNTIME_EVENT_SINK` | `sqlite` | `sqlite`、`http` 或 `none` |
| `AUTOAGENT_SQLITE_PATH` | `.autoagent/runtime.db` | 相对项目根目录的本地 Store 路径 |
| `AUTOAGENT_HTTP_SINK_URL` | 无 | HTTP Sink 接收地址 |
| `AUTOAGENT_HTTP_SINK_TOKEN` | 无 | 可选 Bearer Token |
| `AUTOAGENT_HTTP_SINK_TIMEOUT_SECONDS` | `10` | 单次远程持久接纳超时 |
| `AUTOAGENT_TRACE_HOST` | `127.0.0.1` | 本地 Tracing Server 地址 |
| `AUTOAGENT_TRACE_PORT` | `8765` | 本地 Tracing Server 端口 |
| `AUTOAGENT_TRACE_UI_DIRECTORY` | 包内 UI | 可选静态 UI 目录 |
| `AUTOAGENT_TRACE_REFRESH_SECONDS` | `0.5` | 跨进程 SQLite 实时刷新间隔 |

`.env` 不覆盖真实进程环境。Host Settings 是解析后的不可变值，不把未知环境变量复制进
Runtime State 或持久化记录；HTTP Token 不出现在 Settings 的字符串表示中。

## 三、Host

```python
host = AutoAgentHost.from_project("./autoagent.toml")

result = host.invoke("research", {"topic": "agents"})
loaded = host.restore_session(result.session_id)

host.close()

# 异步装配和关闭都不会阻塞调用方 Event Loop。
async with await AutoAgentHost.afrom_project("./autoagent.toml") as host:
    result = await host.ainvoke("research", {"topic": "agents"})
```

`AutoAgentHost`：

1. 解析项目与环境；
2. 创建并启动 Sink；
3. 按环境创建一个 `AutoAgentApp`；
4. 原子注册全部 Workflow，并把 portable `WorkflowDefinitionSnapshot` 写入可查询 Store；
5. 代理 Workflow Snapshot、Capability Operator 注册，以及 invoke/submit/stream、
   wait/resume/cancel、Checkpoint 加载、Child 控制和 recover；
6. 关闭时先让 App 收敛并导出 Event，再关闭 Sink。

同步和异步 context manager 都会关闭完整 Host。若业务代码与关闭同时失败，业务异常保持为
主异常，关闭失败附加到异常 note；若业务代码没有失败，关闭失败正常向调用方抛出。App
已关闭但 Sink 清理失败时 Host 保持终态，当前及后续所有 close/aclose 调用都观察同一个失败。
同步 `from_project()` 会导入用户模块、初始化本地 Store，并可能发布远程 Workflow definition，
因此不能在运行中的 Event Loop 内调用；异步代码必须使用 `afrom_project()`。若异步启动被取消，
后台装配完成后会重试关闭其 App 与 Sink；持续清理失败会发出 RuntimeWarning，而不是静默
留下无人持有的 Host。`afrom_project()` 会复制调用方 `ContextVar`，但用户模块会在专用线程
导入，因此模块级启动代码必须支持非主线程执行。

`restore_session()` 只从可查询 Store 重建该 Root Session 当前的父子 Runtime 图，调用
Core `load_checkpoint()`；调用方再按状态选择 `recover()` 或 `resume()`。Host 不保存第二份
内存历史，也不自动恢复所有历史 Session。

## 四、Runtime Event Sink 与 Store

### 统一写入契约

`RuntimeEventSink.append(event)` 成功返回表示 Event 已经持久接纳。实现必须满足：

- Event id 幂等；同 id 不同内容必须报错；
- `(session_id, sequence)` 唯一且连续；
- 校验 `previous_event_id` 和 `previous_event_digest`；
- 校验独立 `event_digest`，包括没有后继链路可校验的最后一条 Event；
- 用 Runtime Log 重新规划语义转换，确认 StateOperation 与日志完全一致且可重放；
- Runtime Event、索引和 Trace 投影在一个事务中提交；
- 写入失败不能留下查询可见的部分事务；
- 不保存重复的完整 RuntimeState 或完整 Checkpoint。

### SQLiteRuntimeStore

SQLite 使用 WAL、外键和单写线程。Core RuntimeLoop 只异步等待写线程确认，不直接执行
阻塞 SQLite 调用。数据库保存：

```text
schema_metadata       # 精确 schema version
workflow_definitions  # portable WorkflowDefinitionSnapshot
runtime_events        # RuntimeEvent canonical JSON，唯一完整恢复记录
trace_events          # 从 RuntimeLog 得到的小型安全查询投影
sessions              # 查询索引
invocations           # 查询索引
session_ownership     # Root/Child 关系索引
```

首次创建 Store 时，SQLite Store 以原子排他创建方式预建数据库文件。在 POSIX 系统上，
新建的父目录默认使用 `0700`，数据库以及 SQLite 派生的 WAL/SHM 文件默认使用 `0600`。
这个安全默认只适用于本次真正新建的资源；打开已经存在的目录或用户指定数据库时不会隐式
`chmod`。初始化在建库后失败会保留这个私有空文件，后续启动可安全重试；不能在失败路径
删除它，因为另一个进程可能已经观察到该文件并开始初始化。

Store 只接受当前精确 schema version，不在本模块隐式迁移旧数据库。`sessions`、
`invocations`、`trace_events` 和 `session_ownership` 都只是查询投影：读取、继续追加和恢复时
会把投影的 identity、head、count、SQL envelope 与 canonical Event 重新绑定；已有 canonical
记录但投影缺失属于 Store 损坏，不伪装成 404。Child ownership 必须匹配父 Invocation 原始
`ChildInvocationPlanned` Event/Log 的完整描述，当前 phase 也从父侧 canonical Event 推导，
因此不能通过同时篡改多个投影把 Child 改挂到另一个 Root。

摘要、列表和 Trace 查询不构建完整 Scheduler State。它们在同一个 SQLite 读快照中单次扫描
Session 的 canonical Event/Log 前缀，校验 Event digest/hash chain、Log 到 Trace 的精确投影，
并一次派生全部 Invocation 的 identity、状态、Event 范围、时间、计数和 Child ownership，
再与 SQL 查询索引逐项比较。这样一个包含多个 Invocation 的 Session 页面仍按 Event/Trace
总量近线性读取。Event 入库时已经由 `StateReducer.validate_sealed()` 完成完整语义和
StateOperation 校验；需要 Runtime State 的 `state`、rebuild、Checkpoint 与恢复接口仍走完整
Reducer，不使用轻量摘要结果替代恢复状态。

Checkpoint 由 `runtime_events` 重建：每个 Session 按 sequence 经同一个 `StateReducer`
归约；已接纳的 sealed Event 前缀复用单份 canonical record，只在返回边界解码一次 typed
`RuntimeState`。父侧 Child plan/phase 也在同一个 SQLite 读快照中扫描一次并供兄弟 Child
复用，再从 Root 当前 `child_plans` 递归收集 Child State。历史 Trace
可按 Invocation 查询；某个 Session Event 边界的状态可按 `through_sequence` 重建。历史
Invocation 只重放它自己的 canonical Event 前缀，不要求之后的 Invocation 仍然完好；当前
Checkpoint 则要求整个可达父子图及其投影一致。

### HttpRuntimeEventSink

HTTP Sink 向配置地址发送 `RuntimeEvent` 与 `WorkflowDefinitionSnapshot` JSON，分别使用
Event id 与 Workflow revision id 作为 `Idempotency-Key`。任意非 2xx、超时、初始化或协议
错误都会失败。阻塞 HTTP 请求由有界 worker pool 执行，不占用 RuntimeLoop；调用方注入的
自定义 client 必须支持并发 `post`。Core 在同一个 Session 内逐 Event 等待 `append()`，
因此远端顺序来自 Core 的 Session 串行边界；不同 Session 可以占用不同 worker 并发发送。
`close/aclose` 会排空已经接纳的请求，并让所有并发关闭者观察同一个完成或失败结果。

## 五、Tracing Server

本地 Server 只读 SQLite Store，默认只监听 `127.0.0.1`：

```text
GET /api/v1/health
GET /api/v1/workflows
GET /api/v1/workflows/detail?revision_id=...
GET /api/v1/workflows/sessions?revision_id=...
GET /api/v1/sessions/invocations?session_id=...
GET /api/v1/invocations/detail?invocation_id=...
GET /api/v1/invocations/children?invocation_id=...
GET /api/v1/invocations/trace?invocation_id=...
GET /api/v1/invocations/state?invocation_id=...
GET /api/v1/invocations/stream?invocation_id=...
```

列表使用稳定 cursor 和显式 `limit`。`stream` 使用 SSE：同进程写入由条件通知立即唤醒；
Host 与 Server 分进程时，仅在存在订阅者时以可配置间隔检查 SQLite，不使用常驻 1ms
轮询。进程内唤醒在 POSIX 使用 pipe，在 Windows 使用 event-loop thread-safe callback，
不要求 Selector-only API。Trace 支持 `tail_limit`，返回最新有界窗口、`resume_cursor` 和 `has_earlier`；UI 可以
立即从该快照进入 live，不必先重放全部历史。SSE 支持 query cursor 与 `Last-Event-ID`，
终态追平后发送 `stream_end`。如果响应已经建立后 Store 读取失败，Server 会发送不含底层
异常详情的 `stream_error(code=store_unavailable)` 并结束流；UI 随即关闭 EventSource、停止
live 刷新并显示错误，不会由 EventSource 自动无限重连。Server 不返回 RuntimeEvent 内部的
StateOperation，普通 Trace API 只返回 `TraceEvent`；`state` 返回 reducer 生成的只读状态记录。

身份放在 query 中，因此包含 `/` 或 Unicode 的 id 不改变路由语义。所有纳秒时间在 HTTP
DTO 中使用十进制字符串；State 和 Trace 的任意深层 JSON 值若超出 JavaScript safe integer
范围，也转换为十进制字符串，范围内整数和布尔值保持原类型。该转换只属于只读 Tracing
DTO，不改变 SQLite 中的 canonical RuntimeEvent 或恢复数据。Server 是逻辑只读，但 live
WAL reader 可能由 SQLite 创建 `-wal`/`-shm` sidecar，因此数据库目录仍需可写。它不提供
认证，CLI 默认只允许 loopback；绑定远端地址必须显式传
`--allow-remote-without-auth`。
CLI 的 loopback 模式同时校验 HTTP `Host`，避免浏览器经 DNS rebinding 读取本机 Trace；
显式远端暴露时由部署者负责认证、TLS 和允许的 Host。Server 关闭公开 OpenAPI 与交互式文档路由，
所有响应附带 `nosniff`、禁止嵌入和同源 CSP。API 默认 `no-store`，SSE 使用
`no-cache, no-transform`；带内容哈希的 UI asset 使用 immutable cache，入口 HTML 与自定义
无哈希 asset 每次重新校验。

Spawn 父 Invocation 进入终态后，SSE 会继续等待其直接 Child 全部进入 terminal 并交付最后
的 Child trace，随后才发送 `stream_end`；缺失或不连续的 Trace 投影会返回 Store 错误，
不会提前伪造终态。UI 在启动 live 前先建立可恢复 tail cursor，支持
向前分页加载完整历史，并对 Child burst 合并 projection 刷新。
Child 刷新会越过已经加载的完整分页前缀再探测一页，从而不会在恰好 100 个 Child 时漏掉
第 101 个；重复 cursor 会终止并显示错误，不会形成请求循环。

## 六、Tracing UI

首版本保留 V1 最有价值的只读能力：

- Workflow、Session、Invocation 三级导航；
- Workflow DAG、入口/出口和 Loop 信息；
- Invocation 状态、错误、等待点和父子关系；
- NodeOccurrence/OperatorCall 时间线；
- 实时 Trace 列表与单条事件 Inspector；
- 最新 Runtime State Inspector。

UI 不提供执行、Resume、Cancel、Replay、Fork、Eval 或用户管理。它只消费上面的只读
API，因而以后可以无状态地连接本地 Store 或中心化平台。Invocation 首屏先加载可恢复
Trace tail、摘要和 Child 列表并启动 SSE；完整 Runtime State 由独立后台请求补入，不阻塞
图、时间线和实时事件展示。live 期间每条语义 Trace 都会请求更新 Latest Runtime State，
但 State 重建严格单飞并以 250ms 最小间隔合并 burst；`stream_end` 前会 flush 最后一轮读取。
Workflow、Session 和 Invocation 导航是分页快照，新运行通过顶部 Refresh 显式发现；选中
Invocation 后的 Trace、State 与 Child 变化才进入实时 SSE 路径，避免后台反复重建所有历史
Session 摘要。

## 七、CLI

```text
autoagent compile [--project PATH]
autoagent invoke WORKFLOW_ID --input JSON [--session-id ID] [--entry NODE]
autoagent trace [--database PATH] [--host HOST] [--port PORT]
                [--allow-remote-without-auth]
```

- `compile`：加载全部 Workflow，执行 Compiler 检查并打印 revision；不创建数据库。
- `invoke`：创建 Host，执行到 completed/failed/cancelled/waiting 边界并输出 JSON。
- `trace`：启动只读本地 Tracing Server 和 UI；非 loopback 必须显式确认无认证暴露；
  uvicorn 启动失败返回结构化 `TRACING_SERVER_FAILED` 且始终关闭只读 Store。

所有用户 Python、原生 fd 和子进程输出都与 stdout 机器通道隔离；诊断写 stderr，机器结果
只写一条 JSON 到 stdout。成功退出码为 0，项目/编译/执行错误为 1，
命令参数错误由 argparse 返回 2。

V2 wheel 使用 distribution/package/console script 的统一身份 `autoagent`，是 V1 的替代
升级，不支持把 V1 与 V2 同时安装在同一环境。

## 八、验收标准

- 非法 TOML、入口、环境变量和重复 Workflow id 有稳定诊断；
- SQLite append 幂等、链连续、事务原子，并能从 Event 重建父子 Checkpoint；
- sink 故障时 Core 不确认 Event，重试不会重复 Trace/Invocation 索引；
- Host close 顺序不会丢失最后一批 RuntimeEvent；
- HTTP Sink 使用稳定幂等键并正确传播失败；
- Tracing API 分页、历史状态和 SSE 增量无 1ms 轮询；
- UI TypeScript 测试和 production build 通过；
- V2 Core 原有测试保持全部通过。
