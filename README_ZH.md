# AutoAgent

[English](README.md)

AutoAgent 执行编译后的 Workflow，并在内存中维护最新的 Session 和
Invocation 状态。配置数据库后端后，RuntimeStore 会异步持久化选定的运行信息。

每次 Invocation 可以独立选择记录级别：

```python
app.start()
invocation = app.invoke(
    workflow,
    input={"message": "hello"},
    event_mode="standard",  # "minimal"、"standard" 或 "full"
)
```

## 项目 CLI

对于标准项目，CLI 是统一宿主。Workflow 模块只导出 Workflow 对象，
`auto-agent.toml` 列出这些对象，CLI 负责创建并启动 App：

```bash
autoagent project check
autoagent workflow list
autoagent workflow check weather
autoagent invocation run weather --input-file request.json
autoagent invocation resume weather \
  --session customer-42 \
  --wait-key approval \
  --response-json '{"approved":true}'
autoagent serve --host 127.0.0.1 --port 8765
```

Resume 的值叫 response，是因为它在回应一个未完成的 Wait，而不是 Workflow
最终 output。`--report-file` 会写入与 stdout 完全相同的确定性文本，不会阻止
终端输出。CLI 内部统一调用 App 的异步接口。

配置优先级为：CLI 显式覆盖、进程环境变量、项目根目录 `.env`、框架默认值。
根目录 `.env.example` 包含全部 App、Server 和 OpenAI-compatible Provider
环境配置。不使用 `llm_call` 的 Workflow 可以将 Provider 配置留空。

面向 Coding Agent 的三个可运行 Workflow 位于
[`examples/authoring`](examples/authoring/README.md)，分别覆盖条件编排、持久化
Wait/Resume，以及 LLM/Tool/ReActWorkflow。

`event_mode` 属于 Invocation，而不是 App。相同 App 和 Session 的不同
Invocation 可以动态选择不同模式。`memory` 和 `database` 是 RuntimeStore
后端类型，不是 Event 模式。

## 环境配置

普通应用不需要创建 `AutoAgentSettings`，也不需要显式调用环境加载方法：

```python
app = AutoAgentApp()
app.start()
```

`AutoAgentApp()` 会自动读取当前目录 `.env` 中框架支持的 `AUTOAGENT_*`
配置，然后用同名进程环境变量覆盖。`AutoAgentSettings(...)` 只用于代码显式
覆盖和测试。框架支持的完整部署配置统一记录在 `.env.example`；Workflow
示例或测试自己的变量不放入其中。

App 必须显式启动。先注册 Workflow 以及 Runtime codec/model，再调用
`app.start()`（异步代码使用 `await app.astart()`）。启动阶段负责初始化后端、
重建持久化 Wait，并恢复已注册 Workflow 中未完成的 `created`/`running`
Invocation；invoke、submit 和 resume 不会再惰性启动或恢复 App。

## Runtime Event 模式

| 能力 | `minimal` | `standard` | `full` |
| --- | --- | --- | --- |
| 持久化 Invocation input、状态、最终 result 和 error | 支持 | 支持 | 支持 |
| RuntimeEvent | 不生成 | 图、Operator、Wait、Recovery Event | Standard Event 加内部 phase Event |
| Event 中间 Node/Operator input、output | 不记录 | 不记录 | 记录 |
| Event StateOperations | 不记录 | 不记录 | 记录 |
| 图路径和耗时 Trace | 不支持 | 支持 | 支持 |
| 跨进程 Wait/Resume | 不支持 | 支持 | 支持 |
| 跨进程崩溃恢复 | 不支持 | 支持 | 支持 |
| 重建任意 Event sequence 的 Runtime State | 不支持 | 不支持 | 支持 |
| 历史调试和 Fork 的底层数据 | 不支持 | 不支持 | 支持 |

### `minimal`

只关心 Invocation 对外生命周期时使用。

- 不创建、不保存 RuntimeEvent。
- Invocation 记录保留 input、最新公开状态、最终 result 和终态 error。
- Wait/Resume 只能在当前进程及其内存 RuntimeStore 仍然存在时使用。
- 不持久化 genesis state 和 RecoveryState，因此进程重启后无法继续或恢复。

### `standard`（默认）

用于需要生产 Trace 和持久化恢复，但不需要保存所有中间值的场景。

- 记录图流转、Node 状态、边选择、逻辑 Operator call、Wait/Resume 和
  Recovery 信息。
- 记录时间戳、执行耗时、线程池/并发等待时间、状态和有限 metadata。
- 不保存 Node 内部 phase 的 input/output，也不保存 StateOperations。
- Invocation 可恢复期间维护精简 RecoveryState；`wait.created` 会强制建立恢复点。
  Standard Invocation 终态仍保留最近一次精简 RecoveryState。
- 支持跨进程 Wait/Resume 和崩溃恢复，但不能重建任意历史 Event 时刻的
  Runtime Context。

### `full`

用于详细 Trace、状态回放、调试，以及未来的 Fork 工具。

- 包含全部 Standard Event。
- 增加用户自定义执行阶段：input mapping、map item selection、aggregation
  和 output binding。
- 可以保存 phase 和 Operator 的 input/output。
- 每个 Event 都带有相对上一条 Event 的有序 StateOperations。
- 持久化 sequence 0 的 genesis state，并周期性推进 RecoveryState。
- 可以根据连续 Event 重建任意 sequence 时的 Session/Invocation Context
  和执行状态。目前底层已经支持重建，公开 Fork 服务和 UI 尚未提供。

Full 模式成本更高。大对象可以转成去重的 ArtifactRef，但 Event 和
StateOperations 仍会增加 CPU、内存队列和数据库占用。

## RuntimeEvent 结构

Event sequence 在每个 Invocation 内独立，从 `1` 开始。

| 字段 | 含义 |
| --- | --- |
| `invocation_id`, `sequence` | Invocation 标识和连续的 Invocation 内顺序 |
| `event_type`, `event_name` | Event 大类和精确语义名称 |
| `subject_type`, `subject_id` | Invocation、Node、edge、Operator call、Wait 或 Recovery 对象 |
| `occurred_at_ms` | Event 发生的墙钟时间 |
| `elapsed_ns` | 操作完成时的单调时钟耗时 |
| `timing` | execution、并发槽等待、线程池队列、retry backoff 等分项耗时 |
| `status` | Trace 消费者可直接读取的标准化状态 |
| `payload` | Event 特有的有限 metadata |
| `input`, `output` | 详细值；仅 Full 模式在适用时保存 |
| `operations` | 有序 Runtime State 增量；仅 Full 模式保存 |

RuntimeEvent 在对应状态修改或操作完成后产生。用户自定义 phase 只生成一条完成
Event，通过 `occurred_at_ms` 和 `elapsed_ns` 表示时间，不生成独立的开始 Event。

## Event 清单

| Event 类型 | Event 名称 | Standard | Full | 含义 |
| --- | --- | :---: | :---: | --- |
| `state_change` | `invocation.running` | 是 | 是 | Admission、入口和 input 检查完成，开始或恢复执行 |
| `state_change` | `invocation.completed` | 是 | 是 | 所有被选中的工作完成 |
| `state_change` | `invocation.failed` | 是 | 是 | Invocation 终态失败 |
| `state_change` | `invocation.cancelled` | 是 | 是 | 调用方取消并终止活动任务 |
| `state_change` | `node.running` | 是 | 是 | 创建具体 NodeExecution 并标记 running |
| `state_change` | `node.completed` | 是 | 是 | Node output 和 output binding 成功完成 |
| `state_change` | `node.failed` | 是 | 是 | Node 失败；适用时 payload 会标明失败 phase |
| `state_change` | `node.waiting` | 是 | 是 | Node 因外部 Wait 暂停 |
| `state_change` | `node.skipped` | 是 | 是 | Scheduler 确认该作用域下的 Node 不可达或未选择 |
| `routing` | `edge.evaluated` | 是 | 是 | 一条边完成 condition 评估，包括 selected 结果 |
| `operator_call` | `operator_call.completed` | 是 | 是 | 一次直接逻辑调用，或一次 map/replication 逻辑汇总完成 |
| `wait` | `wait.created` | 是 | 是 | Invocation 到达稳定 Wait，同时强制生成恢复点 |
| `wait` | `wait.resumed` | 是 | 是 | 接受 Wait payload，继续对应 NodeExecution |
| `recovery` | `recovery.requeued` | 是 | 是 | 将进程中断前可恢复的 NodeExecution 重新入队 |
| `recovery` | `recovery.node_skipped` | 是 | 是 | RecoveryPolicy 拒绝一条分支，其他分支可以继续 |
| `recovery` | `recovery.interrupted` | 是 | 是 | Workflow 或 Node policy 禁止继续恢复 |
| `phase` | `input_mapping.completed` | 否 | 是 | Input mapping 完成或失败；Full output 是映射后的 Node input |
| `phase` | `item_selection.completed` | 否 | 是 | 自定义 map item selector 完成或失败 |
| `phase` | `aggregation.completed` | 否 | 是 | 自定义 map/replication aggregator 完成或失败 |
| `phase` | `output_binding.completed` | 否 | 是 | 隔离 Context 成功提交，或失败并整体回滚 |

未配置可选 hook 时不会生成对应 phase Event。例如 MapPolicy 没有自定义 item
selector 时不会生成 `item_selection.completed`，没有 output binding 的 Node
不会生成 `output_binding.completed`。

## 持久化行为

内存 RuntimeStore 始终是最新状态的权威来源。配置数据库后端后，不可变持久化
envelope 会进入队列，并在持久化线程序列化；普通 Workflow 执行不会等待每一条
SQL 写入。App 启动时数据库初始化必须成功。启动后数据库故障会通过持久化健康状态
和日志明确报告，内存执行继续进行，直到 backlog 达到 hard limit 后才拒绝新的
Invocation。

Invocation 执行结束不代表所有 Event 已经入库。Server/UI 应区分内存中的实时
Invocation 状态和数据库 durable cursor。

## Tracing Server 和 UI

`AutoAgentServer` 既可以独立运行，也可以将 Router 挂载到已有 FastAPI：

```python
from fastapi import FastAPI
from autoagent.core.server import AutoAgentServer

server = AutoAgentServer(app)
server.run()                    # 独立 API；存在 ui/dist 时同时托管 UI

host = FastAPI()
host.include_router(server.router)  # 嵌入 API；Router 管理 App 生命周期
```

V1 Tracing API 位于 `/api/v1`。Workflow、Session 和 Invocation 列表使用不透明
cursor 分页。Invocation bootstrap 只返回精确 Workflow revision、精简图状态
projection checkpoint，以及指定上限内的最新 RuntimeEvent。更早的 Event 每次只
加载一页；实时 Event 和 Invocation 状态通过 SSE 推送。Full Event 的 input/output
以及历史 Runtime State 都是独立的按需请求，普通 UI Event 页面不会返回
StateOperations。

Tracing UI 保留 Workflow/Session/Invocation 选择栏、图画布、可调整宽度的
Inspector 和可折叠 Timeline。Inspector 可查看 definition、contract、policy、
图状态、耗时、Operator call、边评估、Wait/Resume，并按需加载单条 Event 详情。
Fork 和 AI 辅助 Design 暂时只作为后续能力，不提供无效的占位交互。
