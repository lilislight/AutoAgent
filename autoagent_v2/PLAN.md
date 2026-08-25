# AutoAgent V2 Core 实施记录

本文只保留已经落地的阶段边界；未实现事项见 `TODO.md`。

## 阶段 1：静态定义与编译

已实现值契约、Operator、Capability、Wait、Workflow、SubWorkflow、Node、Edge、Map、
Stream、WorkflowIR、稳定 Revision、Portable Snapshot 和自然 Loop 分析。Runtime 只读取
不可变 IR。

## 阶段 2：Runtime State

已实现 `StateTransition -> StateOperationBatch -> StateReducer -> RuntimeState`。Batch
是原子状态提交；RuntimeEvent 是可聚合多个 Batch/Log 的 canonical 信封，支持严格
codec、链校验和按前缀重建。

## 阶段 3：Scheduler 与 Loop

已实现 Activation、EdgeResolution、ready NodeOccurrence、all-match 分支、完整 Fan-in、
不可达传播，以及 Self/嵌套/共享 Header Loop 的 scoped occurrence 与边界决议。

## 阶段 4：Executor 与 Context

已实现同步/异步 Operator、全局物理并发限制、Map 有界 Worker、有序聚合、
StreamReducer、OperatorCall 生命周期、Input Mapping、Condition、Output Binding 和
ContextPatch 原子冲突检测。

## 阶段 5：Wait、Child 与恢复

已实现 Wait/Resume/Cancel、Child Workflow await/spawn、Child Map 四组合、稳定 Child
plan/handle、崩溃后 lost call 协调，以及 `Recovery(mode="never" | "replay_safe")`。

## 阶段 6：App 与公开边界

已实现同步/异步 invoke、submit、wait、resume、cancel、recover、stream、Child 控制、
Checkpoint 加载和关闭。一个 Session 同时最多一个活动 Invocation；控制操作使用精确
InvocationRef。

公开 SDK 返回 TraceEvent、UserEvent 和 Checkpoint，不返回 RuntimeEvent/StateOperation。
Host 通过 RuntimeEventSink 接收 canonical Event。Checkpoint 以 Root + 所有已经打开
Invocation 的可达 Child State 为单位；stream/astream 在安全恢复边界提供中间
Checkpoint，并保持严格调用方背压。

## 阶段 7：质量验收

当前测试覆盖 contracts、compiler、DAG/Loop scheduler、executor、Context、Wait/Recovery、
App、Capability、RuntimeLoop、RuntimeEvent sink、Trace 投影、Checkpoint 图、流式安全
边界与并发竞态。统一命令见 `TESTING.md`。

## 阶段 8：Host 与持久化

已实现标准 `autoagent.toml`、严格项目/环境诊断、完整父子 Workflow 注册、SQLite 与 HTTP
RuntimeEvent Sink、Event id 幂等、Session hash chain、Trace 查询投影，以及从 canonical
Event 重建 Root/Child Checkpoint。`AutoAgentHost` 提供同步/异步 invoke、submit、stream、
load/recover 和 App→Sink 有序关闭；Host 不保存第二份历史 RuntimeState。

## 阶段 9：本地 Tracing 与 CLI

已实现只读 FastAPI 查询接口、稳定 cursor、历史 State、tail bootstrap 和 SSE 恢复；本地
UI 包含 Workflow/Session/Invocation 导航、静态图、受限窗口 Timeline、Trace/State
Inspector。CLI 提供 `compile`、`invoke` 和 `trace`，其中 compile 不创建 Runtime Store。
