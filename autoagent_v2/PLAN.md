# AutoAgent V2 Core 实施计划

本计划采用阶段门禁：当前阶段的功能、负面测试和回归测试全部通过后，才进入下一阶段。后续阶段发现基础设计错误时，退回对应阶段修改，并重新运行该阶段及之后所有已完成阶段的测试。

## 执行记录

- 阶段 1 已完成：35 个 `test_phase1_*` 测试通过；`compileall`、公开 import 边界和 diff whitespace 检查通过。当前环境未安装 Ruff，因此未执行 Ruff。
- 阶段 2 已完成并重构：StateOperationBatch 是 Reducer 的原子输入，Runtime Event
  是可聚合多个 Batch/Log 的持久化信封；支持 Event 内 state version 重放和恢复
  前缀原子导入。
- 阶段 3 已完成：9 个 DAG Scheduler 测试通过；与阶段 1、2 合并回归共 56 个测试通过。
- 阶段 4 已完成：普通、Self、嵌套、并行和共享 Header Loop 的 Scope 与边界决议可逐 Event 重建。
- 阶段 5 已完成：同步/异步 Operator、Map、Aggregation、StreamReducer、OperatorCall 和 UserEvent Chunk 已接入。
- 阶段 6 已完成：Input Mapping、Condition、Output Binding、ContextPatch 与并行冲突检测原子提交。
- 阶段 7 已完成：Wait/Resume/Cancel、lost OperatorCall 和 Event 前缀恢复已接入。
- 阶段 8 已完成：AutoAgentApp 的同步、异步、submit、wait、resume、cancel、recover/replay 和 Child Workflow 已接入。
- 阶段 9 已完成：旧 V2 架构与测试已清理；Full 基准记录在 `PERFORMANCE.md`。
- 阶段 10 已完成并经过审查加固：V1 能力矩阵记录在 `V1_CORE_PARITY.md`；App 与两个 Executor 已拆分，Map 失败收敛、活恢复互斥、可操作 Child Handle、多 Revision、Core Port、Recovery 门禁、执行策略和动态 Operator Registry 均有回归测试。再次审查后增加严格定义边界、完整增量 Event、TaskRuntime、RuntimeLoop 和异步门面测试。

## 阶段 1：静态定义与编译

实现 ValueContract、Operator、Capability、Wait、Workflow、SubWorkflow、Node、Edge、Map、Stream、Workflow IR、编译诊断和完整的可归约 Loop 分析。

验收标准：

- Operator 只接受零个或一个业务输入；契约只允许 TypedDict、受限 Pydantic Model 或 None；
- 契约采用名义匹配，不做结构兼容；
- Edge 端点存在，同一 Source/Target 之间最多一条 Edge；
- 同一 Source 的 Complete 与 Error Edge 不指向同一 Target；
- 多入边 Node 必须显式设置 Input Mapping；
- Map、Stream 组合的输入输出契约可由编译结果明确确定；
- 编译结果具有确定的 revision hash；
- Workflow IR 是 Runtime、Scheduler 和 Executor 使用的唯一静态图接口，不要求后续模块读取 Authoring 对象；
- V2 的自然 Loop、嵌套 Loop、共享 Header 合法结构能够编译；不可归约、多入口、无出口、多 Back Edge 和静态控制冲突得到稳定诊断；
- 静态层测试全部通过，且不依赖 Runtime、Scheduler、Executor、Sink 或持久化模块。

测试范围：合法/非法 Operator 契约、名义契约匹配、边约束、分支、Join、多入口/多出口、Map、Stream、Wait、确定性编译、普通 Loop、Self Loop、嵌套 Loop、共享 Header Loop、非法交叉 Loop、无出口 Loop、多 Back Edge Loop 和长图非递归分析。

## 阶段 2：Runtime Event、State 与 Reducer

实现 Session 级 Canonical RuntimeState、StateOperation/Batch、唯一 StateReducer、
Runtime Log、可调节边界的 Runtime Event 和内存 Journal。

验收标准：

- RuntimeState 没有绕过原子 Batch/Reducer 规划边界的写入口；
- 一个 Batch 对应一个原子 State Transition，一个 Event 可包含多个 Batch；
- Event 不保存完整 RuntimeState；
- `reduce(events[:N])` 等于第 N 个 Event 提交后的完整 State，且可在 Event 内按
  state version 重放；
- Reducer 失败不修改旧 State；
- Event sequence、schema version、causation 和时间值可重放；
- RuntimeState、Event 和 Journal 不依赖 Snapshot、Sink、数据库或远程发送。

测试范围：Genesis、Invocation 生命周期、非法转换、重复 Event、sequence 缺口、Event 幂等、原子回滚、每个 Event 前缀重放、序列化往返和 RuntimeState 不可旁路修改。

## 阶段 3：DAG Scheduler

实现 Activation、EdgeResolution、NodeOccurrence Request、Ready Queue、分支、并行、完整 Fan-in 和不可达传播。

验收标准：Scheduler 只读取 Workflow IR 和 RuntimeState，并通过 Runtime Event 改变状态；无独立的权威可变 SchedulerState。

测试范围：一对一、Fan-out、条件分支、全部匹配、多分支并行、完整 Join、部分选中 Join、全部未选中、错误路由、多入口、多出口、确定性 Ready 顺序和逐 Event 重放。

## 阶段 4：Loop Scheduler

将 V2 的 ExecutionScope、LoopIteration、Back/Exit 边界和嵌套 Loop 调度接入 Event Reducer。

验收标准：同一静态 Node 在不同 Scope 中生成独立 NodeOccurrence；每次 Loop 路由都能由 Event 重建；一个 iteration 不能同时 Continue 和 Exit。

测试范围：普通 Loop、Self Loop、零次 Body、条件退出、嵌套 Loop、共享 Header Loop、并行分支中的 Loop、Wait 后继续 Loop、Continue/Exit 冲突、死路和执行次数上限。

## 阶段 5：Executor、Map 与 Stream

实现 OperatorCall 生命周期、异步/同步 Operator、Map 并行限制、有序结果、Aggregation、Stream Iterator/AsyncIterator 和 StreamReducer。

验收标准：真实 Task/Future/Iterator 不进入 RuntimeState；每个实际 OperatorCall 有可重建的逻辑状态；Map 是一次 NodeOccurrence；Stream 最终归约为一次 Executable Output。

测试范围：同步/异步调用、零输入、异常、取消、Map 空列表、并行上限、乱序完成有序聚合、Aggregation 失败、同步/异步 Stream、Reducer 失败、非流返回、资源释放和执行时间统计。

## 阶段 6：数据映射与 Context 原子提交

实现 Input Mapping、Condition、Output Binding、ContextPatch、只读 Context 和并行写冲突。

验收标准：Node Output、ContextPatch、Node 终态和 Scheduler 后继转换属于一致的原子边界；Hook 不能修改权威 Context。

测试范围：入口输入、单入边直传、多入边映射、按 Source Node id 读取、Condition Complete/Error、Patch Set/Delete、Invocation/Session Context、并行非冲突、并行冲突、Binding 失败回滚和 Replay 等价。

## 阶段 7：Wait、Resume、Cancel 与 Recovery

实现可声明请求/响应契约的 Wait、多个 Wait id、Resume Command、Cancel 和进程丢失后的逻辑状态协调。

验收标准：Wait 绑定具体 NodeOccurrence；请求和响应都经过契约验证；运行中的进程对象丢失后通过新 Event 转换为 lost/recovering，不伪造 Python Task 恢复。

测试范围：自定义请求/响应、无效响应、重复 Resume、多个并行 Wait、Wait 与运行分支并存、最新 Context、Cancel/Resume 竞争、运行 Operator 丢失和 Loop 内 Wait。

## 阶段 8：AutoAgentApp、Core API 与回放

实现 AutoAgentApp 的 Workflow 注册与编译缓存，以及 invoke、ainvoke、resume、cancel、replay、recover 和 RuntimeEvent/UserEvent 的内存输出。为未来持久化、Event 发送、远程 Executor 和 Task Runtime 定义窄 Port，但不提供这些外部模块的实现。

验收标准：Core 只产生 Event，不负责持久化或发送；调用方可以从 invoke 结果读取或迭代 Event；从任意 Runtime Event 恢复当时完整 State。

测试范围：Workflow 注册冲突、Revision 缓存、同步/异步 invoke、事件迭代、UserEvent 顺序、完整 Workflow 结果、任意序列 State 查询、终态恢复、等待态恢复、Fork seed、多 Session 隔离，以及未配置任何外部 Port 时的纯 Core 执行。

## 阶段 9：清理与性能验收

删除旧 V2 的 App/Sink/Checkpoint/持久化兼容结构和已经失效的测试，只保留新的 Core 模型。

验收标准：

- 全部 V2 Core 测试通过；
- Core import graph 不包含数据库、Server、Sink 和远程发送；
- 统计单 Node、长 Loop、大 Context 小 Patch、大 Map 的时间、峰值内存、Event 字节数和值复制次数；
- RuntimeState 不在每个 Event 整体 deepcopy；
- Snapshot 不属于 Core 的运行正确性依赖。

## 阶段 10：V1 Core 功能基线验收

从 V1 的实际源码和测试提取 Core 功能清单，建立“V1 能力、V2 对应实现、V2 验收测试、明确舍弃原因”矩阵。该阶段验证功能完整性，不要求 API 或数据模型向后兼容。

验收标准：

- V1 中属于 Workflow、Compiler、Runtime、Scheduler、Executor、Loop、Wait/Resume、Event 和 App 入口的有效能力都有 V2 对应实现或明确的设计性舍弃结论；
- 使用 V1 的真实运行场景反测 V2，发现缺失时退回所属阶段补齐，并重新运行所有受影响阶段；
- 删除 V2 中没有当前用途、没有测试或重复表达同一权威状态的代码；
- 最终代码通过完整测试、类型检查与 import 边界检查，模块职责和公开接口保持最小且清晰。

测试范围：V1 Core 测试场景迁移、端到端 DAG/Loop/Wait 场景、异常与取消路径、逐 Event Replay/Recovery、公开 API 冒烟测试，以及 V1/V2 能力矩阵中的每个保留项。
