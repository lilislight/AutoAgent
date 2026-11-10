# AutoAgent V2 后续事项

更新日期：2026-11-09。

本文记录当前确认的推进顺序、阶段边界和验收目标，不代表下面的 API 已经实现。
当前源码和测试是具体行为的依据；V1 只用于参考，不保留旧 API 或数据模型兼容层。

## 当前基础

当前 V2 已具有 Core Workflow 编译与执行、Node/Operator 边界恢复、父子 Workflow、
Signal、Timer、Select、Wait、Pause、Retry 和 Fork 等基础能力；Host 提供异步
SQLite/PostgreSQL 持久化、驻留策略、Checkpoint 管理和历史状态检查；Server、
本地 UI 与 CLI 已覆盖主要执行和查询操作。后续按实际使用反馈继续完善。

`autoagent.agent` 已提供 LLM Provider、结构化输出、流式输出、Tool、MCP、Skill、
Embedding、媒体处理和调用记录基础。后续重点是补齐组合能力，不是重新实现这些组件。

`autoagent.evaluation` 已接入 Host 的 `ExecutionView`，统一读取中间结果、路径、状态变化、
历史 Context 和业务消息；业务规则不解析 Runtime Event。输入/预期只读、规则和条件身份、
JSON 调用、并行审批及可移植报告已实现。专项业务验收与后续事项见下方清单。

## 推进顺序

1. Evaluation。
2. AutoAgent 开发 Skill：让 LLM 编写、调试和优化 Workflow。
3. Agent Harness 基础补全。
4. 通用 ReAct Workflow。
5. Coding Agent。
6. 中心化平台，后置，不阻塞本地能力建设。

上述顺序是主要交付顺序。开发 Skill、示例和 Evaluation 会随后续 SDK 持续更新，
不要求在开始下一阶段之前一次性覆盖所有未来能力。

## 1. Evaluation

目标：自动验证业务正确性和改动效果，为 LLM 优化 Workflow 提供明确证据。
模块为 `autoagent.evaluation`，通过 Host 执行，不创建第二套 Runtime 或执行历史存储。
唯一评测对象是业务 Workflow；通用 Agent 作为普通 Workflow 参与评测。
独立 Operator/函数单元测试交给 unittest，不新增 Node Eval、Operator Eval 或 Agent 专用
评测体系。Node、Map item、Child 等仅作为完整业务过程中的观测对象。
业务 Scenario、Evaluator 和 Evaluation 模块不直接解析 Runtime Event、delta、投影表或
Artifact 引用；这些由 Host 的统一历史查询封装。用户只读取业务执行事实并编写业务规则。
当前范围是业务正确性及版本比较；耗时、内存衡量暂缓，后续评估复用已有测量设施。
Agent 调用效率、Token 和费用不属于通用 Eval 的内置范围。

### 已交付的基础能力

- [x] Suite、Case、异步 Python Scenario、CheckResult 和可调用 Evaluator 公共契约。
- [x] 单次调用、Session 别名复用、多个 Workflow、基本 Wait 回复和单层父子审批。
- [x] Runner 管理独立 Session/数据库、Case 选择、并发、重复运行、超时及清理。
- [x] 提供 StatusIs、OutputEquals、NodeExecuted、RouteTaken 和同步/异步自定义 Evaluator。
  已删除 Eval 的原始 Event 读取和 EventCount；底层历史解释统一由 Host 负责。
- [x] 区分业务检查失败、执行异常、评估器错误、未评估和中断；预期的 Workflow 失败
  可以满足业务检查，无有效判断或存在未评估检查时不自动通过。
- [x] 通过 Host 固定持久化证据位置；各 Child 使用自己的 Session Event sequence，
  大值按需读取，支持 Context JSON Pointer，报告不复制整份执行历史。
- [x] 原子 JSON 报告、逐 Trial 保存、Invocation/Event 引用和基本代码/环境身份记录。
- [x] 按 Case ID、输入/预期指纹、规则身份和明确声明的业务条件比较；同时显示规则级变化。
  未声明条件或存在不透明规则依赖时，比较为 inconclusive，不认定代码改进。
- [x] TOML Evaluation entrypoint 惰性发现，以及 `eval list/check/run/show/compare`、
  结构化输出、输出文件和进程退出码；提供离线示例。

核对依据为当前 SDK、CLI、示例及 SQLite/PostgreSQL 集成测试。可靠性复现已转为回归测试；
验收同时检查中间过程、实际夹具状态和合法内部重构，不能只凭最终结果相同认定业务正确。

### 业务功能目标

以下能力通过同一个业务视图提供，不为每项功能另建模块或 Service。

| 业务需要 | 统一视图提供的能力 |
| --- | --- |
| 最终状态和结果 | Invocation 结果、错误及业务字段读取；执行状态与暂停控制状态分开表达。 |
| 中间业务结果 | 按 Node 选择发生，读取映射后输入、提交输出、Map 各项及聚合结果。 |
| 业务路径 | 查询指定路由是否真正被采用、必要阶段是否执行、禁止阶段是否发生。 |
| 状态变化 | 查询 Workflow/Node 的状态变化，定位完成、等待、恢复等业务位置。 |
| 历史 Context | 在选定节点完成、等待请求等位置读取 Session/Invocation Context 的指定路径。 |
| 业务交互 | 等待目标审批可回复、节点达到某状态、Child 完成或业务消息出现，再执行操作。 |
| 组合业务 | 在同一个 Case 中组织多轮 Session、多个 Workflow 和父子业务过程。 |
| 对外业务结果 | 读取持久化业务消息，以及夹具提供的外部业务状态。 |
| 回归比较 | 按 Case ID 和稳定业务规则名比较，展示已修复、新增失败及可定位的证据。 |

### 统一执行视图的实现方案

以一个 `ExecutionView` 作为公共读取入口：业务规则通过它查询节点执行、路由、状态变化、
Context、子任务和业务消息。节点与路由对象是查询结果，不是新的测试入口。
Scenario 负责执行和交互；Evaluator 只接收固定位置的只读视图、预期和业务夹具。

职责链为：业务 Scenario/Evaluator -> ExecutionView -> Host 历史查询 -> 已有持久化事实。
统一解释、关联和历史边界处理位于 Host，不放入每个 Evaluator，不修改 Core 调度语义。
已有 Node/Call/Edge 投影、graph_changes、历史 Context 读取和 Artifact 按需读取继续复用。
最新投影只能回答对应最新位置的问题；旧位置通过有界历史查询恢复事实，不能过滤当前
行的 last_sequence 来冒充历史状态。必要索引按实际查询补充，不复制完整 Input/Output。

视图契约必须明确以下语义：

- 以 Workflow Revision 校验 Node/路由选择器；不存在的 ID 报错，不能被解释成“从未执行”。
- 默认只查询所选 Invocation；同名 Child 节点不混入结果。跨 Workflow、Child 或多轮调用
  必须显式选择作用范围，确保断言对象明确。
- 重复发生必须显式选择单次、首末次或全部；期望唯一却匹配多次时报错。
  Node 发生、恢复尝试和 Map item 分开标识，不能把跳过的节点算作实际执行。
- 节点输入表示完成 Input Mapping 后的业务输入；节点输出表示成功完成并提交的输出。
  Map item 结果、聚合结果及尚未提交的阶段结果明确区分；无法读取与合法 JSON null 分开。
- 路由采用以实际分支激活为依据，不以 condition=true 代替；区分已采用、未采用、
  未到达和尚未决定，不把未到达分支直接标记为失败或跳过。
- 用户用节点完成、状态变化或交互请求等位置对象选择历史点，不输入 Runtime Event 序号。
  内部仍保留精确来源和读取上界，用于稳定重读、错误定位及已有调试入口跳转。
- 同一 Session 内按确定的持久化顺序判断；跨 Session 只使用已记录的父子、消息等因果
  关系。没有依据时返回无法判定，不用时间戳或不同 Session 的序号推测先后。
- 视图是不可变观察结果；等待新业务条件返回一个新视图，不改变之前已捕获的状态和结果。
  “截至当前位置未发生”与“整个业务流程从未发生”使用不同的明确检查范围。
- 所有分页、读取预算和缺失证据由框架处理；部分读取不能证明不存在，不能悄悄通过。
  查询结果默认只有必要元数据，大值通过 JSON Pointer 和来源引用按需读取。
- 外部扣款、文件等实际副作用以业务夹具状态为依据；执行记录只证明框架观察到的调用。

保留少量通用业务判定能力和自定义 Python 规则，不引入复杂规则 DSL。
规则可以要求必要阶段、禁止行为、数据关系及因果顺序；不将全部节点或 Event 的精确序列
作为默认正确轨迹。每条规则使用稳定名称，报告自动关联本规则读取的业务对象和历史位置，
给出简短的预期/实际差异，无须用户拼装 Event ID。

### 已修复的可靠性缺口

- [x] 实现上述 Host 统一历史视图和 Eval 业务查询入口，覆盖中间结果、路由、状态及 Context。
  从 Eval 公共接口移除原始 events/event_value/EventCount 依赖；Host 的底层调试接口保留。
- [x] 固定每个 Trial 的输入、预期和评估参数，防止 Scenario/Evaluator 改写嵌套 JSON
  后仍沿用原来的报告指纹；改变评测依据必须显式报错，不能产生通过结论。
- [x] 明确业务规则版本、参数及依赖身份；自动指纹只作辅助，全局参数和辅助函数变化
  不能漏判，不透明或动态依赖无法可靠确认时标记条件未知，不承诺解析任意 Python 依赖。
- [x] 区分被测代码版本与评测条件，记录可确认的外部服务、fixture 和 dataset 配置身份；
  业务标准变化判为不可比较，条件变化或缺失时不给出可接受的改进结论。
- [x] invoke/Signal 等 JSON 操作复用 Host 的合同解码边界，覆盖 Enum 等合法值合同，
  保证同一输入在 Eval、Server 和 CLI 下的执行语义一致。
- [x] 统一业务条件等待与交互：回复审批只等待目标 Wait 可回复，不要求整个 Root 到达
  boundary；支持并行 Node/Child 仍运行时回复，保留歧义报错、超时和 Root 所有权校验。
- [x] 报告和比较落实到稳定业务规则名，包含违例对象、观察位置、预期/实际小片段和
  可供既有调试入口使用的来源；同为失败的 Case 也要显示新增失败与已修复规则。
- [x] 统一内部报告与 CLI 导出文档的读取，支持导出文件直接 show/compare；
  明确报告移动后执行历史的定位与不可用提示。

### 证据和业务场景验收

- [x] 通过统一视图提供持久化业务消息；Host 内部按各 Invocation 的 User Event sequence
  独立固定读取上界，与 Runtime Event sequence 分开，不向业务规则暴露二者的底层计数器。
- [x] 验证中间金额与审批输入一致、Context 只在约定阶段更新、失败路径与禁止阶段正确；
  故意制造“最终结果相同但中间流程违例”的版本，必须被业务规则检出。
- [x] 验证审批前不执行副作用、拒绝审批后不执行副作用、重复提交不重复执行；
  使用业务夹具核对实际状态，不能只检查 Agent 的文字或最终输出。
- [x] 验证多轮 Session Context 继承、跨 Workflow 共享及跨 Session/Trial 隔离，
  包含历史位置读取和大 Context 的指定路径读取。
- [x] 用可控外部依赖和真实业务调用路径验证完整 Workflow；通用 Agent 可作为其中一个
  Workflow 用例，检查业务过程与结果，不建立独立的 Agent/Operator 评测目标或专用指标。
  支持显式选择的真实集成评测，基础验收不依赖凭据或付费服务。
- [x] 验证 Map/循环、多 Child、父子 Signal、并行审批及取消/恢复后的业务结果，
  不要求并行分支具有固定的全局完成顺序。
- [x] 验证多页 Event、大 Artifact、读取预算不足及缺失证据；不完整证据不能通过
  “从未发生某动作”一类否定判断；所有业务规则均通过统一视图，无须解析 Event。
- [x] 验证多次 Node/Map 发生不会隐式只检查末次、错误选择器不会被当作未执行、条件成立
  但路由未实际激活不会被当作已采用；历史视图不受后续回复或恢复影响。
- [x] 合法地调整内部并行或实现细节但保留业务约束的候选版本，应保持通过；
  不要求新旧 Workflow 具有相同的完整节点序列或内部调用实现。

### 已落实的实现顺序

1. 定稿业务查询契约；固定评测依据、规则/条件身份，修复 JSON 调用，将已有复现转成测试。
2. 实现 Host ExecutionView：固定观察范围，统一 Node/Map 结果、路由、状态、历史 Context
   和值引用读取；用历史/当前结果一致性测试验证，不新增 Runtime 或完整数据副本。
3. 实现业务条件等待、父子交互及持久化业务消息；保证历史视图不可变且并行审批可操作。
4. Scenario/Evaluator 全部接到统一视图，移除公共原始 Event 依赖；补业务规则级报告、
   比较和可交换导出，保持 CLI 简洁。
5. 扩展现有示例为完整业务 Suite，覆盖中间结果、路径/状态、Context、副作用和组合流程；
   验证 SDK/CLI、SQLite/PostgreSQL、读取边界和异常清理，完成业务验收。

实现入口为 `Host.get_execution_view()`。业务规则通过 `execution.node(id).one()/all()`、
`input()/output()/items()`、`route(id)`、`transitions()`、`children()`、`interactions()`、
`messages()` 查询；通过 `execution.at(node.completed)` 和 `context(pointer=...)` 读历史 Context。
Scenario 使用 `wait_for()` 等待业务条件，`respond()` 回复指定审批。离线完整业务示例位于
`examples/evaluation_demo/orders.py`，涵盖批准、拒绝、实际扣款和持久化业务回执。

边界：任意历史业务位置没有可靠的 User Event 边界时，`messages()` 明确不可用，不按时间戳
猜测。固定观察中的业务消息使用独立持久化上界。缺失历史和读取预算不足不能被解释成
业务动作未发生。跨 Session 仅通过已记录的创建、结算、Signal/回复关系判断因果顺序。
导出 JSON 包含比较所需的结果和来源位置，不打包数据库；移动报告仍可 show/compare，
深入读取执行历史需要保留报告中标识的数据库。外部服务/dataset 条件通过 `Suite.conditions`
显式声明，自动采集仅包含可确认且不泄露凭据的配置身份。

本轮专项验收：89 项 Eval/ExecutionView/CLI 测试和 28 项历史状态检查测试通过，包含实际
SQLite/PostgreSQL；订单示例通过真实 CLI，wheel 已核对新查询模块及旧接口移除。
整个 V2 的通用回归尚未通过：另有 Server/Fork Timeline 用例失败和 CLI 执行模块超时，
已停止该轮通用回归，不能把本轮专项通过解释为整个仓库全绿。上述问题需要单独处理。
当前 Eval 报告为 schema_version=2；不保留旧 Evidence/EventCount 接口及旧报告模型兼容。

### 后续事项

- [ ] 从历史执行提取 Case 草稿；可复用输入和交互记录，预期结果必须另行确认。
- [ ] 按需要增加 Eval UI，不作为本轮修复的前置要求。
- [ ] 后续设计耗时、内存的测量与比较；本轮不实现性能评分，超时仅用于执行保护。

验收：同一组 Case 能对原版本和候选版本给出可解释的差异，并定位失败证据。
放宽评估标准、替换测试夹具或丢失证据不能被误判为代码改进；JSON 调用与并行交互
不得因经过 Eval 包装而改变原业务语义。
全部业务评测通过统一执行视图完成；评测代码不需要知道 Runtime Event 名称、序号、
payload/delta 结构和数据库表结构。中间业务违规不能被正确的最终结果掩盖。
参考 V1 多步 Case、Evaluator、惰性证据和报告，不照搬单 Workflow 绑定限制及旧 Host API。

## 2. AutoAgent 开发 Skill

目标：让外部 Coding Agent 根据已安装的 V2 SDK 和 CLI 正确创建、诊断和优化项目。
这是项目开发指导，与业务 Agent 使用的 `autoagent.agent.skills` 不同，
也不依赖第 5 阶段的原生 Coding Agent 已完成。

- [ ] 编写 V2 Workflow authoring Skill，覆盖图、值契约、Context 和恢复边界。
- [ ] 提供最小项目模板、规范示例及按需读取的 SDK/CLI 参考，明确 SDK 版本。
- [ ] 定义 Debug/Optimization 流程：读代码、检查编译、查看证据、修复、评测和比较。
- [ ] 指导何时查询摘要、Node/Call 详情或精确历史 Context，避免一次读取完整 Event/大值。
- [ ] 用任务集验证 LLM 的编写、定位、最小修复和回归验证能力。
- [ ] 确定 Skill、参考材料和示例的分发方式，使仓库外项目也能获取准确说明。
- [ ] 随 Harness、ReAct 和 Coding Agent API 增量更新 Skill 和任务集。

验收：外部 LLM 无需依赖框架内部实现，即可用 SDK/CLI 修复真实失败案例，
并交付代码、编译检查、业务评测结果和证据。参考 V1 Skill/Skill Evals，按 V2 重写。

## 3. Agent Harness 基础补全

目标：补齐现有 `autoagent.agent` 的通用组合能力，减少应用重复编写消息和控制逻辑。
这些是模块内部的构件和策略，不分别拆成独立 Service。

- [x] 多轮对话、模型请求、Tool Call/Result、精简最终输出及 Session/Invocation Context 契约。
- [x] 区分完整历史和模型本次输入，支持 Context Policy、输入估算和长 Context 压缩。
- [x] 显式 `compact_context` Node，记录摘要覆盖范围，保留原始历史；支持分批摘要。
- [x] ToolSet、MCP、Skill 的允许、拒绝、审批以及参数级操作权限组合。
- [x] 审批和 Ask User 使用 Core Wait；Session Settings 持久化，执行快照固定设置。
- [x] 参数验证、业务错误作为 Tool Result 返回，模型在轮次上限内自行修正；
  与网络重试、Node Retry 和 Recovery 分开处理。
- [ ] 结构化输出自动修正策略；目前结构校验失败显式报错，不隐式修复或重试。
- [x] Node、Invocation、Session、Workflow（跨 Revision 和时间窗口）、Project 用量限制；
  Token/Cost 记录复用 LLM Usage，支持本地价格数据和自定义计算。
- [x] 基于 Spawn、Signal、Wait/Select 组织子 Agent，自动收消息和有时限等待，复用现有 Runtime。
- [x] SQLite/PostgreSQL 集成测试覆盖多轮、并行工具、审批、父子协作和恢复边界。
- [ ] 用 Evaluation 场景专项验证通用 Agent 的多轮、并行审批和父子业务交互；
  通用 Workflow 的对应 Evaluation 能力已具备，Agent 集成测试不代替该专项验收。
- [ ] 扩充真实长任务的质量与效率评测证据，不以单元测试代替实际任务验证。

验收：构件可以组合为普通 Workflow，模型调用、工具执行、审批均有可观察、
可恢复的 Node/Operator 边界，不支持函数内部任意步骤恢复。

## 4. 通用 ReAct Workflow

目标：用上述构件提供简单、可组合的通用 Agent Workflow。

- [x] `agent_workflow(...)` 构建 API，支持指令、模型、Tool/MCP/Skill、Workspace 和执行策略。
- [x] 普通 Workflow 图：Context 准备、可选压缩、Request 构建、模型调用、工具分流、
  并行执行、审批、Ask User、子任务等待和轮次完成；统一 Back Edge。
- [x] 多轮 Session、父子调用、动态资源刷新和按 Workflow 授权的 Host 资源。
- [x] LLM 输出和聚合 Tool Result 就近提交历史；处理审批拒绝和预算耗尽。
- [x] User Event 区分流式临时内容和持久化完整结果，复用 Trace、Checkpoint、恢复和 Fork。
- [x] 提供 `examples/agent_workflow`、`examples/agent_server_demo` 等示例。
- [x] SDK 内置 Agent 可通过环境变量注册；独立 `/agent` 页面复用 Host API、User Event 和共享标题栏。
- [x] Workspace 管理和 Session 绑定、资源侧栏、模型/审批/访问权限设置；旧聊天查询当前资源目录。
- [ ] 补齐开发 Skill 中对应的编写、调试说明。
- [ ] 调试产品默认 Instructions 和通用 Skills，验证真实任务质量。

验收：少量配置即可创建具备工具调用和多轮能力的 Agent，继续通过现有 Host、CLI、UI
检查执行。内置 Agent 按产品提供默认行为，自定义 Workflow 使用 SDK 构建入口。

## 5. Coding Agent

目标：组合 ReAct、AutoAgent 开发 Skill、CLI 和 Evaluation，形成可验证的本地开发闭环。

- [x] 本地 Workspace 解析、项目指令发现、Skill 发现及三个按需读取工具。
- [x] Python 文件工具：目录浏览、搜索、读取、写入、定点编辑和文件管理；
  参数级路径策略、敏感文件审批、版本检查和文件修改并发控制。
- [x] `execute_command` / `manage_process`：exec、Bash/Pwsh、共享命令规则、参数匹配、
  进程超时、清理、有界输出、stdin 和游标读取；Git/测试可通过 CLI 执行。
- [x] 可替换 Workspace/Execution 接口和本地实现；Workspace 绑定后才暴露相关内置工具。
- [x] 组合模型、Tool、Skill、Context 管理、Ask User 和子 Agent 协作。
- [x] 提供本地可监督使用方式及可交互测试示例。
- [ ] 独立补丁应用/交付流程及完整 Coding Agent 任务验收。
- [ ] 真正的文件系统/依赖环境隔离和沙箱；现有 CLI 使用应用 OS 权限，路径策略和
  缓存/临时目录配置均不构成沙箱。PTY、远程执行适配后续按需增加。
- [ ] 在真实项目上验证定位、修改、编译检查、评测、基线比较和修复迭代完整闭环。
- [ ] 产出可审查的补丁、运行身份、执行/评测证据和失败说明。

### 工具专项优化

以下来自两次实际任务审查：`d5d858ad-a185-48e4-bd1d-282c5cc5da72` 和
`926c4643-da46-4d6c-9c5d-4d35b6f0b4c4`。

- [x] 冻结文件授权保留原有目录锚点，允许同批其他调用创建普通父目录；替换锚点或改变目标仍拒绝。
- [x] 用户拒绝统一为 `tool_approval_denied`，保留可选原因，明确停止操作、不换包装重试；
  UI 支持填写原因。该结果提供模型反馈，不改变用户后续主动授权的能力。
- [x] 模型可见的等待/超时上限与配置一致；新增 `manage_process(action="wait")` 等待退出，
  与输出读取等待、启动等待和进程运行超时分别表达。
- [x] `log_path` 保存有上限的完整命令日志，返回短预览及真实进程退出码，禁止覆盖已有日志；
  Shell 仍保留自身退出语义，不把最后一个命令的状态冒充内部所有命令的状态。
- [x] 搜索结果合并相邻/重叠 Context，匹配位置与共享文本分开返回，减少重复源代码。
- [x] 可选 Workspace 缓存/临时目录配置，重定向常用缓存及临时目录环境变量，复用文件权限校验。

### 明确暂缓

- [ ] 优化默认 Instructions，减少多余辅助脚本、重复环境探测和过长推理；本轮不修改。
- [ ] 单独分析框架调度和持久化耗时，区分模型、外部等待、Map/空分支及数据库开销；
  本轮不调整 Core 调度或持久化。实际任务的总耗时不能直接认定为工具执行性能问题。

验收：增加模型或工具适配器不能代替完整业务闭环验收；工具回归与真实任务质量分别记录。

## 6. 中心化平台：后置

目标：管理多个 Host 的执行、证据和项目开发闭环，本地 Host/Server 继续可独立使用。

- [ ] Pod 端保持 Core + Host，Host 管理 Runtime 和持久化发送，本地 Server 提供执行控制。
- [ ] 中心接收多个 Host 的 Runtime Event、User Event、Artifact、Checkpoint，并提供查询。
- [ ] 设计远程持久化确认、幂等、断线补发、容量限制和故障恢复，不增加 Runtime 状态权威。
- [ ] 关联 Host、项目、Repository、代码版本、Workflow Revision 和执行身份。
- [ ] 设计远程控制所有权、鉴权、租户隔离和路由，避免同一执行被多个 Host 同时推进。
- [ ] 扩展中心化 UI、Inbox、权限和审计；本地通知可按真实审批需求提前补充。
- [ ] 接入 Coding Agent、Evaluation、代码审查和 CI/CD，逐步形成跨部署的优化闭环。

当前仅保持必要身份和接口边界，不提前实现中心平台、全局调度或自动部署。
远程 Operator/Child 执行、触发器和调度按场景另行评估，不作为本地主线的前置条件。

## 持续工程事项

- [ ] 用真实 Agent 长会话、大 Map、长 Wait 和父子场景检查内存、查询成本与恢复性能。
- [ ] 评估 Ruff、Pyright/Mypy、公共协议和 import graph 的工程检查。
- [ ] 按实际编译成本评估定义摘要缓存，先明确 Workflow authoring 对象的可变性边界。
- [ ] 若需要精简 Event 捕获，先证明恢复、Checkpoint 和历史检查等价，不删除恢复必需事实，
  不提前承诺 Full/Standard/Minimal 三套模式。
- [ ] 统一演进错误 phase/code 和诊断契约，避免零散增加含义重叠的 API。

长期记忆/知识检索、模型路由、Prompt 管理和远程执行先按应用需求组合已有能力；
有明确复用需求后再决定是否独立成模块。版本身份、权限、幂等和资源预算贯穿上述阶段。

## 不在当前路线中的改动

- 运行中动态修改 `WorkflowIR`。
- 函数内部任意步骤恢复。
- Map item 沿普通 Edge 独立扩散，或 Stream Chunk 反复激活普通 Node。
- 向 Core 放入数据库、UI、Repository 工作区或中心化平台职责。
- 为每个 Harness 策略另建调度、状态或持久化机制。

## 验收原则

每阶段先设计公共契约，再交付最小可用能力，配套示例、必要测试和 CLI 结果。
Python 使用根目录 `.venv`；Persistence、Host、Server 或 Agent 记录改动必须验证
SQLite 和 PostgreSQL，不能将跳过 PostgreSQL 当作完成。广泛 Runtime 改动执行完整 V2
测试，其他改动执行与范围相符的检查；具体命令和约束以 `AGENTS.md` 为准。

每个新增 `test_*` 方法第一行使用简短 docstring 说明行为或失败边界。
