# AutoAgent V2 后续事项

更新日期：2026-10-23。

本文记录当前确认的推进顺序、阶段边界和验收目标，不代表下面的 API 已经实现。
当前源码和测试是具体行为的依据；V1 只用于参考，不保留旧 API 或数据模型兼容层。

## 当前基础

当前 V2 已具有 Core Workflow 编译与执行、Node/Operator 边界恢复、父子 Workflow、
Signal、Timer、Select、Wait、Pause、Retry 和 Fork 等基础能力；Host 提供异步
SQLite/PostgreSQL 持久化、驻留策略、Checkpoint 管理和历史状态检查；Server、
本地 UI 与 CLI 已覆盖主要执行和查询操作。后续按实际使用反馈继续完善。

`autoagent.agent` 已提供 LLM Provider、结构化输出、流式输出、Tool、MCP、Skill、
Embedding、媒体处理和调用记录基础。后续重点是补齐组合能力，不是重新实现这些组件。

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
新增 `autoagent.evaluation`，通过 Host 执行，不创建第二套 Runtime 或持久化体系。

- [ ] 设计 Suite、Case、执行步骤、Evaluator 和报告的最小公共契约。
- [ ] 支持单次调用、多轮 Session、多个 Workflow、Wait 回复及必要的父子交互。
- [ ] Runner 管理 Case 隔离、并发、超时、清理和失败归因。
- [ ] 提供结果、状态、必要执行行为的确定性检查及自定义 Evaluator 扩展点。
- [ ] 区分业务检查失败、Workflow 执行错误和评测器错误。
- [ ] 支持可替换的 Model/Tool 测试夹具和明确选择的真实集成评测。
- [ ] 记录 Revision、代码版本、模型配置和夹具等可获取的实验身份，说明不可复现因素。
- [ ] 报告引用 Invocation/Event；默认读取摘要和投影，详细值按需获取，不复制整份历史。
- [ ] 比较基线和候选版本的结果、必要路径、耗时及可获得的 Token 用量。
- [ ] 提供简洁 CLI、结构化报告和明确退出码；UI 展示按需要后续补充。
- [ ] 后续从历史执行提取 Case 草稿；可复用输入和交互记录，预期结果必须另行确认。

验收：同一组 Case 能对原版本和候选版本给出可解释的差异，并定位失败证据。
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

- [ ] 明确多轮对话、模型请求、Tool Call/Result、最终输出及 Session/Invocation Context 契约。
- [ ] 区分完整历史和模型本次输入，支持 Context 构建、工具结果裁剪和长 Context 压缩。
- [ ] 摘要涉及模型调用时使用显式 Workflow 步骤，记录覆盖范围，保留原始证据。
- [ ] 复用 ToolSet、MCP 和 Skill，补齐允许、拒绝及需要审批时的组合流程。
- [ ] 审批落在 Core Wait 等可恢复边界，不在 Operator 内部等待不可恢复的人工步骤。
- [ ] 支持有界的参数修正和结构化输出修正，分清它们与网络重试、Node Retry、Recovery 的语义。
- [ ] 复用已有 Node 限制，补充必要的 Agent 轮次、用量预算和明确停止原因。
- [ ] 基于 Spawn/Signal/Wait 组织子 Agent 委派、进展、提问和结果收集，不新增 Agent Runtime。
- [ ] 用 Evaluation 验证多轮、并行工具、审批、父子协作和恢复边界。

验收：构件可以组合为普通 Workflow，模型调用、工具执行、审批均有可观察、
可恢复的 Node/Operator 边界，不支持函数内部任意步骤恢复。

## 4. 通用 ReAct Workflow

目标：用上述构件提供简单、可组合的通用 Agent Workflow。

- [ ] 设计最小构建 API：指令、模型绑定、工具集合、输入/输出契约和有限执行策略。
- [ ] 生成普通 Workflow，使模型、工具、结果回填、修正和退出条件在图中可见。
- [ ] 支持多轮 Session、Tool/MCP/Skill，并能作为 Child Workflow 被调用。
- [ ] 定义消息追加、并行工具结果关联、审批拒绝和预算耗尽行为。
- [ ] 保持流式 User Event、Trace、Checkpoint、恢复和 Fork 语义一致。
- [ ] 迁移已有手写 Agent 示例，减少重复的消息拼接和控制逻辑。
- [ ] 补齐 Evaluation 和开发 Skill 中对应的编写、调试说明。

验收：少量配置即可创建具备工具调用和多轮能力的 Agent，继续通过现有 Host、CLI、UI
检查执行。参考 V1 `react_workflow` 的用途，按 V2 契约重新实现。

## 5. Coding Agent

目标：组合 ReAct、AutoAgent 开发 Skill、CLI 和 Evaluation，形成可验证的本地开发闭环。

- [ ] 提供 Repository 工作区和工具：搜索、读写/应用补丁、Shell、Git 和测试执行。
- [ ] 明确工作区隔离、权限、依赖环境、进程清理、超时和输出容量；现有 Skill 子进程
  执行器使用应用自身 OS 权限，不能当作完整沙箱。
- [ ] 将工作区定义为可替换的执行环境适配边界，初期先完成本地最小实现。
- [ ] 组合模型、工具、Skill、Context 管理和必要的子 Agent 协作。
- [ ] 完成需求分析/问题定位、改代码、编译检查、评测、基线比较和修复迭代。
- [ ] 产出可审查的补丁、运行身份、执行/评测证据和失败说明。
- [ ] 首先提供本地、可监督的使用方式，自动发布、部署和中心化任务分配后置。

验收：在真实 AutoAgent 项目上完成从失败 Invocation 到修复和评测验证的完整任务。
增加模型或工具适配器不能代替闭环验收。

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
