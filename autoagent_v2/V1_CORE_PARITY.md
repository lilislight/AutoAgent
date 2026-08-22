# V1 Core 能力基线

本表依据当前 V1 `autoagent/core` 源码、V1 测试，以及被本次重写替代的旧 V2 测试场景整理。目标是功能核对，不提供 API 兼容层。

| V1 能力 | V2 Core 结论 | 验收位置 |
| --- | --- | --- |
| Workflow/Node/Edge/Operator 与不可变 IR | 保留并重写 | phase1 |
| 结构化编译诊断、稳定 Revision | 保留 | phase1 |
| 多入口、多出口、fan-out、complete fan-in | 保留 | phase1、phase3、phase8 |
| Complete/Error 路由与 Condition | 保留；Condition 必须返回 bool | phase3、phase9 |
| Inline SubWorkflow | 保留为编译时展开 | phase1 |
| 独立 Child Workflow | 保留 await/spawn | phase1、phase8 |
| Capability | 保留；支持动态 Operator Registry、默认实现、启停和自定义 Resolver | phase1、phase9 |
| Map/Replication | 统一为 Map；相同输入由 InputMapping 重复构造 | phase1、phase5、phase8 |
| Stream | 保留最终归约；Chunk 进入 UserEvent，不保留旧 Stream policy/edge | phase5、phase9 |
| 自然、并行、嵌套、Self、共享 Header Loop | 保留并使用 scoped occurrence | phase1、phase4、phase8 |
| 增量并行推进 | 保留，无 Ready batch barrier | phase8 |
| Session/Invocation Context 与并行冲突 | 保留；ContextPatch 与 Node 终态原子提交 | phase6 |
| Wait、多 Wait、Resume、Cancel | 保留；Wait 绑定 occurrence | phase7、phase8 |
| 运行中兄弟分支与 Wait Resume | 保留，Resume 主动唤醒 Scheduler | phase8 |
| Runtime Event、状态重建、任意前缀 Replay | 使用语义增量 Event + 唯一 Reducer 重写 | phase2 至 phase9 |
| Checkpoint 恢复 | 不在 Core 保存重复完整 Checkpoint；新 App 从 Event 前缀恢复 | phase7、phase8 |
| 运行 Operator 恢复 | 旧 Call 标记 lost；默认拒绝重放，显式 replay_safe 才按预算恢复 | phase7、phase8 |
| Runtime/User Event 分离 | 保留；Stream Chunk 已接 UserEvent | phase9 |
| 同步、异步、submit、wait API | 保留 | phase8 |
| Node 执行次数保护 | 保留 App 级默认有限预算 | phase8、phase9 |
| Retry/Fallback/Timeout/Backoff | 已进入 OperatorPolicy；每个物理 Call 有独立事件 | phase5、phase9 |
| 执行资源预算 | 支持 Node occurrence 数、并发、Invocation 内 Call 数和累计 Operator 运行时间 | phase5、phase9 |
| Standard/Minimal Event 模式 | Full 正确性完成后再做等价压缩；不复制三套 Runtime | `TODO.md` |
| Sink、数据库、Snapshot、Server | 明确移出 Core | `TODO.md` |
| Child Handle status/await/cancel | 已实现本地可操作 Handle | phase8 |
| Mailbox、Signal、父子消息、远程 Task | 作为 Task Runtime/Harness Command 实现 | `TODO.md` |
| Trace、Replay UI、Fork、Debug、Evaluation | 基于后续 Event Store 实现 | `TODO.md` |

旧 V2 测试绑定了已删除的 Policy、Sink、Checkpoint 和兼容 API，不能继续作为新架构测试。其有效场景已经迁移到 phase1-phase9 测试；设计上明确推迟的能力记录在上表和 `TODO.md`。

## 当前基线验证说明

2026-08-16 在仓库根目录运行 V1 全量 `unittest`，120 秒内未结束，因此没有把 V1 套件记作通过或失败。超时前仅观察到 V1 数据库持久化容错场景主动记录的错误日志。V2 的能力核对以 V1 源码、测试场景迁移和上表逐项验收为准；V1 本身未被修改。
