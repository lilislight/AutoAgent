# V1 Core 能力基线

本表用于核对能力，不承诺 V1 API 或数据模型兼容。V2 以当前源码和测试为准。

| V1 能力 | V2 Core 结论 | 验收位置 |
| --- | --- | --- |
| Workflow/Node/Edge/Operator、不可变 IR | 重写并保留 | phase1 |
| 编译诊断、稳定 Revision | 保留；增加 Portable Snapshot 与 Hook version | phase1 |
| 多入口/多出口、Fan-out、完整 Fan-in | 保留 | phase1、phase3、phase8 |
| Complete/Error Edge 与 Condition | 保留；Condition 严格返回 bool | phase3、phase9、phase11 |
| Inline SubWorkflow | 编译期命名空间展开 | phase1 |
| 独立 Child Workflow | 支持 await/spawn 及两者与 Map 组合 | phase8、phase11 |
| Capability | Registry 支持 default、priority、启停和受限 Resolver | phase9、phase11 |
| Map/Replication | 统一为一次 NodeOccurrence 内的 Map | phase5、phase8 |
| Stream | StreamReducer 生成单个 Node Output；Chunk 为 UserEvent | phase5、phase9 |
| 自然、Self、嵌套、共享 Header Loop | 使用 scoped NodeOccurrence 保留 | phase1、phase4、phase8 |
| 增量并行推进 | 保留，不设置 ready batch barrier | phase8 |
| Session/Invocation Context | ContextPatch 与 Node 终态原子提交，检测并行路径冲突 | phase6 |
| Wait、多 Wait、Resume、Cancel | 保留；控制操作使用精确 InvocationRef | phase7、phase8 |
| Runtime 状态重建 | StateOperationBatch + 唯一 StateReducer；Host 接收 RuntimeEvent | phase2、phase10 |
| SDK Trace/User Event | 与 canonical RuntimeEvent 分离，结果不泄漏 StateOperation | phase9、checkpoint-trace |
| Checkpoint 与关闭恢复 | 返回 Root/Child Bundle；支持 load + recover/resume | phase8、phase9、phase11 |
| 运行 Operator 恢复 | 标记 lost；默认拒绝，显式 replay_safe 才按次数恢复 | phase7、phase8 |
| 同步/异步 invoke、submit、wait | 保留并增加严格背压 stream/astream | phase8、phase11 |
| Child Handle status/wait/cancel | 本地可操作，加载 Checkpoint 后仍有效 | phase8、phase9 |
| 执行次数保护 | App 级 Invocation Node 执行上限，不是 Node authoring 字段 | phase1、phase9 |
| 并发限制 | App 级同步/异步 Operator 全局上限 + Map 局部上限 | phase8、phase9 |
| Retry/Fallback/Timeout/Backoff | 当前不保留，等待独立语义设计 | `TODO.md` |
| Standard/Minimal 捕获 | 待做等价 capture profile，不复制 Runtime 实现 | `TODO.md` |
| 数据库、历史 Event、Server | 移出 Core；已实现 Host SQLite/HTTP Sink 与本地只读 Tracing | Host/Tracing tests |
| Mailbox、Signal、父子消息、远程 Task | 后续 Command/Task Runtime 原语 | `TODO.md` |
| 历史 Trace/UI | 已实现本地只读 Trace、State、SSE、Graph/Timeline/Inspector | Tracing tests、`ui/` |
| Replay/Fork/Debug | 尚未实现，保留为中心化平台能力 | `TODO.md` |

V2 不恢复旧 Policy、旧单 Session Checkpoint、结果内 canonical Event、历史游标或
事件前缀恢复兼容接口。对外恢复路径是 Checkpoint；Server 的历史恢复路径是 Host 保存
canonical RuntimeEvent、用 Reducer 重建 State，再形成可由 App 加载的 Bundle。

## V1 验证说明

2026-08-16 在仓库根目录运行 V1 全量 `unittest`，120 秒内未结束，因此没有把 V1
套件记作通过或失败。V2 的能力核对来自 V1 源码、有效测试场景迁移和上表逐项验收；
V1 本身未被修改。
