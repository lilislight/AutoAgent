# Core Runtime Graph 重构验证

基线：`92cc4295a6fac9fd5d7111b570c3fd4c6f2be248`。当前结果来自未提交的工作区实现。

## 测量方法

- CPython 3.12，同一环境，前后顺序执行，无并行测试干扰最终测量。
- 使用同一份 `tests/benchmarks/benchmark_runtime_graph.py`；基线从该提交导出的只读源码副本导入，未创建分支或 worktree。
- 每个场景先注册 Workflow，运行 1 次预热及 3 次计时，报告中位数。计时不启用 tracemalloc。
- 前后均等待 Root 和所有 descendants 执行结束；不把旧版 Root 提前返回当作整图完成。Sink 只计数，不保留 Event。
- 另一次执行启用 tracemalloc，记录该次分配的保留内存、峰值，以及卸载 Root 后的保留内存。数值不是容器总内存或 RSS。
- 大输出场景：5 个并发 Root，各自 Map 4 个分支、每个分支再 Spawn 一个 Leaf；共 45 个 Session。每个 Leaf 返回约 1 MiB 文本和 64 行 list/dict 数据。

## 整图执行耗时

| 场景 | 修改前 ms | 修改后 ms | 变化 |
|---|---:|---:|---:|
| 无 Child | 1.770 | 1.679 | -5.1% |
| 32 个 Child | 44.675 | 43.440 | -2.8% |
| 100 个 Child | 134.671 | 135.621 | +0.7% |
| 4 层嵌套 Spawn | 6.910 | 6.352 | -8.1% |
| 5 个 Root，20 个大输出 Leaf | 73.347 | 73.235 | -0.2% |

小场景受调度噪声影响明显。这组数据没有显示持续的执行性能退化，也不构成普遍加速的结论；新模型需要额外记录 joining 边界。

## 内存与卸载

| 场景 | 前/后峰值 MiB | 前/后 Root 卸载后保留 MiB | 前/后剩余 Session |
|---|---:|---:|---:|
| 无 Child | 0.045 / 0.047 | 0.002 / 0.001 | 0 / 0 |
| 32 个 Child | 0.734 / 0.758 | 0.188 / 0.032 | 32 / 0 |
| 100 个 Child | 1.324 / 1.330 | 0.581 / 0.096 | 100 / 0 |
| 4 层嵌套 Spawn | 0.093 / 0.093 | 0.020 / 0.002 | 4 / 0 |
| 5 个 Root，20 个大输出 Leaf | 23.238 / 23.278 | 21.827 / 0.012 | 40 / 0 |

执行期间峰值基本不变。卸载后的改善来自生命周期语义：旧版卸载 Root 后仍保留 Child，新版一次卸载整张图；这不是宣称旧版逐个卸载全部 Child 后仍会保留这些数据。
本测试未施加 512 MiB/1 GiB Docker cgroup 限制，不能据此保证任意 Workflow 不会 OOM。业务 Context、Child 输入和最终结果仍占用运行内存。

## 正确性验证

- 全量 392 项测试通过（`tests.log`）；Core 示例运行成功。
- 覆盖完成屏障、嵌套取消、Child Wait 经 Root resume、并发 ACK、重复结算、失败 ACK 重试、恢复权限与 Map 并发限制。
- 覆盖每个 Child admission 事件断点，包括只有 SessionOpened 的 Child；恢复保持独立 Session sequence，不重新使用已经确认的 sequence。
- 覆盖 planned Child 取消后的 abandoned 标记、缺失已创建 Child 的拒绝、原子图加载、跨 Root 归属冲突、整图卸载和无重复 Parent body 执行。

## 接口与格式变化

- Spawn 输出改为 ChildHandle；App 控制、resident_invocations 和 load 返回 Root InvocationRef。删除 child_invocations/achild_invocations。
- Child Wait 保留。Root 结果的 waits 汇总图中的外部 Wait；resume/submit_resume(root_ref, wait_id, response) 路由到对应 Session。joining_children 本身不结束 join；存在可交付的外部 Wait 时可返回等待输入的边界，status 仍表示 Root 自身的阶段。
- unload_session 返回 RuntimeGraphCheckpoint；AppCheckpoint.graphs 保存完整图集合。默认 capture_checkpoint=False 保持不变。
- SessionCheckpoint 保留为底层快照单元，允许保存尚未开始 Invocation 的 Child Session；App 不再接受独立 SessionCheckpoint 加载。
- RuntimeState/RuntimeEvent schema 升到 6，SessionCheckpoint schema 升到 4，新 Graph/App checkpoint 使用版本 1。旧格式明确拒绝，不添加兼容分支。
- abandoned 只表示失败/取消 Parent 不再创建的 Child Invocation；已创建的 Child 必须保留快照，不能冒充 abandoned。

## 原始结果

- `baseline_graph_final.json` / `after_graph_final.json`：本报告的最终独立对比。
- `baseline_full.json` / `after_full.json`：原有 full_core benchmark 的运行记录。
- `baseline_memory.json` / `after_memory.json`：原有重复 Root 替换内存 benchmark 的运行记录。

当前实现仍保留所有已创建 Child 的必要 State，直到图卸载或同 Session 下一次 Invocation 替换。运行中进一步裁剪已完成 Child 的输入/结果需要单独定义未来 ChildHandle 查询契约。
