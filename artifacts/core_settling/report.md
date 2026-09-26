# Core settling 状态验证

基线：`fe5fb321456a0c3874b7b1beb1738a5e6d943c41`；修改后为当前未提交工作区。

## 行为

- `settling` 统一表示收尾；`pending_outcome` 区分 completed、failed、cancelled。正常无 Child 完成仍直接提交 completed。
- 失败/取消先停止业务推进，等待业务 Task 的清理与 Child 所有权收敛，再写终态与 completed_at_us。
- 成功收尾可被取消；已经确定的失败/取消保留最初原因。重复取消不再次打断取消清理。
- 保留 Child Wait；成功收尾期间仍可通过 Root resume。停止中的分支不暴露可恢复 Wait，提交 resume 时也检查祖先状态。
- 收尾和终态使用同一份不可变结果；终态不重复扫描、清理已释放的执行 payload。
- RuntimeState / RuntimeEvent schema 为 7，SessionCheckpoint schema 为 5。替换旧状态和 Event 名称，不提供旧格式兼容。

## 正确性

- 全量 402 项测试通过，日志见 tests.log；compileall、git diff --check 与 Core 示例通过。
- 新增 10 项测试：自身与 Child 的慢清理、成功收尾改为取消、重复取消、失败原因保留、无 Child 成功快速路径、取消事件前缀恢复、最终 ACK 丢失重试、已无剩余 Child 的成功意图取消及结果冲突校验。
- 原有恢复测试调整为在 invocation.settling 后模拟故障，确认不会重新执行业务。

## 测量方法

- CPython 3.12，同一机器顺序运行基线与修改后实现。基线源码由 git archive 导出到 /tmp，没有新建分支或 worktree。
- benchmark_runtime_graph：预热 1 次，随后 3 次取中位数；另外单独开启 tracemalloc 测量峰值。两版本均等待完整 Graph 结束，Sink 仅计数，不保留 Event。
- benchmark_settling：所有叶子 Operator 开始后才测量 cancel 到返回；预热 1 次，随后 7 次取中位数。断言返回 cancelled 且没有活跃 Invocation Task。
- 最终对比顺序执行，不与测试并行。以下为小规模微基准，少量样本有调度噪声，不能推导普遍加速或容器 RSS/OOM 结论。

## 正常执行

| 场景 | 修改前 ms | 修改后 ms | 变化 |
|---|---:|---:|---:|
| no_child | 1.915 | 1.835 | -4.2% |
| map_32 | 42.535 | 41.701 | -2.0% |
| map_100 | 137.185 | 133.363 | -2.8% |
| nested_4 | 6.242 | 6.527 | +4.6% |
| five_roots_large_outputs | 71.077 | 71.065 | -0.0% |

正常执行的事件数量未增加；这组样本的耗时变化约在 -4% 到 +5%。

| 场景 | 修改前峰值 MiB | 修改后峰值 MiB |
|---|---:|---:|
| no_child | 0.047 | 0.047 |
| map_32 | 0.762 | 0.762 |
| map_100 | 1.328 | 1.332 |
| nested_4 | 0.093 | 0.094 |
| five_roots_large_outputs | 23.274 | 23.281 |

## 取消执行

| 场景 | 修改前 ms | 修改后 ms | 变化 |
|---|---:|---:|---:|
| cancel_leaf | 0.393 | 0.480 | +22.1% |
| cancel_map_32 | 7.843 | 10.664 | +36.0% |
| cancel_nested_4 | 1.018 | 1.258 | +23.6% |

取消路径变慢是本次语义调整的实际成本：活跃 Invocation 需要分别持久确认收尾意图和最终结果。32 Child 场景取消阶段的事件数由 65 增至 98，耗时增加约 2.82 ms。若真实 Sink 的确认延迟较高，额外事件还会增加等待时间，本测量未包含该延迟。

## 文件

- before.json / after.json：正常执行和内存原始结果。
- cancel_before.json / cancel_after.json：取消阶段原始结果。
- tests.log：完整测试输出。
