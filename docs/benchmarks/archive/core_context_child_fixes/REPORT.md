# Core Context 与 Child 性能优化

## 实现范围

只修改 V2 Core 和对应测试/Benchmark，保留上一轮基线。没有修改外围模块、事件持久化方案或记录格式，没有创建分支或提交。

- Context 冲突索引：精确路径版本仍引用当前 State；额外保存严格祖先路径的后代最大版本。查询目标路径的祖先和后代，保留原有冲突条件。索引惰性建立，正常提交仅在 Sink ACK 后增量更新，Checkpoint 安装清除旧索引、下一次写入重建，卸载释放。普通自定义 Planner 替换走重建后备路径。
- 预览复用：使用已确认 OutputBound 中的不可变 Patch。只在 Context 对象、版本表、操作元组、started_sequence 和提交相对起始版本关系全部一致时复用候选 Context；修订号按实际提交 sequence 重写。发生变化重新计算。缓存限于一次节点完成流程，异常、取消后清空并关闭，继承 ContextVar 的任务也无法继续填充旧缓存。
- Child：派生 ExecutionIndex 保存计划的未完成数量，标准阶段事件 ACK 后只增量更新一个单元的贡献；其他批量 State 变化重建。Repository 记录当前失败/取消 Session ID，在成功路径避免构造全组 Child State。失败、取消和最终 ChildAwaitReady 仍保留原有收敛和终态验证。
- Core 没有重新持有已确认 Event 历史。索引只持有当前版本表、路径、ID 和计数。

## 主要 Benchmark

使用上一轮相同脚本和规模；计时、tracemalloc、调用计数、事件重放分开。时间为中位数，配置及完整原始样本见 before.json/after.json。Context 为加载的合成历史、固定当前 Context 大小；Child 为全部先进入 Wait 后逐个恢复，创建和关闭不计时。

| 场景 | 修改前 / ms | 修改后 / ms | 变化 |
| --- | ---: | ---: | ---: |
| 无历史，写一个键 | 1.150 | 1.135 | -1.4% |
| 1 万历史，写一个键 | 2.039 | 1.269 | -37.8% |
| 5 万历史，写一个键 | 5.911 | 1.294 | -78.1% |
| 100 个 Child 恢复 | 256.211 | 239.672 | -6.5% |
| 300 个 Child 恢复 | 798.959 | 723.758 | -9.4% |
| 1,000 个 Child 恢复 | 2706.522 | 2403.383 | -11.2% |

1,000 个 Child 的 _settle_child 全组 State 读取：1,000,000 → 0。这里消除的是反复收集 State 的成功路径，不表示整个流程不再访问 Child，也不表示失败路径没有扫描。

主表中的新增分配峰值基本持平：5 万历史写入约 47.6 → 48.2 KiB，1,000 个 Child 约 3244.2 → 3244.4 KiB。这不包括计时前已经存在的 State 和索引。

## 常见场景回退检查

从 Git HEAD 导出只读源码到临时目录，不创建 Worktree/Fork。将基线包放到 sys.path 首位，使用当前同一 benchmark_core_execution 脚本，前后交替 3 轮；每轮内部含 5 次计时和独立内存测量。表中取三轮中位数。

| 场景 | 修改前 / ms | 修改后 / ms | 变化 |
| --- | ---: | ---: | ---: |
| 5 万元素输入链 | 68.415 | 69.284 | +1.3% |
| 同 Session 连续 100 次 | 88.797 | 88.830 | +0.0% |
| 100 次 Loop | 105.681 | 107.888 | +2.1% |

最终小任务基本持平，大输入链和 Loop 仍有约 1–2% 的小幅耗时增加；不能声称所有场景都提速。最终三轮 Loop 的 after 均略高于 before，需要保留为成本。第一版对空 Patch 也准备索引，出现约 6–8% 的回退；已改为有实际写入才启用索引和缓存。initial/ 保存该中间版本数据，不是最终结果。

## 索引驻留内存与首次成本

benchmark_context_index_cost 单独测量已经存在的 revisions 之外的索引成本，5 万条记录：

| 路径形态 | 严格祖先条目 | 新增驻留内存 | 首次建立中位数 |
| --- | ---: | ---: | ---: |
| 平铺键 | 0 | 0.27 KiB | 5.65 ms |
| 共享同一个父路径 | 1 | 0.55 KiB | 15.32 ms |
| 每个键有不同父路径 | 50,000 | 4904.07 KiB | 17.49 ms |

版本历史没有裁剪，冲突所需信息全部保留。嵌套路径多时这是明确的空间换时间，尤其只有少量写入的 Session 未必划算。首次构建也有 O(历史路径数 × 深度) 成本；主表预热后结果不能当成首次写入延迟。当前没有改变公开 Checkpoint/Event Schema。

## 验证

- 新增 8 项测试：随机路径查询与全表扫描对照、随机批次成功和错误顺序差分、预览复用/版本失效/真实提交修订号、真实工作流只计算一次非空 Patch、丢 ACK 不提前发布索引及精确 Event 重试、逐个 Child 恢复与重放、Child 取消收敛及安装/释放索引、取消后继承缓存关闭。
- 原有 ACK 索引对照测试加入 child_remaining 校验。
- Benchmark 独立验证 Context 的 7 条新 Event，以及父任务加 32 个 Child 共 33 个 Session、329 条 Event：编码往返、逐 Session 重放、State 往返、输出和 Child 终态均正确。
- 最终全量发现 340 项：332 通过，8 个既有外围导入错误。错误仍涉及已删除的 InvocationOpened/InMemoryEventJournal，模块与上一轮一致；并非全仓全绿。见 tests.log。
- Core/tests 编译和 git diff --check 通过。

## 剩余边界

- Repository 存在失败/取消 Session 时，has_failed_child 仍扫描计划成员以排除或处理失败。未把异常路径伪装成常数时间。
- 同批 Context Patch 的路径关系检查、分块映射目录复制、复杂类型的严格 JSON 恢复仍保留，本轮没有扩大范围。
- 自定义 Planner 使用后备重建路径；这里的性能数字针对默认 Core 执行路径。
- 回归覆盖不代表所有并发交错或安全风险都已形式化证明；没有删减冲突、ACK、隔离、取消和恢复检查来换取速度。

## 复现

```bash
.venv/bin/python -m tests.benchmarks.benchmark_core_context_child
.venv/bin/python -m tests.benchmarks.benchmark_context_index_cost
.venv/bin/python -m tests.benchmarks.benchmark_core_execution
.venv/bin/python -m unittest discover -s tests -v
```

before.json 保留上一轮测量；after.json、execution_{before,after}_{0,1,2}.json 为最终数据，comparison.json 汇总，source_manifest.json 记录基线提交与最终 Core 源文件校验和。
