# Core Context 与 Child 性能基线

日期：2026-09-12。源码：`18d9c4fa2aa4700de63a39a74480378735e0ac42`。本轮只新增 Benchmark 和测量产物，没有修改 Core。

## 方法与边界

- 使用 Python 3.12.13，公开同步 App API 和默认空 Sink；没有用户 IO、持久化 IO 或慢 Hook。同步 API 的线程桥接成本包含在计时中。
- Context：从真实已完成 State 构造并经 Checkpoint 校验、加载的合成历史，历史路径代表已删除键，当前 Context 大小固定。测量同 Session 再次执行单节点、写入一个键。每组预热一次，再测 7 次，报告中位数。历史数不包含预热写入的 hot 路径。
- 空 Context Patch 对照同样保留 Output Binding 阶段，单独一轮运行，不应将微小组间差异视为显著收益。
- Child：每个 Child 先进入 Wait，父任务也进入 waiting，然后逐个 resume，直到父任务完成。计时包含全部 resume 和最终 join，不包含创建/入场/关闭。每组独立 App，预热一轮、测量 5 轮。
- tracemalloc 在独立的一轮开启。峰值仅表示计时窗口内新增 Python 分配，不是 RSS，也不包括预先存在的历史版本、Child State 或总驻留内存。
- Child State 读取次数在独立插桩轮测量，只计 _settle_child 内生成器发起的 repository.state 调用；插桩不进入耗时/内存轮。
- 输出与完成状态断言计入计时；完整事件编码往返和重放校验单独运行。不同规模顺序运行，属于当前机器基线，不是优化前后对照。

## Context 历史增长

| 历史版本条数 | 写一个键 / ms | 空 Patch / ms | 写入新增峰值 / KiB |
| ---: | ---: | ---: | ---: |
| 0 | 1.150 | 0.995 | 36.1 |
| 1,000 | 1.301 | 1.129 | 45.0 |
| 10,000 | 2.039 | 1.030 | 41.2 |
| 50,000 | 5.911 | 0.983 | 47.6 |

非空 Patch 的端到端耗时随历史增长，空 Patch 保持约 1 ms。结合 transitions.py 中对 revisions.items() 的扫描，确认历史版本查询值得优先优化。新增分配没有呈现与历史条数成比例的大幅增长；这不意味着历史本身没有驻留内存成本。

## 等待中的父任务逐个接收 Child 完成

| Child 数 | 全部恢复并完成 / ms | 每个 Child 平均 / ms | 全组 State 读取次数 | 新增峰值 / KiB |
| ---: | ---: | ---: | ---: | ---: |
| 10 | 25.835 | 2.583 | 100 | 195.8 |
| 100 | 256.211 | 2.562 | 10,000 | 635.5 |
| 300 | 798.959 | 2.663 | 90,000 | 1325.9 |
| 1,000 | 2706.522 | 2.707 | 1,000,000 | 3244.2 |

全组 State 读取次数严格为 N²，确认存在重复扫描。不过本次规模下每个 Child 平均耗时约 2.6–2.7 ms，总耗时仍接近线性，尚不能把该扫描认定为主导瓶颈或推断优化收益百分比。测试没有测量 Child 创建成本，也没有把正常保留的当前 Child State 称为 Event 历史泄漏。

## 正确性与安全边界

- Context：加载 10,000 条历史后正常写入，7 条新 Event 编码往返，并从加载的 State 重放，对照当前 State；保留的旧 State 版本记录数量不变。
- Child：32 个 Wait Child 加父任务，共 33 个 Session、329 条 Event，逐 Session 编码往返、连续重放与当前 State 对照；全部 Child 输出、顺序、终态和父任务输出正确。所有计时规模也校验输出和 Child 终态。
- 全量测试发现 332 项，324 通过；8 个既有外围模块导入错误，具体见 tests.log，与此前报告一致。错误涉及已删除的 InvocationOpened/InMemoryEventJournal，被 Host、CLI、Tracing、持久化测试引用。全仓测试并非全绿。
- 已通过的测试覆盖 Context 并发冲突与失败原子性、旧快照不变、输入隔离、严格契约校验、ACK 前不发布、丢失 ACK 后重试同一个 Event、ACK 后释放 Event、取消边界、派生索引一致性、Child 生命周期及线程/notifier 释放。
- 这些是已覆盖行为的回归证据，不构成对所有输入、并发交错或安全问题的完全证明。本轮没有通过删减校验、改变 ACK/取消语义来获取性能数据。

## 后续方案

1. 优先处理 Context 冲突查询。先评估按路径祖先/后代查询最新修订的可重建索引，避免为单键写入扫描全部历史。索引应从已确认 State 派生，在 ACK 后发布，恢复时重建；仍检测父子路径冲突、同批路径关系和失败原子性。对照当前算法做随机差分测试，并计入索引内存成本。
2. 不能按固定数量直接裁剪旧版本记录；仍在运行的 Occurrence 可能依赖旧 started_sequence 进行冲突检测。也不能跨 await 无条件复用 preview 结果。
3. Child 可评估维护每个计划的未终态数量和失败状态索引，正常成功完成只更新单个计数；失败、取消、恢复路径仍保持原有收敛语义。仅在同一 Benchmark 证明收益且索引与 State 一致后采用。
4. 对每个实现候选都重新跑相同规模、空 Patch 对照、小工作流和完整回归；若收益不足或内存明显增加，不引入额外结构。当前数据支持先做 Context，Child 放在后面。

## 复现

```bash
.venv/bin/python -m tests.benchmarks.benchmark_core_context_child > /tmp/core-context-child.json
.venv/bin/python -m unittest discover -s tests -v
```

原始样本见 results.json。基准脚本：tests/benchmarks/benchmark_core_context_child.py。
