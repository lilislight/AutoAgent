# Core 执行数据生命周期回收对比

## 结论

已实现默认回收：Node 完成后释放内部 Call/Wait 数据；Node output 保留到最后一个调度消费者完成；Invocation 终止后释放执行数据，保留公开结果和业务 Context。大文本循环的内存不再按历史输出量累计。

31 个原始场景重跑：30 个完成，1 个因活跃 Map 数据触发 RSS 保护线。优化前为 24 个完成、7 个触发保护线。保护停止不代表实际发生 Docker OOM，也不能将停止位置当作完成场景的峰值。

## 实现与正确性边界

- 清理操作加入既有 Runtime Event 的 StateDelta；不新增 Event，不改变外部 Event payload，正常执行与重放使用同一份变更。
- NodeCompleted/NodeFailed 清空所属 OperatorCallState.input/output、已完成 Wait 的 request/response；保留 ID、状态、时间、指标及错误信息。
- 消费者包括 ready/running/waiting 的 occurrence activation、未消费 EdgeResolution、尚未关闭 LoopBoundaryResolution。只有引用计数归零且不是工作流出口时才回收 Node output。
- 工作流出口在 InvocationCompleted 前保留；InvocationCompleted 提交公开结果后释放 occurrence.output。失败和取消也清理内部载荷。
- ExecutionIndex 只保存 ID/计数；按触及实体增量维护，安装 checkpoint 时重建。规划不修改当前索引；Sink ACK 后才发布新 State 和索引。
- 未确认 Event 仍保留原样重试所需的 Event/candidate；已确认历史由外部持有者负责。旧 State/Event 的不可变对象不被就地修改。
- 执行器及时解除 Operator 路径对旧 Scheduler snapshot 的引用，聚合后释放原始 Map 临时结果，完成后清理不再需要的局部变量；驱动任务等待期间不持有旧图快照。
- RuntimeRepository.commit 新增可选 output_node_ids，由 App 在 Node 完成时提供出口 ID。直接使用 Repository 而不提供出口信息，或使用自定义 Planner 时，保守保留 Node 输出至 Invocation 终止；不会猜测出口。
- State 中已回收 input/output 使用 None。历史执行详情应从外部 Runtime Events 获取，不能再依赖终态 State 保存历史大输出。

## 测试方法

独立子进程、仅 Core，使用立即异步确认且不保留 Event 的 Sink。没有 Host、数据库、真实 LLM SDK、持久化或持有大结果的调用方。文本为每次新生成的 1–2 MiB ASCII 字符串；数据库结果为约 1 MiB compact JSON 对应的 33,822 个 list/dict 行。该形状冻结后的 Python 容器约 10.85 MiB，不能把 JSON 字节数直接当成内存大小。

3/5 个并发 invocation；Map 并发 4/8；全局 Operator 并发上限 32。以 448/896 MiB RSS watchdog 给 512 MiB/1 GiB 留出名义余量，并依据实际环境可用内存进一步限制。未在真实 512 MiB/1 GiB Docker cgroup 内运行，生产服务及外部模块还会占内存。

完成场景采用 after_run 的进程 ru_maxrss，包含进程基础开销；停止场景使用父进程采样值。tracemalloc 仅用于独立 trace 场景，其 RSS 不用于常规容量推断。

本轮修改前重新测了 5 个代表场景和执行延迟，其余 baseline 来自上一轮相同脚本的审计结果，已原样保存在 before/original_results.json。全部优化后结果来自最终源码。每种大数据场景单次独立运行，耗时用于观察趋势，非统计显著性结论。

## 代表场景：峰值 RSS

| 场景 | 修改前 MiB | 修改后 MiB | 结果 |
|---|---:|---:|---|
| 单 invocation，3 节点/轮，2 MiB/输出，60 轮 | 400.2 | 44.6 | 完成 |
| 5 并发，各 10 轮，2 MiB/输出 | 340.5 | 62.0 | 完成 |
| 单 invocation，12 次约 1 MiB JSON 行结果向下游传递 | 321.2 | 105.5 | 完成 |
| 3 并发 × Map4，仅 1 轮 | 334.9 | 334.9 | 完成 |
| 3 并发 × Map4，3 轮 | 449.5 | 335.4 | 修改前触发保护线；完成 |
| 5 并发文本 + Map4，3 轮 | 896.6 | 510.5 | 修改前触发保护线；完成 |
| 完整历史 prompt，18 次各追加 1 MiB | 226.0 | 74.7 | 完成 |
| 5 并发 × Map8，目标 4 轮 | 449.1 | 449.4 | 修改前触发保护线；第一轮仍触发保护线 |

5 并发文本的 25/40 轮场景原来分别触发 448/896 MiB 保护线，现在均完成，峰值约 62 MiB。Map4 的第一轮峰值几乎不变，但后续轮次不再叠加历史结果；5×Map8 的活跃数据峰值仍过高，需要另外控制并发、每次返回量或分页。

## 结束后实际仍存活的数据

独立 tracemalloc 测量，单位 MiB；不包含测量启动前已分配的 Python 基础对象：

| 场景 | 修改前 Python live | 修改后 Python live | 解释 |
|---|---:|---:|---|
| 30 次 2 MiB 文本输出 | 60.102 | 2.091 | 约 2 MiB 是显式覆盖保存的最新 Context |
| Map2 行结果，聚合为小值 | 21.717 | 0.049 | 大行结果已从 Core 释放 |

Python 分配器可能保留已经释放的内存页，因此 RSS 不必立刻回落到启动值。默认 close 仍不会自动卸载所有已完成 Session；本次改变的是 State 保留的数据，不是 Session 生命周期 API。

## 执行开销

已注册的小数据串行图，3 次测量中位数；不含编译，单位 ms：

| 节点数 | 修改前 | 修改后 | 变化 |
|---|---:|---:|---:|
| 50 | 34.61 | 36.63 | +5.9% |
| 100 | 77.55 | 77.99 | +0.6% |
| 200 | 162.06 | 158.86 | -2.0% |
| 400 | 323.58 | 332.31 | +2.7% |

增量计数及清理操作有 CPU 成本。此次小数据运行变化约 -2% 至 +6%，不能称为普遍执行加速；大文本 60 轮耗时由约 333 ms 降到 231 ms。

重放既有 Event 前缀（5 次中位数），单位 ms：

| Event 数 | 修改前 | 修改后 | 变化 |
|---|---:|---:|---:|
| 50 | 1.68 | 1.88 | +11.5% |
| 100 | 3.21 | 3.62 | +12.9% |
| 200 | 6.42 | 7.24 | +12.7% |
| 400 | 14.09 | 14.68 | +4.2% |

回收增加 StateDelta 操作，因此重放有额外开销；400 个 Event 约增加 0.6 ms。完整 benchmark 中保留 Event 的 collector 会继续持有大数据，它的峰值不能用于判断 Core 是否回收；这正是外部所有权边界。

## 验证

- 新增 8 项生命周期测试：逐 Event 重放与索引重建；分支/Join/Wait checkpoint；Map 已完成单元与聚合阶段恢复；ACK 失败原样重试；显式 Context 与外部 Event 共享安全；取消清理；多个出口；嵌套循环每个 Node 完成前缀恢复。
- 原有数据合约校验、输入隔离、错误处理和并发测试继续通过。
- 全量发现 376 项：368 项通过，8 项为原有外围模块导入错误，涉及已移除的 InvocationOpened / InMemoryEventJournal。未改动 Host/Tracing 模块。详细输出见 tests.log。
- Core 与新增测试 compileall、git diff --check 通过。
- 30 个完成的 benchmark 均验证 Call 大数据引用为零、pending Event 为零；执行卸载的场景 resident Session 为零。

## 保留的边界

- 显式追加 Context 会继续增长；context_only_append 仍约 220 MiB，这是业务保留的数据。完整历史 prompt 本身仍会变大，但旧 Call 不再累计保留每份完整前缀。
- Invocation 的公开 input/output/context、独立 Child Invocation 的公开结果、Child Plan 输入及可恢复关系不自动删除。循环大量创建独立 Child Session 时，仍需按其生命周期处理。
- Call/Occurrence 元数据依然按执行次数增长；本次不移除调度身份、时间、错误和恢复判定信息。
- 仍在执行或等待恢复的 Map 单元结果保留至所属 Node 完成，较慢的聚合或业务 Hook 也可能需要活跃数据。
- 外部 Sink、调用方保留的 Event、checkpoint、结果或显式 State 快照属于外部持有者，Core 解除引用不会强制销毁其数据。

## 全部场景

| 场景 | 修改前峰值 MiB | 修改后峰值 MiB | 修改后状态 |
|---|---:|---:|---|
| text_1m_10cycles | 67.9 | 39.5 | completed |
| text_1m_30cycles | 127.8 | 40.4 | completed |
| text_1m_60cycles | 218.3 | 39.9 | completed |
| text_2m_10cycles | 99.7 | 43.8 | completed |
| text_2m_30cycles | 219.8 | 43.7 | completed |
| text_2m_60cycles | 400.2 | 44.6 | completed |
| text_no_context | 219.8 | 43.8 | completed |
| same_text_reference | 40.2 | 40.1 | completed |
| context_only_overwrite | 44.5 | 43.9 | completed |
| growing_prompt_single | 226.0 | 74.7 | completed |
| growing_prompt_5_invocations | 922.1 | 223.4 | completed |
| context_only_append | 219.8 | 220.6 | completed |
| text_3_invocations | 309.7 | 53.9 | completed |
| text_5_invocations | 340.5 | 62.0 | completed |
| text_5_invocations_512_limit | 460.0 | 62.1 | completed |
| text_5_invocations_1g_limit | 905.2 | 62.3 | completed |
| rows_forward | 321.2 | 105.5 | completed |
| rows_strip_input | 216.8 | 86.6 | completed |
| map4_rows_single | 184.6 | 137.9 | completed |
| map4_rows_3_invocations_one_cycle | 334.9 | 334.9 | completed |
| map4_rows_3_invocations | 449.5 | 335.4 | completed |
| map8_rows_5_invocations | 449.1 | 449.4 | rss_guard_stopped |
| mixed_3_invocations_one_cycle | 341.0 | 341.3 | completed |
| mixed_3_invocations_512_limit | 454.2 | 342.1 | completed |
| mixed_5_invocations_1g_limit | 896.6 | 510.5 | completed |
| sequential_new_sessions | 188.5 | 43.6 | completed |
| sequential_reused_session | 99.0 | 40.7 | completed |
| trace_text_unload | 100.4 | 44.2 | completed |
| trace_context_overwrite | 44.4 | 44.4 | completed |
| trace_close_only | 100.6 | 44.3 | completed |
| trace_map_rows | 158.0 | 157.8 | completed |

## 复现

```bash
.venv/bin/python -m unittest tests.test_core_payload_retention -v
.venv/bin/python -m tests.benchmarks.benchmark_core_runtime_memory --output /tmp/core-payload-retention-rerun
.venv/bin/python -m tests.benchmarks.benchmark_full_core
```

原始数据：before/、after/；统一对比：comparison.json；最终 Core 和 benchmark 源码 SHA-256：metadata.json。
