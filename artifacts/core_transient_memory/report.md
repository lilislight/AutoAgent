# Core 临时数据副本与聚合回收优化

## 结论与范围

本轮在上一轮生命周期回收的基础上，进一步减少执行期间的临时副本，并把 Map 内部数据的回收提前到聚合结果确认时。下文“修改前”已经包含上一轮优化，不是最初会持续累积大输出的 Core。

只修改 Core 执行器、合约转换和状态转换，另增加测试与 benchmark。聚合函数接口、并发限制、业务 Context 和 Session 生命周期保持原有语义。

## 内存与大数据执行对比

| 场景 | 修改前峰值 MiB | 修改后峰值 MiB | 峰值变化 | 耗时 s（前 → 后） |
|---|---:|---:|---:|---|
| 大行结果连续传递 12 次 | 105.3 | 92.3 | -12.3% | 3.515 → 3.257 |
| 大行结果、下游只取小输入 | 86.7 | 73.4 | -15.3% | 1.876 → 1.758 |
| 3 并发 × Map4，无聚合 | 158.7 | 127.6 | -19.6% | 0.836 → 0.805 |
| 3 并发 × Map4，有聚合 | 335.0 | 257.3 | -23.2% | 1.434 → 1.315 |
| 5 并发文本 + Map4，3 轮 | 510.6 | 415.7 | -18.6% | 7.050 → 6.700 |

聚合后进入 output binding 时，Python 存活内存从 **5.305 MiB 降至 0.052 MiB**，仍持有大结果的 Call 数量从 4 变成 0。此处是阶段性 heap，不是整个进程 RSS；释放后 Python 分配器仍可能保留内存页。

## 实现

1. 没有用户聚合函数的内置 Operator/Map 路径，不再把原始 Python 输出累计保存在 `unit_results` 中；后续使用已确认的 Call record。直接调用 NodeExecutor 的默认行为保持不变，自定义执行器不强制接受新参数。
2. 对已证明序列化等价的普通 JSON 合约，校验后的值直接进入记录冻结边界，省去一次完整 `dump_python` 容器构造。输入恢复也省去只为相等性比较而生成的序列化副本。模型、别名、Enum、tuple、自定义验证/序列化继续使用原有路径。
3. Input Mapping 完成后及时释放此前 thaw 的 incoming 和 invocation input 局部引用，避免贯穿整个 Operator 调用。
4. Aggregated 的 StateDelta 原子清理 Call input/output 和 execution.mapped_input，保留 aggregate_output、Call 状态与恢复标识。尚有运行中 Call 时拒绝提前提交 Aggregated。ACK 失败时仍保留原 State，重试原 Event。

现有聚合函数仍接收可变 Python 对象；没有把用户输入或聚合结果接口改成不可变容器。即便是丢弃原始结果的路径，从 checkpoint 恢复的 Call output 也先经过原有合约校验，再释放临时对象。

## 测试方法

- 原有大数据脚本：每个场景独立子进程，非持有型 Sink，无 Host、数据库或持久化，最终结果很小。JSON 行数据形状与上一轮相同。
- 新增无聚合 Map：3 个 invocation、每个 Map4，每个结果含 16,000 行，最后一个 Node 只输出行数。
- 新增聚合阶段 heap：单 invocation、Map4、每个结果 4,096 行，使用 tracemalloc 在 output binding 时观察仍存活的数据。该场景带 profiler 的 RSS 不与普通容量场景混用。
- 完成场景峰值使用 after_run 时的 ru_maxrss；保护停止场景使用父进程采样。RSS watchdog 模拟名义 512 MiB/1 GiB 预算，不是真实 Docker cgroup OOM 测试。
- 4 个原有大数据场景、2 个新增场景和完整执行 benchmark 都在修改前重新测量。其余 baseline 来自上一轮保留的结果；全部修改后结果使用最终源码。
- 大数据场景为单次运行，耗时只表示本次观察，不能视作统计显著性或生产环境保证。

## 验证与限制

新增 9 项测试覆盖：记录转换的输入隔离；复杂模型的别名、序列化、Enum/tuple；大整数、Unicode、非有限浮点；无聚合 Map 恢复；聚合 ACK 前后回收和重放；聚合函数修改原始结果不影响 Event；聚合失败 ACK 的原样重试与恢复；拒绝清理运行中 Call；直接执行器默认接口；恢复数据的严格合约校验。其中部分测试同时覆盖多个边界。

全量发现 385 项，377 项通过；8 项是原有外围模块导入错误，涉及已移除的 InvocationOpened / InMemoryEventJournal。完整日志见 tests.log。Core、新测试与 benchmark 编译检查、git diff --check 通过。

原有 31 个内存场景与新增 2 个场景共 33 个：32 个完成，5 并发 × Map8 仍在约 449 MiB 触发保护停止。当前全量聚合语义要求保留正在使用的全部结果，临时副本优化不能消除这部分工作集。

显式增长的 Context、公开结果、Child Session 的数据及外部持有的 Event/checkpoint，依然按原有所有权语义保留。

## 小数据执行与受控复测

完整 benchmark 的首次小数据串行结果增加约 10%–13%，因此追加了独立进程复测。修改前包从本轮变更逆向重建，全部 Core 文件 SHA-256 与修改前记录一致；无须修改当前工作区或切换 Git 分支。两个版本使用相同的已注册图，计时排除编译，每个规模预热 1 次后取 9 次中位数，每次计时前执行 GC。

| 节点数 | 修改前 ms | 修改后 ms | 变化 |
|---|---:|---:|---:|
| 50 | 40.22 | 40.41 | +0.5% |
| 100 | 80.23 | 83.08 | +3.6% |
| 200 | 165.09 | 167.64 | +1.5% |
| 400 | 348.68 | 358.50 | +2.8% |

受控样本仍有约 0.5%–3.6% 的小数据执行开销，不能称为普遍加速。大数据场景此次耗时约下降 4%–8%；这次取舍的主要收益是减少大容器副本及其存活时间。两组原始耗时记录均保留，未用复测覆盖首次结果。

完整 benchmark 的初始结果（3 次串行图中位数）：

| 节点数 | 修改前 ms | 修改后 ms |
|---|---:|---:|
| 50 | 36.82 | 40.38 |
| 100 | 74.78 | 83.60 |
| 200 | 147.36 | 166.63 |
| 400 | 319.78 | 352.22 |

## 全部内存场景

| 场景 | 修改前 MiB | 修改后 MiB | 修改后状态 |
|---|---:|---:|---|
| text_1m_10cycles | 39.5 | 39.7 | completed |
| text_1m_30cycles | 40.4 | 40.1 | completed |
| text_1m_60cycles | 39.9 | 40.2 | completed |
| text_2m_10cycles | 43.8 | 43.7 | completed |
| text_2m_30cycles | 43.7 | 44.1 | completed |
| text_2m_60cycles | 44.6 | 44.3 | completed |
| text_no_context | 43.8 | 44.2 | completed |
| same_text_reference | 40.1 | 39.8 | completed |
| context_only_overwrite | 43.9 | 44.3 | completed |
| growing_prompt_single | 74.7 | 74.9 | completed |
| growing_prompt_5_invocations | 223.4 | 223.2 | completed |
| context_only_append | 220.6 | 220.3 | completed |
| text_3_invocations | 53.9 | 54.1 | completed |
| text_5_invocations | 62.0 | 62.4 | completed |
| text_5_invocations_512_limit | 62.1 | 62.3 | completed |
| text_5_invocations_1g_limit | 62.3 | 62.1 | completed |
| rows_forward | 105.3 | 92.3 | completed |
| rows_strip_input | 86.7 | 73.4 | completed |
| map4_rows_single | 137.9 | 112.2 | completed |
| map4_rows_3_invocations_one_cycle | 335.0 | 257.3 | completed |
| map4_rows_3_invocations | 335.4 | 257.3 | completed |
| map8_rows_5_invocations | 449.4 | 449.6 | rss_guard_stopped |
| mixed_3_invocations_one_cycle | 341.3 | 263.5 | completed |
| mixed_3_invocations_512_limit | 342.1 | 269.2 | completed |
| mixed_5_invocations_1g_limit | 510.6 | 415.7 | completed |
| sequential_new_sessions | 43.6 | 44.3 | completed |
| sequential_reused_session | 40.7 | 41.0 | completed |
| trace_text_unload | 44.2 | 43.9 | completed |
| trace_context_overwrite | 44.4 | 44.4 | completed |
| trace_close_only | 44.3 | 44.1 | completed |
| trace_map_rows | 157.8 | 135.5 | completed |
| map_no_aggregate | 158.7 | 127.6 | completed |
| aggregate_binding_heap | 65.9 | 60.2 | completed |

## 复现与原始数据

```bash
.venv/bin/python -m unittest tests.test_core_transient_memory -v
.venv/bin/python -m tests.benchmarks.benchmark_core_transient_memory --output /tmp/core-transient-rerun
.venv/bin/python -m tests.benchmarks.benchmark_core_runtime_memory --output /tmp/core-memory-rerun
.venv/bin/python -m tests.benchmarks.benchmark_core_transient_latency --output /tmp/core-latency.json
.venv/bin/python -m tests.benchmarks.benchmark_full_core
```

`before/`、`after/` 保存原始结果；`comparison.json` 汇总 33 个内存场景；`change.patch` 仅包含本轮 Core 变更，可用于从最终代码构建修改前的临时 benchmark 包；`before/hashes.json` 与 `metadata.json` 分别记录修改前和最终源码哈希。受控延迟脚本可用 `--package-root` 指定基线包。
