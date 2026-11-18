# Core 大输出、循环与并发内存审查

## 结论

当前 Core **仍存在明显 OOM 风险**，尤其是 512 MiB / 1 GiB 容器中运行 3–5 个 Invocation，并伴随循环、累计 LLM 输入或 Map 大查询结果时。已经避免完整 Runtime Event 历史常驻、共享内部不可变数据并清理部分执行工作区，但尚未实现有界的活动 Invocation 数据保留。

本轮只新增 benchmark 与测量文件，没有修改 Core 实现，也没有运行 V1 对照。因此可以确认当前风险及已有共享机制有效，不能给出相对 V1 的节省百分比。

## 测量边界

- 每个场景一个独立子进程；Python/Core 基线 RSS 约 35 MiB。
- 纯 Core + 最小测试 Operator：无 Host、模型 SDK、数据库连接、持久化、外部结果缓存。Sink 只 ACK 和记录标量采样数据，不保留 Event/Delta/State。最终公共 Result 是小字典。
- 普通场景关闭 tracemalloc，用 Linux `ru_maxrss` 和 `/proc/<pid>/status` 测量真实进程 RSS。`trace_*` 场景单独测 Python 活对象；其 RSS 包含 profiler 开销，不用于推算普通业务容量。
- 常规循环：每轮 3 个节点，每个返回全新 1/2 MiB ASCII 文本，invocation context 默认覆盖 `latest`。每个节点输出与同一个 Call 输出的对象身份相同，不将这些引用重复计算为两份数据。
- 查询结果：每份紧凑 JSON 约 1 MiB，包含 33,822 行、每行两个整数。这是许多小 list/dict 对象的压力场景，不代表所有 1 MiB JSON 都有相同内存倍率。
- Map 场景每轮 4/8 个查询，aggregate 只返回小摘要；同时运行 3/5 个独立 Root Invocation，共享 App 的 32 个 Operator 并发槽。
- 混合场景每轮执行「2 MiB 文本 → Map(4) → 2 MiB 文本」，每个查询返回约 1 MiB JSON 对应的 list/dict。
- 没有真的在 Docker 内制造 OOM。父进程每 10 ms 监视 RSS，分别在 448/896 MiB 安全线附近主动终止，保留容器余量。采样会略有超调。实际环境 cgroup 未配置这些容器上限。
- 驱动在再次运行时会进一步根据可用内存及可读取的 cgroup 剩余内存降低工作进程预算；结果中记录实际守卫值。测试守卫属于 benchmark，不是 Core 的内存限制功能。
- 共 31 个场景：24 个完成，7 个触发预设 RSS 守卫；无意外错误。完成场景核对循环结果、Call 数、驻留 Session 数、pending Event 清空和卸载结果。每例一次独立测量，数字不是统计容量保证。

## 最值得关注的结果

| 场景 | 运行时峰值 RSS |
| --- | ---: |
| 1 Invocation，3 × 1 MiB/轮，60 轮 | 218.3 MiB |
| 1 Invocation，3 × 2 MiB/轮，60 轮 | 400.3 MiB |
| 3 Invocation，3 × 2 MiB/轮，各 15 轮 | 309.7 MiB |
| 5 Invocation，3 × 2 MiB/轮，各 10 轮 | 340.0 MiB |
| 5 Invocation 持续上述循环，512 MiB 目标 | 第 14 轮附近触发 448 MiB 守卫；采样约 460 MiB |
| 5 Invocation 持续上述循环，1 GiB 目标 | 第 29 轮附近触发 896 MiB 守卫；采样约 905 MiB |
| 约 1 MiB 查询结果依次穿过 12 次节点执行 | 320.4 MiB |
| 同样 12 次执行，Input Mapping 裁剪后续输入 | 216.8 MiB |
| 3 Invocation，各 Map(4) 查询，只运行一轮 | 335.0 MiB |
| 上述 Map 继续第二轮 | 触发 448 MiB 守卫 |
| 5 Invocation，各 Map(8) 查询 | 首轮尚未完成即触发 448 MiB 守卫 |
| 3 Invocation，混合文本 + Map，一轮 | 341.0 MiB |
| 混合场景 3 Invocation 继续循环 | 第二轮触发 448 MiB 守卫 |
| 混合场景 5 Invocation 继续循环 | 第三轮触发 896 MiB 守卫 |

这些只包含测试进程本身。真实服务的模型/数据库客户端、HTTP 层、其他进程和缓存会进一步占用容器内存。

## 数据实际留在哪里

### 每轮的历史仍属于当前 Runtime State

[OperatorCallState](/home/chengqian/projects/AutoAgent/autoagent/core/runtime/state.py:49) 同时保存 input/output。[转换逻辑](/home/chengqian/projects/AutoAgent/autoagent/core/runtime/transitions.py:135) 在 Call 完成时写入 output；[Node 完成逻辑](/home/chengqian/projects/AutoAgent/autoagent/core/runtime/transitions.py:159) 清空 NodeExecutionState 工作区，但仍保留 occurrence.output 和 OperatorCallState。

因此当前 State 是「这个 Session 当前 Invocation 的累计执行状态」，并非「当前循环这一轮」。Runtime Event 不常驻与这些历史值不常驻，是两个不同问题。

固定大小独立文本的主要保留量接近：

```text
活跃/驻留 Invocation 数 × 每轮大输出节点数 × 循环次数 × 每次独立输出大小
+ Python/Core 基线 + 执行中的临时值 + 分配器保留空间
```

5 Invocation × 3 节点 × 2 MiB = 每轮累计新增约 30 MiB 大文本，故加内存只会延迟触顶。

### 共享是有效的，但不能合并每轮的新数据

90 次调用复用同一个 2 MiB 字符串对象，峰值约 40 MiB；90 次各自产生新 2 MiB 字符串，峰值约 220 MiB。正常非 Map 文本用例的 Call.output 与 Node.output 均为同一对象。不能把字段数直接当作完整数据副本数；也不能把不同轮次的新输出当作同一份数据。

### Context 覆盖、Context 追加和累计输入不同

- 只在 context 中覆盖 2 MiB 值，节点本身输出小：60 轮峰值约 44.5 MiB，单独 trace 运行结束仍活跃的 Python 分配约 2.2 MiB。
- 同样的大值不断追加到 context：30 轮峰值约 220 MiB，最终历史确实需要保存 180 MiB 文本。
- 即使 context 只保存最新完整 prompt，每次把不断增长的 prompt 传给 Operator，历史 Call.input 仍会保存各个前缀。

累计 prompt 专项测试每次只新增 **1 MiB 输出**，共 18 次调用：单 Invocation RSS 约 **226 MiB**。Call 输入/输出中的独立字符串合计约 170 MiB，再加最新 context prompt 等活跃值。5 Invocation 在完成目标前达到约 922 MiB，触发 896 MiB 守卫。

当每轮真正拼接出新的完整字符串时，累计 Call.input 大小可以接近 `B × (1 + 2 + … + N)`，即平方增长。若业务用可共享消息对象而非拼接字符串，具体倍率不同，但旧输入容器仍可能累积。

### 1 MiB JSON 不等于 1 MiB Python 内存

独立表示探针见 `representation.json`：同一份约 1 MiB JSON 对应的 Python list/dict 约 **8.26 MiB**，Core 冻结后的独立存活对象约 **10.85 MiB**。JSON 是传输体积，不含每个 dict、整数、引用及不可变包装的 Python 对象成本。

另外，[下游输入构造](/home/chengqian/projects/AutoAgent/autoagent/core/executor/workflow_executor.py:1172) 会 thaw 上游输出，再经过输入契约验证/序列化并形成新的 Call.input。串行查询用例保留 12 份输出数组和 12 份输入数组；裁剪中间输入后减少到 13 个大数组，但上游完整结果仍然保留。

这兼有数据表示的固有成本、持久保留的独立容器，以及边界校验/冻结过程中的瞬时副本；不能将进程 RSS 全部称为 Core 额外开销。

### Map 聚合小结果不能回收 Call 大结果

Map aggregate 在各个 Call 完成后执行。即使 aggregate 输出只是小摘要，Call 的完整查询结果已进入 State。降低 `max_parallelism` 有助于降低同时执行和转换时的峰值，但不会自动消除整个循环累计保留的调用结果。

### Invocation 完成不等于 Session 被卸载

依次执行 5 次、每次 30 个 1 MiB 文本输出：

- 使用不同 Session：最后保留 5 个 Session、150 MiB 独立文本，峰值约 188.5 MiB。
- 复用同一 Session：最后只保留当前 Invocation 的 30 MiB 文本，峰值仍约 99 MiB。替换过程中旧 State 的临时引用、分配器复用空间都会影响峰值，不能只看最后对象量。

[未指定 session_id 时会生成新 ID](/home/chengqian/projects/AutoAgent/autoagent/core/app/app.py:787)。不再需要的已完成 Session 应显式 unload。复用 Session 也不会自动清除业务主动累积的 Session context。

### close 默认不构造 checkpoint，但也不清空 State

[close 路径](/home/chengqian/projects/AutoAgent/autoagent/core/app/app.py:1635) 停止任务、结算 Event 后返回；没有清空所有 Repository State。

单独 trace 测试中，60 MiB 文本在 close 后仍有约 60.1 MiB Python 分配存活；删除 App 并 GC 后降至约 0.02 MiB。显式 unload 的对应测试则从约 60.1 MiB 降到约 0.03 MiB。

结构化数据释放后，RSS 可能仍高于 Python 活对象量：分配器和 profiler 有保留开销。这不等于 State 仍持有那些值，但容器内存规划仍须考虑实际 RSS。

上次关闭默认 checkpoint 的优化减少了生命周期边界的工作，不能解决循环期间这些数据的保留。

## 建议优先级

1. **为成功完成的节点制定大值回收规则。** 先审查何时可以释放 OperatorCall 的完整 input/output，只保留必要元数据；再基于数据依赖活跃性回收已经没有消费者的 occurrence.output。普通文本 Call/Node 共享同一份值，所以只清理 Call 引用不足以解决全部文本增长。必须保留正在运行、Wait/Child 恢复、未完成 Map、join 和循环边界仍需的数据，并保持 Event 回放与 checkpoint/recovery 一致。不能完成一个节点就无条件删除所有历史。
2. **降低结构化数据在内部边界上的重复构造。** 寻找经过验证的不可变值能否复用为 canonical Call.input，同时继续为用户 Operator 提供隔离的可变输入；不以取消验证或共享用户可修改对象换取内存。
3. **明确驻留与生命周期策略。** 完成且无需继续查询/恢复的 Session 及时 unload；评估 close 是否可以释放 App 自己拥有的 State。外部注入 Repository 的所有权、可选 checkpoint 和并发调用语义需单独确认。
4. **加入内存容量控制。** 现有 Operator 并发和 Node 执行次数限制不等于内存预算。考虑并发 Invocation 准入、单结果/Map 总量限制及可观测的大值保留量；Core 无法阻止用户 Operator 自身任意分配内存，因此这些限制也不等于操作系统隔离。

实施上述修改前的业务侧措施：查询结果在 Operator 返回前裁剪/聚合或分页，控制 LLM prompt/history 窗口，避免只依赖 Map aggregate 缩小结果，并及时 unload 不再需要的 Session。这些都需要遵守业务实际需要的数据语义。

不能据这些测量承诺「512 MiB 足够」或「1 GiB 足够」。在你的 3–5 Invocation、循环和 Map 大结果组合下，当前 Core 应视为尚未完成内存有界化。

## 完整结果

`trace_*` 行仅用于分配诊断，其 RSS 带 profiler 开销。

| 场景 | 结果 | 运行时峰值 RSS MiB | 完成后 RSS MiB | 卸载后 RSS MiB |
| --- | --- | ---: | ---: | ---: |
| text_1m_10cycles | 完成 | 67.9 | 67.9 | 36.1 |
| text_1m_30cycles | 完成 | 127.8 | 127.8 | 98.9 |
| text_1m_60cycles | 完成 | 218.3 | 218.3 | 99.3 |
| text_2m_10cycles | 完成 | 99.7 | 99.7 | 36.0 |
| text_2m_30cycles | 完成 | 219.8 | 219.8 | 159.9 |
| text_2m_60cycles | 完成 | 400.3 | 400.3 | 222.3 |
| text_no_context | 完成 | 219.8 | 219.8 | 159.9 |
| same_text_reference | 完成 | 40.2 | 40.2 | 40.2 |
| context_only_overwrite | 完成 | 44.5 | 44.5 | 36.8 |
| context_only_append | 完成 | 219.8 | 219.8 | 159.8 |
| text_3_invocations | 完成 | 309.7 | 309.7 | 97.9 |
| text_5_invocations | 完成 | 340.0 | 340.0 | 222.3 |
| text_5_invocations_512_limit | 安全线停止 | 460.0 | — | — |
| text_5_invocations_1g_limit | 安全线停止 | 905.2 | — | — |
| rows_forward | 完成 | 320.4 | 320.4 | 52.3 |
| rows_strip_input | 完成 | 216.8 | 207.8 | 50.4 |
| map4_rows_single | 完成 | 184.6 | 167.7 | 44.5 |
| map4_rows_3_invocations_one_cycle | 完成 | 335.0 | 264.1 | 45.2 |
| map4_rows_3_invocations | 安全线停止 | 449.5 | — | — |
| map8_rows_5_invocations | 安全线停止 | 449.1 | — | — |
| sequential_new_sessions | 完成 | 188.5 | 188.5 | 162.6 |
| sequential_reused_session | 完成 | 99.0 | 96.3 | 96.3 |
| trace_text_unload | 完成 | 100.4 | 100.4 | 36.9 |
| trace_context_overwrite | 完成 | 44.4 | 44.4 | 44.4 |
| trace_close_only | 完成 | 100.6 | 100.6 | — |
| trace_map_rows | 完成 | 158.0 | 147.2 | 97.0 |
| mixed_3_invocations_one_cycle | 完成 | 341.0 | 275.7 | 43.9 |
| mixed_3_invocations_512_limit | 安全线停止 | 454.2 | — | — |
| mixed_5_invocations_1g_limit | 安全线停止 | 896.6 | — | — |
| growing_prompt_single | 完成 | 226.1 | 226.1 | 55.2 |
| growing_prompt_5_invocations | 安全线停止 | 922.1 | — | — |

## 复现与证据

- `results.json`：场景参数、守卫、状态和全部标量采样点。
- `summary.json`：峰值、执行后、卸载后和 Python 活对象摘要。
- 每个场景 `.jsonl` / `.stderr`：独立工作进程输出。
- `representation.json`：list/dict 的 JSON、Python 原始表示和冻结表示差异。
- `metadata.json`：环境、Core 源文件哈希和验证数量。

```bash
.venv/bin/python -m tests.benchmarks.benchmark_core_runtime_memory
.venv/bin/python -m tests.benchmarks.benchmark_core_runtime_memory --cases text_1m_10cycles
.venv/bin/python -m tests.benchmarks.benchmark_core_runtime_memory --representation
```

没有改动 V1、Core 产品代码、Host 或持久化模块；没有通过降低输入隔离/恢复安全性进行优化。本轮没有重复全量功能测试，因为产品实现未改动；benchmark 的完成路径和统计断言已验证，脚本编译与 `git diff --check` 通过。
