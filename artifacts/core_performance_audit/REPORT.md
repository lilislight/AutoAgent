# Core 性能复审

本轮只新增 Benchmark 与测量产物，没有修改 Core 实现。以下结论基于当前工作区（包括上一轮尚未提交的优化），不基于旧设计文档。Sink 内部实现、用户 Hook 的业务执行时间不列为 Core 缺陷。

## 方法和复现

```bash
.venv/bin/python -m tests.benchmarks.benchmark_core_audit
.venv/bin/python -m tests.benchmarks.benchmark_core_dispatch
```

`results.json`：7 次微基准中位数，分配峰值另开 tracemalloc 测量，构建输入不计入时间。宽度测试使用合成 State 隔离单条路径，并不是同规模完整工作流的端到端耗时。

`chain400.prof`、`loop300.prof`、`map2000.prof`：在真实 Runtime 线程中执行已编译 Workflow；对应 `_profile.txt` 列出累计时间与自身时间。Profiler 会改变耗时，用于定位热点，不能把各函数累计时间相加，也不能直接与普通 Benchmark 对比。Operator 是简单 async 函数；Loop 条件是简单同步函数，其调度会进入 Core 线程池。

`dispatch.json`：500 次串行空函数调用，5 次中位数。线程与 pipe 数量在独立一轮中统计，计时不包含打点开销。标准线程池仅作为测量参照，没有替换生产实现。

## 1. 最终 State 的宽映射复制仍是主要扩展性瓶颈

位置：[operations.py](/home/chengqian/projects/AutoAgent/autoagent/core/runtime/operations.py:319)。`_PathEdit` 遇到 Mapping 时执行 `dict(original)`。一个 Delta 内已经做到同一祖先只复制一次，但相邻 Event 仍各自复制。

只修改一个 Call 的 `execution_duration_ns`：

| Call 数量 | 单次耗时 | 峰值分配 |
| --- | ---: | ---: |
| 100 | 0.035 ms | 8.6 KiB |
| 10,000 | 0.801 ms | 385.3 KiB |
| 50,000 | 4.911 ms | 3.13 MiB |

Map 每个单元至少产生 CallStarted/CallCompleted，Call 集合逐渐增大，因此这些复制的累计工作量可能达到 O(N²)。真实 Map 2,000 单元的 profile 也把 `apply_runtime_delta` / `_PathEdit` 列为主要热点，证明不只是合成测试中的现象。

建议：针对 occurrences/operator_calls 等大表评估结构共享的持久化映射或分块映射。保持 Event 粒度、ACK 顺序和旧 State 不可变；不能直接原地修改当前字典。普通 dict 分桶若每次仍复制完整顶层桶表，也不能笼统宣称 O(1)。需要同时测量查询、迭代、序列化和小工作流成本。

## 2. Context 路径存在可优先解决的重复工作

位置：[transitions.py](/home/chengqian/projects/AutoAgent/autoagent/core/runtime/transitions.py:486)、[context.py](/home/chengqian/projects/AutoAgent/autoagent/core/context.py:74)。

- 空补丁仍 `dict(revisions)`。50,000 条版本记录、零修改，耗时 **3.907 ms**，峰值 **4.13 MiB**。这些版本可能来自同 Session 之前的运行，后续不写 Context 的节点也会承担成本。
- 每个操作扫描所有历史 revision；每个操作又独立调用 `apply_context_operation`，重复复制修改路径的祖先。
- 同一张 10,000 键 Context 上修改 1 / 10 / 100 个不同键，耗时分别 **1.17 / 6.73 / 65.50 ms**。这是无历史 revisions 的情况，已能显示逐操作复制的成本。
- WorkflowExecutor 在条件执行前做 preview，NodeCompleted 时再应用补丁，成功路径重复处理相同修改。

建议按难度分步处理：

1. 空 operations 直接返回原 Context / revisions；避免无效版本副本。
2. 批量补丁使用一个事务内的路径复制工作区，共享祖先只复制一次。保留操作顺序、父子路径覆盖、冲突检测、出错时旧 State 不变。
3. 给版本冲突查询建立路径前缀索引；安全时按活跃执行边界清理已不可能冲突的旧版本，不能直接删除全部历史记录。
4. 若要复用 preview，必须验证提交时对应 Context 版本没有变化，并按最终提交 sequence 记录 revisions；不能无条件缓存，因为 preview 和提交之间仍有 await 和其他事件。

## 3. Core 的同步调用调度频繁创建线程和 pipe

位置：[node_executor.py](/home/chengqian/projects/AutoAgent/autoagent/core/executor/node_executor.py:752)、[future.py](/home/chengqian/projects/AutoAgent/autoagent/core/executor/future.py:89)。

`_BurstThreadPool` 在队列变空后退出线程。串行同步 Operator、条件或同步 Stream 的 next/add 因此可能每次创建新线程。POSIX Future bridge 在最后一个 waiter 离开后关闭 notifier，下一次重新创建 pipe；已经完成的 Future 也经过这条路径。

| 500 次调用 | 耗时 | 创建线程 | 创建 pipe |
| --- | ---: | ---: | ---: |
| 当前 BurstThreadPool，空函数 | 94.07 ms | 500 | 500 |
| 标准复用线程池参照，相同 Future bridge | 47.40 ms | 1 | 500 |
| 直接等待已完成 Future | 11.58 ms | 0 | 500 |

这是空函数测试，测到的是 Core 调度成本，不是用户 Hook 执行慢。

建议：保留惰性启动和并发上限，评估可复用工作线程；idle 时通过条件变量等待而不是轮询。Future bridge 可评估 loop 生命周期内复用 notifier，或使用线程安全回调；completed Future 可评估快速路径。需要验证取消传播、同步函数取消后仍占容量、流关闭、跨平台行为以及 App 关闭时资源释放。常驻空闲线程/FD 是资源取舍，不能只拿吞吐提升作结论。

## 4. Node 完成和 Loop 仍扫描已完成的历史 Occurrence

位置：[transitions.py](/home/chengqian/projects/AutoAgent/autoagent/core/runtime/transitions.py:170)、[loop_scheduler.py](/home/chengqian/projects/AutoAgent/autoagent/core/scheduler/loop_scheduler.py:127)、[Loop scope 判断](/home/chengqian/projects/AutoAgent/autoagent/core/scheduler/loop_scheduler.py:522)。

NodeCompleted 无条件构造 `remaining` 列表，即使本次已经有下游 ready、根本不需要判断 waiting。历史数量 100 → 10,000 → 50,000：

- 仍有下游 ready：规划耗时 **0.052 → 0.556 → 2.751 ms**。
- 无新 ready，需要终态判断：**0.047 → 1.359 → 6.674 ms**。

这些仅为 Planner 耗时，不含 Reducer 的最终 State 复制。LoopPlanner 另外复制 known_occurrences 和 resolution 映射，并按 scope 扫描历史 occurrence。上一轮索引主要服务 Executor，尚未覆盖这些 Planner 查询。

建议：先把 `remaining` 的构造放到确实需要的分支；进一步使用 ACK 后维护的状态计数与按 Loop scope 分组的活跃工作索引。规划时结合当前 Delta 的局部变化计算，仍从原始 State 验证恢复/外部输入。Loop 复杂嵌套、revive、Wait 和错误路径需要单独覆盖。

## 5. 大批量 Child 的单单元阶段变化复制整个 units

位置：[transitions.py](/home/chengqian/projects/AutoAgent/autoagent/core/runtime/transitions.py:243)。每次 Child phase change 执行 `list(plan.units)`，修改一项，再转换为 tuple。

100 / 10,000 / 50,000 单元时，仅一次 phase 的规划耗时 **0.013 / 0.098 / 0.586 ms**；50,000 时峰值约 **784 KiB**。各单元逐个经过 opened/accepted/terminal，累计复制可能 O(U²)。当前测量是孤立 Planner 成本，未测完整 50,000 Child 启动过程。

建议：需要大量 Child 时评估分块 units 或结构共享的索引容器，并加终态计数。只把 Delta 改成 tuple 内的叶子路径仍会复制整条 tuple，不能消除根本问题。优先级低于通用 State 大表和 Context。

## 6. 大数据输入恢复仍走完整 JSON 往返

位置：[contract.py](/home/chengqian/projects/AutoAgent/autoagent/core/operators/contract.py:183)。默认节点输入即使来自 Core 已拥有的值，仍可能经过 thaw → JSON 检查 → dumps → validate_json → dump_python → canonical 比较。

含 50,000 个整数的 TypedDict：`restore` **10.72 ms**，峰值约 **2.56 MiB**；`validate_python` 参照为 **0.80 ms**、约 **391 KiB**。

两者语义并不等价，这不是可以直接替换后获得的确定收益。建议先按契约缓存元数据，区分外部恢复与可信内部值来源；为确实可证明等价的简单契约建立转换快路径。Enum、alias、Pydantic 模型、自定义验证器、缺省值和 canonical 检查必须保留原语义。

## 次要项与排除项

- Runtime payload 在 Planner 和 RuntimeEvent 构造阶段重复验证；profile 中可见同一批 Event 的两倍验证调用。可评估私有已验证路径，但收益小于宽表和 Context，公开构造/恢复的验证不能删除。
- Stream chunk 仍有 validate 后再 to_record 的重复验证，可沿用上一轮已验证值编码的思路；本轮未单独测量该路径收益。
- 当前没有重新发现已确认 Runtime Event 的历史缓冲；Repository 只保留未确认提交。User Event 的 journal 在交付后有 drain；不能仅看到 `_events` 字段就判定内存泄漏。
- 大 State 中保留执行所需 Call/Occurrence 和快照语义，不等同于泄漏；若要删除这些数据，需要先定义恢复、查询和子任务语义，不能作为普通性能优化直接裁剪。

建议优先顺序：Context 空补丁/批量操作和不必要的完成扫描；然后结构共享大表与同步调度资源复用；最后再推进复杂的 Loop scope 索引、Child 容器和契约快路径。没有改动 Sink 等待、Hook 业务行为或持久化模块。
