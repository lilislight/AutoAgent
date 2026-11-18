# Core A/B 优化与验证

> 当前状态：B 已按用户要求回退，A 保留。下文的 A/B 实现与前后数据是回退前的历史测量，不代表当前代码仍包含 B。回退验证见 rollback_B.md。

## 范围

实现 A（Planner 复用运行计数）和 B（WorkflowIR 静态 Loop 查询）。C 不做：Session.updated_at_us 的 Delta 操作、Reducer、Event/Checkpoint Schema 均未改变。没有修改外围模块，也没有创建分支或提交。

## A：复用现有计数

- 默认 Repository 给默认 TransitionPlanner 的 WaitRequested、ChildAwaitSuspended、NodeCompleted/NodeFailed 传入当前已确认 State 的 ExecutionIndex。
- 只读 occurrence_counts，按当前 Occurrence 的原状态扣除其贡献；Node 完成仍保留 ready/revived 阻止进入 waiting 的原条件。waiting Occurrence 和 Wait 记录计数保持区分。
- 没有新增持久字段或常驻计数表，索引仍由 Repository 在 ACK 后更新；独立 Planner 调用和自定义 Planner 保留原扫描及原签名。
- 没有把取消、恢复等需要枚举条目的路径改为计数。

## B：预计算静态关系

- WorkflowIR 增加 Node、Back/Entry/Exit Edge、Header 到 Loop 的只读查询表，保留原查询方法。containing_loops 保留 (-len(node_ids), id) 排序，Entry/Exit/Header 保留源顺序，Back 保留首个匹配。
- 预计算 Loop 的 frozenset 成员集合，供 LoopScheduler 的成员判断及严格包含比较复用；未把共享 Header 的兄弟关系错误改为祖先关系，也未额外存储整套祖先/后代表。
- 无 Loop 的 IR 共用空查询表；索引字段不参与相等比较，不改变定义哈希语义。替换或重新构造 IR 会重新计算。

## 测量方法

- 基线是本轮改动前的完整 Core 源码副本，包含前几轮优化；只读临时副本，不是 Worktree/Fork。校验基线提交见 source_manifest.json。
- Planner 使用保留 100/1,000/10,000/50,000 个 Occurrence 的合成 State，包含一个 running 和一个 waiting；只测 Planner，不包含 State 构造和既有索引构建。每轮批量调用 10 次，7 轮取中位数。
- 拓扑查询每轮调用四个查询方法 100 组，7 轮取每组中位数。兄弟 Loop 为独立的编译后 Loop IR 副本；嵌套 10/50 层通过真实编译器生成。
- IR 构建测量使用 dataclasses.replace 重建 WorkflowIR，不包含完整 Workflow 编译。驻留和峰值分配在独立 tracemalloc 轮测量，保留构建结果；不是 RSS。
- 普通工作流用 benchmark_core_execution 前后交替三轮，每轮内部五次计时；与内存测量、功能测试分开执行。

## Planner 结果

| 50,000 Occurrence 场景 | 修改前 / ms | 修改后 / ms |
| --- | ---: | ---: |
| WaitRequested | 3.440 | 0.013 |
| ChildAwaitSuspended | 3.745 | 0.011 |
| NodeCompleted，无新 ready | 6.927 | 0.027 |

以上是大型保留历史下的 Planner 耗时，不能当作整个工作流的提速比例。A 不增加索引驻留结构。

## Loop 查询与空间成本

| 场景 | 四次查询：前 / μs | 后 / μs | IR 构建：前 / ms | 后 / ms | IR 新分配驻留：前 / KiB | 后 / KiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 无 Loop | 1.505 | 0.586 | 0.005 | 0.006 | 1.3 | 1.4 |
| 100 个兄弟 Loop | 15.673 | 0.622 | 0.176 | 0.398 | 54.6 | 119.9 |
| 1,000 个兄弟 Loop | 131.458 | 0.638 | 2.310 | 4.978 | 639.6 | 1196.6 |
| 10 层嵌套 | 4.223 | 0.613 | 0.019 | 0.070 | 9.0 | 26.6 |
| 50 层嵌套 | 20.828 | 0.611 | 0.068 | 0.804 | 30.2 | 248.3 |

B 是明确的预计算取舍：1,000 个兄弟 Loop 的 IR 驻留新增约 557 KiB，构建增加约 2.67 ms；50 层嵌套增加约 218 KiB，构建由约 0.068 ms 增至 0.804 ms。嵌套时索引大小取决于总 Node–Loop 成员关系，不能宣称只随 Loop 数线性增长。适用于重复调度和复用 IR，不保证仅构造一次、查询极少的场景更划算。

## 普通工作流交替对照

| 场景 | 修改前 / ms | 修改后 / ms | 变化 |
| --- | ---: | ---: | ---: |
| 5 万元素输入链 | 67.600 | 66.256 | -2.0% |
| 同 Session 连续 100 次 | 85.555 | 84.164 | -1.6% |
| 100 次 Loop | 104.008 | 105.302 | +1.2% |

小幅差异不能直接视为稳定收益。Loop 三轮 after 有高于也有低于 before 的结果，整体中位数约慢 1%；本轮没有观察到稳定的大幅普通工作流回退，也没有证明全部场景都会加速。

## 正确性

- 新增 6 项测试：150 组随机状态对四种事件比较 indexed/scan Delta；索引保持不变；waiting Occurrence 与 Wait 记录区分；丢 ACK 时 State/计数不提前发布、重试同一个 Event，并验证 C 的时间操作仍在；自定义 Planner 原签名；随机拓扑顺序/重复成员/首匹配/不可变性；真实编译的嵌套及共享 Header 拓扑查询对照。
- 全量发现 352 项：344 通过，8 个既有外围导入错误，仍为 InvocationOpened/InMemoryEventJournal 的旧引用，见 tests.log。没有新增失败，但全仓并非全绿。
- 现有事件重放、Checkpoint、取消、Loop/Child、旧 State 不变及 ACK 后重建索引对照测试通过。
- Core/tests 编译和 git diff --check 通过。

## 复现

```bash
.venv/bin/python -m tests.benchmarks.benchmark_core_planner_topology
.venv/bin/python -m tests.benchmarks.benchmark_core_execution
.venv/bin/python -m unittest tests.test_core_planner_topology -v
.venv/bin/python -m unittest discover -s tests -v
```

before.json/after.json 保留原始样本，execution_{before,after}_{0,1,2}.json 保留交替测量，comparison.json 汇总。
