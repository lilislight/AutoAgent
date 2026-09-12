# Core 执行与事件所有权重构实测

## 实现范围

直接修改当前 V2 的 `autoagent/core`，以及对应测试、benchmark 和本报告产物。保留此前已完成的内存共享重构。没有创建分支/Fork，也没有修改 Host、SQLite、Tracing、UI 或示例产品代码。

- 删除 Core EventStore 的实现、导出、注入参数，以及 Repository 的历史缓冲、`events` / `drain_events` 接口。
- Sink ACK 后发布 State，释放已确认 Event 的长期引用。仅未确认提交保留同一个 Event 和候选 State；失败/取消后重复发送原 Event。无 Sink 时直接提交，不积累历史。
- Event 与 State 可以共享不可变业务值；交接不触发深复制，也不允许 Sink 修改共享值。需要的历史由外部 Sink 保存。
- 默认下游输入映射不再解冻无用的 Invocation 输入；入口和自定义 Mapper 保留完整、隔离的输入。
- Operator 的输入/输出在同一边界已验证后直接编码。公开 `to_record` 和严格恢复验证保持不变。
- 添加只含 ID 和计数的派生索引：已启动/运行中 Occurrence、生命周期计数、按 Occurrence 查 Call/Wait。ACK 后增量更新，安装 Checkpoint 时重建，卸载 Session 时移除。自定义 Repository 无索引能力时从 State 构建。
- Scheduler 与 TransitionPlanner 使用原映射加局部改动的临时视图，避免复制整张规划映射；初始 State 仍物化为不可变映射。
- State 查询与 Session 锁仅在未命中时创建默认对象。

事件顺序、Sink ACK 等待、锁和用户 Hook 的执行契约保持一致。Event / Checkpoint 格式未改变；恢复仍使用外部提供的 Event 或 Checkpoint，Core 不回查事件存储。

## 测量方法

基线是本轮编辑前的完整 Core 和 tests 副本 `/tmp/autoagent-execution-before`，包含上一轮尚未提交的内存优化；不是 Git HEAD。使用同一个 Python 3.12.13 解释器。

为控制运行环境的时间波动，三轮交替执行前后版本（前→后、后→前、前→后），每个进程串行运行。下表取三轮结果的中位数。execution / memory 的每轮耗时又取 5 次的中位数，且计时与 tracemalloc 分开；full 的已编译链每轮 3 次，Map 等原有 full 场景每轮单次并启用 tracemalloc，因此不能把两种耗时口径直接比较。

新增存活内存是完成热身和计时后，单独启用 tracemalloc，再执行一次测量批次并 GC 后仍存活的新分配；不是整个进程 RSS，也不是整个 State 总大小。Sink 只 ACK、不保留 Event。不同场景的 State 大小和生命周期不同，不能据此宣称所有工作流的内存均为常数。

`paired/` 是最终比较原始结果；`before_*.json` / `after_*.json` 是早期单轮记录。复现：

```bash
.venv/bin/python artifacts/core_execution_refactor/run_benchmarks.py /tmp/autoagent-execution-before
```

## 执行耗时

变化为 `(修改后 / 修改前 - 1)`，负数表示减少。

| 场景 | 修改前 | 修改后 | 变化 |
| --- | ---: | ---: | ---: |
| 21 节点，大输入 50,000 个整数 | 319.67 ms | 74.48 ms | -76.7% |
| 同 Session 连续执行 100 次 | 90.14 ms | 90.55 ms | +0.5% |
| 100 次 Loop 迭代 | 130.36 ms | 130.99 ms | +0.5% |
| 嵌套数据流水线 1,500 行 | 76.42 ms | 61.53 ms | -19.5% |
| Map 500 单元（含 tracemalloc） | 720.48 ms | 721.62 ms | +0.2% |
| 已编译 400 节点链 | 405.31 ms | 377.71 ms | -6.8% |

## 分配与存活内存

| 场景 | 修改前 | 修改后 | 变化 |
| --- | ---: | ---: | ---: |
| 大输入链：新增存活内存 | 2464.3 KiB | 2378.4 KiB | -3.5% |
| 大输入链：峰值分配 | 4316.2 KiB | 4749.9 KiB | +10.0% |
| 连续 100 次：新增存活内存 | 836.4 KiB | 9.1 KiB | -98.9% |
| 连续 100 次：峰值分配 | 1159.7 KiB | 448.5 KiB | -61.3% |
| Loop 100 次：新增存活内存 | 724.4 KiB | 208.1 KiB | -71.3% |
| Loop 100 次：峰值分配 | 1015.4 KiB | 599.7 KiB | -40.9% |

释放事件历史显著减少同 Session 重复执行的存活分配；大输入链的执行耗时显著下降，但峰值分配没有随之下降。小任务与 Loop 没有一致、显著的执行加速，索引维护和临时视图也有自身成本。本轮主要收益集中在消除无用大输入复制及长期事件引用，不能概括成所有场景全面提速。

## 验证

- 基线全量发现：315 项，307 通过，8 个既有外围模块导入错误。
- 修改后全量发现：320 项，312 通过，同样 8 个导入错误；详情 `tests.log`，基线 `before_tests.log`。
- 新增/调整测试覆盖 ACK 丢失的同对象重试、候选 State/索引不可见、确认后 Event 可被回收、并发提交顺序、取消、按需输入与自定义 Mapper 隔离。
- Loop、Map、Wait/resume、Child、Checkpoint 安装的每次提交均对照从完整 State 重建的索引；卸载检查索引释放。
- 运行已有 `examples/core_workflow_api_demo.py` 的恢复/Child 路径和两个分支，共 97 条 Event。文件 Sink 输出位于 `examples/`；全部记录往返、所有事件前缀、Checkpoint 与 Reducer 恢复结果一致。Sink 由验证脚本持有，不属于 Core。
- Core/tests 编译检查、`git diff --check` 通过。

8 个既有失败模块为 `test_child_persistence_integrity`、`test_cli`、`test_host_lifecycle`、`test_host_project`、`test_host_runtime_store`、`test_runtime_checkpoint_trace`、`test_tracing_server`、`test_user_event_persistence`；仍引用先前移除的 Core API，按限定范围未修改。

## 仍存在的开销

不可变 State 的最终宽映射写时复制仍随宽度增长；本轮消除了部分规划过程的额外副本，没有引入持久化映射容器。LoopPlanner 自身的临时集合/映射、终态与错误路由的部分扫描仍保留。派生索引也随当前 Invocation 的 Occurrence/Call/Wait 数量增长，但不会按该 Session 的历次 Invocation 累积历史 Event。
