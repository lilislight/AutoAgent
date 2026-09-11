# Core 功能检查与 Runtime Events 导出

本次直接运行 `examples/core_workflow_api_demo.py` 的 `main()`，只在运行时注入文件 Event Sink，并将 Checkpoint 输出路径指向本目录。没有修改示例或产品源码。另外两条路径使用同一个 `build_workflow()`，只改变输入及审批结果。

## 先看这些文件

- `runtime_events.json`：三个场景的全部 **97 条**规范 Runtime Event，缩进排版，包含完整 payload、delta.operations、标识、sequence 和时间字段，没有裁剪。
- `runtime_events.jsonl`：同一批事件，每行一条，方便程序处理。
- `timeline.txt`：按场景列出的事件简表，含完整 payload 和 operation 数量；完整 StateDelta 请看 JSON。
- `example_verification.json`、`semantic_checks.json`：示例运行和回放校验结果。
- `core_tests.log`、`core_test_summary.json`：Core 测试完整日志及模块统计。
- `full_tests.log`：全库测试完整日志，包括尚未适配的旧接口导入错误。
- `benchmark.json`：当前 Core 基准运行结果。

## 场景结果

| 场景目录 | 结果 | Runtime Events |
| --- | --- | ---: |
| `approved_recovery_child/` | 原示例：Wait → 卸载 Checkpoint → 新 App 加载/恢复 → 审批通过 → 并行检查/汇合 → spawn Child → 父子完成 | 44（父 37，子 7） |
| `approval_rejected/` | 审批拒绝 → 拒绝分支完成，不创建 Child | 19 |
| `manual_review/` | 审批通过，但风险检查失败 → 人工审核分支完成，不创建 Child | 34 |

各场景目录同时包含完整 JSON、JSONL、Timeline 和最终 State；原示例目录还包含父、子 Checkpoint。

合并文件按场景运行顺序及 Sink 接收顺序排列。**sequence 只在各自 Session 内排序**，父子 Session 分别从 1 开始。源 App 和恢复 App 使用同一个文件 Sink，因此恢复前后的父 Session 事件没有断开。

## 实际验证

- 20 个 Core 测试模块共 **294 项通过，0 失败、0 错误、0 跳过**。覆盖编译/类型契约、Loop、Scheduler、Map/Stream/Capability、Context、Wait、Child、取消/关闭、并发、Checkpoint、恢复和提交失败边界，详细用例见测试日志。
- 三个示例场景全部完成；97 条 Event 序列化往返一致、每个 Session 的 sequence 连续、每一个前缀均可回放并通过 State 验证。
- 保存的 5 个 Checkpoint/关闭快照均与对应事件前缀回放结果相同；父 Session 的 Checkpoint + 后续事件与完整回放一致。
- 原示例父 Session 第 13 条 Event 同时让 risk 和 inventory ready；第 14、15 条分别记录两个 Node 的 Started。
- Wait 恢复没有重复 NodeStarted，也没有重新执行已完成的 normalize Operator。
- 已经停在 Wait 的恢复不会产生未知调用恢复处理，因此本示例没有 `recovery.applied`。Map、Capability、异常、取消等路径由测试覆盖，不应期望正常订单示例产生全部事件类型。
- Python 编译及 `git diff --check` 通过；UI 的 13 项测试通过。`benchmark.json` 保留的是此前 Core 重构后的基准结果，本次时间协议调整未重跑基准。

## 检查边界

全库 discovery 报告 302 项，其中上述 294 项通过，另外 **8 个模块加载错误**，未执行这些模块中的用例：`test_child_persistence_integrity`、`test_cli`、`test_host_lifecycle`、`test_host_project`、`test_host_runtime_store`、`test_runtime_checkpoint_trace`、`test_tracing_server`、`test_user_event_persistence`。原因是仍引用已移除的 `InMemoryEventJournal`、`InvocationOpened`、`StateTransition` 等旧接口。这包含未迁移的 Core 持久化/Checkpoint 测试及外围集成测试，因此不能称全库功能已全部验证通过。

此次输出已随 Core 时间协议更新重新生成：发生时间采用 Unix 微秒，字段为 `*_at_us`；耗时仍为 `*_duration_ns`。Runtime Event/State schema 为 5，Session Checkpoint schema 为 3。旧纳秒格式不兼容。事件关联字段已从当前产品模型移除。外围时间协议仍待后续重构。未执行外部服务集成。测试通过不代表穷尽所有执行组合。

## 重新运行示例导出

从仓库根目录执行（覆盖本目录对应的运行输出）：

```bash
.venv/bin/python artifacts/core_runtime_review/run_examples.py > artifacts/core_runtime_review/examples_run.log 2>&1
.venv/bin/python artifacts/core_runtime_review/verify_exports.py
```

导出器通过 `runtime_event_sink` 接收 Runtime Events；`stream()` 中的 User Events 不作为 Runtime Events 混入文件。文件 Sink 对每条记录 flush/fsync 后才返回，因此测得的本次示例耗时包含文件持久化开销；独立基准未使用该文件 Sink。
