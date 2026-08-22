# AutoAgent V2 Core

这是独立的 AutoAgent V2 Core 项目，包含：

- Workflow 静态定义、严格值契约、Compiler 和不可变 WorkflowIR；
- DAG、并行 Join、自然 Loop 和 scoped NodeOccurrence Scheduler；
- 同步/异步 Operator、Map、StreamReducer、Wait、执行策略和 Child Workflow Executor；
- 原子 StateOperationBatch、可聚合 Runtime Event、唯一 StateReducer、RuntimeState、
  Event 前缀恢复和 Crash Recovery；
- 同步、异步、submit、stream、resume、cancel、wait 和可操作 Child Handle 的 App 入口；
- App 级同步/异步 Operator 全局并发限制和有界 Map 调度；
- 可注入 Journal、UserEvent Journal、Scheduler、Executor 和 Operator Registry Port。

Core 不包含数据库、Event 发送、Server、远程 Task Runtime 或 UI。

运行测试：

```bash
../.venv/bin/python -m unittest discover -s tests -v
```

运行 Full 模式基准：

```bash
../.venv/bin/python -m tests.benchmarks.benchmark_full_core
```

完整 Core 分层见 `CORE_DESIGN.md`，静态模型见 `workflow.md`，Runtime 模型见
`runtime.md`，测试矩阵见 `TESTING.md`，后续模块顺序见 `TODO.md`。
