# Core 类型恢复测量与有限优化

## 结论与范围

本轮只扩展现有内部 Python 恢复快路径的准入条件：普通 TypedDict 中的可空 JSON 字段，以及值类型为 str/bool/int/None 的 Literal。递归类型、配置过的 TypedDict、Annotated、自定义验证/序列化、Model、Enum、tuple 和一般多分支 Union 仍走原严格 JSON 恢复。

生产代码仅改动 autoagent/core/operators/contract.py 的注解与 schema 判定；公开 restore、严格验证、规范化编码对照、每次调用的可变数据隔离均保留。此前尚未提交的 Context/Child 改动保持不变。

## 测量方法

- 首轮 before.json/after.json 是探索测量；最终结论使用 before_0..2.json 与 after_0..2.json 的交替测量。
- 每个恢复微测量和真实单节点工作流分别预热一次、计时 7 次，tracemalloc 独立一轮。汇总取三轮中位数。
- 工作流通过公开同步 App API，预注册 Workflow，同 Session 重复执行，无慢业务 Hook 或持久化 Sink；计时排除 App 初始化、编译注册、关闭和输入 Model 的初次准备。
- 恢复占比在单独插桩轮测量，不混入主耗时；仅统计 Operator 输入的 _restore_internal，不包括其他输入编码、输出校验及 State/Event 冻结等工作。
- 基线使用本轮修改前的 contract.py 中两个准入函数，独立进程替换同名函数；其余 Core 与最终版本相同，因此包含此前 Context/Child 优化。已核对保存的基线文件与 HEAD 对应文件完全一致，校验和见 source_manifest.json。

## 最终性能

| 场景 | 修改前 / ms | 修改后 / ms | 耗时变化 |
| --- | ---: | ---: | ---: |
| 1 万整数可空批次：恢复 | 1.962 | 0.856 | -56.4% |
| 1 万嵌套记录：恢复 | 13.787 | 12.629 | -8.4% |
| 1 万整数可空批次：完整工作流 | 18.771 | 17.856 | -4.9% |
| 1 万嵌套记录：完整工作流 | 121.088 | 117.709 | -2.8% |
| 1 万嵌套 Model：完整工作流，对照 | 122.921 | 123.198 | +0.2% |

恢复峰值分配：

| 场景 | 修改前 / KiB | 修改后 / KiB |
| --- | ---: | ---: |
| 可空整数批次 | 509.7 | 156.8 |
| 嵌套记录 | 4166.6 | 3750.6 |

这些是测量窗口内新增 Python 分配峰值，不是 RSS。可空整数批次工作流峰值也由约 1.09 MiB 降至 0.80 MiB。没有通过缓存恢复后的可变对象获得收益。

## 收益边界

- 首轮独立 profiling 中，1 万嵌套 TypedDict 的 Operator 输入恢复约占工作流 9.6%，嵌套 Model 约 12.7%；小输入恢复仅几十微秒。因此不能把恢复函数的加速比例当成整个 Core 的提升比例。
- 整数列表等较简单数据形态收益较大；逐行嵌套对象的收益较小。实际工作流约改善 3–5%，不代表所有负载。
- 小工作流约 1 ms，测量波动明显。例如未改变实现的 Model 小工作流对照也波动约 11%；不能将该波动归因于本轮快路径。原始样本全部保留。
- 通用 Model、自定义验证器和一般 Union 没有扩展。恢复只占端到端耗时的一部分，尚不值得为了它们引入更复杂、更难证明等价的准入规则。

## 正确性

- 新增 6 项测试，覆盖 300 组随机嵌套记录、缺失/额外字段、错误类型、可空容器、Literal、超大整数、Unicode surrogate、非有限浮点、非 JSON 数据。
- 比较原严格 JSON restore 与内部恢复的接受/拒绝、恢复结果和规范化记录；确认 Model、配置、验证器、Enum/tuple 和多类型 Union 保持回退。
- 验证两次恢复的嵌套可变数据互不共享；真实 Operator 修改输入后，用户原输入和 Runtime State 输入不变，Event 重放与最终 State 一致。
- 全量发现 346 项：338 通过，8 个既有外围模块导入错误，仍是已删除的 InvocationOpened/InMemoryEventJournal 引用，见 tests.log。并非全仓全绿。
- 编译与 git diff --check 通过。

## 复现

```bash
.venv/bin/python -m tests.benchmarks.benchmark_core_contracts
.venv/bin/python -m unittest tests.test_core_contract_fast_paths -v
.venv/bin/python -m unittest discover -s tests -v
```

交替对照的基线只需从 source_manifest.json 指定提交读取原 contract.py，在独立 Python 进程中用 runpy.run_path 载入，将当前 contract 模块的 _simple_json_annotation 和 _simple_adapter_schema 绑定为原函数，再运行相同 Benchmark。此方式不修改工作区文件或创建 Worktree。
