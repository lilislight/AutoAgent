# Core 内存与执行性能优化验证

## 结果

修改限于 Core、对应测试及本报告的运行产物。没有引入持久化去重、存储引用、Fork、外部依赖或 Host/Tracing 改造。

| 场景 | 修改前 | 修改后 | 降幅 |
| --- | ---: | ---: | ---: |
| 1,500 行嵌套输出，两节点 + 输出绑定：执行中位耗时 | 320.19 ms | 77.96 ms | 75.7% |
| 同场景：峰值分配 | 14.84 MB | 5.14 MB | 65.4% |
| 50,000 个 Call 中更新四个字段：中位耗时 | 19.90 ms | 4.79 ms | 75.9% |
| 同场景：峰值分配 | 5.21 MB | 3.29 MB | 36.9% |
| 已注册 400 节点链：执行中位耗时 | 562.88 ms | 411.54 ms | 26.9% |
| Map 500：执行耗时（开启 tracemalloc） | 7,207.28 ms | 725.17 ms | 89.9% |
| Map 500：峰值分配 | 62.18 MB | 2.89 MB | 95.4% |
| 400 条 Event 前缀回放：中位耗时 | 33.58 ms | 17.38 ms | 48.2% |

MB 使用十进制字节换算。峰值分配来自 tracemalloc，不是进程 RSS，也不是整套历史数据总占用。Map 场景的巨大收益来自消除 Call 推进时对相同映射输入/workspace 的反复构造。

## 测量方法与原始记录

- 基线提交：`91f0c98c5baed8d23ce917f4631db1d43a2cfab0`，本轮开始时产品源码无未提交修改。
- 修改前已记录 `full_before.json`、`focused_before.json`；初次 full benchmark 与基线测试曾并行，因此没有用它做最终对比。
- 最终对比使用 `git archive` 导出的同一基线源码，保存在 `/tmp/autoagent-memory-before`，没有创建分支或 worktree；使用完全相同的虚拟环境解释器。
- 两套基准按“旧 full → 旧 focused → 新 full → 新 focused”串行执行，没有同时运行本任务的测试或其他基准。
- `full_before_isolated.json` / `full_after.json`：现有 `tests.benchmarks.benchmark_full_core`。
- `focused_before_isolated.json` / `focused_after.json`：新增 `tests.benchmarks.benchmark_in_memory_refactor`，相同脚本在前后版本运行。
- focused 每项计时取 5 次中位数；预热后计时，编译和文件持久化不在计时内。内存使用单独的 tracemalloc 运行，避免计时受分配追踪干扰。
- 现有 full 基准中的链、Map、Context 三个场景各计时一次且开启 tracemalloc；registered chain 取 3 次中位数；replay 取 5 次中位数。
- `comparison.json` 保存所有时间、内存指标的机器可读前后对比。
- 系统调度和主机负载仍会造成波动，结果表明本机这些工作负载的变化，不等同于生产吞吐承诺。

## 实现

1. `values.py`：使用 Core 私有不可变容器标识已接管的值；外部 dict/list/mappingproxy 仍验证和隔离，已接管值直接共享。一次外部遍历保留重复引用、拒绝循环，不把任意外部 mappingproxy 当成可信对象。
2. `operations.py`：Planner 内部的 Operation 直接持有 typed 状态对象/不可变值。外部构造和记录解码在入口执行隔离、解码，Reducer 不再进行 record 编码/解码。
3. `operations.py`：一条 Delta 使用私有更新工作区，每个受影响祖先只复制一次；操作仍按原顺序执行。替换/删除父路径会丢弃其下先前的编辑；任何失败都不会修改旧 State 或 Operation 的值。
4. `transitions.py`：移除热路径 `_encode_runtime` 和状态 record 构造。Call 完成按字段更新，其他新建或浅替换 typed 对象保留未改字段的引用。
5. `workflow_executor.py`：普通调用和 Map 从已接受的 Call 输出继续推进，Aggregation 从已接受事件继续推进，Invocation 结束复用 Exit 输出；不再为了跨内部阶段重复转换输出。用户可变结果视图仍隔离。
6. `events.py`：解码 ContextPatch 时接管其值，避免恢复后出现可变嵌套值或 list/tuple 表示差异。

序列、语义事件边界、微秒发生时间、纳秒耗时和“提交确认后发布 State”未改变。未把存储格式转换从整个公共 API 中删除；它仍存在于显式 codec、Checkpoint 和用户类型边界。

## 正确性

- `tests_after.log`：307 项 Core 测试全部通过，包含新增 13 项共享/隔离/事务测试。
- 新测试覆盖 Event → Delta → State 身份共享、Context 包装共享、不可变边界、用户返回值残留引用、并发 Operator 输入隔离、旧 State 不变、映射别名隔离、父路径替换、删除后新增、tuple 索引移动、尾部失败原子性、未知存储结果后的相同 Event 重试，以及 record 回放一致性。
- 新的不可变容器继续维持 Hook Context 的只读契约；没有为了性能取消用户侧隔离。
- `examples_run.log` / `examples/`：原 Core 订单示例以及拒绝审批、人工审核分支已重跑。共 97 条 Runtime Events，所有前缀回放及保存的 Checkpoint 校验通过。
- Python compileall 和 `git diff --check` 通过。
- `full_suite_after.log`：全库 discovery 为 315 项，307 项通过，8 个旧接口测试模块导入失败。`tests_before.log` 已存在同样的 8 个加载错误，本轮没有新增此类错误，也没有通过跳过失败用例伪装全库通过。

## 保留的边界

- 普通 dict 映射仍需要浅复制：更新成本与沿途映射宽度有关，不宣称已经达到纯粹的 O(depth)。批量更新消除了同一 Delta 内重复复制相同映射。
- 任意外部值首次进入 Core 仍需验证和隔离；用户调用/类型恢复及显式序列化仍有必要成本。
- 内存 EventStore 仍保留历史，结构共享减少重复对象，并不自动提供历史保留上限。
- 默认共享的是已经接管的 Core 值，不保证与用户函数原始返回对象为同一对象。
- JSON 中 payload 与 delta 的重复内容依旧存在，这是未来 Sink/存储层的职责。

## 重跑当前版本

```bash
.venv/bin/python -m tests.benchmarks.benchmark_full_core
.venv/bin/python -m tests.benchmarks.benchmark_in_memory_refactor
.venv/bin/python -m unittest tests.test_core_memory_sharing -v
```

应串行运行基准，避免与测试同时占用 CPU。执行 `compare_benchmarks.py` 可从本目录原始结果重建比较数据。
