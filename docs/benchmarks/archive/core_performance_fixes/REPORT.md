# Core 性能修复与验证

## 实现范围

直接修改当前 V2 Core 及对应测试和 Benchmark。保留此前已完成的内存优化；没有新建 Fork/分支，没有修改 Host、SQLite、Tracing 或 UI。

1. **Context 批处理**：空补丁直接复用 Context/revisions；同一批修改共用路径工作区；历史版本只筛选一次，随后检查可能冲突的路径。仍保留重复路径、父子路径冲突和出错原子性。
2. **完成与 Loop 规划**：有新 ready/revived 时不再构造历史 remaining 列表；Loop 使用临时覆盖视图，按 scope 查询 ACK 后维护的 occurrence 索引。自定义 Scheduler/Repository 保留原调用和扫描后备路径。
3. **大表结构共享**：512 项以下沿用普通不可变映射，大表采用 128 项有序块及分区查找目录。更新复制受影响块和目录，保持插入顺序；删除/重新插入遵循 dict 顺序。稀疏块达到阈值后重建，避免删除追加长期积累空块。
4. **Child units**：大计划使用不可变分块序列，阶段变化只替换目标块。初始化和 Checkpoint 解码选择相应表示；记录仍编码成原有数组格式。
5. **同步调度**：惰性、限量 daemon 线程通过条件变量等待并复用，关闭时唤醒、清理；空闲线程释放最后一次调用/结果引用。POSIX notifier 固定在 RuntimeLoop 生命周期内复用，已完成 Future 保留异步取消边界但不建立 pipe。没有增加 idle 轮询。
6. **类型转换**：普通 JSON TypedDict 在内部输入准备时走经约束的 Python 验证路径，保留 canonical 比较。自定义 schema、验证/序列化规则、Model、Enum、复杂注解及不等价记录回退原有 JSON 路径；公开 restore 不变。Stream chunk 已验证后直接编码，避免重复验证。
7. **小对象成本**：缓存有限数量的 dataclass 字段名，并将大表辅助类型导入移出更新热路径，降低小 State 的额外开销。

未改变 Event ACK 顺序、未确认提交重试、Hook 业务行为、锁语义和 Event/Checkpoint 记录版本。Core 仍不持有已确认 Runtime Event 历史。

## 测量方法

基线为本轮修改前的源码副本 `/tmp/autoagent-audit-fix-before`，包含之前尚未提交的优化，并非 Git HEAD。前后使用同一个 Python 3.12 环境和同一份 Benchmark。

- `before.json` / `after.json`：7 次计时的中位数；tracemalloc 在另一次执行中记录峰值。输入准备一般不计时；内部输入转换探针两边都包含整数列表构造。Map 为已注册 Workflow，通过同 Session 实际执行并验证结果长度。
- 合成大表探针区分 cold 和 steady：前者包括把大普通映射转成分块表示，后者先应用一次更新，测量后续更新。不是把纯合成单字段更新等同于一次完整 Event 提交。
- `paired/`：额外三轮交替执行前后版本，用于小任务、Loop、大输入链，表中取三轮中位数。避免从单轮微小波动推断收益。
- `before_full.json` / `after_full.json`：原有完整 Core Benchmark，部分耗时启用了 tracemalloc，保留原始结果供核对。
- 峰值是测量窗口内 Python 新分配峰值，不是进程 RSS 或整张 State 的总内存。Profiler/持久化文件 IO 不混入主表。

## 耗时

| 场景 | 修改前 | 修改后 | 减少 |
| --- | ---: | ---: | ---: |
| 5 万 Call：稳定单字段更新 | 7.5782 ms | 0.0378 ms | 99.5% |
| 5 万 Child units：稳定阶段更新 | 0.5390 ms | 0.0160 ms | 97.0% |
| 1 万键 Context：100 项补丁 | 69.9815 ms | 3.9873 ms | 94.3% |
| 5 万条版本记录：空补丁 | 5.1385 ms | 0.0002 ms | 100.0% |
| 5 万整数：内部输入转换 | 12.3876 ms | 5.5176 ms | 55.5% |
| 500 次同步空调用 | 99.4870 ms | 38.6065 ms | 61.2% |
| Map 100 单元 | 25.5124 ms | 21.8120 ms | 14.5% |
| Map 1,000 单元 | 290.3331 ms | 235.3974 ms | 18.9% |
| Map 5,000 单元 | 3045.8078 ms | 1221.1574 ms | 59.9% |
| 大输入 21 节点链（交替测量） | 80.72 ms | 72.13 ms | 10.6% |
| 同 Session 连续 100 次（交替测量） | 100.00 ms | 95.25 ms | 4.7% |
| Loop 100 次（交替测量） | 142.40 ms | 113.27 ms | 20.5% |

## 分配与资源

- 5 万 Call 稳定更新的峰值分配：3209.8 KiB → 19.2 KiB。
- 5 万 Child units 阶段更新的峰值分配：784.5 KiB → 10.8 KiB。
- Map 5,000 单元的峰值分配：9974.3 KiB → 8867.8 KiB。

独立计数测试在一个实际 RuntimeLoop 中执行 500 次同步空函数，包括 App 创建和关闭：Operator 线程 **500 → 1**，pipe **501 → 2**。修改后的两根 pipe 分别服务 RuntimeLoop 的动作队列和固定的 Future notifier，关闭后均释放。复用意味着一个曾执行同步任务的 App 会保留空闲 worker，直到关闭；这些线程阻塞等待，不轮询，数量受上限约束。

## 一次性成本与仍存在的限制

- 外部大普通映射首次转换有成本。合成 50,000 项 cold 更新为 **7.27 → 19.66 ms**；不能把 steady 的约 0.04 ms 当作首次转换耗时。正常大表从小表增长、初始化或 Checkpoint 解码时选择表示，后续更新分摊这一成本。
- 分块不是 O(1) 持久化 HAMT：值更新仍复制约 N/128 个块目录引用；增删键还复制一个哈希分区，均匀散列下约 N/256 项。极端散列碰撞会退化。这里没有引入第三方依赖。
- Context 有实际写入时仍会扫描一次版本记录；尚未引入完整前缀树，也没有裁剪版本历史。preview 与最终提交仍分别验证/计算，避免错误复用跨 await 的过期结果。
- 无新 ready 的最终等待判断、异常路径等仍有部分扫描。没有为消除一次终态扫描而放宽恢复验证。
- 简单契约以外仍走严格 JSON 恢复；类型快路径不能推广到任意 Pydantic/Enum/自定义 validator。
- 某些低收益重复 payload 验证仍保留。本轮没有减少事件边界来换吞吐，也没有裁剪执行恢复需要的 State。

## 正确性验证

- 新增 12 项测试：随机映射事务与 dict 顺序对照、快照不变/块共享、删除重插及压缩、Child units 编码往返、批量 Context 顺序与冲突、严格契约回退、线程复用与关闭、完成 Future 取消边界及 notifier 释放。
- 实际执行 530 单元 Map，对照完整事件重放与 Checkpoint State；实际执行 513 个 Child，验证跨块阶段更新、全部终态及 State 编码往返。
- 全量发现 **332 项：324 通过，8 个既有外围导入错误**。错误模块仍为 child persistence、CLI、Host lifecycle/project/store、runtime checkpoint trace、tracing server、user event persistence，按 Core 范围未改动。
- 原示例及分支导出 **97 条 Runtime Event**，检查记录往返、每个事件前缀、Checkpoint 与重放 State，并在每次 ACK 后对照重建索引。
- Core/tests 编译及 `git diff --check` 通过。

## 复现

```bash
.venv/bin/python -m tests.benchmarks.benchmark_core_fixes
.venv/bin/python -m tests.benchmarks.benchmark_full_core
.venv/bin/python -m tests.benchmarks.benchmark_core_execution
.venv/bin/python -m unittest tests.test_core_performance_fixes -v
.venv/bin/python -m unittest discover -s tests -v
```

所有测量文件和示例输出都位于本目录；`comparison.json` 汇总主要数值，`tests.log` 保留全量测试失败的具体既有导入错误。
