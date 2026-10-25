# V2 执行速度与 SQL 优化实施方案

日期：2026-10-23；进度更新：2026-10-24。状态：步骤 0–7 已实施并分别验证；步骤 8（可选）已完成定向实验，保留 0 ms 默认等待；步骤 9 主对比、专项和 SQL 诊断已完成；长测一组完成、另一组提前结束、两组未执行（按用户要求加快收尾），结果见最终报告。此前完整回归有已存在的天气示例入口测试失败，详见步骤 4–7 报告。

步骤 9 结果：[最终验收与 B0 累计对比](benchmarks/v2-optimization-step-09/README.md)。360 组五轮主对比、144 组专项、12 组诊断完成；durable 归一化耗时减少 20.4%，buffered 减少 40.0%。SQLite durable 一小时长测完成；buffered 约 53.5 分钟主动结束，PostgreSQL 长测未执行。完整回归 1764 项、PostgreSQL 321 项、无跳过；旧天气入口测试仍有两个失败子场景，不能称全部验收通过。

步骤 0 结果：[固定基线与诊断报告](benchmarks/v2-optimization-step-00/README.md)。104 组运行、2,580 次正式调用全部通过；该阶段仅建立基线。

步骤 1 结果：[新增事实与 Node 去重报告](benchmarks/v2-optimization-step-01/README.md)。232 组运行、4,952 次调用通过；完整回归 1,682 项通过。主目标正式验证 p50 降低约 7.7%–12.6%，其余场景分别记录，部分场景尚未证明端到端收益。本步已停止。

步骤 2 结果：[Writer 索引与临界区报告](benchmarks/v2-optimization-step-02/README.md)。244 组 Server 运行、4,904 次调用完成；冻结阶段完整回归 1,687 项通过，测量后当前工作区补测 218 项集成通过。8,192 条 pending 下新增入队耗时降低 43.6%；判定为结构改善，整体加速尚未证明。PostgreSQL Context 的耗时差异、复核不确定性及一次已补跑的控制进程中断均单独记录。本步已停止。

步骤 3 结果：[精简查询与有界图缓存报告](benchmarks/v2-optimization-step-03/README.md)。293 组真实 Server 运行、6,801 次调用完成；冻结阶段 1,701 项测试通过（PostgreSQL 306 项），当前 Provider/Host 补测 96 项通过（PostgreSQL 6 项）。无条件 Chain 的投影图正文和条件 SELECT 各从 120 次降到 0；128 Node 大图初筛耗时降低约 28.5%/49.8%（SQLite/PostgreSQL）。判定为结构改善，暂未证明整体加速；Loop 正式复核的耗时分布与 I/O 波动、索引读写空间代价、Checkpoint 保守清空缓存等限制均已记录。本步已停止，未开始步骤 4。

步骤 4–7 结果：[缓存、批量读写、编码与完成通知报告](benchmarks/v2-optimization-steps-04-07/README.md)。用户授权四个主要步骤连续实施；各中间包单独验证，主表计入 434 次 Server 运行、3260 次 HTTP 调用。修正 terminal 批次的 Node ordinal 缓存失效后，三轮合并对照的 72 对延迟和 CPU 均下降：buffered 六场景耗时降低约 14.7%–44.5%，durable 多数场景约 6%–13%。这些是本轮相对配平控制包的结果，不是 B0 至今的同轮累计收益。完整初版回归 1740 项（PostgreSQL 313 项）有一个天气 CLI 测试方法的两个已有失败子场景；最终修正后定向/集成 436 项通过，后续当前 Agent 补测 172 项通过。步骤 6 SQLite Loop 的单独复核仍有不利结果，原始样本与缓存/Checkpoint/并行源码更新边界均保留。

步骤 8 结果：[多 Session group commit 实验报告](benchmarks/v2-optimization-step-08/README.md)。主实验在同一冻结包上比较 0/1/5 ms、1/8/32 个并发 Session，两种后端共 72 次完整 Server、1272 次调用；增加观察路径探索及诊断后共 80 次完整 Server、1367 次调用，另有一组 32 并发观察实验主动中止并单列。三轮没有证明稳定的并发吞吐收益；1 ms 使单 Session 的进程 p95 中位数增加约 10.7%/15.9%（SQLite/PostgreSQL），5 ms 增加约 48.0%/62.9%。不修改产品 Writer 或默认值，也不计入累计优化收益。回归中单独修复上一阶段订阅注册时的取消清理竞态，最终 137 项通过（PostgreSQL 63 项），零跳过。另确认并发终态观察存在查询放大，未在本步修复。

本文是供后续实施者执行的分步方案。方案编写时仅完成历史结果复核、源码审查和只读查询计划检查；当前执行进度以上述状态及阶段报告为准。

## 1. 目标与范围

目标是在保留 V2 完整记录、durable 确认、恢复和历史查询语义的前提下，降低 Server 调用延迟、CPU 消耗和 SQL 交互次数，同时保持有界内存。

主要修改范围是 `autoagent/host/persistence/`，必要时涉及 Host 完成观察、驻留计量及相应测试。Core 的执行语义不是本轮重构目标。V1 仅作为性能参考，不修改归档实现，不引入兼容层。

实施必须逐步进行：每一步先建立对照，再修改、验证、测量、记录结论。不得把所有优化一次性叠加后只跑最终测试。

本文中的步骤是实施建议，不替代用户对代码修改的授权。正式开始时，以当前源码、测试和用户最新指令为准；保留工作区中其他任务的未提交修改。

## 2. 已有证据及限制

### 2.1 Server 延迟

SQLite、Artifact 阈值 8 KiB，单位 ms，指标为调用 p50：

| 场景 | V1 standard | V1 full | V2 durable | V2 buffered |
| --- | ---: | ---: | ---: | ---: |
| chain-10-16k | 40.8 | 60.1 | 942.2 | 377.9 |
| loop-100-16k | 273.8 | 611.6 | 9,501.3 | 4,088.2 |
| map-100-16k | 65.9 | 114.7 | 2,900.6 | 1,147.1 |
| context-append | 133.9 | 197.2 | 2,451.5 | 1,104.0 |
| text-overwrite | 540.6 | 1,301.1 | 3,072.6 | 1,570.6 |
| unique-history | 30.7 | 40.2 | 491.7 | 216.3 |

PostgreSQL 的 V2 也存在明显差距，例如 chain-10-16k 的 V1 full / V2 durable / V2 buffered 分别为 72.2 / 687.6 / 272.8 ms。因此问题不只是 SQLite 后端。

这些耗时包含真实 HTTP、完成观察和持久化屏障，不是纯 Operator 执行时间。V1 standard 为后续补测，未与 V2 同时重跑；共享主机和分批时段会影响绝对延迟。

### 2.2 CPU、存储与 SQL

SQLite chain-10-16k 每次调用的进程 CPU 时间约为：V1 full 52 ms、V2 durable 772 ms、V2 buffered 382 ms。CPU 是测量区间累计值除以调用数，不是调用 CPU 的 p50，也不能直接用它除以延迟推导精确 CPU 占比。

unique-history 中，V2 durable 的 Artifact 阈值从 1 MiB 改为 8 KiB，存储从约 106.2 MiB 降到 35.3 MiB，但延迟仍约 492 ms。调整阈值主要解决存储放大，不能代替执行路径优化。

更早一轮、10 个工作 Node、5 次调用的 SQL 诊断如下：

| 模式 | SQL 语句 | COMMIT |
| --- | ---: | ---: |
| V1 full | 80 | 10 |
| V2 buffered | 2,996 | 30 |
| V2 durable | 6,796 | 365 |

此 SQL 诊断包含 Host 观察和后台活动，不是新一轮 8 KiB 负载的精确分解。必须在步骤 0 重新采集，不能直接用于计算某个函数的优化收益。

### 2.3 本次源码及查询计划复核

- `ProjectionBatch.put()` 对新增事实记录也先执行 `get()`。已检查的一个 chain-10-16k V2 数据库有 6 次 Invocation、370 条 Runtime Event、567 条 `graph_changes`；后者按当前路径逐条检查旧值。
- `Storage.projections()` 在处理父子 Invocation 外键顺序时写入部分 Node，随后又写入全部 Node，存在重复 UPSERT。
- `outgoing()` 重复加载 Workflow 定义、扫描全部 Edges，并在无条件 Edge 场景也查询求值记录。
- SQLite 对上述求值查询显示 `SCAN e`；Node ordinal 查询则已经使用 `node_ordinal` 覆盖索引。
- 当前已有事务内投影缓存、分组 UPSERT 和部分批量 INSERT。优化应针对其边界，不应描述成“完全没有批处理”。

### 2.4 对比的语义边界

- V1 standard 不保存完整 StateOperations，不能按任意 Event 序列重建历史状态，不是与 V2 等价的性能目标。
- V1 full 和 V2 durable 的确认与崩溃边界也不同，不能只比较名字中的 full。
- 单 Session 的 durable 执行等待上一条 Event 的持久化确认后才继续；扩大 batch 无法合并尚未产生的后续依赖 Event。
- 原报告的 profiler 在多线程异步阻塞函数上的 CPU 归因不能作为忙等证据。使用未开启 profiler 的结果判断真实延迟。
- 本轮 8 KiB 测试没有重跑一小时负载。不能把短进程 RSS 峰值当作长期无增长证明。

## 3. 不允许破坏的契约

1. durable 模式只能在对应数据库事务成功提交后确认 append；Core 不提前发布候选状态。
2. 相同 Event 的重复提交保持幂等；同 ID 不同内容、序列冲突和 Operator 绑定冲突仍被拒绝。
3. accepted 与 durable frontier 分开。入队、提交、失败和取消不得混用两个边界。
4. Event、Artifact 引用和当前要求同步可见的投影保持事务一致性。
5. 保留全部规范 Runtime Event、StateOperation 和历史图变化，不通过省略记录实现加速。
6. 历史读取固定在指定 Session Event sequence 之后；后续 Event 和其他 Invocation 的新状态不能泄漏进去。
7. 队列、缓存和临时编码数据保持有界，不能用不断增长的历史对象缓存换取速度。
8. `flush()` 保持捕获调用时 accepted ticket 的边界语义。全局排空仅用于停止生产后的基准收尾，不成为公共 flush 的新要求。
9. 取消观察不取消 Workflow；取消等待 append 不得破坏已经接纳的数据和重试身份。
10. 父子 Invocation、Fork、恢复、Checkpoint、User Event 和维护路径遵守相同一致性要求。

## 4. 每一步共用的效果评估协议

### 4.1 三个比较对象

每一步必须同时保存：

- **B0：初始基线。** 完成测量工具准备后、任何产品优化前的固定 V2 源码。
- **A：本步骤之前。** 上一个已接受步骤的固定源码。
- **B：本步骤之后。** 只包含本步骤新增优化的固定源码。

比较 A/B 判断本步骤增量收益，比较 B0/B 判断累计收益。源码快照必须包含实际未提交代码，不能只记录 `git HEAD`。基准控制器、worker、业务负载、依赖版本和配置也要记录 hash。

`--source-root` 只固定 V2 的 `autoagent/` 包，不固定 `tests/benchmarks/server_comparison/`。测试工具发生变化时，必须用同一版工具重测 A 与 B。结果目录不可复用覆盖。

### 4.2 固定条件

- V1/V2 均运行原生 Server，通过真实 HTTP 调用；不使用外部 LLM。
- 主对照固定 Artifact 为 8192 bytes，分别测 durable、buffered。
- 保持队列预算、并发、观察方式、日志、Checkpoint 和内存监控开关相同，除非该步骤明确测试其中一个变量。
- SQLite 和 PostgreSQL 均测试；PostgreSQL 使用独立的临时数据库。
- 保留默认 1 MiB 阈值的最终回归，避免只在 8 KiB 下有效。本方案不自动修改 SDK 默认阈值。
- 重型测试串行执行，不并发运行 A/B Server 压测。记录后台负载和异常干扰。
- 用 A/B/B/A 或交替轮次降低时段偏差；当前工具单次命令的随机排列不能代替不同 source-root 之间的交错。
- 保留成功、失败、超时、拒绝、RSS guard 和注入故障的独立统计，不能把失败样本悄悄排除后只报告成功延迟。

### 4.3 测试层次

| 层次 | 每步要求 | 用途 |
| --- | --- | --- |
| 正确性 | 受影响模块测试，实际启用 PostgreSQL | 验证契约与故障边界 |
| 定向诊断 | 相关场景的 SQL 指纹、阶段计时、缓存与队列计数 | 证明目标机制真的改变 |
| 标准性能 | 六个 artifact 场景，两种后端、两种模式 | 检查端到端收益和跨场景回退 |
| 专项压力 | 按步骤增加慢库、缓存淘汰、并发或大历史 | 验证该优化特有的风险 |
| 最终验收 | 全部测试、长时负载和存储检查 | 接受完整优化结果 |

标准性能每个配置建议至少 5 个独立 Server 进程。初筛可先 3 个，但不能据此宣称稳定的小幅收益。chain/loop/map/context/text 正式验收建议每进程至少 20 次测量调用；unique-history 保持至少 128 次。预热单独记录。

每个步骤都运行标准性能初筛；确认方向后，对目标场景及任何疑似回退场景使用完整样本量。最终验收对全部场景使用完整样本量。不得把初筛当作最终 p95 结论。

### 4.4 必须记录的指标

| 维度 | 指标及口径 |
| --- | --- |
| 延迟 | HTTP submit、完成观察、最终排空、端到端 p50/p95；启动单列 |
| CPU | Server 进程测量区间 CPU / 完成调用数；不混入启动和预热 |
| SQL | 按 admission/write/projection/observation/maintenance 分类；SQL 指纹、执行次数、批量行数、COMMIT 次数 |
| 归一化 | SELECT/Event、DML/Event、事务/Invocation、Event/事务；运行外活动单列 |
| 队列 | accepted/durable frontier、pending 数量、retained bytes、排空时间 |
| 内存 | 外部采样峰值 RSS、空闲 RSS、缓存字节数、长时后半段 RSS 趋势 |
| 存储 | 关闭后物理大小、逻辑记录数、表与索引大小、Artifact 引用和 GC 正确性 |
| 语义 | 最终输出、历史状态、恢复结果、记录完整性及预期错误 |

事务 elapsed time、SQL cursor 累计时间、CPU 时间不等价，也可能重叠，不能直接相加或相减推导全部开销。连接/线程切换计数用于定位，不直接等同于耗时。

SQLite 统计 DB/WAL/SHM 和关闭后的 Checkpoint；PostgreSQL 分开报告数据与索引分配，共享数据库服务 RSS/WAL 不伪装成应用开销。20 ms RSS 采样可能漏掉短峰值，必要时增加单独的分配诊断，不把该诊断延迟混入正常结果。

### 4.5 效果判定与继续条件

先用 A/A 重复测量估计主机波动。建议把 5% 作为初始性能回退警戒线；若 A/A 波动更大，使用实测波动范围并增加样本。此值是实验判据，不是产品 SLA。

每步结果只能标记为以下之一：

- **已证明收益：** 正确性通过，目标机制改善，端到端或 CPU 改善在独立进程中可复现，其他关键场景没有超出噪声的回退。
- **结构改善，暂未证明加速：** 例如 SQL 减少，但延迟变化落在噪声内。只能以明确的后续依赖或复杂度收益保留，记录理由，不能宣传为加速。
- **需要修正：** 目标场景改善，但其他场景、内存或错误边界退化；修正并复测后才能接受。
- **撤回本步：** 正确性不通过，或没有可解释收益却增加维护成本。仅撤回本步改动，保留用户和其他任务的修改。

报告同时给出绝对值和相对值：耗时降低率为 `(A - B) / A`，加速倍数为 `A / B`。比较独立进程分布，避免仅将全部请求混合成一个 p50 掩盖某轮异常。若使用 bootstrap，按独立进程或配对轮次重采样。

RSS 增加超过 `max(5 MiB, A 的 5%)` 触发审查；有界缓存若带来固定开销，须给出预算、淘汰和平台期证据。任何随历史持续增长的新增内存、越过预算或 RSS guard 均不能以速度收益抵消。

## 5. 步骤 0：建立新的可归因基线

### 修改与产物

1. 保存工作区状态、V2 源码快照、依赖版本和有效配置；不清理其他任务修改。
2. 复核现有基准逐调用的排空逻辑，确保停止生产后 queued 为 0 且 accepted/durable frontier 相等。
3. 在测试工具中补充分阶段统计：admission 校验、驻留计量、编码、SQL 读写、投影计算、COMMIT 和观察。
4. SQL 诊断按指纹聚合，区分 execute 次数与 executemany 行数；只保存计数和耗时，不采集业务 payload 或凭据，不无限保存每条 SQL。
5. 新增条件分支多、历史求值记录多的场景，以及缓存容量以上多 Revision/多 Session 场景。新场景先对未优化版本采集结果。
6. 固定测量工具后创建 B0，运行六个 artifact 场景和 A/A 波动校准。

### 如何看效果

这一阶段不宣称产品加速。比较诊断开关关闭/开启时的 CPU、延迟和 RSS，说明测量开销；正式速度结果始终使用关闭重型诊断的路径。

交付 B0 数据、SQL 次数按阶段分解、每 Event 开销、平均事务 batch 大小、A/A 波动。无法分清观察 SQL 与写入 SQL 时，不进入基于 SQL 数量的收益结论。

## 6. 步骤 1：消除新增事实记录的读前检查和重复写入

### 修改范围与方案

主要文件：`projections.py`、`storage.py`。

1. 将“更新当前状态”与“追加本 Event 的新事实”分开。
2. 仅当写入路径已确认 Event 为新增时，`graph_changes`、`edge_evaluations` 直接构造新行，不逐行 `get()`。
3. 新事实按表批量 INSERT；保留 Event 全局身份与序列校验。
4. 消除父子 Invocation 写入过程中已经执行过的 Node UPSERT，保留外键所需的拓扑顺序。
5. 投影重建、accepted 查询 overlay、Fork 等入口不能未经证明复用“数据库中不存在该行”的假设。

### 正确性验证

覆盖新增 Event、完全相同的重复 Event、同 ID 不同内容、提交成功但确认丢失、父子 Invocation 同批处理、投影重建。恢复后每个 Event 的图变化和求值行既不重复也不缺失。

### 如何看效果

- 目标：已确认新增的图变化/求值记录不再产生逐行存在性 SELECT。
- 目标：一个最终 Node 投影行不因同一次依赖处理被无意义地写两遍。
- 测 chain、loop、map 与条件分支专项的 SELECT/Event、DML 行数、CPU/call、p50/p95。
- 逻辑历史行数与有效内容应保持一致；随机 ID、时间戳等差异需规范化比较。

机制没有按预期改变则修正本步；SQL 已减少而延迟未改善时，按“结构改善”记录，并查明剩余主要耗时，不能虚报加速。

## 7. 步骤 2：降低入队数据结构和临界区开销

### 修改范围与方案

主要文件：`writer.py`。

1. 用 `(kind, identity) -> Entry` 索引替代 pending 身份线性扫描。
2. 维护各 stream 的 pending 尾部序列，避免扫描队列找最新 sequence。
3. 用有界前缀迭代取得 batch，替代 `list(self.pending)[:batch_count]`。
4. 缩短 condition 持锁区间，避免普通数据库 I/O 长时间阻止消费者更新队列。
5. 保留 admission 串行顺序；锁外 await 后重新验证必要状态。保持内存 SQLite 的 mutation 串行约束和锁顺序。

pending 索引必须覆盖正在提交但尚未确认的 Entry，在提交成功或明确关闭清理时才移除。故障、取消和背压不能留下与 deque 不一致的索引。这里不引入无限增长的已提交身份集合。

### 正确性验证

覆盖队列已满、多个生产者、重复提交正在写入的 Event、取消入队等待、提交失败、关闭、内存 SQLite，以及共享对象引用计数释放。

### 如何看效果

- 测 slow-storage 和 buffered map/loop 的 admission 等待、pending 峰值、CPU、排空时间。
- 对不同队列长度做定向诊断，确认身份查找不再随 pending 数线性增加。
- 检查队列为零后索引和对象引用释放；不能只看请求提前返回。
- 正常 durable 队列较短，本步骤收益可能较小，必须如实报告。

## 8. 步骤 3：精简查询列与 Workflow 出边处理

### 修改范围与方案

主要文件：`storage.py`、`writer.py`、`projections.py`、必要时 `schema.py`。

1. 存在性、sequence 和绑定检查仅选择所需列。仅在确实命中重复身份、需要内容比较时恢复完整来源记录。
2. 缓存按不可变 Revision 标识的精简图元数据，建立 source Node 到全部出边的索引。
3. 保留未选中和不同 on 状态出边的图变化语义，不能只缓存最终选中的 Edge。
4. 仅在需要已计算条件结果时查询 `edge_evaluations`，无条件路径和无需求值的 skipped 路径避免无用 SQL。
5. 对条件分支专项执行 SQLite/PG EXPLAIN，评估 `(invocation_id, source_occurrence_id, source_status)` 索引；单独对比新增索引的查询收益、写入耗时和空间。
6. 不重复添加已有 `node_ordinal` 索引。图元数据缓存有字节上限，定义删除、重建和关闭时正确失效。

### 正确性验证

覆盖成功/失败路由、条件真/假、没有求值结果、skipped Node、多个 Revision 共存、缓存淘汰和定义维护。缺失条件结果时不得编造选择结论。

### 如何看效果

- 无条件 chain 场景的条件求值 SELECT 应消失；图定义加载从每 Node 重复加载变为受缓存命中情况约束。
- 条件历史增长场景比较查询计划、实际读取工作量和耗时；不要把空表 EXPLAIN 当作实测收益。
- 比较大图重复执行时投影 CPU、缓存命中率、RSS 平台期。
- 索引单独测 A/B，收益不成立就不保留，不与缓存收益混为一谈。

## 9. 步骤 4：增加有界的 Writer 已提交元数据缓存

### 修改范围与方案

主要文件：`writer.py`、`storage.py`、`projections.py`、`maintenance.py`。

1. 在已有事务内缓存之外，为 Writer 增加有界的已提交投影元数据缓存。
2. 优先覆盖 sessions、invocations、活跃 node_executions/operator_calls 等反复读取的行；只存元数据和引用，不缓存完整输入、输出和历史。
3. 本事务修改置于独立 overlay，成功 COMMIT 后再提升为已提交缓存。
4. accepted stream frontier 与 committed frontier 分开维护；待提交 Entry 的索引不能因 LRU 淘汰而丢失。
5. 首次访问或缓存未命中读取数据库。缓存“没有某 ID”不能成为永久不存在证明。
6. 维护操作统一处理失效；提交结果不确定时丢弃受影响缓存或重新读取。
7. 不将 Writer 缓存用于历史读取、其他连接的 SQL 快照或跨进程读者。
8. 如需新增部署预算配置，通过 Host 环境配置接入，更新 `.env.example` 和相关测试。

### 正确性验证

覆盖回滚、COMMIT 后确认丢失、崩溃重启、导入、Fork、Checkpoint anchor、rebuild/purge/GC/定义删除及强制缓存淘汰。历史查询在指定 sequence 的结果不得受缓存中新状态影响。

### 如何看效果

- 重点看单 Session durable chain/loop：跨事务重复 SELECT/Event 是否下降。
- 同时测多 Session、超过缓存容量的多 Revision 和持续唯一内容请求。
- 记录命中、未命中、淘汰、缓存字节数及每次调用 CPU。
- 缓存收益仅在短重复场景成立、唯一历史场景内存持续增长时，本步不通过。

## 10. 步骤 5：批量预取并按依赖顺序写入

### 修改范围与方案

主要文件：`storage.py`、`projections.py`。

将事务逐条查写调整为“收集键、分组预取、按序计算、按依赖写入、一次提交”。本步骤较大，应拆成 5A、5B、5C，每个子步骤都测效果。

| 子步骤 | 修改 | 单独观察的效果 |
| --- | --- | --- |
| 5A | 整个 batch 汇总 Artifact hash，分块查询、去重写入 Artifact 和依赖 | Artifact SELECT/batch、哈希查询次数、CPU、重复/唯一 payload 对照 |
| 5B | 分组预取 Event 身份、stream frontier、所需投影和 ordinal | SELECT/Event、批内校验耗时、事务时间 |
| 5C | 按表和列形状批量写来源及引用；当前状态行只写最终值 | execute 次数与实际写入行数、Event/事务、COMMIT 时间 |

实现要求：

- 保持全局 Event ID、每 stream sequence 和 store_order 的约束，批内相互冲突也必须检查。
- 按数据库参数上限分块，处理单条记录大于 batch 目标但仍在允许预算内的情况。
- 相同 Artifact 在本批只插入一次；引用不能丢失；不将唯一内容测试换成重复内容测试掩盖成本。
- 批内后续 Event 必须看到前面 Event 的状态推进。当前投影可能加载前面已写来源，应提供批内来源查找或保留依赖分段，不能简单把全部 INSERT 移到末尾。
- 当前状态投影可以合并到批末最终值；Runtime Event 和 `graph_changes` 的中间历史不能合并丢弃。
- 全批成功提交后才推进 committed cache/frontier、释放对象和完成 durable Future。
- 严格保留 buffered 入队时原有的冲突返回边界；不得未经设计把同步冲突变成后台 Writer fault。

### 正确性验证

覆盖混合 Runtime/User Event、重复 Event、batch 内冲突、跨 batch 引用、父子 Invocation、Fork 来源读取、Checkpoint anchor、事务中间异常与确认丢失后的重试。

### 如何看效果

每个子步骤运行六场景初筛和定向 SQL 诊断，再对最终组合做完整比较。buffered 和多 Session 可能更受益；单 Session durable batch 接近 1 时，不要求凭空减少事务数。

报告 SQL 交互次数和真实写入行数，证明收益不是漏写数据。相同数量的 Event 对应更少 execute 才是目标。临时 batch 数据增加引起的 RSS 回退必须一并报告。

## 11. 步骤 6：减少重复编码和对象遍历

### 修改范围与方案

主要文件：`storage.py`、`codec.py`、`writer.py`；必要时 `host/runtime.py` 和驻留计量边界。

1. 测清 `to_record`、Artifact 编码/哈希、JSON 文本生成、light_record 和 retained_objects 各自调用次数与时间。
2. 在 prepare 中生成最终需要的 payload/delta 文本，避免事务执行阶段在 Writer loop 再做同一份编码。
3. 复用仅在本 batch 有效的编码结果，严格限制缓存字节数和生命周期。
4. 在预算内控制原对象、编码结果和待写参数同时存在的峰值；批次结束和异常后释放引用。
5. 审查 Host 驻留计量与 Writer 队列计量的重复遍历，只有在口径和共享对象语义匹配时才复用计算。

不能用 JSON 字节数直接替换 Python 驻留内存，不删除背压计量，不以不受控并发编码提高吞吐。

### 正确性验证

覆盖嵌套值、共享对象、大文本、唯一内容、Artifact hash/恢复、队列容量拒绝、异常 traceback 保留和关闭释放。

### 如何看效果

- 对 text-overwrite、context-append、unique-history 比较编码 CPU、调用次数、整体 CPU 和峰值 RSS。
- 对小文本 chain 检查额外调度成本，避免大对象快、小请求更慢。
- 单独诊断 Writer loop 阻塞时间，正式速度测试关闭 profiler。
- 编码减少但临时副本导致峰值失控则不接受。

## 12. 步骤 7：用提交通知改善完成观察

### 修改范围与方案

主要文件：`host/observation.py` 及 Persistence 的提交通知接口。

1. 复用现有 `wait_committed()` / `wait_trace_commit()` 机制，避免固定 100 ms 空轮询。
2. 先获取通知 revision，再读取状态；未满足完成条件时等待该 revision 变化，避免丢失唤醒。
3. 每次唤醒重新检查 SQL 已提交状态，不能把 accepted 当作 durable 完成。
4. 处理 Writer fault/close、Runtime needs_recovery 等非提交状态变化，并为无法获得通知的读者保留有界兜底检查。
5. 控制全局提交通知引起的惊群；大量无关 Session 写入时，等待者查询率不能无限放大。

### 正确性验证

覆盖提交发生在状态查询前后、超时、取消观察、多个等待者、无关 Session 高频提交、数据库故障、Runtime 中断和关闭。零超时与 boundary/terminal 行为保持原语义。

### 如何看效果

- 单独记录观察 SQL/调用、终态提交至观察返回的延迟、等待者 CPU。
- 对短 chain 看尾部延迟；对并发 Session 看查询放大和 p95。
- 写入路径耗时若未变，必须将本步收益标为“观察延迟改善”，不能宣称 Runtime 执行本身更快。

## 13. 步骤 8：评估多 Session group commit

本步骤是测量驱动的可选优化，不默认修改 durable_batch_delay_ms。

已完成本轮定向实验，结果及限制见[步骤 8 报告](benchmarks/v2-optimization-step-08/README.md)。额外等待未达到保留条件，默认值维持 0 ms；并发观察查询问题单列，步骤 9 未开始。

### 修改与试验

1. 先记录独立 Session 数与实际 Event/事务，确认存在可合并的同时待提交 Event。
2. 在保持 durable 确认边界下，对比当前策略与有限的合并等待策略。
3. 同时测单 Session、8 个 Session 和更高并发，分别报告延迟、公平性、吞吐与队列内存。
4. 优先利用已就绪的工作合并，不能让单 Session 为不存在的后续依赖 Event 空等。

### 如何看效果

只有在并发吞吐可复现提高、单请求 p95 与背压没有不可接受退化时才保留。若只是降低 COMMIT 数但调用更慢，记录负面结果，不调整默认值。单 Session durable 的顺序确认是语义约束，不通过提前 ACK 绕过。

## 14. 步骤 9：完整验收与最终对比

### 功能验收

运行受影响模块和完整 V2 测试，实际启用 PostgreSQL。至少覆盖 Persistence、Host、Server、历史状态、Fork、Checkpoint、Agent recording 和执行观察。

按当前仓库约定使用根环境：

```bash
.venv/bin/python -m unittest tests.test_host_persistence tests.test_state_inspection tests.test_forks tests.test_execution_observation tests.test_server_comparison -v
.venv/bin/python -m tests.postgres
.venv/bin/python -m compileall -q autoagent tests
git diff --check
```

PostgreSQL 跳过项不能算通过。若完整套件因同一进程 RSS 累积造成 Host admission 干扰，定位原因后使用每模块独立进程运行完整清单并汇总；公开失败记录，不能只重跑通过的模块。

### 性能验收

1. B0 与最终版本重新交错跑六个 artifact 场景，两种数据库、两种模式。
2. 对默认 Artifact 阈值、条件历史、大图重复执行、并发 Session、慢库、故障和缓存淘汰做补充。
3. SQLite 和 PostgreSQL 分别运行至少一小时 durable/buffered 稳定负载；记录实际完成次数、CPU/调用和 RSS 后半段趋势。测试串行进行，不能因达到 duration 前调用数耗尽而声称跑满一小时。
4. 长时增加唯一内容和新 Session/Revision 变化，防止所有缓存都只命中同一小对象。此负载需在步骤 0 建立可复用的工具，不能冒充现有 soak 已覆盖。
5. 最终表同时列 V1 standard、V1 full、V2 B0、V2 优化后；V1 沿用旧数据必须标注“历史参考”，要宣称当前直接加速倍数则同轮重测。
6. 检查关闭后存储、记录数和历史状态，说明新增索引或缓存配置带来的成本。

交付每步报告、原始数据、固定源码 hash、最终对比表、失败及撤回的方案。没有效果的步骤也保留实验记录。

## 15. 可复用的基准命令

以下命令供正式实施时使用；本文编写阶段未运行。`step-01-before` 等目录名需要按当前步骤替换，保证每个 source/output 路径唯一。

### 15.1 保存实际 V2 源码

在修改该步骤代码之前执行一次，修改后以另一个目录保存 B。不要重复覆盖快照。

```bash
.venv/bin/python - <<'PY'
from pathlib import Path
import shutil

target = Path('.autoagent/performance/v2-optimization/sources/step-01-before')
target.mkdir(parents=True, exist_ok=False)
shutil.copytree('autoagent', target / 'autoagent',
                ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
PY
```

另外保存工作区 diff、依赖列表和测试工具 hash 到实验目录。不得把含数据库凭据的原始配置复制进公共报告。

### 15.2 六场景初筛

```bash
.venv/bin/python -m tests.benchmarks.server_comparison.run \
  --suite artifact --backend sqlite --repeat 3 \
  --profiles v2-artifacts-8k v2-buffered-artifacts-8k \
  --source-root .autoagent/performance/v2-optimization/sources/step-01-before \
  --output .autoagent/performance/v2-optimization/step-01/before/sqlite-screen

.venv/bin/python -m tests.postgres start

.venv/bin/python -m tests.benchmarks.server_comparison.run \
  --suite artifact --backend postgres --repeat 3 \
  --profiles v2-artifacts-8k v2-buffered-artifacts-8k \
  --source-root .autoagent/performance/v2-optimization/sources/step-01-before \
  --output .autoagent/performance/v2-optimization/step-01/before/postgres-screen
```

修改 source-root/output 后对 B 执行同样命令。正式交错比较时，按独立轮次执行 `--repeat 1`，每次使用新的输出目录，完成至少 5 轮；不要将全部 A 跑完很久以后才跑全部 B。

### 15.3 正式样本与诊断

```bash
.venv/bin/python -m tests.benchmarks.server_comparison.run \
  --suite artifact --backend sqlite --repeat 5 --calls 20 \
  --cases chain-10-16k loop-100-16k map-100-16k context-append text-overwrite \
  --profiles v2-artifacts-8k v2-buffered-artifacts-8k \
  --source-root .autoagent/performance/v2-optimization/sources/step-01-before \
  --output .autoagent/performance/v2-optimization/step-01/before/sqlite-measured

.venv/bin/python -m tests.benchmarks.server_comparison.run \
  --suite artifact --backend sqlite --repeat 5 \
  --cases unique-history \
  --profiles v2-artifacts-8k v2-buffered-artifacts-8k \
  --source-root .autoagent/performance/v2-optimization/sources/step-01-before \
  --output .autoagent/performance/v2-optimization/step-01/before/sqlite-unique

.venv/bin/python -m tests.benchmarks.server_comparison.run \
  --suite extended --backend sqlite --repeat 3 \
  --cases sql-chain-10 cpu-diagnostic \
  --profiles v2-artifacts-8k v2-buffered-artifacts-8k \
  --source-root .autoagent/performance/v2-optimization/sources/step-01-before \
  --output .autoagent/performance/v2-optimization/step-01/before/sqlite-diagnostic
```

这些是单版本命令模板；正式 A/B 仍按上一节拆轮交错，并替换 backend 运行 PostgreSQL。SQL 诊断负载不等于 artifact 负载，不能把二者当同一批原始数据；新增阶段指标需在步骤 0 明确实现。

### 15.4 长时与报告

```bash
.venv/bin/python -m tests.benchmarks.server_comparison.run \
  --suite soak --backend sqlite --duration 3600 --guard-mib 640 \
  --profiles v2-artifacts-8k v2-buffered-artifacts-8k \
  --source-root .autoagent/performance/v2-optimization/sources/final \
  --output .autoagent/performance/v2-optimization/final/sqlite-soak

.venv/bin/python -m tests.benchmarks.server_comparison.report \
  --runs .autoagent/performance/v2-optimization/step-01/before/sqlite-screen \
  --output .autoagent/performance/v2-optimization/step-01/reports/before-sqlite
```

当前报告聚合器不按源码 hash 区分同名配置。因此 A、B 必须分别生成报告，再构建阶段对比；禁止把同一 profile 的两个代码版本直接交给聚合器混成一个统计组。步骤 0 可以扩展实验版本维度，但必须验证不会混组。诊断与正常速度数据也要分开。

## 16. 每一步的结果报告模板

每步创建 `step-NN/report.md`，至少包含以下内容。原始数据放在忽略的 `.autoagent/performance/v2-optimization/` 中；需要长期共享时另导出不含凭据的报告和汇总。

```markdown
# Step NN：优化名称

状态：未实施 / 已证明收益 / 结构改善未证明加速 / 需要修正 / 已撤回

## 修改与假设

- 修改文件、源码 hash、基准工具 hash、依赖与有效配置。
- 预期减少哪类工作；本次只改变哪些变量。

## 正确性

- 测试命令、通过/失败/跳过数量、PostgreSQL 是否实际运行。
- 幂等、恢复、历史查询、引用和背压的证据。

## 增量效果 A → B

| 后端/模式/场景 | 指标 | A | B | 变化 | 样本与波动 | 判定 |
| --- | --- | ---: | ---: | ---: | --- | --- |
| SQLite/durable/chain | p50 ms | 待测 | 待测 | 待测 | 独立进程数 | 待定 |
| SQLite/durable/chain | CPU ms/call | 待测 | 待测 | 待测 | 同口径 | 待定 |
| SQLite/durable/chain | SELECT/Event | 待测 | 待测 | 待测 | 诊断单列 | 待定 |
| SQLite/durable/chain | peak RSS MiB | 待测 | 待测 | 待测 | 生命周期 | 待定 |

按其余后端、模式和场景展开；补充 p95、队列、存储和专项指标。

## 累计效果 B0 → B

列出同口径指标，不能只展示最佳场景。

## 机制证据与风险

- 预期消失的查询/遍历是否真的消失。
- 是否出现新的内存、锁等待、查询或写放大。
- 失败、超时、RSS guard、噪声和未覆盖边界。

## 决策

- 保留 / 修正复测 / 撤回；理由与下一步。
- 原始结果、SQL 诊断、日志、源码快照和命令位置。
```

## 17. 本轮暂不实施的方向

- 不通过 SQLite `synchronous=NORMAL/OFF` 或提前 ACK 改变 durable 保证。
- 不把默认模式改为 buffered，不删除 Event 或 StateOperation。
- 不无限缓存历史对象，不盲目启用可能持有大绑定参数的 SQLite statement cache。
- 不先重写 SQLAlchemy/驱动。若 SQL 数量下降后跨线程开销仍占主导，再独立评估整笔 SQLite 事务在专用同步 worker 中执行；两种后端分别测量。
- 不立即异步化所有投影。该方案需要独立的 projection watermark、读写一致性、历史读取和恢复设计。
- 不把大图编译的复杂度问题混入每 Event SQL 优化；注册/启动耗时单独测量。
- 不预先承诺提升几倍；后续决策以每步实验结果为依据。

## 18. 参考入口

- [原始 Server 对比报告](benchmarks/server-comparison-2026-10-22/README.md)
- [8 KiB 对比报告](../.autoagent/performance/artifact-8k-2026-10-23/report/README.md)
- [V1 standard 补充对比](../.autoagent/performance/artifact-8k-2026-10-23/report-standard/README.md)
- [基准工具说明](../tests/benchmarks/server_comparison/README.md)
- [Writer](../autoagent/host/persistence/writer.py)
- [SQL 事务与编码准备](../autoagent/host/persistence/storage.py)
- [投影维护](../autoagent/host/persistence/projections.py)
- [Schema 与索引](../autoagent/host/persistence/schema.py)
- [维护操作](../autoagent/host/persistence/maintenance.py)
- [完成观察](../autoagent/host/observation.py)
- [Core commit 确认边界](../autoagent/core/runtime/repository.py)

`.autoagent` 下的历史报告属于本地忽略产物，其他 checkout 不一定存在；缺失时不能伪称已复核，应使用本文历史数字作为参考并重新建立 B0。
