# V2 优化完成后的性能对比与数据库迁移

本报告仅整理已有测量数据；本次没有重新启动基准 Server、运行 benchmark 或运行测试套件。数据库迁移另做实际数据的完整性与可读性校验，不产生新的性能样本。

## 结论

- 从 B0（未进行执行/SQL 优化，但 Artifact 已设为 8 KiB）到当前版本，已有历史数据的端点比较：buffered 归一化耗时降低 **62.8%**，durable 降低 **30.3%**。这是跨轮次参考，不能等同于本轮同机交替 A/B 的因果收益。
- 最近一轮“控制与投影分离”相对查询优化后的版本：buffered 降低 **25.6%**，durable 降低 **1.3%**。durable 的改善很小，并非所有负载提速。
- 与历史 V1 full 相比，当前 V2 buffered 的大文本场景已经更快；Chain、Map、独立历史仍有明显差距。V1 standard 大多数场景仍更快。
- 运行期 RSS 整体保持在此前水平，Loop/大文本有下降，Context 有小幅增加。按需展示查询的 RSS 和延迟单独列出，不能把缓存预算当成整个进程内存上限。
- 移除五张明细投影表，业务输入输出仍保存在 Runtime Event/Artifact。PostgreSQL 存储下降明显；SQLite 大文本主要由事实数据占据，收益较小。
- 保持前端功能，但重建投影使首次读取变慢。100 次循环的 Trace 首次读取约 0.5–0.7 秒，缓存命中约 50–70 毫秒。
- 两个本地示例数据库已迁移至 format 6 / projection 5，保留原库备份。第 6 步离线数据库投影尚未实现。

## 比较范围与统计口径

| 版本 | 含义 | 已有数据来源 |
|---|---|---|
| B0 | 最初执行/SQL 优化前的冻结 V2；Artifact 已为 8 KiB | 步骤 9 中重新运行的 B0 |
| P0 | 原步骤 0–9 完成后的 V2 | 步骤 9 final |
| Q1 | 后续查询优化完成后的 V2 | query-optimization final，亦是最近一轮 before |
| Q2 | 当前控制与展示投影分离后的 V2 | projection-control after |
| V1 standard/full | 归档 V1 的历史 Server 测量 | 步骤 9 中同轮测量的 V1 |

正式持久化测试均启动原生 Server，通过 HTTP 提交、观察执行并等待持久化屏障；无外部 LLM。两种数据库、六种负载，每个配置五个独立进程，每进程预热后测量 20 次；独立历史为 128 次。表中 p50/p95 为五个进程对应分位数的中位数，不是合并所有请求后重算的分位数。
Artifact 阈值 8192 bytes；buffered 的 batch_delay_ms 为 100，durable_batch_delay_ms 为 0。旧报告中的“0 ms”指 durable 等待，不能理解成 buffered 等待也为 0。最早默认 Artifact 阈值变化的收益不再混入 B0→Q2。
P0 与查询优化 before、Q1 与投影优化 before 已逐文件核对冻结 Python 源码一致；当前产品源码与 Q2 冻结包也一致。最近一轮正式 before 复用了 Q1 的 120 组结果，并另有 24 组当前时段交替 A/B 复核。
B0/P0/V1 与 Q1/Q2 来自不同轮次。主要运行依赖版本相同；持久化 Worker 的变化加入无持久化分支，原持久化分支未改变。共享主机没有独占 CPU/cgroup，端点比较仍有时间漂移，不提供未测量的置信区间。
V1 standard 不保留同等完整恢复历史；V1 full 的异步记录与 flush 边界也不同于 V2 durable 逐 Event 提交后确认。下表展示实际产品路径差距，不宣称持久化语义完全等价。

## 累计和最近一轮的耗时变化

各负载等权，对 p50 的“后/前”比值取几何平均，再换算耗时下降比例；不直接相加各阶段百分比。

| 后端 | 模式 | B0→Q2 耗时下降（跨轮次参考） | Q1→Q2 耗时下降（最近一轮） | B0→Q2 Server CPU/调用下降 |
|---|---|---:|---:|---:|
| sqlite | buffered | 64.93% | 24.64% | 63.25% |
| sqlite | durable | 32.72% | 0.60% | 34.17% |
| postgres | buffered | 60.54% | 26.53% | 56.39% |
| postgres | durable | 27.81% | 2.03% | 30.23% |
| both | buffered | 62.80% | 25.59% | 59.97% |
| both | durable | 30.31% | 1.32% | 32.23% |

## buffered 完整速度表

单位 ms。V1 两列为历史参考；B0→Q2 的下降为端点直接计算。

| 后端 | 场景 | V1 standard | V1 full | B0 | P0 | Q1 | Q2 当前 | B0→Q2 下降 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| sqlite | 10 Node 串行链 | 44.9 | 65.7 | 435.4 | 245.4 | 144.4 | 149.2 | 65.7% |
| sqlite | 100 次循环 | 315.5 | 704.4 | 4993.4 | 2198.5 | 2087.6 | 1039.2 | 79.2% |
| sqlite | 100 单元 Map | 71.3 | 134.9 | 1432.7 | 955.1 | 656.5 | 474.7 | 66.9% |
| sqlite | Context 追加 | 137.2 | 221.7 | 1176.2 | 648.8 | 463.1 | 363.5 | 69.1% |
| sqlite | 大文本覆盖 | 534.0 | 1364.2 | 1699.2 | 1168.2 | 1119.3 | 811.1 | 52.3% |
| sqlite | 独立历史 | 31.6 | 41.5 | 220.1 | 134.9 | 135.7 | 117.5 | 46.6% |
| postgres | 10 Node 串行链 | 53.1 | 77.3 | 323.2 | 177.1 | 145.7 | 132.7 | 58.9% |
| postgres | 100 次循环 | 371.4 | 732.4 | 3515.1 | 1746.1 | 1341.7 | 885.0 | 74.8% |
| postgres | 100 单元 Map | 82.8 | 139.5 | 1082.6 | 758.3 | 587.9 | 441.1 | 59.3% |
| postgres | Context 追加 | 179.1 | 215.6 | 905.4 | 543.6 | 457.9 | 336.9 | 62.8% |
| postgres | 大文本覆盖 | 670.8 | 1342.2 | 1357.3 | 967.9 | 974.3 | 648.1 | 52.3% |
| postgres | 独立历史 | 28.5 | 37.8 | 188.2 | 131.2 | 133.1 | 94.9 | 49.6% |

## durable 完整速度表

| 后端 | 场景 | B0 ms | P0 ms | Q1 ms | Q2 当前 ms | B0→Q2 下降 | Q1→Q2 下降 |
|---|---|---:|---:|---:|---:|---:|---:|
| sqlite | 10 Node 串行链 | 1083.5 | 806.2 | 696.5 | 674.9 | 37.7% | 3.1% |
| sqlite | 100 次循环 | 10870.2 | 8424.5 | 7205.3 | 6801.3 | 37.4% | 5.6% |
| sqlite | 100 单元 Map | 3311.7 | 2859.5 | 2362.9 | 2380.0 | 28.1% | -0.7% |
| sqlite | Context 追加 | 2815.8 | 2192.3 | 1897.3 | 1835.8 | 34.8% | 3.2% |
| sqlite | 大文本覆盖 | 3232.1 | 2670.2 | 2468.3 | 2503.7 | 22.5% | -1.4% |
| sqlite | 独立历史 | 611.8 | 428.7 | 376.0 | 401.1 | 34.4% | -6.7% |
| postgres | 10 Node 串行链 | 817.7 | 627.1 | 528.3 | 519.3 | 36.5% | 1.7% |
| postgres | 100 次循环 | 7719.2 | 6360.5 | 5410.9 | 4949.1 | 35.9% | 8.5% |
| postgres | 100 单元 Map | 2517.8 | 2230.8 | 1910.3 | 1992.7 | 20.9% | -4.3% |
| postgres | Context 追加 | 2032.5 | 1627.4 | 1481.6 | 1481.7 | 27.1% | -0.0% |
| postgres | 大文本覆盖 | 2590.6 | 2078.4 | 2068.2 | 2070.0 | 20.1% | -0.1% |
| postgres | 独立历史 | 363.7 | 290.6 | 291.1 | 274.2 | 24.6% | 5.8% |

## 最近一轮 p95、CPU 和内存

CPU 为正式测量区间 Server 进程跨线程累计 CPU/完成调用数，排除独立 PostgreSQL 服务。p95 是墙钟分位数，不能与平均 CPU 相减推导精确 I/O 等待。RSS 为运行期每 20 ms 外部采样的进程峰值，单位 MiB。

| 后端 | 模式 | 场景 | p95 ms Q1→Q2 | CPU ms/调用 Q1→Q2 | 运行期峰值 RSS MiB Q1→Q2 |
|---|---|---|---:|---:|---:|
| sqlite | buffered | 10 Node 串行链 | 181.3→180.5 | 143.5→159.0 | 97.8→96.7 |
| sqlite | buffered | 100 次循环 | 2212.0→1130.4 | 2008.0→1138.5 | 118.9→105.7 |
| sqlite | buffered | 100 单元 Map | 683.8→521.3 | 654.0→503.0 | 102.4→100.9 |
| sqlite | buffered | Context 追加 | 493.4→409.2 | 484.0→375.5 | 137.2→138.3 |
| sqlite | buffered | 大文本覆盖 | 1186.7→856.2 | 1087.0→831.5 | 173.3→170.4 |
| sqlite | buffered | 独立历史 | 161.3→145.8 | 123.7→125.2 | 98.2→98.0 |
| sqlite | durable | 10 Node 串行链 | 730.6→710.7 | 594.0→562.5 | 93.5→89.4 |
| sqlite | durable | 100 次循环 | 10661.6→7548.8 | 5984.5→5658.5 | 100.9→96.0 |
| sqlite | durable | 100 单元 Map | 2429.9→2685.8 | 2030.5→1961.5 | 97.9→93.2 |
| sqlite | durable | Context 追加 | 1949.3→1970.5 | 1604.5→1563.0 | 123.4→124.5 |
| sqlite | durable | 大文本覆盖 | 2586.4→2753.6 | 2126.0→2166.0 | 133.0→132.9 |
| sqlite | durable | 独立历史 | 423.8→454.3 | 331.2→351.3 | 97.2→93.2 |
| postgres | buffered | 10 Node 串行链 | 185.8→150.9 | 120.5→131.0 | 94.8→94.9 |
| postgres | buffered | 100 次循环 | 1423.3→934.8 | 1351.5→904.0 | 109.1→101.7 |
| postgres | buffered | 100 单元 Map | 649.6→485.1 | 567.0→446.0 | 101.5→98.6 |
| postgres | buffered | Context 追加 | 505.3→376.4 | 429.5→328.5 | 129.6→131.3 |
| postgres | buffered | 大文本覆盖 | 1090.9→687.9 | 906.0→659.5 | 157.4→142.6 |
| postgres | buffered | 独立历史 | 140.3→119.1 | 93.9→98.4 | 95.8→95.7 |
| postgres | durable | 10 Node 串行链 | 569.6→568.7 | 437.5→414.5 | 95.1→91.7 |
| postgres | durable | 100 次循环 | 5562.4→5704.0 | 4348.5→3834.0 | 105.7→99.2 |
| postgres | durable | 100 单元 Map | 2157.5→2187.1 | 1561.5→1461.5 | 98.9→95.0 |
| postgres | durable | Context 追加 | 1511.3→1561.6 | 1209.5→1180.5 | 124.1→126.1 |
| postgres | durable | 大文本覆盖 | 2210.2→2528.7 | 1726.5→1710.5 | 127.3→126.6 |
| postgres | durable | 独立历史 | 325.3→331.8 | 249.2→238.8 | 99.7→95.7 |

SQLite buffered Chain 本轮正式 p50 144.4→149.2 ms（慢 3.3%），当前时段交替复核为 148.3→158.0 ms（慢 6.5%），不是所有短链路提速。PostgreSQL buffered Chain 的 p50 下降，但 CPU 120.5→131.0 ms/调用，不能将墙钟改善等同于 CPU 全面下降。

## B0 到当前的运行内存与存储

存储为关闭后物理占用，单位 MiB。SQLite 包括当时仍存在的 DB/WAL/SHM；PostgreSQL 包括数据库系统开销、索引和 TOAST，不含共享 WAL 的归因。因此这是本组固定调用量的实测占用，不是每条记录大小，也不能直接横向比较两种数据库。

| 后端 | 模式 | 场景 | RSS MiB B0→Q2 | 存储 MiB B0→Q2 | 存储 MiB Q1→Q2 | 本轮存储下降 |
|---|---|---|---:|---:|---:|---:|
| sqlite | buffered | 10 Node 串行链 | 97.2→96.7 | 12.75→11.20 | 12.25→11.20 | 8.6% |
| sqlite | buffered | 100 次循环 | 114.9→105.7 | 98.23→76.39 | 98.17→76.39 | 22.2% |
| sqlite | buffered | 100 单元 Map | 103.9→100.9 | 25.17→22.81 | 24.77→22.81 | 7.9% |
| sqlite | buffered | Context 追加 | 134.4→138.3 | 53.09→48.37 | 52.40→48.37 | 7.7% |
| sqlite | buffered | 大文本覆盖 | 174.1→170.4 | 459.76→457.13 | 459.87→457.13 | 0.6% |
| sqlite | buffered | 独立历史 | 96.6→98.0 | 35.93→31.26 | 36.02→31.26 | 13.2% |
| sqlite | durable | 10 Node 串行链 | 93.9→89.4 | 12.25→10.65 | 12.28→10.65 | 13.3% |
| sqlite | durable | 100 次循环 | 98.7→96.0 | 93.71→75.18 | 94.08→75.18 | 20.1% |
| sqlite | durable | 100 单元 Map | 94.1→93.2 | 24.85→22.33 | 24.12→22.33 | 7.4% |
| sqlite | durable | Context 追加 | 122.8→124.5 | 51.29→46.78 | 51.35→46.78 | 8.9% |
| sqlite | durable | 大文本覆盖 | 134.4→132.9 | 458.75→454.60 | 458.85→454.60 | 0.9% |
| sqlite | durable | 独立历史 | 94.6→93.2 | 35.50→31.04 | 35.07→31.04 | 11.5% |
| postgres | buffered | 10 Node 串行链 | 97.4→94.9 | 14.87→12.15 | 14.73→12.15 | 17.5% |
| postgres | buffered | 100 次循环 | 110.0→101.7 | 67.71→38.37 | 66.49→38.37 | 42.3% |
| postgres | buffered | 100 单元 Map | 98.9→98.6 | 23.26→19.15 | 23.25→19.15 | 17.6% |
| postgres | buffered | Context 追加 | 131.3→131.3 | 29.66→22.64 | 29.54→22.64 | 23.4% |
| postgres | buffered | 大文本覆盖 | 163.0→142.6 | 29.29→22.35 | 29.25→22.35 | 23.6% |
| postgres | buffered | 独立历史 | 94.5→95.7 | 25.71→18.44 | 25.28→18.44 | 27.1% |
| postgres | durable | 10 Node 串行链 | 97.0→91.7 | 14.78→12.24 | 14.81→12.24 | 17.4% |
| postgres | durable | 100 次循环 | 101.2→99.2 | 66.67→38.23 | 67.56→38.23 | 43.4% |
| postgres | durable | 100 单元 Map | 95.8→95.0 | 23.47→19.04 | 23.28→19.04 | 18.2% |
| postgres | durable | Context 追加 | 126.0→126.1 | 29.71→22.85 | 29.54→22.85 | 22.6% |
| postgres | durable | 大文本覆盖 | 130.9→126.6 | 29.39→22.52 | 29.40→22.52 | 23.4% |
| postgres | durable | 独立历史 | 95.5→95.7 | 25.75→18.76 | 25.72→18.76 | 27.1% |

PostgreSQL buffered Loop：本轮存储 66.49→38.37 MiB，下降 42.3%；SQLite 对应 98.17→76.39 MiB，下降 22.2%。SQLite buffered 大文本约 459.87→457.13 MiB，仅下降 0.6%，说明该负载事实数据占比高，删除展示投影无法解决全部存储问题。
Q2 的元数据投影缓存默认总预算 16 MiB、单项 4 MiB、TTL 60 秒，构建预算 128 MiB；缓存不留输入输出正文。这些是缓存/构建限制，不是整个 Server 的 RSS 上限。新版本没有再做一小时长测，不能用旧版长测替代当前的长期内存验收。

## SQL 与写入量

以下为独立诊断进程的五次 Chain 调用，SQL 计数取预热后的增量；明细行数为诊断库最终累计值，包含预热。它不是 SQL 更新次数。

| 后端 | 模式 | 版本 | SELECT | SQL 总数 | 写批次 | Runtime Event 增量 | 五表累计明细行 |
|---|---|---|---:|---:|---:|---:|---:|
| sqlite | buffered | Q1 | 403 | 535 | 10 | 310 | 714 |
| sqlite | buffered | Q2 | 260 | 360 | 10 | 310 | 0 |
| sqlite | durable | Q1 | 883 | 3458 | 310 | 310 | 714 |
| sqlite | durable | Q2 | 465 | 2465 | 310 | 310 | 0 |
| postgres | buffered | Q1 | 216 | 284 | 8 | 310 | 714 |
| postgres | buffered | Q2 | 305 | 342 | 6 | 310 | 0 |
| postgres | durable | Q1 | 799 | 3004 | 310 | 310 | 714 |
| postgres | durable | Q2 | 461 | 2126 | 310 | 310 | 0 |

- Event 数量保持 310；没有通过删除恢复事实或降低持久化确认语义换取速度。
- `node_executions`、`operator_calls`、`waits`、`edge_evaluations`、`graph_changes` 不再落库。保留 `sessions`、`invocations`，并增加仅记录活跃工作的 `active_nodes`、`active_waits`；工作结束即删除活跃行。
- durable 的 SQL 总数下降，但仍是 310 个写批次对应 310 条 Event，正式耗时收益仅 1.3%。Server CPU 仍然显著，本次数据不能证明剩余耗时全由 fsync 造成。
- 查询并非全面下降：PostgreSQL buffered Chain 的 SELECT 216→305，SQL 总数 284→342。Q2 中有 60 次写事务 `active_nodes` 查询和 61 次观察路径查询。消除活跃索引的多余存在性读取仍是明确的后续优化点。
- 诊断中的 `projection_write` 现在只表示最小控制索引写入；嵌套阶段耗时不能相加，driver COMMIT 也包含只读事务，不等于磁盘 fsync 次数。

## 展示查询的代价

| 后端 | 模式 | 场景 | 版本 | 首次 Trace ms | 缓存命中 Trace ms | 进程生命周期峰值 RSS MiB |
|---|---|---|---|---:|---:|---:|
| sqlite | buffered | trace-chain | Q1 | 9.6 | 8.3 | 93.4 |
| sqlite | buffered | trace-loop | Q1 | 10.1 | 9.2 | 109.7 |
| sqlite | buffered | trace-chain | Q2 | 87.4 | 22.4 | 95.9 |
| sqlite | buffered | trace-loop | Q2 | 710.9 | 71.6 | 110.6 |
| sqlite | durable | trace-chain | Q1 | 9.4 | 8.1 | 92.2 |
| sqlite | durable | trace-loop | Q1 | 11.3 | 10.0 | 98.7 |
| sqlite | durable | trace-chain | Q2 | 90.3 | 22.5 | 94.2 |
| sqlite | durable | trace-loop | Q2 | 720.2 | 71.6 | 104.6 |
| postgres | buffered | trace-chain | Q1 | 7.6 | 7.3 | 92.6 |
| postgres | buffered | trace-loop | Q1 | 8.3 | 8.9 | 100.2 |
| postgres | buffered | trace-chain | Q2 | 65.8 | 21.4 | 94.3 |
| postgres | buffered | trace-loop | Q2 | 500.5 | 48.4 | 104.9 |
| postgres | durable | trace-chain | Q1 | 7.4 | 7.1 | 92.9 |
| postgres | durable | trace-loop | Q1 | 8.6 | 8.3 | 97.9 |
| postgres | durable | trace-chain | Q2 | 69.0 | 20.8 | 94.1 |
| postgres | durable | trace-loop | Q2 | 502.4 | 63.0 | 102.6 |

每个 Invocation 先冷读一次，再连续读两次；各次请求分开统计。UI 功能保留不代表查询耗时保持原值。缓存淘汰只影响重建延迟，执行控制、恢复和输入输出的可用性不依赖缓存。大 Root 超过构建预算时会返回容量错误；单项超出保留预算时不会常驻。
第 6 步离线数据库投影可以缓解冷查询，但当前没有实现，也没有把其预期收益计入结果。

## 无持久化的已有参考

这是之前对冻结 P0 的测量，不是本次 Q2 的新结果。两版 Core 使用同一 HTTP 包装；V2 为 runtime_event_sink=None，而不是原生 Host（Host 需要数据库）。每组五进程、每进程 20 次，独立历史与持久化表的 128 次条件不同。

| 场景 | V1 standard p50 ms | V1 full p50 ms | V2 P0 p50 ms | V1 standard 峰值 RSS MiB | V1 full 峰值 RSS MiB | V2 P0 峰值 RSS MiB |
|---|---:|---:|---:|---:|---:|---:|
| 10 Node 串行链 | 10.75 | 17.11 | 14.08 | 76.7 | 82.4 | 75.1 |
| 100 次循环 | 127.29 | 299.14 | 153.20 | 145.7 | 207.5 | 79.6 |
| 100 单元 Map | 23.40 | 43.37 | 32.91 | 79.2 | 124.7 | 76.9 |
| Context 追加 | 34.37 | 57.43 | 42.67 | 105.3 | 119.7 | 102.0 |
| 大文本覆盖 | 143.31 | 165.79 | 84.95 | 500.9 | 515.3 | 100.2 |
| 独立历史 | 6.19 | 9.44 | 8.04 | 72.5 | 75.1 | 74.5 |

该轮无持久化 V2 在六场景均快于 V1 full；相对 standard，五场景慢约 20%–41%，大文本更快。它支持“主要数倍差距出现在 Host/持久化/观察路径”的判断，但不能将不同 HTTP/Host 路径的墙钟直接相减来分配各层成本。

## 已有正确性证据及限制

- 最近一轮 336 组记录，其中复用 120 组相同源码的 Q1 正式对照，实际新增 216 组；328 组完成业务、8 组为预期持久化故障，审计差异 0。
- 已有回归为逐模块执行的 105 个模块、1798 项测试，其中 PostgreSQL 336 项，无跳过。仅原有天气 CLI 两个子项失败，原因是引用已删除的 examples.deepseek_weather_assistant.__main__；不能称完整套件全绿。
- 同一批固定 Event 比较 Graph、Timeline、Replay、Dataset 和 Node/Call 详情，结果差异 0；浏览器验证了实时图、等待响应、Output 按需读取、Replay 和 Child 跳转。前端已有 28 项测试及构建通过。
- 覆盖投影故障不影响控制/恢复、缓存淘汰、历史快照隔离、并发提交、独立查询池、不可变 Dataset 分页、无 GC 源释放等边界。没有为本报告重跑这些测试。
- 旧的整进程全套测试曾出现累计 RSS 引发的准入失败；最终采用与基线一致的逐模块验证。保留原失败日志，不将它们描述为通过。

## 本次数据库迁移

迁移对象为实际找到的两个本地示例数据库；性能基准历史库、V1 库没有修改。根 .env 指向的 /tmp/autoagent-local-ui-demo.db 当前不存在，没有创建空库冒充迁移。没有发现需要迁移的已配置 PostgreSQL 业务库。

| 示例 | 原格式 | 目标格式 | Session | Invocation | Runtime Event | Checkpoint | 活跃 Wait | LLM 调用记录 |
|---|---|---|---:|---:|---:|---:|---:|---:|
| deepseek_weather_assistant | 5/4 | 6/5 | 3 | 4 | 85 | 3 | 0 | 9 |
| local_ui_demo | 2/2 | 6/5 | 14 | 18 | 455 | 8 | 1 | 0 |

迁移方法：获取原库 Writer 锁，使用 SQLite Backup API 保存完整备份；在独立副本创建当前 schema，复制事实和 LLM 记录，重建控制目录与活跃索引。校验成功后原路径原子替换，未在应用启动时增加兼容层或自动迁移。

天气库的 85 条 Event、3 个 Checkpoint 和 9 条 LLM 记录保持原内容，只升级一个旧 Workflow Snapshot 的外层 schema 标记。
本地 UI 库原有 Core 模型也较旧：455 条 Event 升级为当前 schema 17，使用已有调用/激活历史补充重试计数、Node 执行计数与原始输入；7 个 Workflow Snapshot 升级外层 schema；8 个 Checkpoint 按相同 Session/sequence 的历史状态升级并重算摘要和身份。Checkpoint ID 映射保存在迁移收据中，旧 Checkpoint 链接应使用映射后的 ID。Event、Session、Invocation、Workflow Revision、Operator 和 User Event ID 保持不变。

迁移后核对：两个 SQLite integrity_check 均为 ok、外键错误 0；所有 Event 的顺序、身份及原有 payload/delta 业务字段保留；Session/Invocation 的既有目录字段与等待状态一致；所有现存 Checkpoint 和 Workflow Snapshot 可由当前模型解码。逐 Event 校验了实际历史 State，并检查所有 Invocation 的 Trace、Timeline、Replay 和 Node/Call 详情可读。没有启动、响应或重跑业务 Invocation。

另外直接对照原库备份，核对了天气库 14 个 Node、12 个 Call，以及本地 UI 库 92 个 Node、79 个 Call、6 个 Wait 的全部身份；合计 300 份已有 Input/Output/Request/Response 值逐项相等。重建元数据后，原有业务值并未丢失。

原库备份及机器可读收据：

- `.autoagent/migrations/2026-10-25-format6/weather.before.sqlite`
- `.autoagent/migrations/2026-10-25-format6/local-ui.before.sqlite`
- `.autoagent/migrations/2026-10-25-format6/weather.json`
- `.autoagent/migrations/2026-10-25-format6/local-ui.json`
- `.autoagent/migrations/2026-10-25-format6/migrate.py` 为本次离线工具；不是持续维护的多版本迁移框架。

## 原始证据位置

- [步骤 9 对比](../v2-optimization-step-09/README.md)：B0、P0 与 V1，完整逐轮摘要在同目录 summary.json。
- `.autoagent/performance/query-optimization-2026-10-25/{report.md,summary.json,provenance.json}`：查询优化。
- `.autoagent/performance/projection-control-2026-10-25/{report.md,summary.json,results.jsonl,provenance.json,final-evidence.json}`：投影分离。
- `.autoagent/performance/no-persistence-2026-10-25/{report.md,summary.json}`：无持久化参考。
- [本报告机器可读数据](summary.json)：整合上述已测数据，不含密钥、原始业务正文或数据库连接密码。
