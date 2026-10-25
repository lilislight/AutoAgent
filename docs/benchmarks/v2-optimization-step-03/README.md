# V2 优化步骤 3：精简查询与有界图元数据缓存

## 结论

判定：**结构改善，暂未证明整体加速；保留本步**。精简列读取、按 source Node 索引的有界 Revision 缓存和条件查询裁剪均已验证，保持完整记录与原有确认语义。

Chain buffered 正式复核的进程 p50 中位数，SQLite 降低 14.2%、PostgreSQL 降低 9.4%，CPU 分别降低约 11.6%/11.5%；SQLite 延迟不确定区间跨零，不能保证稳定的 14.2% 收益。128 Node 大图初筛耗时降低约 28.5%/49.8%，RSS 降低约 9.9/25.9 MiB，但每配置仅 2 进程 × 3 调用，属于探索性证据。

标准 durable 多数场景接近持平；PostgreSQL Loop 的进程中位数比值与配对结果方向相反，延迟尚未能定论。索引在 10 万行历史的 SQL 查询中显著受益，同时增加写入与空间成本，不能当作所有调用的净加速。

## 修改范围

- Admission 的存在性、sequence、Operator 绑定读取仅查询必要列；重复身份仍恢复完整来源记录并比较内容。
- Writer 按不可变 Revision 缓存脱离定义正文的出边元数据，以 source Node 建索引；保留相反状态、未选中与 skipped 出边。条件结果缺失时不编造选择。
- 仅为尚缺少已计算结果的匹配条件出边查询 `edge_evaluations`；本事务 overlay 已有结果直接使用。查询只读取 Edge ID 和条件值。
- 默认 `AUTOAGENT_WORKFLOW_GRAPH_CACHE_BYTES=4194304`。LRU 按保留对象大小加每项开销计费，过大的图不缓存；维护、关闭、失败/取消的写事务清空缓存。计费是保守估算，不代表进程 RSS 精确上限，也不限制读取与构建图时的瞬时分配。
- 只在 Writer 共享缓存。历史查询、accepted overlay 与 rebuild 使用各自连接的 SQL 快照，避免跨连接可见性错误。
- 新增非唯一索引 `edge_source_lookup(invocation_id, source_occurrence_id, source_status)`。Writer 打开现有有效数据库时补建缺失索引；只读打开不执行 DDL。格式版本不变，没有重复增加 `node_ordinal` 索引。

## 对照与验证

冻结阶段完整回归：99 个模块、1701 项测试通过，其中 PostgreSQL 306 项，无跳过。当前工作区另有 4 个 Provider 文件更新，均保留，未混入 A/B 性能源码；补跑当前 Provider、Backend、Embedding、Capability、Host Agent 共 96 项测试通过（PostgreSQL 6 项），补测期间源码没有再次变化。原冻结阶段测试包含成功/错误路由、条件真/假/缺失、skipped、多 Revision、缓存淘汰、回滚失效、定义删除/重建/关闭，以及只读打开不建索引。

共 293 次独立真实 Server 运行、6801 次正式 HTTP 调用。逐次输出一致，五张事实/投影表行数配对一致；所有调用后 queued_count/retained_bytes 为零，accepted/durable frontier 相等。预热另计；不把这些预热后的结果当作冷缓存首次调用分布。追加诊断脚本有 1 次导入路径错误，发生在 Server/数据库创建前，0 次测量调用；修复后在新目录重跑，原空目录保留，不计成功性能样本。

A 是本步开始时的当前 V2，B 为本步实现后的冻结完整包。两者共用冻结测试工具；每轮反转 A/B 次序，均用新数据库，Artifact 阈值 8192 bytes，内存监控关闭。正常测速关闭 SQL 诊断，诊断组单独启用。PostgreSQL 14.24，fsync/synchronous_commit/full_page_writes 均开启；详细配置见 provenance.json。新增索引另用“B 去除索引”与 B 比较，差异仅 schema.py/storage.py 两处。

工作区包含其他任务的未提交改动，均保留。A 已包含步骤 2 测量之后的其他改动，所以本报告只能比较本步 A/B，不能直接与历史绝对值相减得出累计收益。包、工具、脚本 hash 与当前工作区差异见 provenance.json。重型任务串行，但没有私有 CPU/cgroup 隔离，主机负载仍可能影响结果。Loop 正式复核期间采到 I/O full 压力约 20%，后续采样回落到约 3.6%；这些是共享主机线索，不能据此确定每个进程差异的原因。一次进程列表读取因自动审批服务额度不足而未执行，未收集该项证据。

下表均取独立进程指标中位数。正的耗时/CPU 降低率表示改善。CPU 为测量区间整个 Server CPU 时间 / 调用数，非逐请求 CPU p50。RSS 为外部 20 ms 采样的整个 Server 峰值，不含共享 PostgreSQL 服务。存储为关闭后的数据库物理占用。

## 标准六场景初筛

每配置 3 个独立进程，每进程正式调用数 5/3/5/3/3/128。小样本初筛不能宣称生产 p95 或稳定的小幅提升。

| 后端 | 模式 | 场景 | A/B p50 ms | 耗时降低 | CPU 降低 | RSS 差 MiB | 存储差 MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| postgres | durable | chain-10-16k | 688.6/700.3 | -1.7% | -0.7% | -3.5 | +0.00 |
| postgres | durable | context-append | 1810.4/1819.7 | -0.5% | -5.1% | -1.3 | +0.09 |
| postgres | durable | loop-100-16k | 6520.6/6649.9 | -2.0% | -5.6% | -1.3 | -0.04 |
| postgres | durable | map-100-16k | 2354.5/2350.6 | +0.2% | -2.4% | -0.0 | -0.15 |
| postgres | durable | text-overwrite | 2408.0/2408.6 | -0.0% | -0.5% | -1.0 | +0.04 |
| postgres | durable | unique-history | 365.1/364.5 | +0.2% | +3.3% | -0.4 | -0.05 |
| postgres | buffered | chain-10-16k | 280.4/249.2 | +11.1% | +20.3% | -4.2 | -0.02 |
| postgres | buffered | context-append | 819.1/809.6 | +1.2% | +3.9% | -1.0 | -0.05 |
| postgres | buffered | loop-100-16k | 2751.3/2812.1 | -2.2% | -0.4% | -1.6 | +0.77 |
| postgres | buffered | map-100-16k | 1048.4/1026.6 | +2.1% | +1.7% | -0.1 | +0.06 |
| postgres | buffered | text-overwrite | 1247.2/1252.7 | -0.4% | +0.3% | +0.4 | -0.02 |
| postgres | buffered | unique-history | 180.9/171.5 | +5.2% | +5.1% | -0.3 | +0.13 |
| sqlite | durable | chain-10-16k | 936.8/936.3 | +0.0% | -0.8% | -0.5 | -0.04 |
| sqlite | durable | context-append | 2466.6/2486.8 | -0.8% | -2.7% | +0.7 | -0.05 |
| sqlite | durable | loop-100-16k | 9096.5/8875.6 | +2.4% | +1.0% | +1.0 | -0.08 |
| sqlite | durable | map-100-16k | 3015.2/2889.2 | +4.2% | +2.8% | +0.3 | -0.03 |
| sqlite | durable | text-overwrite | 2946.4/2816.1 | +4.4% | -1.1% | +0.3 | +0.06 |
| sqlite | durable | unique-history | 499.3/497.1 | +0.4% | +3.7% | -0.5 | +0.30 |
| sqlite | buffered | chain-10-16k | 370.1/333.4 | +9.9% | +10.4% | -2.5 | +0.04 |
| sqlite | buffered | context-append | 1105.9/985.3 | +10.9% | +5.5% | -1.6 | +1.67 |
| sqlite | buffered | loop-100-16k | 4148.4/3750.6 | +9.6% | +8.2% | -0.1 | +0.25 |
| sqlite | buffered | map-100-16k | 1337.4/1288.4 | +3.7% | +3.7% | -0.0 | -0.37 |
| sqlite | buffered | text-overwrite | 1544.9/1475.4 | +4.5% | +5.7% | +4.4 | -1.41 |
| sqlite | buffered | unique-history | 220.9/208.9 | +5.4% | +5.7% | -1.0 | -0.08 |

## Chain 正式测试与 Map/Loop/Context 回退复核

Chain 每个后端/模式、以及 SQLite buffered Map、PostgreSQL durable Loop/Context，分别 5 个独立进程 × 20 次正式调用。Map 因首轮初筛回退约 19.6% 追加，其三轮初筛中位数实际已改善 3.7%；Loop 三轮初筛耗时增加约 2.0%、CPU 增加 5.6%，因此追加正式复核和 before/after/without-index SQL 诊断。Context 初筛 CPU 增加 5.1%，延迟近乎持平，同样追加正式复核。原始样本全部保留。保留全部配对值；bootstrap 仅基于 5 个进程对，属于探索性不确定区间，不是生产保证。

| 后端/场景 | 模式 | A/B p50 ms | 耗时降低 | CPU 降低 | A/B p95 ms | 每轮耗时降低 | 配对降低中位数 | 配对 bootstrap 95% |
| --- | --- | ---: | ---: | ---: | ---: | --- | ---: | --- |
| postgres/context-formal | durable | 1594.2/1572.6 | +1.4% | +2.5% | 1680.6/1676.7 | +4.5%, +4.4%, -0.1%, +2.3%, +1.1% | +2.3% | [-0.1%, +4.5%] |
| postgres/chain-formal | durable | 794.6/694.4 | +12.6% | +5.2% | 824.4/820.9 | +0.1%, +7.2%, +13.3%, +6.3%, +14.2% | +7.2% | [+0.1%, +14.2%] |
| postgres/chain-formal | buffered | 293.9/266.3 | +9.4% | +11.5% | 350.9/335.3 | +13.0%, +12.4%, +4.8%, +2.8%, +8.2% | +8.2% | [+2.8%, +13.0%] |
| sqlite/chain-formal | durable | 951.2/944.0 | +0.8% | +3.4% | 1104.4/1075.0 | +8.3%, +1.0%, -4.4%, +0.9%, +0.9% | +0.9% | [-4.4%, +8.3%] |
| sqlite/chain-formal | buffered | 406.2/348.7 | +14.2% | +11.6% | 463.6/434.6 | +33.6%, +20.2%, +33.1%, +8.9%, -2.6% | +20.2% | [-2.6%, +33.6%] |
| postgres/loop-formal | durable | 6903.7/8533.2 | -23.6% | -1.4% | 11112.6/9458.3 | -26.3%, +7.1%, +11.9%, +0.5%, +15.8% | +7.1% | [-26.3%, +15.8%] |
| sqlite/map-formal | buffered | 1314.3/1292.0 | +1.7% | +2.1% | 1366.5/1356.8 | +1.9%, -0.7%, -2.7%, +1.7%, +1.6% | +1.6% | [-2.7%, +1.9%] |

Loop 正式复核：A 进程 p50 约 6.8–9.7 秒，B 约 5.8–9.0 秒。进程中位数比值显示耗时增加 23.6%，但五对配对降低率中位数为 +7.1%，区间跨零；CPU 的进程中位数增加 1.4%，四对配对值却下降。全部数值保留，不能选择有利的统计量宣称改善，也不能据当前样本确认持续回退。此处仍有未决延迟风险，不能宣称已排除回退。Map 与 Context 正式复核没有复现初筛的大幅 CPU/延迟回退。bootstrap 对进程对重采样，区间对应进程 p50 中位数比值。

## SQL 路径诊断

单独启用诊断钩子，数据为正式调用区间的计数中位数，不与无钩子延迟混用。Graph/条件 SELECT 仅统计 projection_compute；其他观察查询仍按其 SQL 快照读取。

| 后端 | 模式 | 场景 | 总 SQL A/B | 图正文 SELECT A/B | 条件 SELECT A/B | 投影计算 ms A/B |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| postgres | durable | conditional-history | 27355/25287 | 288/1 | 288/16 | 2653.8/1566.6 |
| postgres | durable | diagnostics-on | 11803/11159 | 120/0 | 120/0 | 925.2/601.8 |
| postgres | buffered | conditional-history | 14469/12030 | 288/0 | 288/0 | 3546.9/1772.2 |
| postgres | buffered | diagnostics-on | 5584/4801 | 120/0 | 120/0 | 685.5/377.0 |
| sqlite | durable | conditional-history | 30992/28854 | 288/0 | 288/16 | 3617.3/2055.6 |
| sqlite | durable | diagnostics-on | 13364/12673 | 120/0 | 120/0 | 1282.4/853.2 |
| sqlite | buffered | conditional-history | 16603/14102 | 288/0 | 288/0 | 5903.9/3116.2 |
| sqlite | buffered | diagnostics-on | 6457/5862 | 120/0 | 120/0 | 1182.6/819.0 |

## Loop 三版本 SQL 诊断

PostgreSQL durable，1 进程 × 3 次测量调用，开启 SQL 钩子，仅用于机制诊断。without-index 为相同优化代码去除候选索引。不能将这些延迟混入无钩子正式复核。

| 版本 | p50 ms | CPU ms/call | SQL | Runtime Event/批次 | 投影计算累计 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| after | 7029.3 | 5653.3 | 33800 | 1836/1836 | 1968.6 |
| before | 7162.0 | 5786.7 | 35090 | 1836/1836 | 2451.5 |
| without-index | 6855.8 | 5610.0 | 33817 | 1836/1836 | 1965.1 |

## 大图与缓存压力

64 个 Revision、128 次轮换调用及 16 KiB 缓存预算场景分别测量；压力组仅 B，用于验证容量与淘汰边界，不伪造没有运行的 A 对照。压力表的缓存数值为最后一个进程结束时的实值。Revision 轮换组每版本 1 个进程，仅作机制诊断，不宣称稳定加速。128 Node 大图为 2 进程 × 3 次调用，属于探索性结果。没有执行长时生产浸泡，不能据此证明 RSS 永不增长。

| 组 | 后端 | 版本 | p50 ms | CPU ms/call | RSS MiB | 缓存 entries/bytes/budget | hits/misses/evictions |
| --- | --- | --- | ---: | ---: | ---: | --- | --- |
| churn | postgres | after | 167.9 | 135.2 | 115.1 | 1/2191/4194304 | 709/65/0 |
| churn | postgres | before | 177.7 | 143.5 | 115.0 | —/—/— | —/—/— |
| churn | sqlite | after | 187.6 | 184.5 | 116.2 | 1/2191/4194304 | 709/65/0 |
| churn | sqlite | before | 200.4 | 196.4 | 116.5 | —/—/— | —/—/— |
| graph | postgres | after | 2452.2 | 2616.7 | 140.4 | —/—/— | —/—/— |
| graph | postgres | before | 4886.7 | 4823.3 | 166.3 | —/—/— | —/—/— |
| graph | sqlite | after | 4254.4 | 4451.7 | 162.8 | —/—/— | —/—/— |
| graph | sqlite | before | 5951.5 | 6126.7 | 172.7 | —/—/— | —/—/— |
| pressure | postgres | after | 169.6 | 136.6 | 115.1 | 1/2191/16384 | 645/129/121 |
| pressure | sqlite | after | 187.6 | 183.3 | 116.9 | 1/2191/16384 | 645/129/121 |

轮换与压力组均有 129 个 Session（含预热），触发 1 个 Checkpoint；当前 Maintenance.run 连 Checkpoint 配置操作也保守清空缓存。因此默认预算组结束仅剩 1 项（2191 bytes），hits/misses 为 709/65；不能将此结束值当作运行峰值。16 KiB 压力组结束同样为 1 项，hits/misses/evictions 为 645/129/121。当前失效策略会降低频繁 Checkpoint 时的跨 Invocation 命中率，后续可单独细化；本步保留完整测量，不额外混入该改动。

## 索引单独评估

10 万条具有有效外键的合成条件历史，3 轮新数据库 A/B。该夹具用于查询计划与 INSERT 成本，不是可恢复的完整 Runtime 历史。查询形状与投影一致，ANALYZE 后执行 EXPLAIN；每类查询先预热 5 次、正式 100 次。写入耗时只计 edge_evaluations 的 10 万行 INSERT，不代表完整 Workflow 写入。

| 后端 | 命中 p50 无/有索引 ms | 缺失 p50 无/有索引 ms | INSERT 无/有索引 ms | INSERT 增加 | 索引 MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| sqlite | 8.439/0.692 | 8.313/0.581 | 454.3/515.4 | +13.4% | 2.61 |
| postgres | 7.379/0.966 | 6.855/0.764 | 6728.6/6796.4 | +1.0% | 1.22 |

首轮 PostgreSQL 命中查询由全表过滤 99,900 行改为 Index Scan 读取 100 行；整个计划 Shared Hit Blocks 从 948 降为 18，缺失查询从 934 降为 2。SQLite 从遍历 Runtime Event 再按 event_id 找求值改为直接按新三列索引检索。排序仍保留，不能声称完全无扫描。查询计划、实际读取行数与 buffer 信息保留在 index-data.json。真实 Server 条件历史专项另外运行 3 进程 × 16 调用，无 SQL 钩子，完整报告其代价；小历史下不能期待复现 10 万行夹具的倍数。

| 后端 | 去索引 B / B p50 ms | 耗时降低 | CPU 降低 | 存储差 MiB |
| --- | ---: | ---: | ---: | ---: |
| postgres | 895.4/894.2 | +0.1% | -1.2% | +0.01 |
| sqlite | 1146.3/1158.5 | -1.1% | -0.9% | -0.11 |

保留索引的依据是已填充大历史的检索成本，而非短 Workflow 的整体速度。真实 Server 表中的存储差是独立数据库整库物理占用之差，不能把负值理解为索引节省空间；索引本体空间以独立夹具统计为准。夹具使用短 ID，空间数字不能直接作为生产每 10 万行的固定成本。

## 复现与证据

原始本地目录：`.autoagent/performance/v2-optimization/step-03/`。其中 before/after/without-index 为完整源码快照，harness-final 为固定工具；execute.py 按 phase 串行运行且保留每个 Server 的 config、result、calls、memory、日志及 SQLite 数据库。PostgreSQL 用完删除独立数据库，物理统计在 result 内。含本地连接信息的 config 不导出到 docs。

- `summary.json`：进程指标、SQL 指纹及缓存计数。
- `comparisons.json`、`formal-pairs.json`：A/B 与全部配对结果。
- `index-data.json`：索引独立实验与非空表执行计划。
- `provenance.json`：源码/工具 hash、测试、来源差异与验证口径。

步骤 3 完成后停止，步骤 4 等待用户授权。
