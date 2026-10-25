# V2 优化步骤 2：Writer 索引与临界区

## 结论

判定：**结构改善，暂未证明整体加速**。保留本步是因为普通 Event 入队的身份/尾部查找已从全队列扫描改为平均 O(1)，batch 复制受 batch_count 约束，慢 SQL 不再阻止已提交数据确认。8,192 条 pending 下，新 Event 入队耗时降低 43.6%；队列排空和标准 HTTP 场景没有同等幅度收益。

慢存储正式专项：SQLite p50 基本持平（耗时 +0.4%），PostgreSQL 降低 0.6%。PostgreSQL Chain 初筛回退未在 5 进程 × 20 调用复核中复现。Context 默认观察的进程 p50 中位数仍慢 5.5%，配对不确定区间跨零，CPU 基本持平，Host.join 对照降低约 0.8%；该未决耗时差异完整保留，不据此宣称没有回退风险。

## 修改范围

产品代码仅修改 `autoagent/host/persistence/writer.py`：

1. pending 身份使用 `(kind, identity) -> Entry` 索引。覆盖正在提交但尚未确认的记录；提交成功才删除，故障保留待提交尾部，关闭时清理。没有增加历史身份缓存。
2. pending 尾部使用 `(kind, stream) -> Entry` 索引。前一条提交时不会删除较新的尾部；最后一条提交后释放。Runtime Event 按 Session、User Event 按 Invocation 分流。
3. 在数据库 await 前记住数值形式的 accepted sequence；Checkpoint 同样先固定 pending frontier 与导入锚点集合，避免队列提交期间校验漂移。
4. batch 使用 `islice` 只复制最多 batch_count 条。普通 admission SQL 不持有 condition；admission 仍串行，容量检查与入队更新仍在 condition 内，await 后重新检查关闭/故障状态。
5. 内存 SQLite 保留 mutation 串行约束，容量等待前释放 mutation。数据库结构、存储格式、确认、重试、flush frontier 和完整记录规则未调整。

## 正确性验证

冻结阶段的完整回归按模块隔离执行：98 个模块、1687 项测试通过，其中 PostgreSQL 300 项，无跳过。另通过 compileall 和 diff whitespace 检查。

测量期间另有工作区改动：datasets.py、server/app.py 与新增 family_links.py，均保留，未混入 A/B 源码；Writer 仍与 B 的 hash 一致。测量后补跑当前工作区 Writer、Live Trace、Server、Server Comparison 四个模块，218 项通过（PostgreSQL 105 项，无跳过）。不能将冻结版本的性能数字直接归给这些后续改动；来源差异与补测日志单独记录。

新增/补充验证包括：in-flight 重复提交与取消不会取消真实写入；满队列取消不留下身份；先前 Entry 提交不删除较新的 stream 尾部；多个生产者保持顺序；慢 admission SQL 不阻止已提交数据 ACK 与容量释放；单连接内存 SQLite 不发生读写重叠；故障保留索引且关闭清空。原有 Checkpoint、恢复、accepted overlay、父子 Invocation、丢失确认重试与共享对象释放测试继续通过。

## 对照与口径

A 是步骤 1 完成后的固定源码，文件 hash 已确认一致；B 是本步完成后的固定源码。双方共用同一版基准工具与诊断钩子，保存完整包及工具 hash。

本任务的重型测试串行运行，但没有私有 CPU/cgroup 隔离，不能排除主机其他负载；测量期间工作区确有其他改动。主机前后 load average 记录在 provenance.json，仅作环境线索，不能据其排除瞬时干扰。

共 244 个独立 Server 运行、4904 次正式 HTTP 调用，全部完成且逐次校验输出及排空。SQLite/PG，Artifact 8192 bytes，内存监控关闭；所有重型运行串行，A/B 每轮反转。额外 6 个独立进程运行 Writer 队列扩展诊断。

另有 1 个队列诊断尝试因控制进程收到 SIGTERM（退出 143，原因未确定）而中断；目录和进程退出记录保留于 interruptions.json，完整结果未生成，不并入成功延迟样本。残留 Server 已单独关闭，并在新目录续跑；未覆盖原始尝试。

标准六场景每配置 3 个进程，属于初筛；每进程调用数与原基线相同（5/3/5/3/3/128），不足以声称稳定的小幅收益或生产 p95。慢存储 buffered 专项每配置 5 个独立进程、每进程 20 次测量调用。预热不计调用数。诊断钩子仅用于单独的 queue/admission 场景；正常速度、正式复核和 join 对照均未启用，不能混用其延迟。

下表 p50/p95、CPU 与 RSS 均取独立进程指标中位数。CPU 是测量区间进程 CPU / 调用数，不是逐请求 CPU p50。RSS 为 20 ms 外部采样的整个 Server 进程峰值，PG 服务内存不计入。正的耗时/CPU 降低率表示改善，正的 RSS 差表示增加。

## 队列长度扩展诊断

真实 SQLite Writer/User Event 路径，在提交 gate 阻塞期间建立 pending。每个深度测 4,000 次同对象重复提交，另测 64 次新 Event（含实际数据库与 codec）；setup 和排空不计入入队耗时。各值取 3 个独立进程中位数。重复路径无需重新编码，不能当作完整 LLM/Workflow 调用加速倍数。

| pending 条数 | 重复入队 A µs | B µs | 重复路径加速 | 新增入队 A µs | B µs | 降低 | 排空 A/B ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 1.020 | 0.593 | 1.7× | 717.7 | 713.8 | 0.5% | 66.2/64.3 |
| 64 | 2.278 | 0.575 | 4.0× | 710.1 | 699.3 | 1.5% | 122.2/118.5 |
| 1024 | 18.974 | 0.571 | 33.2× | 762.7 | 705.2 | 7.5% | 1020.4/999.6 |
| 8192 | 147.803 | 0.584 | 253.3× | 1248.1 | 704.2 | 43.6% | 7614.5/7613.2 |

每个深度重复入队的完整 pending 迭代次数 A/B 为 4,000/0；64 次新增入队为 128/0。`islice` 的 batch 前缀读取仍正常遍历所需前缀。排空后对象表、pending、身份索引与尾部索引均为空。索引上限受 queue_count 约束；retained_bytes 仍衡量原有共享对象计量，不伪称包含所有 Python 索引开销。

## 标准 HTTP 场景：本步 A → B

| 后端 | 模式 | 场景 | A p50 ms | B p50 ms | 耗时降低 | CPU 降低 | RSS 差 MiB | A/B p95 ms |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| postgres | durable | chain-10-16k | 685.8 | 791.8 | -15.5% | -1.2% | +1.2 | 1002.5/820.4 |
| postgres | durable | context-append | 1584.7 | 1680.8 | -6.1% | -0.8% | +0.7 | 1660.7/2154.2 |
| postgres | durable | loop-100-16k | 5834.6 | 5818.2 | +0.3% | +0.0% | +0.3 | 5946.2/5943.1 |
| postgres | durable | map-100-16k | 2034.6 | 2029.7 | +0.2% | -2.1% | -0.0 | 2101.3/2682.4 |
| postgres | durable | text-overwrite | 2199.2 | 2195.5 | +0.2% | -0.4% | -0.8 | 2207.7/2412.2 |
| postgres | durable | unique-history | 358.8 | 358.2 | +0.2% | +1.3% | +0.1 | 393.0/380.0 |
| postgres | buffered | chain-10-16k | 254.3 | 246.4 | +3.1% | -5.6% | +0.4 | 264.1/281.1 |
| postgres | buffered | context-append | 753.9 | 750.3 | +0.5% | -0.5% | -1.3 | 771.9/794.5 |
| postgres | buffered | loop-100-16k | 2674.7 | 2570.7 | +3.9% | +1.8% | +0.6 | 3043.3/2598.6 |
| postgres | buffered | map-100-16k | 931.4 | 898.4 | +3.5% | +1.1% | +0.1 | 949.8/927.3 |
| postgres | buffered | text-overwrite | 1133.4 | 1116.1 | +1.5% | +1.3% | -0.9 | 1171.0/1150.4 |
| postgres | buffered | unique-history | 174.8 | 174.1 | +0.4% | -0.1% | -0.2 | 203.6/202.2 |
| sqlite | durable | chain-10-16k | 814.9 | 825.4 | -1.3% | +0.3% | -0.1 | 831.3/908.6 |
| sqlite | durable | context-append | 2103.4 | 2189.5 | -4.1% | -2.8% | -1.2 | 2133.8/2224.7 |
| sqlite | durable | loop-100-16k | 7888.4 | 8086.7 | -2.5% | -1.2% | -1.4 | 8146.9/8292.3 |
| sqlite | durable | map-100-16k | 2747.7 | 2793.7 | -1.7% | -1.1% | +0.4 | 2781.8/2855.2 |
| sqlite | durable | text-overwrite | 2756.2 | 2702.6 | +1.9% | +2.2% | +1.3 | 2800.7/2797.8 |
| sqlite | durable | unique-history | 491.4 | 490.4 | +0.2% | +0.6% | -0.2 | 511.8/506.8 |
| sqlite | buffered | chain-10-16k | 309.5 | 309.1 | +0.1% | +0.0% | -0.2 | 336.9/336.4 |
| sqlite | buffered | context-append | 925.8 | 913.8 | +1.3% | +1.0% | -1.9 | 965.3/968.6 |
| sqlite | buffered | loop-100-16k | 3858.9 | 3807.5 | +1.3% | +0.7% | -0.1 | 3871.6/3859.9 |
| sqlite | buffered | map-100-16k | 1133.9 | 1127.0 | +0.6% | +0.8% | -0.3 | 1142.7/1140.8 |
| sqlite | buffered | text-overwrite | 1412.3 | 1419.1 | -0.5% | +0.2% | -0.8 | 1427.7/1424.7 |
| sqlite | buffered | unique-history | 197.5 | 197.9 | -0.2% | -0.7% | -0.4 | 230.3/225.9 |

## 慢存储正式专项

10 个工作 Node、64 KiB 输出，每批实际写入前注入 20 ms 延迟，buffered；5 进程 × 20 调用。此处没有 SQL/队列诊断钩子。flush 包括读取输出前后的持久化屏障。

| 后端 | A/B p50 ms | 耗时降低 | CPU 降低 | A/B flush ms | RSS 差 MiB | 每轮 p50 降低率 |
| --- | --- | ---: | ---: | --- | ---: | --- |
| postgres | 318.5/316.6 | +0.6% | +1.1% | 37.5/36.7 | -0.7 | -0.3%, -0.6%, -1.1%, +1.1%, +5.6% |
| sqlite | 404.2/405.6 | -0.4% | +0.4% | 83.3/83.6 | -0.9 | +4.5%, +1.1%, -5.6%, -0.3%, -1.2% |

## 疑似回退复核：PostgreSQL durable Context

标准初筛该配置的 p50 回退超过 5%，因此追加 5 个独立进程 × 20 次调用，保持相同源码、负载和工具，无诊断钩子。初筛样本全部保留，不能用复核结果替换原表。

| A p50 ms | B p50 ms | 耗时降低 | CPU 降低 | A/B p95 ms | 每轮 p50 降低率 |
| ---: | ---: | ---: | ---: | --- | --- |
| 1583.9 | 1670.7 | -5.5% | -0.4% | 1699.9/1736.7 | -6.4%, -6.0%, +2.9%, -0.3%, +0.4% |

同一配置的 A 侧进程 p50 自身跨度为 5.3%。五轮配对降低率中位数 -0.28%，对 5 个进程对进行配对 bootstrap，耗时降低率探索性 95% 区间约 [-6.4%, +2.9%]，跨过零。进程 p50 呈约 1.58/1.67 秒分组，两组进程中位数之比会放大这次分组比例差异。原始 +5.5% 耗时结果仍保留；不能据此宣称加速，也尚不足以确认持续回退。bootstrap 仅 5 对样本，不是生产保证。

## 疑似回退复核：PostgreSQL Chain

标准初筛 durable 的 p50 与 buffered 的 CPU 出现超过 5% 的回退，追加两个模式各 5 进程 × 20 次调用。

| 模式 | A/B p50 ms | 耗时降低 | CPU 降低 | A/B p95 ms | 每轮 p50 降低率 |
| --- | --- | ---: | ---: | --- | --- |
| durable | 679.0/672.7 | +0.9% | +0.1% | 696.4/696.3 | -0.5%, +14.5%, -0.4%, +14.9%, -1.2% |
| buffered | 238.8/237.1 | +0.7% | +0.9% | 335.3/336.6 | +0.3%, -0.1%, -0.4%, -0.5%, +5.6% |

## Context 完成观察诊断

相同 Context 负载、durable 和 8192 bytes Artifact，使用已有 Host.join 等待接口，5 进程 × 20 调用；仍通过真实 HTTP 调用并保留持久化屏障。默认 wait_execution 每 100 ms 查询一次；Host.join 还会构建完整 InvocationResult，因此本诊断只比较 join 自身 A/B，用于交叉检查等待机制，不能用两个接口的绝对耗时相减推导纯轮询成本。默认观察接口与产品代码均未改变。

| A p50 ms | B p50 ms | 耗时降低 | CPU 降低 | A/B p95 ms | 每轮 p50 降低率 |
| ---: | ---: | ---: | ---: | --- | --- |
| 1757.3 | 1742.4 | +0.8% | +1.7% | 2252.4/1794.2 | -0.8%, +2.0%, +4.2%, +1.3%, +0.7% |

额外 durable SQL 诊断（每进程 5 次调用）显示 Chain A/B 均为 310 Runtime Event、310 批次，admission SQL 685/685，write_transaction SQL 2360/2360，投影计算 1140/1140，投影写入 1335/1335。Context 均为 760 Runtime Event，投影计算 2490/2490、投影写入 3335/3335；采样边界内多出 1 个 Checkpoint，批次 764/765。该诊断未发现 Runtime Event 批次碎片化或主投影工作增加；不能把这些单进程计数当作尾延迟统计。


## 单独诊断：admission、队列与排空

admission wait 是取得 admission lock 的等待，hold 包含校验、codec、容量等待和入队，不是纯 CPU。下表等待按已提交 Runtime Event 数归一化；queue peak 是整个进程（含预热）的精确高水位。flush 为每次 HTTP 调用的屏障耗时中位数。

| 后端 | 场景 | pending 峰 A/B | retained 峰 KiB A/B | admission wait ms/Event A/B | hold ms/Event A/B | flush ms A/B | Event/batch A/B | SQL A/B |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| postgres | queue-loop-100-16k | 403/401 | 2681.802/2678.032 | 0.000/0.001 | 1.742/1.753 | 346.8/389.6 | 122.40/122.40 | 19304/19294 |
| postgres | queue-map-100-16k | 135/133 | 1236.214/1226.919 | 0.000/0.001 | 2.233/2.189 | 88.6/86.6 | 54.00/54.00 | 8690/8684 |
| postgres | queue-slow-storage | 62/62 | 1367.287/1367.287 | 0.001/0.000 | 1.059/1.011 | 36.6/27.6 | 38.75/44.29 | 2754/2715 |
| sqlite | queue-loop-100-16k | 476/481 | 3178.155/3213.484 | 0.001/0.001 | 2.350/2.356 | 511.5/507.5 | 153.00/153.00 | 22331/22223 |
| sqlite | queue-map-100-16k | 173/174 | 1609.509/1600.404 | 0.001/0.001 | 3.042/3.118 | 92.5/84.9 | 54.00/56.84 | 9445/9431 |
| sqlite | queue-slow-storage | 62/62 | 1367.287/1367.287 | 0.001/0.001 | 1.570/1.689 | 67.8/70.1 | 31.00/31.00 | 3126/3232 |

本步不精简 SQL 列或缓存 Workflow 图元数据。总 SQL 次数可能因 batch 分组和观察次数变化，不能将其当作步骤 3 的查询优化。诊断 elapsed 存在包含关系，不相加推导总请求时间。

## 累计参考：B0 → B

B0 是步骤 0 的历史测量，未与本步交错；累计值仅作参考，本步因果判断看当前 A/B。

| 后端 | 模式 | 场景 | B0 p50 ms | B p50 ms | 降低 |
| --- | --- | --- | ---: | ---: | ---: |
| postgres | durable | chain-10-16k | 682.2 | 791.8 | -16.1% |
| postgres | durable | context-append | 1818.2 | 1680.8 | +7.6% |
| postgres | durable | loop-100-16k | 6746.9 | 5818.2 | +13.8% |
| postgres | durable | map-100-16k | 2115.7 | 2029.7 | +4.1% |
| postgres | durable | text-overwrite | 2442.6 | 2195.5 | +10.1% |
| postgres | durable | unique-history | 361.3 | 358.2 | +0.9% |
| postgres | buffered | chain-10-16k | 276.5 | 246.4 | +10.9% |
| postgres | buffered | context-append | 794.4 | 750.3 | +5.5% |
| postgres | buffered | loop-100-16k | 2949.6 | 2570.7 | +12.8% |
| postgres | buffered | map-100-16k | 931.6 | 898.4 | +3.6% |
| postgres | buffered | text-overwrite | 1287.0 | 1116.1 | +13.3% |
| postgres | buffered | unique-history | 190.7 | 174.1 | +8.7% |
| sqlite | durable | chain-10-16k | 938.3 | 825.4 | +12.0% |
| sqlite | durable | context-append | 2453.1 | 2189.5 | +10.7% |
| sqlite | durable | loop-100-16k | 9082.5 | 8086.7 | +11.0% |
| sqlite | durable | map-100-16k | 2879.1 | 2793.7 | +3.0% |
| sqlite | durable | text-overwrite | 2926.7 | 2702.6 | +7.7% |
| sqlite | durable | unique-history | 493.1 | 490.4 | +0.5% |
| sqlite | buffered | chain-10-16k | 345.5 | 309.1 | +10.5% |
| sqlite | buffered | context-append | 986.3 | 913.8 | +7.4% |
| sqlite | buffered | loop-100-16k | 4044.4 | 3807.5 | +5.9% |
| sqlite | buffered | map-100-16k | 1151.7 | 1127.0 | +2.1% |
| sqlite | buffered | text-overwrite | 1547.2 | 1419.1 | +8.3% |
| sqlite | buffered | unique-history | 215.0 | 197.9 | +8.0% |

## 记录与存储

全部逐次输出在相同轮次 A/B 间一致；每次调用队列与 retained bytes 均为零，accepted/durable frontier 一致。五张逻辑表 runtime_events、graph_changes、edge_evaluations、node_executions、operator_calls 的记录数逐轮相等，详情见 summary.json。关闭后物理分配如下；PG 分配包含索引/TOAST，不能据小幅波动声称持久化格式变小。

| 后端 | 模式 | 场景 | A MiB | B MiB |
| --- | --- | --- | ---: | ---: |
| postgres | durable | chain-10-16k | 11.22 | 11.20 |
| postgres | durable | chain-10-16k-formal | 14.80 | 14.99 |
| postgres | durable | context-append | 13.57 | 13.52 |
| postgres | durable | context-append-formal | 29.77 | 29.74 |
| postgres | durable | loop-100-16k | 20.48 | 20.60 |
| postgres | durable | map-100-16k | 13.88 | 13.89 |
| postgres | durable | text-overwrite | 14.02 | 13.85 |
| postgres | durable | unique-history | 25.73 | 25.76 |
| postgres | buffered | chain-10-16k | 11.18 | 11.15 |
| postgres | buffered | chain-10-16k-formal | 14.79 | 14.77 |
| postgres | buffered | context-append | 13.46 | 13.53 |
| postgres | buffered | loop-100-16k | 20.31 | 20.07 |
| postgres | buffered | map-100-16k | 13.74 | 13.76 |
| postgres | buffered | slow-storage-formal | 14.91 | 14.93 |
| postgres | buffered | text-overwrite | 13.76 | 13.69 |
| postgres | buffered | unique-history | 25.49 | 25.42 |
| sqlite | durable | chain-10-16k | 6.78 | 6.75 |
| sqlite | durable | context-append | 14.36 | 14.38 |
| sqlite | durable | loop-100-16k | 22.35 | 22.47 |
| sqlite | durable | map-100-16k | 11.19 | 11.14 |
| sqlite | durable | text-overwrite | 107.92 | 107.90 |
| sqlite | durable | unique-history | 35.44 | 35.35 |
| sqlite | buffered | chain-10-16k | 6.75 | 6.80 |
| sqlite | buffered | context-append | 14.58 | 14.48 |
| sqlite | buffered | loop-100-16k | 23.86 | 24.14 |
| sqlite | buffered | map-100-16k | 11.74 | 11.71 |
| sqlite | buffered | slow-storage-formal | 23.21 | 23.52 |
| sqlite | buffered | text-overwrite | 107.88 | 108.23 |
| sqlite | buffered | unique-history | 35.68 | 35.58 |

## 限制与下一步

本步没有执行一小时 soak；短进程 RSS 和索引清空验证不能替代长期无增长证明。没有调整 SDK 的 Artifact 默认值，没有降低完整记录或 durable 保证。

下一步仍等待用户授权：步骤 3 的最小列查询、按不可变 Revision 缓存图元数据、出边索引和条件求值读取优化。步骤 3 尚未实施。

## 原始资料与复现

- [summary.json](summary.json)：每配置分布、记录数、CPU、RSS、存储与诊断。
- [comparisons.json](comparisons.json)：本步变化与每轮配对结果。
- [scaling.json](scaling.json)：逐深度入队测量与迭代次数。
- [formal-pairs.json](formal-pairs.json)：五轮配对变化、A/A 跨度和探索性不确定区间。
- [provenance.json](provenance.json)：产品/工具 hash、完整测试记录与校验结论。
- 本地 `.autoagent/performance/v2-optimization/step-02/` 保存 before/after 源码、harness、execute.py、verify.py、report.py、逐次调用及进程日志；含连接信息的 config 不导出。重跑必须使用新的 output，不能复用数据库。
- 补充执行脚本：`execute-queue-resume.py`（中断续跑）、`execute-formal.py`（慢存储）、`execute-regression.py`（Context/Chain 复核）、`execute-admission-diagnostic.py`（SQL）、`execute-join.py`（join）；脚本 hash 也记录在 provenance.json。
- 通用扩展诊断入口：`.venv/bin/python -m tests.benchmarks.benchmark_writer_admission --source-root <固定源码目录> --output <新文件.json>`。HTTP 对照使用 `tests.benchmarks.server_comparison.run` 的 `--suite artifact --profiles v2-artifacts-8k v2-buffered-artifacts-8k --backend sqlite|postgres --source-root ... --repeat 3 --output ...`；跨源码轮次交错顺序见 execute.py。
