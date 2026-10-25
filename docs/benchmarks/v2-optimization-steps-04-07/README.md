# V2 优化步骤 4–7：缓存、批量读写、编码与完成通知

## 结论

本轮四步合并在六场景三轮配对中均改善：72 对延迟和 CPU 都下降。SQLite buffered 的进程中位耗时降低约 17.0%–44.5%，PostgreSQL buffered 约 14.7%–38.4%；durable 多数场景约 6%–13%，Unique 的观察延迟改善更大，不能全部解释为 Runtime 计算加速。RSS 有升有降；新增缓存不意味着所有场景更省内存。

这是本轮步骤 4–7 相对配平控制包的结果，不是步骤 1–7 相对 B0 的最终累计结论。样本为每配置 3 个独立进程，仍需步骤 9 的长时及同轮 B0/V1 验收。

## 范围与口径

本轮按用户授权连续实现步骤 4–7，冻结每个中间包分别验证。步骤 8 可选合并提交实验和步骤 9 最终长时验收未执行。Core 执行语义、Runtime Event 历史和 durable 确认边界保持原合同。

共 434 次原生 Server 运行、3260 次正式 HTTP 调用。业务输出校验一致，逐调用队列与 retained_bytes 为零；最终 accepted/durable frontier 相等。配对版本的 Runtime/User Event、定义、Session、Invocation、Node、Operator、Wait、图变化与条件事实行数一致。自动 Checkpoint 及相关 Artifact/引用数量可能受后台调度影响，差异完整列于 `variable-counts.json`，不把它们当作完全相同的写入工作量。

完整回归实际运行 1740 项，PostgreSQL 313 项，无跳过。`tests.test_deepseek_weather_example` 的同一 CLI 测试有两个失败子场景：仍执行任务开始前已删除的 `examples/deepseek_weather_assistant/__main__.py`。其余模块通过；未改动该示例入口。完整套件不能描述为全部通过。该完整套件在最终 ordinal 修正前运行，修正后 Persistence/Observation 等 124 项和 Host/Server/Fork/历史等 312 项再次通过（PostgreSQL 分别 57/151 项），见 `correction-verification.json`。

每个单步正常初筛为每配置 1 个独立进程，合并对照为 3 个独立进程，轮次交替 A/B 顺序。Chain/Map 每进程 5 次，Loop/Context/Text 3 次，Unique 16 次，预热另计。Unique 是短历史对照；不与历史 128 次调用存储直接相减。小样本不构成生产尾延迟保证。

固定工具、相同输入、Artifact 8192 bytes、内存监控关闭；诊断组另开 SQL 钩子，不能把诊断耗时混入正常速度。步骤 4 初筛还包含当时两个 Provider 文件的并行改动，只作初筛参考；最终合并控制包已配平这些文件。最终包还通过当前 Agent 的 172 项补测，覆盖 Models、Provider、Embedding、Capability、Host Agent 与 recording；实际 PostgreSQL 数量见 `current-agent-verification.json`。步骤 4 正常初筛使用初始工具，其后的工具仅增补诊断计数，正常执行路径相同。共享主机无 CPU/cgroup 隔离。冻结后又有 Agent 文件更新，保留在当前工作区，未纳入 A/B。测试与源码边界见 `late-source-changes.json`。

CPU 为 Server 测量区间 CPU/调用；RSS 为整个 Server 生命周期外部 20 ms 采样峰值，不含共享 PostgreSQL 服务。存储为关闭后的物理文件/数据库分配，SQLite 包括残留 WAL/SHM，PostgreSQL 包括数据库基线与索引。百分比正值表示耗时或 CPU 降低；不得跨步骤相加。

## 实现

- 步骤 4：Writer 独占有界正向元数据 LRU，只保留投影和来源引用。事务 overlay 在 COMMIT 返回后发布；回滚、结果不确定、维护和关闭清空。历史/accepted 查询不使用该缓存。默认 `AUTOAGENT_WRITER_METADATA_CACHE_BYTES=4194304`，已接入 Host 与 `.env.example`。计费是保留大小估算，不是精确 RSS 上限。
- 步骤 5A：整批 Artifact hash 去重并分块预取，先写 body 再写依赖。
- 步骤 5B：分组预取来源身份、所需投影、User Event frontier 和 Node ordinal；逐 Event 校验连续 sequence，并保留批内状态推进。
- 步骤 5C：按来源表批量插入，引用与 Checkpoint 成员分块写入；按父子外键依赖写入最终投影。Fork 读取历史来源/图事实前设置依赖屏障；Runtime Event 和 graph_changes 不合并删除。
- 步骤 6：编码线程完成最终 JSON 文本，准备结果释放重复业务容器；短键 memo 有字节/数量上限，字符串哈希避免大块拼接副本。只发布仍活跃的本批 Invocation 元数据，减少 terminal 批次无用复制；Node ordinal 仅在增长后发布。背压与 Python 驻留计量未删除。复查补充修正了 terminal 批次仍可能保留旧 ordinal 的失效问题；实际 buffered 失败恢复测试先复现 `[1,2,2]`，修正后为 `[1,2,3]`。
- 步骤 7：先订阅再读状态，按 Invocation/Root 筛选终态和边界提交通知；查询期间的提交不会丢失唤醒，取消后释放订阅，fault/close 唤醒等待者。保留 100 ms 有界兜底以观察 Runtime-only 恢复变化、buffered accepted 状态和外部提交。原 `wait_execution` 的 accepted 状态合同保留，通知不构成 durable ACK；持久化完成仍由 flush/frontier 确认。

## 四步合并对照

存储差异包括自动 Checkpoint 数量/时机和物理页分配的变化，不能简单解释为编码压缩收益。下面 A 使用本轮步骤 4 开始前的持久化代码；为排除期间无关改动，已配平两个 Provider 文件，只有本轮目标文件不同。B 是 ordinal 修正后的步骤 7 包。它不是历史 B0；不能据此声称整个步骤 1–7 的累计收益。每配置 3 进程，列出进程指标中位数；所有配对值另存 JSON。

| 后端 | 模式 | 场景 | A/B p50 ms | 耗时降低 | CPU 降低 | RSS 差 MiB | 存储差 MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| postgres | durable | chain-10-16k | 579.0/544.0 | +6.1% | +8.1% | +0.4 | +0.00 |
| postgres | durable | context-append | 1584.7/1482.1 | +6.5% | +4.2% | -0.5 | -0.05 |
| postgres | durable | loop-100-16k | 5782.5/5353.8 | +7.4% | +4.8% | +2.5 | -0.54 |
| postgres | durable | map-100-16k | 2089.0/1877.1 | +10.1% | +6.1% | +2.0 | +0.03 |
| postgres | durable | text-overwrite | 2244.5/1959.7 | +12.7% | +9.1% | -1.0 | -0.10 |
| postgres | durable | unique-history | 362.3/276.5 | +23.7% | +6.7% | +0.5 | +0.05 |
| postgres | buffered | chain-10-16k | 216.6/146.6 | +32.3% | +30.9% | +0.2 | -0.01 |
| postgres | buffered | context-append | 710.1/493.2 | +30.6% | +29.0% | -0.2 | +0.06 |
| postgres | buffered | loop-100-16k | 2493.1/1535.9 | +38.4% | +38.8% | -1.2 | -0.27 |
| postgres | buffered | map-100-16k | 930.3/671.0 | +27.9% | +30.1% | -1.5 | +0.02 |
| postgres | buffered | text-overwrite | 1103.1/940.8 | +14.7% | +12.6% | -8.2 | +0.08 |
| postgres | buffered | unique-history | 167.1/133.1 | +20.3% | +28.6% | -0.3 | -0.02 |
| sqlite | durable | chain-10-16k | 822.2/718.7 | +12.6% | +7.1% | +1.1 | -0.07 |
| sqlite | durable | context-append | 2173.7/1934.3 | +11.0% | +11.2% | +0.2 | +0.07 |
| sqlite | durable | loop-100-16k | 7862.1/7270.5 | +7.5% | +9.5% | +3.9 | +0.04 |
| sqlite | durable | map-100-16k | 2849.0/2499.5 | +12.3% | +10.9% | +2.8 | +0.04 |
| sqlite | durable | text-overwrite | 2697.7/2404.1 | +10.9% | +9.7% | -0.4 | -0.08 |
| sqlite | durable | unique-history | 482.1/358.9 | +25.6% | +10.8% | +1.1 | -0.05 |
| sqlite | buffered | chain-10-16k | 319.7/185.9 | +41.8% | +40.5% | +1.1 | +0.10 |
| sqlite | buffered | context-append | 908.8/628.0 | +30.9% | +33.9% | +2.5 | +0.55 |
| sqlite | buffered | loop-100-16k | 3443.7/1910.2 | +44.5% | +42.5% | -6.6 | +0.02 |
| sqlite | buffered | map-100-16k | 1143.4/806.6 | +29.5% | +33.0% | -6.2 | +0.47 |
| sqlite | buffered | text-overwrite | 1360.6/1129.8 | +17.0% | +15.1% | -5.6 | +0.17 |
| sqlite | buffered | unique-history | 181.8/134.1 | +26.2% | +31.2% | +0.5 | -0.04 |

## 步骤 4单独初筛

每配置只有 1 个独立进程，结果用于定位，不宣称稳定收益。步骤 6/7 此表是 ordinal 失效修正前的初版快照，普通成功负载通过输出/事实校验；修正后的最终包以三轮合并对照及 Loop 复核为准，不混合两版源码的结果。相邻步骤测量时段不同；小幅差异尤其应谨慎。

| 后端 | 模式 | 场景 | A/B p50 ms | 耗时降低 | CPU 降低 | RSS 差 MiB | 存储差 MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| postgres | durable | chain-10-16k | 699.5/699.8 | -0.0% | +8.5% | +0.3 | -0.01 |
| postgres | durable | context-append | 1699.2/1705.1 | -0.3% | +3.6% | -0.5 | +0.03 |
| postgres | durable | loop-100-16k | 6696.5/7007.5 | -4.6% | -6.1% | +3.1 | +0.16 |
| postgres | durable | map-100-16k | 5034.9/4348.3 | +13.6% | +12.4% | +1.5 | -0.20 |
| postgres | durable | text-overwrite | 2883.5/2777.6 | +3.7% | +2.1% | -1.9 | +0.05 |
| postgres | durable | unique-history | 917.3/877.2 | +4.4% | +7.2% | -0.0 | +0.02 |
| postgres | buffered | chain-10-16k | 284.9/227.2 | +20.3% | +21.6% | +0.2 | -0.06 |
| postgres | buffered | context-append | 852.8/887.8 | -4.1% | -2.9% | +3.9 | -0.08 |
| postgres | buffered | loop-100-16k | 3280.1/3140.2 | +4.3% | -5.2% | +1.8 | -0.30 |
| postgres | buffered | map-100-16k | 1015.0/1013.2 | +0.2% | -1.4% | +0.1 | +0.07 |
| postgres | buffered | text-overwrite | 1218.2/1249.3 | -2.6% | -10.2% | +2.1 | +0.02 |
| postgres | buffered | unique-history | 176.7/178.2 | -0.9% | +1.0% | +0.6 | +0.00 |
| sqlite | durable | chain-10-16k | 970.5/812.6 | +16.3% | +22.8% | +0.9 | -0.07 |
| sqlite | durable | context-append | 2737.0/2511.0 | +8.3% | +11.0% | +1.6 | -0.01 |
| sqlite | durable | loop-100-16k | 16754.5/18445.4 | -10.1% | +6.1% | +2.3 | -0.30 |
| sqlite | durable | map-100-16k | 3457.3/3150.0 | +8.9% | +11.4% | +2.0 | -0.30 |
| sqlite | durable | text-overwrite | 3090.9/2844.1 | +8.0% | +7.2% | +0.2 | -0.03 |
| sqlite | durable | unique-history | 636.3/488.4 | +23.2% | +32.1% | +0.4 | -0.07 |
| sqlite | buffered | chain-10-16k | 389.5/392.0 | -0.6% | -5.6% | +1.8 | -0.72 |
| sqlite | buffered | context-append | 1140.5/1317.6 | -15.5% | -14.6% | -0.8 | +2.71 |
| sqlite | buffered | loop-100-16k | 5000.2/5334.1 | -6.7% | -7.2% | +2.6 | -1.16 |
| sqlite | buffered | map-100-16k | 1400.7/1490.2 | -6.4% | -5.0% | +1.3 | +1.65 |
| sqlite | buffered | text-overwrite | 1602.9/1926.1 | -20.2% | -9.4% | +0.6 | +3.24 |
| sqlite | buffered | unique-history | 194.2/194.1 | +0.1% | -3.6% | +0.5 | +0.28 |

## 步骤 5单独初筛

每配置只有 1 个独立进程，结果用于定位，不宣称稳定收益。步骤 6/7 此表是 ordinal 失效修正前的初版快照，普通成功负载通过输出/事实校验；修正后的最终包以三轮合并对照及 Loop 复核为准，不混合两版源码的结果。相邻步骤测量时段不同；小幅差异尤其应谨慎。

| 后端 | 模式 | 场景 | A/B p50 ms | 耗时降低 | CPU 降低 | RSS 差 MiB | 存储差 MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| postgres | durable | chain-10-16k | 690.0/693.4 | -0.5% | +0.0% | +0.2 | -0.05 |
| postgres | durable | context-append | 1725.3/1857.0 | -7.6% | -7.0% | -0.7 | -0.08 |
| postgres | durable | loop-100-16k | 6612.2/6392.6 | +3.3% | +2.4% | -0.1 | -0.05 |
| postgres | durable | map-100-16k | 2345.9/2250.3 | +4.1% | +7.3% | -0.2 | +0.25 |
| postgres | durable | text-overwrite | 2301.6/2329.8 | -1.2% | -0.5% | -2.0 | -0.04 |
| postgres | durable | unique-history | 367.1/367.1 | -0.0% | +3.7% | +0.5 | -0.23 |
| postgres | buffered | chain-10-16k | 271.5/190.9 | +29.7% | +30.3% | +1.0 | -0.04 |
| postgres | buffered | context-append | 818.5/636.6 | +22.2% | +22.8% | -1.0 | -0.05 |
| postgres | buffered | loop-100-16k | 2905.6/1842.9 | +36.6% | +36.3% | -4.0 | +0.69 |
| postgres | buffered | map-100-16k | 1091.9/825.0 | +24.4% | +25.9% | -0.4 | -0.02 |
| postgres | buffered | text-overwrite | 1356.6/986.8 | +27.3% | +18.9% | -3.1 | -0.02 |
| postgres | buffered | unique-history | 176.9/137.5 | +22.3% | +25.8% | +0.3 | +0.01 |
| sqlite | durable | chain-10-16k | 823.9/832.9 | -1.1% | -1.2% | +0.2 | +0.02 |
| sqlite | durable | context-append | 2242.9/2130.1 | +5.0% | +6.5% | +2.5 | -0.14 |
| sqlite | durable | loop-100-16k | 8218.6/8103.9 | +1.4% | +2.8% | -0.9 | +0.32 |
| sqlite | durable | map-100-16k | 2907.1/2884.0 | +0.8% | +0.5% | -0.8 | -0.04 |
| sqlite | durable | text-overwrite | 2961.6/2788.5 | +5.8% | +6.9% | +0.8 | +0.02 |
| sqlite | durable | unique-history | 491.4/490.7 | +0.2% | -1.5% | +0.2 | -0.06 |
| sqlite | buffered | chain-10-16k | 355.3/310.4 | +12.6% | +31.6% | -0.5 | +0.14 |
| sqlite | buffered | context-append | 1019.8/672.4 | +34.1% | +35.5% | -2.7 | -0.21 |
| sqlite | buffered | loop-100-16k | 4132.8/2202.9 | +46.7% | +44.1% | -12.2 | -0.78 |
| sqlite | buffered | map-100-16k | 1404.0/991.4 | +29.4% | +33.8% | -3.8 | +1.09 |
| sqlite | buffered | text-overwrite | 1559.8/1242.6 | +20.3% | +19.2% | +0.3 | +0.04 |
| sqlite | buffered | unique-history | 202.3/150.5 | +25.6% | +32.9% | -0.2 | +0.17 |

## 步骤 6单独初筛

每配置只有 1 个独立进程，结果用于定位，不宣称稳定收益。步骤 6/7 此表是 ordinal 失效修正前的初版快照，普通成功负载通过输出/事实校验；修正后的最终包以三轮合并对照及 Loop 复核为准，不混合两版源码的结果。相邻步骤测量时段不同；小幅差异尤其应谨慎。

| 后端 | 模式 | 场景 | A/B p50 ms | 耗时降低 | CPU 降低 | RSS 差 MiB | 存储差 MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| postgres | durable | chain-10-16k | 583.4/587.5 | -0.7% | +2.5% | -0.3 | -0.03 |
| postgres | durable | context-append | 1604.3/1592.4 | +0.7% | +1.3% | -0.4 | -0.24 |
| postgres | durable | loop-100-16k | 5721.8/5990.8 | -4.7% | -3.6% | +0.5 | -0.02 |
| postgres | durable | map-100-16k | 2284.1/2158.0 | +5.5% | +5.4% | +0.5 | -0.17 |
| postgres | durable | text-overwrite | 2418.7/2173.1 | +10.2% | +10.2% | -1.9 | -0.07 |
| postgres | durable | unique-history | 367.2/362.8 | +1.2% | +5.3% | -0.3 | -0.04 |
| postgres | buffered | chain-10-16k | 277.1/159.3 | +42.5% | +18.6% | -0.5 | +0.08 |
| postgres | buffered | context-append | 560.0/557.8 | +0.4% | +1.9% | +3.7 | -0.12 |
| postgres | buffered | loop-100-16k | 1619.7/1732.5 | -7.0% | -4.4% | +1.2 | +0.28 |
| postgres | buffered | map-100-16k | 811.9/772.7 | +4.8% | +5.8% | -2.6 | +0.09 |
| postgres | buffered | text-overwrite | 998.6/1120.8 | -12.2% | +2.0% | -6.6 | +0.08 |
| postgres | buffered | unique-history | 137.7/134.2 | +2.5% | +6.2% | -0.8 | +0.17 |
| sqlite | durable | chain-10-16k | 829.6/827.0 | +0.3% | +4.0% | +0.0 | +0.05 |
| sqlite | durable | context-append | 2140.2/2115.6 | +1.1% | -2.1% | -1.9 | +0.08 |
| sqlite | durable | loop-100-16k | 8116.0/7975.1 | +1.7% | -0.6% | +0.6 | +0.05 |
| sqlite | durable | map-100-16k | 3179.0/3048.1 | +4.1% | +4.4% | +0.4 | +0.13 |
| sqlite | durable | text-overwrite | 2643.5/2580.4 | +2.4% | +3.7% | -0.4 | -0.05 |
| sqlite | durable | unique-history | 495.7/491.5 | +0.9% | +6.0% | -1.0 | +0.02 |
| sqlite | buffered | chain-10-16k | 321.9/201.7 | +37.3% | +5.9% | +0.4 | +0.92 |
| sqlite | buffered | context-append | 690.0/664.1 | +3.8% | +6.5% | +1.3 | +0.44 |
| sqlite | buffered | loop-100-16k | 2246.9/2398.6 | -6.8% | -8.6% | +3.9 | +0.37 |
| sqlite | buffered | map-100-16k | 912.8/947.6 | -3.8% | -2.6% | -1.2 | +0.17 |
| sqlite | buffered | text-overwrite | 1349.7/1278.7 | +5.3% | +2.5% | +1.2 | -0.02 |
| sqlite | buffered | unique-history | 150.3/149.0 | +0.9% | +4.9% | -0.1 | +0.26 |

## 步骤 7单独初筛

每配置只有 1 个独立进程，结果用于定位，不宣称稳定收益。步骤 6/7 此表是 ordinal 失效修正前的初版快照，普通成功负载通过输出/事实校验；修正后的最终包以三轮合并对照及 Loop 复核为准，不混合两版源码的结果。相邻步骤测量时段不同；小幅差异尤其应谨慎。

| 后端 | 模式 | 场景 | A/B p50 ms | 耗时降低 | CPU 降低 | RSS 差 MiB | 存储差 MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| postgres | durable | chain-10-16k | 585.8/607.3 | -3.7% | -3.4% | +0.5 | +0.17 |
| postgres | durable | context-append | 1725.2/1674.0 | +3.0% | +2.8% | -0.3 | +0.00 |
| postgres | durable | loop-100-16k | 6101.7/5974.0 | +2.1% | -0.3% | +0.5 | -0.62 |
| postgres | durable | map-100-16k | 2045.6/2034.4 | +0.5% | -1.9% | -0.2 | +0.18 |
| postgres | durable | text-overwrite | 2179.2/2227.0 | -2.2% | -6.5% | -4.2 | +0.26 |
| postgres | durable | unique-history | 357.8/297.7 | +16.8% | -0.0% | -0.1 | +0.25 |
| postgres | buffered | chain-10-16k | 194.1/172.5 | +11.1% | +12.6% | -0.5 | -0.12 |
| postgres | buffered | context-append | 576.7/573.1 | +0.6% | +1.2% | -1.4 | -0.20 |
| postgres | buffered | loop-100-16k | 1614.9/1633.3 | -1.1% | -0.8% | +1.9 | -0.13 |
| postgres | buffered | map-100-16k | 776.7/761.8 | +1.9% | -0.6% | -1.5 | +0.00 |
| postgres | buffered | text-overwrite | 993.8/994.5 | -0.1% | +1.4% | +1.3 | +0.02 |
| postgres | buffered | unique-history | 138.6/133.7 | +3.5% | -1.5% | +0.2 | -0.05 |
| sqlite | durable | chain-10-16k | 814.9/744.1 | +8.7% | -1.6% | -0.2 | +0.13 |
| sqlite | durable | context-append | 1982.4/2013.2 | -1.6% | -1.5% | -2.8 | +0.05 |
| sqlite | durable | loop-100-16k | 7910.1/7534.1 | +4.8% | +4.7% | -0.6 | -0.01 |
| sqlite | durable | map-100-16k | 2739.7/2697.7 | +1.5% | -3.7% | -0.1 | +0.13 |
| sqlite | durable | text-overwrite | 2573.5/2537.9 | +1.4% | -1.5% | +1.7 | -0.06 |
| sqlite | durable | unique-history | 483.5/402.7 | +16.7% | -1.1% | +0.3 | +0.07 |
| sqlite | buffered | chain-10-16k | 205.4/206.4 | -0.5% | +5.3% | -0.9 | +0.07 |
| sqlite | buffered | context-append | 664.2/623.0 | +6.2% | +1.0% | +4.1 | -0.14 |
| sqlite | buffered | loop-100-16k | 2076.8/2023.9 | +2.5% | +1.4% | +1.6 | -0.74 |
| sqlite | buffered | map-100-16k | 880.9/882.5 | -0.2% | +1.1% | +0.3 | -1.16 |
| sqlite | buffered | text-overwrite | 1194.5/1187.7 | +0.6% | +0.0% | -0.1 | +6.77 |
| sqlite | buffered | unique-history | 144.5/134.6 | +6.9% | -5.3% | +0.4 | -0.04 |

## SQL 诊断

每个配置为 5 次 Chain 调用、310 个 Runtime Event。总 SQL 包含观察与后台工作；这些计数可能随时序变化。Artifact IN 查询仅统计 write_transaction。诊断组只用于解释机制。

| 步骤 | 后端 | 模式 | 总 SQL A/B | SELECT A/B | Artifact IN A/B |
| --- | --- | --- | ---: | ---: | ---: |
| 04 | postgres | durable | 5604/4328 | 3399/2123 | 250/250 |
| 04 | postgres | buffered | 2357/2316 | 1725/1686 | 250/250 |
| 04 | sqlite | durable | 6343/5074 | 3461/2195 | 250/250 |
| 04 | sqlite | buffered | 2905/2847 | 1931/1871 | 250/250 |
| 05a | postgres | durable | 4304/4300 | 2099/2095 | 250/250 |
| 05a | postgres | buffered | 2376/2128 | 1744/1497 | 250/10 |
| 05a | sqlite | durable | 5079/5060 | 2199/2181 | 250/250 |
| 05a | sqlite | buffered | 2837/2716 | 1865/1743 | 250/10 |
| 05b | postgres | durable | 4321/4023 | 2116/1818 | 250/250 |
| 05b | postgres | buffered | 2218/1425 | 1586/794 | 10/10 |
| 05b | sqlite | durable | 5068/4753 | 2188/1873 | 250/250 |
| 05b | sqlite | buffered | 2607/1903 | 1632/931 | 10/10 |
| 05c | postgres | durable | 3994/3994 | 1789/1789 | 250/250 |
| 05c | postgres | buffered | 1422/834 | 788/743 | 10/10 |
| 05c | sqlite | durable | 4725/4744 | 1848/1866 | 250/250 |
| 05c | sqlite | buffered | 1944/1319 | 953/869 | 11/10 |
| 06 | postgres | durable | 4000/4017 | 1795/1812 | 250/250 |
| 06 | postgres | buffered | 844/822 | 749/731 | 10/10 |
| 06 | sqlite | durable | 4755/4725 | 1875/1848 | 250/250 |
| 06 | sqlite | buffered | 1296/1293 | 830/836 | 11/11 |
| 07 | postgres | durable | 3970/3998 | 1765/1793 | 250/250 |
| 07 | postgres | buffered | 730/753 | 641/663 | 9/9 |
| 07 | sqlite | durable | 4747/4708 | 1872/1833 | 250/250 |
| 07 | sqlite | buffered | 1386/1245 | 947/811 | 10/10 |
| cumulative | postgres | durable | 5565/3953 | 3360/1748 | 250/250 |
| cumulative | postgres | buffered | 2234/661 | 1624/594 | 250/5 |
| cumulative | sqlite | durable | 6293/4693 | 3418/1819 | 250/250 |
| cumulative | sqlite | buffered | 2767/1174 | 1791/743 | 250/10 |

## 大图、并发与慢库补充

大图每版本 2 进程 × 3 调用；步骤 6 Loop 复核每配置 3 进程 × 5 调用，其 A/B 单独指步骤 6 前/后；其余补充 A/B 指本轮 4–7 合并，其他场景每配置 1 进程。它们不能当作长期稳定性或生产 p95 结论。步骤 6 SQLite Loop 复核的进程中位耗时增加 3.0%、CPU 增加 5.4%，但三对耗时降低率约为 +1.2%/-3.8%/+2.6%，方向混合；保留未决小幅回退风险，不能宣称已排除。PostgreSQL 三对均小幅改善。合并 4–7 的 SQLite buffered Loop 三对均改善，属于另一个对照，不用于掩盖步骤 6 的不利结果。

| 后端 | 模式 | 场景 | A/B p50 ms | 耗时降低 | CPU 降低 | RSS 差 MiB | 存储差 MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| postgres | durable | concurrent-8 | 7462.6/5735.6 | +23.1% | +13.5% | +1.8 | -0.20 |
| postgres | durable | slow-storage | 1947.9/1909.6 | +2.0% | +2.8% | -0.1 | -0.01 |
| postgres | buffered | concurrent-8 | 5665.8/5164.9 | +8.8% | +7.5% | +2.9 | +0.02 |
| postgres | buffered | graph-128 | 2441.1/2102.9 | +13.9% | +16.3% | +5.9 | -0.02 |
| postgres | buffered | slow-storage | 234.2/166.0 | +29.1% | +35.1% | +1.2 | -0.04 |
| postgres | buffered | step-06-loop-review | 1763.1/1684.4 | +4.5% | +2.4% | +1.2 | +0.08 |
| postgres | buffered | unique-churn | 169.6/133.5 | +21.3% | +22.1% | +1.6 | -0.38 |
| sqlite | durable | concurrent-8 | 12540.0/11527.7 | +8.1% | +9.7% | +1.0 | -0.13 |
| sqlite | durable | slow-storage | 2244.2/2102.9 | +6.3% | +9.9% | +0.6 | -0.03 |
| sqlite | buffered | concurrent-8 | 7340.7/6826.0 | +7.0% | +6.4% | +2.0 | -0.08 |
| sqlite | buffered | graph-128 | 4303.6/2212.1 | +48.6% | +47.9% | -30.0 | -0.25 |
| sqlite | buffered | slow-storage | 309.0/210.2 | +31.9% | +34.0% | +0.6 | +0.00 |
| sqlite | buffered | step-06-loop-review | 2224.8/2291.3 | -3.0% | -5.4% | -0.1 | +0.88 |
| sqlite | buffered | unique-churn | 192.1/138.9 | +27.7% | +25.5% | +0.7 | -0.16 |

## 缓存压力

64 个 Revision、128 次唯一内容/新 Session 请求；压力组 metadata/graph 预算各 16 KiB，只有 B，用于验证容量，不伪造 A 对照。以下 peak 是各次提交完成后采到的最大计费值，不是瞬时 RSS；维护会保守清空缓存。

| 后端 | 场景 | 命中/未命中/淘汰 | 提交后 peak bytes | 结束 bytes | 预算 bytes |
| --- | --- | ---: | ---: | ---: | ---: |
| postgres | metadata-pressure | 5/2727/175 | 16289 | 8291 | 16384 |
| postgres | unique-churn | 16/2709/0 | 277006 | 1417 | 4194304 |
| sqlite | metadata-pressure | 50/2887/866 | 16212 | 13836 | 16384 |
| sqlite | unique-churn | 202/2709/0 | 1814569 | 36688 | 4194304 |

## 证据与限制

- `summary.json` 保存进程指标、行数及独立 SQL 诊断，`comparisons.json` 保存全部配对降低率；不选择有利轮次。
- `source-hashes.json` 固定每个阶段的完整包；原始 Server 日志、逐调用校验、内存采样和测试日志保留在本地 `.autoagent/performance/v2-optimization/steps-04-07/`，连接配置不发布。
- `provenance.json` 记录完整回归、各中间版本验证、工具 hash、先前失败/中断及边界。
- 长时 RSS、B0 与最终版本及 V1 同轮重测仍属于步骤 9。小样本没有证明的收益不能用减少 SQL 或局部微基准替代。
