# V2 优化步骤 0：固定基线与诊断

状态：步骤 0 已完成，停止等待下一步指令。产品执行路径与 B0 源码快照一致。

正式运行 104 组，完成 2,580 次测量调用，全部通过输出校验。逐调用 queued/retained 为零，最终 accepted/durable frontier 相等。标准基线和诊断对照各配置 3 个独立进程；新增专项各配置 1 个进程，尚不用于稳定的尾延迟或缓存容量结论。

## 标准基线 B0

下表为独立 Server 进程指标的中位数；p95 为各进程请求 p95 的中位数。每组 3 个独立进程，属于初筛基线。未进行一小时测试。

| 后端 | 模式 | 场景 | 调用 p50 ms | 调用 p95 ms | CPU ms/call | 峰值 RSS MiB | 进程 p50 范围 ms |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| postgres | durable | chain-10-16k | 682.2 | 815.7 | 558.0 | 94.4 | 682.2–692.2 |
| postgres | buffered | chain-10-16k | 276.5 | 288.0 | 238.0 | 96.1 | 264.6–278.0 |
| postgres | durable | context-append | 1818.2 | 1888.3 | 1480.0 | 100.9 | 1817.2–1842.1 |
| postgres | buffered | context-append | 794.4 | 822.0 | 750.0 | 105.2 | 775.7–875.0 |
| postgres | durable | loop-100-16k | 6746.9 | 6780.7 | 5443.3 | 96.9 | 6726.7–6768.6 |
| postgres | buffered | loop-100-16k | 2949.6 | 2969.1 | 2823.3 | 104.2 | 2904.3–2950.5 |
| postgres | durable | map-100-16k | 2115.7 | 2139.0 | 1700.0 | 95.0 | 2110.1–2128.4 |
| postgres | buffered | map-100-16k | 931.6 | 947.9 | 906.0 | 98.8 | 913.7–938.0 |
| postgres | durable | text-overwrite | 2442.6 | 2470.5 | 1996.7 | 108.0 | 2363.7–2450.8 |
| postgres | buffered | text-overwrite | 1287.0 | 1316.0 | 1160.0 | 142.4 | 1269.6–1401.9 |
| postgres | durable | unique-history | 361.3 | 478.8 | 302.9 | 95.4 | 360.1–361.6 |
| postgres | buffered | unique-history | 190.7 | 218.6 | 150.7 | 94.8 | 190.7–191.9 |
| sqlite | durable | chain-10-16k | 938.3 | 959.0 | 760.0 | 91.9 | 938.1–947.5 |
| sqlite | buffered | chain-10-16k | 345.5 | 434.7 | 362.0 | 95.1 | 341.7–372.3 |
| sqlite | durable | context-append | 2453.1 | 2516.6 | 2036.7 | 100.4 | 2439.5–2540.8 |
| sqlite | buffered | context-append | 986.3 | 1094.6 | 1033.3 | 112.1 | 983.9–1087.0 |
| sqlite | durable | loop-100-16k | 9082.5 | 9133.4 | 7740.0 | 93.5 | 9065.8–9325.9 |
| sqlite | buffered | loop-100-16k | 4044.4 | 4092.1 | 4153.3 | 109.4 | 4015.4–4109.2 |
| sqlite | durable | map-100-16k | 2879.1 | 2907.1 | 2346.0 | 92.5 | 2845.4–2886.1 |
| sqlite | buffered | map-100-16k | 1151.7 | 1156.8 | 1200.0 | 101.7 | 1132.0–1163.5 |
| sqlite | durable | text-overwrite | 2926.7 | 2980.9 | 2573.3 | 114.6 | 2915.0–3021.4 |
| sqlite | buffered | text-overwrite | 1547.2 | 1570.2 | 1506.7 | 153.0 | 1531.7–1553.8 |
| sqlite | durable | unique-history | 493.1 | 605.6 | 417.5 | 94.7 | 489.2–494.3 |
| sqlite | buffered | unique-history | 215.0 | 243.1 | 206.6 | 97.6 | 213.8–215.5 |

## A/A 波动与诊断开销

诊断关闭与开启为相同的 10 Node、16 KiB、10 次测量调用。区间为跨独立进程 p50 的极差占中位数，不是置信区间，也不是配对统计显著性。正式优化的小幅收益需要增加交错轮次。

| 后端 | 模式 | off p50 ms | on p50 ms | 延迟变化 | CPU 变化 | RSS 变化 MiB | off A/A 极差 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| sqlite | durable | 941.3 | 947.8 | +0.7% | +4.7% | +0.4 | 0.8% |
| sqlite | buffered | 348.1 | 362.3 | +4.1% | +2.5% | +0.5 | 2.5% |
| postgres | durable | 701.4 | 797.9 | +13.8% | +7.7% | +0.2 | 2.3% |
| postgres | buffered | 264.1 | 282.3 | +6.9% | +13.4% | +1.4 | 4.5% |

## 诊断分解

以下为 diagnostics-on 的每进程 10 次调用；SQL 计数与耗时按阶段划分。计时为 inclusive elapsed，嵌套或并行阶段不可相加；admission_and_ack 包含队列和 durable 确认等待，不能当作纯校验 CPU。driver_commit 是实际驱动提交时间，commit 回调计数可能包括无写入事务。参数组数不是受影响行数。

### postgres / v2-artifacts-8k

SQL 13339，SELECT 8339，COMMIT 716；Runtime Event 620；SELECT/Event 13.45；Event/write batch 1.00。

| SQL 阶段 | 执行次数 | cursor 累计 ms |
| --- | ---: | ---: |
| admission_and_ack | 1370 | 278.6 |
| background | 60 | 25.1 |
| observation_sql | 699 | 218.6 |
| projection_compute | 3230 | 389.0 |
| projection_write | 3260 | 645.4 |
| write_transaction | 4720 | 628.7 |

| inclusive 阶段 | 调用数 | 累计 elapsed ms |
| --- | ---: | ---: |
| admission_and_ack | 620 | 6528.6 |
| driver_commit | 716 | 502.3 |
| encoding | 620 | 182.4 |
| host_residency | 650 | 45.1 |
| observation_sql | 76 | 751.4 |
| observation_wait | 10 | 7437.2 |
| projection_compute | 625 | 1038.8 |
| projection_write | 620 | 2453.4 |
| queue_residency | 620 | 34.8 |
| write_transaction | 620 | 4742.5 |

### postgres / v2-buffered-artifacts-8k

SQL 6333，SELECT 5123，COMMIT 50；Runtime Event 620；SELECT/Event 8.26；Event/write batch 62.00。

| SQL 阶段 | 执行次数 | cursor 累计 ms |
| --- | ---: | ---: |
| admission_and_ack | 760 | 170.3 |
| background | 60 | 23.2 |
| observation_sql | 623 | 130.1 |
| projection_compute | 1920 | 223.6 |
| projection_write | 80 | 61.9 |
| write_transaction | 2890 | 372.5 |

| inclusive 阶段 | 调用数 | 累计 elapsed ms |
| --- | ---: | ---: |
| admission_and_ack | 620 | 567.4 |
| driver_commit | 50 | 23.0 |
| encoding | 10 | 189.3 |
| host_residency | 650 | 39.4 |
| observation_sql | 20 | 1580.6 |
| observation_wait | 10 | 2628.2 |
| projection_compute | 773 | 810.2 |
| projection_write | 10 | 109.9 |
| queue_residency | 620 | 33.9 |
| write_transaction | 10 | 1534.3 |

### sqlite / v2-artifacts-8k

SQL 14872，SELECT 8516，COMMIT 736；Runtime Event 620；SELECT/Event 13.74；Event/write batch 1.00。

| SQL 阶段 | 执行次数 | cursor 累计 ms |
| --- | ---: | ---: |
| admission_and_ack | 1990 | 591.3 |
| background | 709 | 155.6 |
| observation_sql | 963 | 478.3 |
| projection_compute | 3230 | 744.9 |
| projection_write | 3260 | 852.1 |
| write_transaction | 4720 | 967.1 |

| inclusive 阶段 | 调用数 | 累计 elapsed ms |
| --- | ---: | ---: |
| admission_and_ack | 620 | 8873.0 |
| driver_commit | 736 | 1361.2 |
| encoding | 620 | 173.2 |
| host_residency | 650 | 45.7 |
| observation_sql | 93 | 1147.2 |
| observation_wait | 10 | 9445.4 |
| projection_compute | 624 | 1432.0 |
| projection_write | 620 | 2588.9 |
| queue_residency | 620 | 34.8 |
| write_transaction | 620 | 5591.3 |

### sqlite / v2-buffered-artifacts-8k

SQL 7235，SELECT 5250，COMMIT 61；Runtime Event 620；SELECT/Event 8.47；Event/write batch 31.00。

| SQL 阶段 | 执行次数 | cursor 累计 ms |
| --- | ---: | ---: |
| admission_and_ack | 1380 | 543.9 |
| background | 100 | 60.7 |
| observation_sql | 729 | 356.8 |
| projection_compute | 1947 | 502.9 |
| projection_write | 173 | 53.4 |
| write_transaction | 2920 | 674.2 |

| inclusive 阶段 | 调用数 | 累计 elapsed ms |
| --- | ---: | ---: |
| admission_and_ack | 620 | 999.1 |
| driver_commit | 61 | 73.5 |
| encoding | 20 | 176.8 |
| host_residency | 650 | 38.7 |
| observation_sql | 21 | 2170.9 |
| observation_wait | 10 | 3285.0 |
| projection_compute | 792 | 1233.9 |
| projection_write | 20 | 144.1 |
| queue_residency | 620 | 32.9 |
| write_transaction | 20 | 2127.7 |

## 专项负载

| 后端 | 模式 | 场景 | 运行数 | 完成调用 | p50 ms | 状态 |
| --- | --- | --- | ---: | ---: | ---: | --- |
| postgres | v2-artifacts-8k | conditional-history | 1 | 16 | 1130.9 | {'completed': 1} |
| postgres | v2-buffered-artifacts-8k | conditional-history | 1 | 16 | 524.4 | {'completed': 1} |
| postgres | v2-artifacts-8k | revision-session-churn | 1 | 128 | 475.4 | {'completed': 1} |
| postgres | v2-buffered-artifacts-8k | revision-session-churn | 1 | 128 | 198.3 | {'completed': 1} |
| sqlite | v2-artifacts-8k | conditional-history | 1 | 16 | 1518.2 | {'completed': 1} |
| sqlite | v2-buffered-artifacts-8k | conditional-history | 1 | 16 | 667.7 | {'completed': 1} |
| sqlite | v2-artifacts-8k | revision-session-churn | 1 | 128 | 496.5 | {'completed': 1} |
| sqlite | v2-buffered-artifacts-8k | revision-session-churn | 1 | 128 | 219.0 | {'completed': 1} |

conditional-history 每次请求包含 16 条条件分支，复用 Session；revision-session-churn 轮换 64 个 Workflow Revision，每次创建新 Session。当前没有拟新增缓存，因此不声称 64 个 Revision 已经超过未来缓存容量；后续缓存步骤必须用小预算强制淘汰并复测。

## 原始资料与限制

- 原始 config、Server 日志、调用、RSS、manifest 和执行脚本：仓库本地 `.autoagent/performance/v2-optimization/step-00/`。原始 config 含本地数据库连接，未导出到公共报告。
- B0 实际源码：该目录 `source/autoagent/`；工具快照：`harness/`；依赖与 hash：`environment.json`。
- 完整 SQL 指纹、每轮分解和聚合指标见 [summary.json](summary.json)。
- background SQL 包含未单独包裹的后台工作和结果读取，不擅自归类为写入 SQL。
- 本步骤只建立基线，不宣称优化加速；正常速度来自关闭诊断的 artifact 场景。

## 本步骤的修改与效果判定

- 增加测试进程专用的分阶段计时、SQL 指纹、参数组和 batch 统计；关闭诊断时不安装 hook。没有修改 `autoagent/` 产品源码。
- 新增条件历史、Revision/Session 轮换和诊断开关匹配负载；保存实际源码、工具、依赖及有效配置。公开的可复核元数据见 [provenance.json](provenance.json)。
- 修正测试收尾：`wait_execution` 查询 accepted 视图，输出读取查询 durable 视图。先等待持久化屏障再读取输出，之后再次排空，两个屏障都计入调用耗时。未修改 SDK 的 flush 边界。
- 初轮旧读取顺序出现了连续调用时输出暂时为 null 的校验失败，关闭后的数据库包含正确输出；初轮和中断数据保存在本地 `preliminary/`，没有并入本轮正式统计。修正后全部 2,580 次正式调用通过。
- 在首轮无诊断速度测量期间，细化了仅在诊断开启时安装的 observer SQL 归类；该模块在速度测试中不导入。各轮 manifest 保留真实工具 hash，最终工具快照保存了此修正。后续 A/B 使用最终同版工具。
- 效果判定：**测量准备完成，不宣称产品加速。** 诊断本身存在开销，PostgreSQL durable 的匹配对照 p50 增加 13.8%，不得用诊断运行代替正常速度结果。

## 支持下一步的证据

两个后端的诊断中，每 10 次 chain 调用都存在 950 次 `graph_changes` 读前查询。durable 的 Event/write batch 为 1；buffered 的 SELECT/Event 仍为 8.26（PG）和 8.47（SQLite）。这支持步骤 1 先消除新增事实记录的冗余读取与重复 Node 写入。

COMMIT 计数包含只读快照事务；不能把全部 COMMIT 都等同于写入或 fsync。SQL fingerprint/parameter_sets 也不是受影响行数。阶段时间是 inclusive elapsed，不能相加；admission_and_ack 中的数据库查询可以单列，但该计时包含确认等待，不是纯 admission 校验耗时。

A/A 极差来自每组 3 个独立进程，当前只用于初筛；标准基线部分场景的进程差异超过 5%。后续小幅优化须交错重测并增加样本，不套用单一 5% 阈值。这一步未运行长期 soak、真实 LLM 或完整产品测试套件。

## 验证与交接

```bash
.venv/bin/python -m unittest tests.test_server_comparison -v
.venv/bin/python -m compileall -q tests/benchmarks/server_comparison tests/test_server_comparison.py
git diff --check
```

最终回归 9 项通过、0 跳过，包含实际 PostgreSQL 和 SQLite 的原生 Server 调用、慢存储屏障、诊断、Revision 轮换与观察归类。完整日志保存在本地 `verification.log`。另外逐行验证 2,580 条结果及 104 个最终屏障，确认产品源码 hash 与 B0 完全一致。

步骤 1 尚未开始。后续实施者先使用本地 `source/` 作为 A，保存步骤 1 的独立 B 快照，按主方案重新进行效果对比。
